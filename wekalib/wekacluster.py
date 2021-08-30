import datetime
import json
import logging
import os
import socket
import time
from logging import getLogger, DEBUG, StreamHandler

import urllib3

import wekalib.exceptions
from wekalib.wekaapi import WekaApi
from wekalib.wekatime import wekatime_to_lokitime, datetime_to_wekatime

log = getLogger(__name__)


class APICall(object):
    def __init__(self, opaque, method, parms):
        self.opaque = opaque
        self.method = method
        self.parms = parms
        self.result = dict()
        self.status = "good"


####################################################################################################
# Host definition
####################################################################################################
class WekaHost(object):

    def __init__(self, hostname, cluster, ip=None, async_thread=False, timeout=10.0):
        self.name = hostname  # what's my name?
        self.cluster = cluster  # what cluster am I in?
        self.api_obj = None
        self.ip = ip
        self.host_in_progress = 0
        self._thread = None
        self.status = None
        self.timeout = timeout

        # log.debug(f"authfile={tokenfile}")

        log.debug(f"*********** self.ip = {self.ip}")
        # prefer to use the ip addr, if present
        if self.ip is not None:
            connect_name = self.ip  # should be a dataplane ip
        else:
            connect_name = self.name  # aka hostname

        # log.debug(f"tokens={self.cluster.apitoken}")

        try:
            self.api_obj = WekaApi(connect_name, tokens=self.cluster.apitoken, scheme=cluster._scheme,
                                   verify_cert=cluster.verify_cert, timeout=timeout)
        except wekalib.exceptions.APIError as exc:
            log.debug(f"APIError caught {exc}")
            raise
        except Exception as exc:
            log.debug(f"Error creating WekaApi object: {exc}")
            self.api_obj = None
            if exc.message == "host_unreachable":
                if self.ip is None:
                    log.critical(f"Host {connect_name} is unreachable - is it in /etc/hosts and/or DNS?")
            raise

        cluster._scheme = self.api_obj.scheme()  # scheme is per cluster, if one host is http, all are
        self._scheme = cluster._scheme

    # HOST api call
    def call_api(self, method=None, parms={}):
        start_time = time.time()
        log.debug(f"calling Weka API on host {self}/{method}")
        try:
            result = self.api_obj.weka_api_command(method, parms)
        except Exception as exc:
            raise

        # show that we're working, and how long the calls are taking
        log.info(f"elapsed time for host {self}/{method}: {round(time.time() - start_time, 2)} secs")
        return result

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name

    def __eq__(self, other):
        if other is None:
            return False
        elif isinstance(other, WekaHost):
            return other.name == self.name
        elif type(other) == str:
            return other == self.name
        raise NotImplementedError

    def __hash__(self):
        return hash(self.name)

    def scheme(self):
        return self._scheme


####################################################################################################
# Cluster definition
####################################################################################################
# per-cluster object
class WekaCluster(object):

    # collects on a per-cluster basis
    # clusterspec format is "host1,host2,host3,host4" (can be ip addrs, even only one of the cluster hosts)
    # ---- new clusterspec is a list of hostnames/ips (remains backward compatible)
    # auth file is output from "weka user login" command, and is REQUIRED
    def __init__(self, clusterspec, authfile, force_https=False, verify_cert=True, timeout=10.0, backends_only=True):
        # object instance global data
        self.errors = 0
        self.clustersize = 0
        self.name = ""
        self.release = None
        self.verify_cert = verify_cert
        self.backends_only = backends_only

        self.cloud_url = None
        self.cloud_creds = None
        self.event_descs = None
        self.cloud_proxy = None
        self.cloud_http_pool = None
        self.timeout = timeout

        if force_https:
            self._scheme = "https"
        else:
            self._scheme = "http"

        if type(clusterspec) == str:
            self.orig_hostlist = clusterspec.split(",")  # takes a comma-separated list of hosts
        else:
            self.orig_hostlist = clusterspec

        self.host_dict = dict()  # host:WekaHost dictionary (none is intentional...)
        # self.dataplane_accessible = True
        self.dataplane_accessible = False  # for now - dp might be congested!
        self.last_event_timestamp = None
        self.last_get_events_time = None
        self.outstanding_api_calls = list()

        # get our auth tokens - these will raise exceptions on failure
        self.authfile = self.find_token_file(authfile)
        self.get_tokens()

        # log.debug(f"self.apitoken = {self.apitoken}")

        # fetch cluster configuration
        self.initialize_hosts()

        # get the cluster name via a manual api call    (failure raises and fails creation of obj)
        api_return = self.call_api(method="status", parms={})
        self.name = api_return['name']
        self.guid = api_return['guid']
        self.release = api_return['release']

        # ------------- end of __init__() -------------

    def __str__(self):
        return str(self.name)

    def sizeof(self):
        return len(self.host_dict)

    def get_hostobj_byname(self, name):
        if name in self.host_dict:
            return self.host_dict[name]
        else:
            log.debug(f"name='{name}', {self.host_dict}")
            return None

    def initialize_hosts(self):
        # we need *some* kind of host(s) in order to get the hosts_list below
        last_error = None

        log.debug(f"Initializing hosts from original")
        # create objects for the hosts; recover from total failure
        for hostname in self.orig_hostlist:
            try:
                self.host_dict[hostname] = WekaHost(hostname, self)
            except wekalib.exceptions.APIError as exc:
                if isinstance(exc, wekalib.exceptions.NewConnectionError):
                    log.debug(f"Unable to create WekaHost '{hostname}' {exc}")
                    last_error = exc
                    pass  # try next host - this one is unreachable
                elif isinstance(exc, wekalib.exceptions.LoginError) or isinstance(exc, wekalib.exceptions.SSLError):
                    # terminal condition
                    log.debug(f"Login or SSL error")
                    raise
                log.error(f"Weka API error caught: {exc}")
            except Exception as exc:
                # log.error(traceback.format_exc())
                log.debug(f"Unable to create WekaHost '{hostname}'")
                last_error = exc
                # raise

        if len(self.host_dict) == 0:

            if last_error is None:
                log.debug(f"Unable to create any WekaHosts (unknown error?)")
            else:
                log.debug(f"Unable to create any WekaHosts ({last_error})")
            raise last_error

        # end of initialize_hosts()

    def refresh(self):
        if len(list(self.host_dict.keys())) == 0:
            self.initialize_hosts()

        #
        # get the rest of the cluster (bring back any that had previously disappeared, or have been added)
        #
        try:
            api_return = self.call_api(method="hosts_list", parms={})  # list of hosts from cluster
        except:
            raise

        # brute-force refresh of all from the cluster hosts_list - needs to be smarter

        self.clustersize = 0

        # loop through host in the host list from the cluster (api_return is a list of dicts, one per host)
        for host in api_return:
            hostname = host["hostname"]
            log.debug(f"adding host {hostname}, {host['state']}, {host['status']}, backends_only={self.backends_only}")
            if host["state"] == "ACTIVE" and host["status"] == "UP":
                if not self.backends_only or (host["mode"] == "backend" and self.backends_only):
                    tryagain = True  # allows us to retry if dataplane ip is inaccessible
                    while tryagain:
                        # check if it's already in the list
                        if hostname not in self.host_dict.keys():
                            # not in the list - try to add it
                            # if self.dataplane_accessible:   # if it's accessible, use it
                            #    log.debug("dataplane is accessible!")
                            #    host_dp_ip = host['host_ip']
                            # else:
                            #    log.debug("dataplane is NOT accessible!")
                            #    host_dp_ip = None
                            host_dp_ip = None  # temp/Vince
                            self.dataplane_accessible = False

                            # save some trouble, and make sure names are resolvable
                            nameerror = False
                            try:
                                socket.gethostbyname(hostname)
                            except socket.gaierror as exc:
                                log.critical(f"Hostname {hostname} not resolvable - is it in /etc/hosts or DNS?")
                                nameerror = True
                                tryagain = False  # it succeeded, so no need to try again
                            except Exception as exc:
                                log.critical(exc)
                                nameerror = True
                                tryagain = False  # it succeeded, so no need to try again
                                continue  # just skip it
                            if nameerror:
                                #  this is not good - don't abort the loop if one host fails dns lookup!
                                # raise wekalib.exceptions.NameNotResolvable(hostname)
                                log.error(f"hostname {hostname} is not resolvable, skipping")
                                tryagain = False  # it succeeded, so no need to try again
                                continue  # just skip it

                            try:
                                log.debug(f"creating new WekaHost instance for host {hostname}")
                                self.host_dict[hostname] = WekaHost(hostname, self, ip=host_dp_ip, async_thread=False)
                            except Exception as exc:
                                if self.dataplane_accessible:
                                    log.info(
                                        f"Error ({exc}) trying to contact host on dataplane interface: {host_dp_ip}, using hostnames")
                                    tryagain = True  # this is really just for clarity
                                else:
                                    log.error(f"Error ({exc}) trying to contact host {hostname}, removing from config")
                                    tryagain = False
                                self.dataplane_accessible = False
                                last_exception = exc
                            else:  # else on the try:
                                tryagain = False  # it succeeded, so no need to try again
                        else:
                            log.debug(f"{hostname} already in list")
                            # Ensure the submission thread is running
                            tryagain = False  # it succeeded, so no need to try again

        log.debug(
            f"wekaCluster {self.name} refreshed. Cluster has {len(api_return)} members, {list(self.host_dict.keys())} are online")
        log.debug(f"{self.host_dict.keys()}")
        if len(list(self.host_dict.keys())) == 0:
            # we have no hosts?
            raise wekalib.exceptions.CommunicationError(
                "Cluster inaccessible.  Are the hosts online, and are their names resolvable?")

    def hosts(self):
        return self.host_dict.keys()

    # cluster-level call_api() will retry commands on another host in the cluster on failure
    def call_api(self, method=None, parms={}):
        last_exception = None

        # select a host to talk to (first one is fine), cycle through until one works or cluster is down
        for hostname, host in self.host_dict.items():

            # api_return = None
            log.debug(f"host is {host}")

            try:
                log.debug(
                    f"calling Weka API on cluster {self}, host {host}, method {method}")
                api_return = host.call_api(method, parms)
            except wekalib.exceptions.IOStopped:
                log.error(f"IO Stopped on Cluster {self}?")
                raise
            except wekalib.exceptions.HTTPError as exc:
                log.error(f"host returned error response: {exc}")
                raise
            except Exception as exc:
                last_exception = exc
                # something went wrong...  stop talking to this host from here on.  We'll try to re-establish communications later
                log.error(
                    f"cluster={self}, error {exc} spawning command {method}/{parms} on host {hostname}. Retrying on next host.")
                # print(traceback.format_exc())
                continue  # move on to next host

            # success!  Return the data
            return api_return

        # ran out of hosts to talk to!
        log.debug(f"****************** last_exception is {last_exception}")
        raise last_exception

        # ------------- end of call_api() -------------

    # interface to weka-home
    def home_events(self,
                    num_results=None,
                    start_time=None,
                    end_time=None,
                    severity=None,
                    type_list=None,
                    category_list=None,
                    sort_order=None,
                    by_digested_time=False,
                    show_internal=False
                    ):
        # Weka Home uses a different API style than the cluster... 
        url = f"{self.cloud_url}/api/{self.guid}/events/list"
        headers = {"Authorization": "%s %s" % (self.cloud_creds["token_type"], self.cloud_creds["access_token"])}

        fields = {}
        if num_results is not None:
            fields["lmt"] = num_results

        if start_time is not None:
            fields["frm"] = start_time

        if end_time is not None:
            fields["to"] = end_time

        if severity is not None:
            fields["svr"] = severity

        if type_list is not None:
            log.error("not implemented")

        if category_list is not None:
            log.error("not implemented")

        if sort_order is not None:
            fields["srt"] = sort_order

        if by_digested_time:
            fields["dt"] = "t"
        else:
            fields["dt"] = "f"

        if show_internal:
            fields["intr"] = "t"
        else:
            fields["intr"] = "f"

        log.debug(f"GET from weka-home: {fields} --- {headers}")

        # get from weka-home
        try:
            log.debug(f"calling request {url} {fields} {headers}")
            resp = self.cloud_http_pool.request('GET', url, fields=fields, headers=headers)
            # log.debug(f"request call complete")
        except Exception as exc:
            log.critical(f"GET request failure: {exc}")
            return []

        # log.debug(f"weka home response status: {resp.status}")
        # log.debug(f"weka home response data: {resp.data}")

        if resp.status != 200:
            log.error(f"BAD weka home response status: {resp.status}")
            return []

        events = json.loads(resp.data.decode('utf-8'))

        if len(events) == 0:
            log.info("no events!")
            return []

        # format the descriptions; they don't come pre-formatted
        for event in events:
            event_type = event["type"]
            if event_type in self.event_descs:
                format_string = self.event_descs[event_type]["formatString"]
                params = event["params"]
                event["description"] = format_string.format(**params)
            else:
                log.error(f"unknown event type {event['type']}")

        return events

    # get events from Weka
    def setup_events(self):
        log.debug("getting events")

        # weka-home setup
        self.cloud_url = self.call_api(method="cloud_get_url", parms={})
        log.debug(f"cluster={self.name}, cloud_url='{self.cloud_url}'")
        self.cloud_creds = self.call_api(method="cloud_get_creds", parms={})
        temp = self.call_api(method="events_describe", parms={"show_internal": False})

        # make a dict of {event_type: event_desc}
        self.event_descs = {}
        for event in temp:
            self.event_descs[event["type"]] = event

        # need to do something with this
        self.cloud_proxy = self.call_api(method="cloud_get_proxy", parms={})
        if len(self.cloud_proxy["proxy"]) != 0:
            log.debug(f"Using proxy={self.cloud_proxy['proxy']}")
            self.cloud_http_pool = urllib3.ProxyManager(self.cloud_proxy["proxy"], timeout=5.0)
        else:
            self.cloud_http_pool = urllib3.PoolManager()
            url = urllib3.util.parse_url(self.cloud_url)
            log.debug(f"no proxy: url.scheme is: {url.scheme}")
            # if url.scheme == "https":   # weka home cloud is https, but weka home local is http!
            # self.cloud_http_pool = urllib3.HTTPSConnectionPool(url.host, retries=3, timeout=5)
            # else:
            #    self.cloud_http_pool = urllib3.HTTPConnectionPool(url.host, retries=3, timeout=5)

    def get_events(self):
        end_time = datetime_to_wekatime(datetime.datetime.utcnow())
        events = self.home_events(
            num_results=100,
            start_time=self.last_event_timestamp,
            end_time=end_time,
            severity="INFO")

        # note the time of this last fetch, if it was successful (failure will cause exception)
        self.last_get_events_time = end_time

        return self.reformat_events(events)

        # ------------- end get_events() ----------

    # takes in a list of dicts - [{event},{event},{event}].  Change to a dict of {timestamp:{event},timestamp:{event}} so we can sort by timestamp
    def reformat_events(self, weka_events):
        event_dict = {}
        for event in weka_events:
            event_dict[wekatime_to_lokitime(event["timestamp"])] = event
        return event_dict

    # returns an absolute path to a file, if it exists, or None if not
    def find_token_file(self, token_file):
        search_path = ['.', '~/.weka', '.weka']

        log.info(f"looking for token file {token_file}")
        if token_file is None:
            return None

        test_name = os.path.expanduser(token_file)  # will expand to an absolute path (starts with /) if ~ is used
        log.debug(f"name expanded to {test_name}")

        if test_name[0] == '/':
            # either token_file was already an abssolute path, or expansuser did it's job
            if os.path.exists(test_name):
                return test_name
            else:
                # can't expand a abs path any further, and it does not exist
                return None

        # search for it in the path
        for path in search_path:
            base_path = os.path.expanduser(path)
            test_name = os.path.abspath(f"{base_path}/{token_file}")
            log.info(f"Checking for {test_name}")
            if os.path.exists(test_name):
                log.debug(f"Found '{test_name}'")
                return test_name

        log.debug(f"token file {token_file} not found")
        raise wekalib.exceptions.AuthFileError(f"Unable to find token file '{token_file}'")

    #   end of find_token_file()

    # reads tokens from tokenfile
    def get_tokens(self):
        log.info(f"Using authfile {self.authfile}")
        try:
            with open(self.authfile) as fp:
                self.apitoken = json.load(fp)
            # log.debug(f"self.apitoken = {self.apitoken}")
            return
        except Exception as exc:
            errors = wekalib.exceptions.AuthFileError(f"Unable to parse auth token file '{self.authfile}': {exc}")

        raise errors
    #   end of get_tokens()


if __name__ == "__main__":
    logger = getLogger()
    logger.setLevel(DEBUG)
    log.setLevel(DEBUG)
    FORMAT = "%(filename)s:%(lineno)s:%(funcName)s():%(message)s"
    # create handler to log to stderr
    console_handler = StreamHandler()
    console_handler.setFormatter(logging.Formatter(FORMAT))
    logger.addHandler(console_handler)

    print("creating cluster")
    cluster = WekaCluster("172.20.0.128,172.20.0.129,172.20.0.135")

    print("cluster created")

    print(cluster)

    print(cluster.call_api(method="status", parms={}))
