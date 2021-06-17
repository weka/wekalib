from logging import debug, info, warning, error, critical, getLogger, DEBUG, StreamHandler
import logging

from wekalib.wekaapi import WekaApi, find_token_file
import wekalib.wekaapi as wekaapi
from wekalib.wekatime import wekatime_to_lokitime, lokitime_to_wekatime, wekatime_to_datetime, lokitime_to_datetime, \
    datetime, datetime_to_lokitime, datetime_to_wekatime
from wekalib.circular import circular_list
import traceback
import urllib3
import datetime
import time
from threading import Lock, Thread
import json
import queue

log = getLogger(__name__)


class APIException(Exception):
    def __init__(self, error_code, error_msg):
        self.error_code = error_code
        self.error_msg = error_msg

class APICall(object):
    def __init__(self, opaque, method, parms):
        self.opaque = opaque
        self.method = method
        self.parms = parms
        self.result = dict()
        self.status = "good"


class WekaHost(object):

    def __init__(self, hostname, cluster, ip=None, async_thread=False):
        self.name = hostname  # what's my name?
        self.cluster = cluster  # what cluster am I in?
        self.api_obj = None
        self.ip = ip
        self._lock = Lock()
        self.host_in_progress = 0
        self.submission_queue = cluster.submission_queue
        self._thread = None
        self.status = None

        # log.debug(f"authfile={tokenfile}")

        log.debug(f"*********** self.ip = {self.ip}")
        # prefer to use the ip addr, if present
        if self.ip is not None:
            connect_name = self.ip      # should be a dataplane ip
        else:
            connect_name = self.name    # aka hostname

        #if self.ip is not None:
        #    try:
        #        # does it really matter if it's an IP or name?  What do I care here, if it's handled at a higher level?
        #        # or should it be handled here and NOT at a higher level?   (I'm thinking cluster level might be better)
        #        # Maybe give both ip and name to WekaApi?
        #        # ----- make sure this doesn't cause the stats to lose the hostname and show up under the ip addr!!!!!!!!
        #        self.api_obj = WekaApi(self.ip, tokens=self.cluster.apitoken, scheme=cluster._scheme)
        #    except Exception as exc:
        #        log.error(f"Error creating WekaApi object: {exc}")
        #        # log.error(traceback.format_exc())
        #        self.api_obj = None
        #        raise

        # if the ip didn't work, try using the hostname (needs DNS/hosts file)
        try:
            self.api_obj = WekaApi(connect_name, tokens=self.cluster.apitoken, scheme=cluster._scheme)
        except Exception as exc:
            log.debug(f"Error creating WekaApi object: {exc}")
            self.api_obj = None
            if exc.message == "host_unreachable":
                log.critical(f"Host {connect_name} is unreachable - is it in /etc/hosts and/or DNS?")
            raise

        cluster._scheme = self.api_obj.scheme()  # scheme is per cluster, if one host is http, all are
        self._scheme = cluster._scheme

        if async_thread:
            # start the submission thread for this host
            log.debug(f"starting submission thread for host {self.name}")
            self._thread = Thread(target=self.submission_thread, args=(self.cluster.submission_queue,), daemon=True)
            self._thread.start()
            log.debug(f"self._thread is NOW {self._thread}")
            log.debug(f"submission thread for host {self.name} started")


    # HOST api call
    def call_api(self, method=None, parms={}):
        start_time = time.time()
        #with self._lock:
            #self.host_in_progress += 1
            #log.debug(f"calling Weka API on host {self}/{method}, {self.host_in_progress} in progress for host")
        log.debug(f"calling Weka API on host {self}/{method}")
        try:
            result = self.api_obj.weka_api_command(method, parms)
        except Exception as exc:
            #with self._lock:
            #    self.host_in_progress -= 1
            raise
        #with self._lock:
        #    self.host_in_progress -= 1
        if method == "stats_show":
            log.info(f"elapsed time for host {self}/{method}/{parms['category']}/{parms['stat']:15}: {round(time.time() - start_time, 2)} secs")
        else:
            log.info(f"elapsed time for host {self}/{method}: {round(time.time() - start_time, 2)} secs")
        return result


    def submission_thread(self, submission_queue):
        log.debug(f"submission thread for host {self.name} starting...")
        while True:
            log.debug(f"submission thread for host {self.name} waiting on queue")
            call = self.submission_queue.get()  # an APICall object
            log.debug(f"{self.name}: dequeued request {call.method}, {call.parms}")
            try:
                call.result = self.call_api(call.method, call.parms)
                call.status = "good"
                log.debug(f"{self.name}: completed request {call.method}, {call.parms}")
            except Exception as exc:
                log.debug(f"{self.name}: error from api {exc}")
                call.result =  exc
                call.status = "error"
                if type(exc) == wekaapi.HttpException:
                    if exc.error_code == 502:  # Bad Gateway
                        log.error(f"{self.name}: Error 502 - resubmitting {call.method}, {call.parms}")
                self.submission_queue.put(call)  # error - resubmit
                self.submission_queue.task_done()
                return  # exit thread so we don't take more requests?
            self.submission_queue.task_done()

    # if the submission thread hasn't been started or is dead, start it
    def check_submission_thread(self):
        log.debug(f"self._thread is {self._thread}")
        if self._thread is not None and not self._thread.is_alive():
            log.debug(f"Submission thread for host {self.name} has died")
            self._thread.join()
            self._thread = None
        if self._thread is None:
            log.debug(f"starting submission thread for host {self.name}")
            self._thread = Thread(target=self.submission_thread, 
                        args=(self.submission_queue,), daemon=True)
            self._thread.start()
            log.debug(f"self._thread is NOW {self._thread}")
            log.debug(f"submission thread for host {self.name} started")

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


# per-cluster object
class WekaCluster(object):

    # collects on a per-cluster basis
    # clusterspec format is "host1,host2,host3,host4" (can be ip addrs, even only one of the cluster hosts)
    # auth file is output from "weka user login" command.
    # autohost is a bool = should we distribute the api call load among all weka servers
    def __init__(self, clusterspec, authfile=None, autohost=True):
        # object instance global data
        self.errors = 0
        self.clustersize = 0
        self.name = ""
        self.release = None
        # self._scheme = "https"
        self._scheme = "http"

        self.cloud_url = None
        self.cloud_creds = None
        self.event_descs = None
        self.cloud_proxy = None
        self.cloud_http_pool = None

        if type(clusterspec) == str:
            self.orig_hostlist = clusterspec.split(",")  # takes a comma-separated list of hosts
        else:
            self.orig_hostlist = clusterspec
        self.hosts = None
        self.host_dict = dict()         # host:WekaHost dictionary (none is intentional...)
        self.async_host_dict = None   # host:WekaHost dictionary
        self.dataplane_accessible = True
        self.authfile = authfile
        self.loadbalance = autohost
        self.last_event_timestamp = None
        self.last_get_events_time = None
        self.submission_queue = queue.Queue()
        self.outstanding_api_calls = list() 

        # fetch cluster configuration
        self.apitoken = wekaapi.get_tokens(find_token_file(self.authfile))

        try:
            self.initialize_hosts()
        except:
            raise

        #try:
        #    self.refresh_config()
        #except:
        #    raise

        # get the cluster name via a manual api call    (failure raises and fails creation of obj)
        api_return = self.call_api(method="status", parms={})
        self.name = api_return['name']
        self.guid = api_return['guid']
        self.release = api_return['release']


        # ------------- end of __init__() -------------

    def __str__(self):
        return str(self.name)

    def sizeof(self):
        return len(self.async_host_dict)

    def initialize_hosts(self):
        # we need *some* kind of host(s) in order to get the hosts_list below

        log.debug(f"Initializing hosts from original")
        # create objects for the hosts; recover from total failure
        for hostname in self.orig_hostlist:
            try:
                self.host_dict[hostname] = WekaHost(hostname, self)
            except Exception as exc:
                log.error(traceback.format_exc())
                log.debug(f"Unable to create WekaHost '{hostname}'")
                last_error = exc
                #raise

        if len(self.host_dict) == 0:
            
            log.debug(f"Unable to create any WekaHosts ({last_error})")
            raise last_error

        # end of refresh_from_clusterspec()


    # set things up for async calls - get a list of cluster hosts, make sure we have WekaHosts for them all, etc
    def initialize_async_subsystem(self):

        # if we've had failures, reset to the list of hosts in the config file so we can pull from the cluster
        #if len(self.hosts) == 0:
        #    self.hosts = circular_list(list(self.orig_host_dict.keys()))

        #
        # get the rest of the cluster (bring back any that had previously disappeared, or have been added)
        #
        try:
            api_return = self.call_api(method="hosts_list", parms={})   # list of hosts from cluster
        except:
            raise

        # clear
        #if self.async_host_dict is None:
        #    self.async_host_dict = dict()

        # brute-force refresh of all from the cluster hosts_list - needs to be smarter
        self.async_host_dict = dict()

        self.clustersize = 0

        # loop through host in the host list from the cluster
        for host in api_return:
            hostname = host["hostname"]
            if (host["auto_remove_timeout"] == None or host["mode"] == "backend") and \
                            host["state"] == "ACTIVE" and host["status"] == "UP": # only working backend servers
                tryagain = True     # allows us to retry if dataplane ip is inaccessible
                while tryagain:
                    # check if it's already in the list
                    if hostname not in self.async_host_dict.keys():
                        if self.dataplane_accessible:   # if it's accessible, use it
                            log.debug("dataplane is accessible!")
                            host_dp_ip = host['host_ip']
                        else:
                            log.debug("dataplane is NOT accessible!")
                            host_dp_ip = None

                        try:
                            log.debug(f"creating new WekaHost instance for host {hostname}")
                            self.async_host_dict[hostname] = WekaHost(hostname, self, ip=host_dp_ip, async_thread=True)
                        except Exception as exc:
                            log.info(f"{type(exc)}")
                            if self.dataplane_accessible:
                                log.info(f"Error ({exc}) trying to contact host on dataplane interface: {host_dp_ip}, using hostnames")
                                tryagain = True # this is really just for clarity
                            else:
                                log.error(f"Error ({exc}) trying to contact host {hostname}, removing from config")
                                tryagain = False
                            self.dataplane_accessible = False
                            last_exception = exc
                        else:   # else on the try:
                            tryagain = False    # it succeeded, so no need to try again
                    else:
                        log.debug(f"{hostname} already in list")


        # make sure submission threads are running
        #for hostname, host in self.host_dict.items():
        #for hostname in self.hosts.list:  # use the non-circular version ;)
        #    self.host_dict[hostname].check_submission_thread()  # only start those on the hosts list (active hosts)

        #log.debug(f"host list is: {str(self.hosts)}")

        log.debug(f"wekaCluster {self.name} refreshed. Cluster has {len(api_return)} members, {self.async_host_dict} are online")

        #if len(self.hosts) == 0:
        #    raise last_exception


    # a good way to execute a lot of api calls quickly - queue to submission threads
    def async_call_api(self, opaque, method=None, parms={}):
        call = APICall(opaque, method, parms)
        self.outstanding_api_calls.append(call) # save a copy so we can get results
        self.submission_queue.put(call)     # queue it for execution


    def wait_async(self):
        self.submission_queue.join()        # wait for them
        all_results = self.outstanding_api_calls # copy the list
        self.outstanding_api_calls = list() # clear the list
        return all_results


    # Synchronous API call - for async calls, use async_call_api()
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
            except wekaapi.WekaApiIOStopped:
                log.error(f"IO Stopped on Cluster {self}?")
                raise
            except Exception as exc:
                log.debug(f"caught exception {exc}")
                last_exception = exc
                # something went wrong...  stop talking to this host from here on.  We'll try to re-establish communications later
                log.error(
                    f"cluster={self}, error {exc} spawning command {method}/{parms} on host {hostname}. Retrying on next host.")
                # print(traceback.format_exc())
                continue

            # success!  Return the data
            return api_return

        # ran out of hosts to talk to!
        if type(last_exception) == wekaapi.HttpException:
            if last_exception.error_code == 502:  # Bad Gateway
                raise APIException(502, "No hosts available")
        raise APIException(100, "General communication failure")   # raise last_exception instead?


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
            #log.debug(f"request call complete")
        except Exception as exc:
            log.critical(f"GET request failure: {exc}")
            return []

        #log.debug(f"weka home response status: {resp.status}")
        #log.debug(f"weka home response data: {resp.data}")

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
    def get_events(self):
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
            #if url.scheme == "https":   # weka home cloud is https, but weka home local is http!
            #self.cloud_http_pool = urllib3.HTTPSConnectionPool(url.host, retries=3, timeout=5)
            #else:
            #    self.cloud_http_pool = urllib3.HTTPConnectionPool(url.host, retries=3, timeout=5)

        #end_time = datetime.datetime.utcnow().isoformat() # needs iso format
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
