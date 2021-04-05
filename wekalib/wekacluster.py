from logging import debug, info, warning, error, critical, getLogger, DEBUG, StreamHandler
import logging

from wekalib.wekaapi import WekaApi
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

    def __init__(self, hostname, cluster):
        self.name = hostname  # what's my name?
        self.cluster = cluster  # what cluster am I in?
        self.api_obj = None
        self._lock = Lock()
        self.host_in_progress = 0
        self.submission_queue = cluster.submission_queue
        self._thread = None
        self.status = None

        # log.debug(f"authfile={tokenfile}")

        try:
            self.api_obj = WekaApi(self.name, tokens=self.cluster.apitoken, timeout=10, scheme=cluster._scheme)
        except Exception as exc:
            log.error(f"Error creating WekaApi object: {exc}")
            # log.error(traceback.format_exc())
            self.api_obj = None

        if self.api_obj is None:
            # can't open API session, fail.
            log.error("WekaHost: unable to open API session")
            raise Exception("Unable to open API session")

        cluster._scheme = self.api_obj.scheme()  # scheme is per cluster, if one host is http, all are
        self._scheme = cluster._scheme



    def call_api(self, method=None, parms={}):
        start_time = time.time()
        with self._lock:
            self.host_in_progress += 1
            log.debug(f"calling Weka API on host {self}/{method}, {self.host_in_progress} in progress for host")
        try:
            result = self.api_obj.weka_api_command(method, parms)
        except Exception as exc:
            with self._lock:
                self.host_in_progress -= 1
            raise
        with self._lock:
            self.host_in_progress -= 1
        log.info(f"elapsed time for host {self}/{method}: {round(time.time() - start_time, 2)} secs")
        return result


    def submission_thread(self, submission_queue):
        #log.debug(f"submission thread for host {self.name} starting...")
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
        self.cluster_in_progress = 0

        self.cloud_url = None
        self.cloud_creds = None
        self.event_descs = None
        self.cloud_proxy = None
        self.cloud_http_pool = None

        self.orig_hostlist = clusterspec.split(",")  # takes a comma-separated list of hosts
        self.hosts = None
        self.host_dict = {}  # host:WekaHost dictionary
        self.authfile = authfile
        self.loadbalance = autohost
        self.last_event_timestamp = None
        self.last_get_events_time = None
        self.submission_queue = queue.Queue()
        self.outstanding_api_calls = list() 

        # fetch cluster configuration
        self.apitoken = wekaapi.get_tokens(self.authfile)
        try:
            self.refresh_config()
        except:
            raise

        # get the cluster name via a manual api call    (failure raises and fails creation of obj)
        api_return = self.call_api(method="status", parms={})
        self.name = api_return['name']
        self.guid = api_return['guid']
        self.release = api_return['release']


        # ------------- end of __init__() -------------

    def __str__(self):
        return str(self.name)

    def sizeof(self):
        return len(self.hosts)

    def refresh_config(self):
        # we need *some* kind of host(s) in order to get the hosts_list below
        if self.hosts is None or len(self.hosts) == 0:
            log.debug(f"Refreshing hostlists from original")
            # create objects for the hosts; recover from total failure
            for hostname in self.orig_hostlist:
                try:
                    hostobj = WekaHost(hostname, self)
                    self.host_dict[hostname] = hostobj
                except:
                    #log.error(traceback.format_exc())
                    raise
            self.hosts = circular_list(list(self.host_dict.keys()))

        # get the rest of the cluster (bring back any that had previously disappeared, or have been added)
        try:
            api_return = self.call_api(method="hosts_list", parms={})
        except:
            raise

        self.clustersize = 0
        #self.hosts = circular_list(list())  # create an empty list

        for host in api_return:
            hostname = host["hostname"]
            if host["auto_remove_timeout"] == None or host["mode"] == "backend":
                self.clustersize += 1
                if host["state"] == "ACTIVE" and host["status"] == "UP":
                    # check if it's already in the list
                    if hostname not in self.host_dict.keys():
                        try:
                            log.debug(f"creating new WekaHost instance for host {hostname}")
                            hostobj = WekaHost(hostname, self)
                            self.host_dict[hostname] = hostobj
                        except:
                            pass
                    else:
                        log.debug(f"{hostname} already in list")
                    if hostname not in self.hosts.list:
                        self.hosts.insert(hostname)
                    self.host_dict[hostname].status = "UP"
                else:
                    log.debug(f"{hostname} is not UP")
                    if hostname in self.host_dict.keys():   # make sure we've marked it DOWN
                        self.host_dict[hostname].status = "DOWN"
                        self.hosts.remove(hostname)

        # make sure submission threads are running
        #for hostname, host in self.host_dict.items():
        for hostname in self.hosts.list:  # use the non-circular version ;)
            self.host_dict[hostname].check_submission_thread()  # only start those on the hosts list (active hosts)

        log.debug(f"host list is: {str(self.hosts)}")

        log.debug("wekaCluster {} refreshed. Cluster has {} members, {} are online".format(self.name, self.clustersize,
                                                                                           len(self.hosts)))


    # a good way to execute a lot of api calls quickly
    def async_call_api(self, opaque, method=None, parms={}):
        call = APICall(opaque, method, parms)
        self.outstanding_api_calls.append(call) # save a copy so we can get results
        self.submission_queue.put(call)     # queue it for execution


    def wait_async(self):
        self.submission_queue.join()        # wait for them
        all_results = self.outstanding_api_calls # copy the list
        self.outstanding_api_calls = list() # clear the list
        return all_results


    # cluster-level call_api() will retry commands on another host in the cluster on failure
    def call_api(self, method=None, parms={}):
        hostname = self.hosts.next()
        if hostname is None:
            raise APIException(100, "General communication failure")
        host = self.host_dict[hostname]

        # api_return = None
        last_exception = None
        log.debug(f"host is {host}")

        while host is not None:
            self.cluster_in_progress += 1
            try:
                log.debug(
                    f"calling Weka API on cluster {self}, host {host}, method {method}, {self.cluster_in_progress} in progress for cluster")
                api_return = host.call_api(method, parms)
            except wekaapi.WekaApiIOStopped:
                log.error(f"IO Stopped on Cluster {self}")
                raise
            # <class 'wekaapi.HttpException'> error (502, 'Bad Gateway') - if we get this, leader failing over; wait a few secs and retry?
            # except wekaapi.HttpException as exc:
            #    last_exception = exc
            except Exception as exc:
                log.debug(f"caught exception {exc}")
                last_exception = exc
                self.cluster_in_progress -= 1
                # something went wrong...  stop talking to this host from here on.  We'll try to re-establish communications later
                last_hostname = str(host)
                self.hosts.remove(host)  # it failed, so remove it from the list
                hostname = self.hosts.next()
                if hostname is None:
                    break  # fall through to raise exception
                host = self.host_dict[hostname]
                self.errors += 1
                log.error(
                    f"cluster={self}, error {exc} spawning command {method}/{parms} on host {last_hostname}. Retrying on {host}.")
                # print(traceback.format_exc())
                continue

            self.cluster_in_progress -= 1
            return api_return

        # ran out of hosts to talk to!
        if type(last_exception) == wekaapi.HttpException:
            if last_exception.error_code == 502:  # Bad Gateway
                raise APIException(502, "No hosts available")
        raise APIException(100, "General communication failure")


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
            #log.debug(f"calling request {url} {fields} {headers}")
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
            self.cloud_http_pool = urllib3.ProxyManager(self.cloud_proxy["proxy"], timeout=5)
        else:
            # self.cloud_http_pool = urllib3.PoolManager()
            url = urllib3.util.parse_url(self.cloud_url)
            self.cloud_http_pool = urllib3.HTTPSConnectionPool(url.host, retries=3, timeout=5)

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
