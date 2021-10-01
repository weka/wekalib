#
# wekaapi module
#
# Python implementation of the Weka JSON RPC API.
#
# Author: Vince Fleming, vince@weka.io
#

from __future__ import division, absolute_import, print_function, unicode_literals

import re

try:
    from future_builtins import *
except ImportError:
    pass

import sys
import json
import time
import uuid
import traceback

import urllib3

try:
    import http.client

    httpclient = http.client
except ImportError:
    import httplib

    httpclient = httplib

try:
    from urllib.parse import urlencode, urljoin, urlparse
except ImportError:
    from urlparse import urljoin, urlparse

from logging import getLogger, DEBUG, StreamHandler, Formatter

import wekalib.exceptions

log = getLogger(__name__)


class WekaApi():
    def __init__(self, host, scheme='https', port=14000, path='/api/v1', timeout=30.0, tokens=None, verify_cert=True):

        self._scheme = scheme
        self._host = host
        self._port = port
        self._path = path
        self._conn = None
        self._timeout = urllib3.util.Timeout(total=timeout, connect=2.0, read=timeout)
        self.headers = {}
        self._tokens = tokens
        self.STEMMode = False

        # log.debug(f"tokens are {self._tokens}")   # should never be None

        # forget scheme (https/http) at this point...   notes:  maxsize means 2 connections per host, block means don't make more than 2
        if scheme == "http":
            self.http_conn = urllib3.HTTPConnectionPool(host, port=port, maxsize=2, block=True, retries=1,
                                                        timeout=self._timeout)
        else:
            self.http_conn = urllib3.HTTPSConnectionPool(host, port=port, maxsize=2, block=True, retries=1,
                                                         timeout=self._timeout,
                                                         cert_reqs='CERT_NONE',  # not sure what to do with this
                                                         assert_hostname=verify_cert)
        if not verify_cert:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        if self._tokens is not None:
            self.authorization = '{0} {1}'.format(self._tokens['token_type'], self._tokens['access_token'])
            self.headers["Authorization"] = self.authorization
        else:
            self.authorization = None

        self.headers['UTC-Offset-Seconds'] = time.altzone if time.localtime().tm_isdst else time.timezone
        self.headers['CLI'] = False
        self.headers['Client-Type'] = 'WEKA'

        try:
            self._login()
        except wekalib.exceptions.STEMModeError:
            self.STEMMode = True
        except Exception:
            raise

        log.debug("WekaApi: connected to {}".format(self._host))

    def scheme(self):
        return self._scheme

    @staticmethod
    def format_request(message_id, method, params):
        return dict(jsonrpc='2.0',
                    method=method,
                    params=params,
                    id=message_id)

    @staticmethod
    def unique_id(alphabet='0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'):
        number = uuid.uuid4().int
        result = ''
        while number != 0:
            number, i = divmod(number, len(alphabet))
            result = alphabet[i] + result
        return result

    @staticmethod
    def _parse_url(url, default_port=14000, default_path='/api/v1'):
        parsed_url = urlparse(url)
        # scheme = parsed_url.scheme if parsed_url.scheme else "https"
        scheme = parsed_url.scheme if parsed_url.scheme else "http"
        if scheme not in ['http', 'https']:
            # scheme = "https"
            scheme = "http"
        m = re.match('^(?:(?:http|https)://)?(.+?)(?::(\d+))?(/.*)?$', str(url), re.I)
        assert m
        return scheme, m.group(1), m.group(2) or default_port, m.group(3) or default_path

    # reformat data returned so it's like the weka command's -J output
    @staticmethod
    def _format_response(method, response_obj):
        raw_resp = response_obj["result"]
        if method == "status":
            return (raw_resp)

        # log.debug(f"response is {json.dumps(raw_resp, indent=2)}")

        resp_list = []  # our output

        if method == "stats_show":
            for nodeid, cat_dict in raw_resp.items():  # "NodeId<41>": {"ops": [{ "stats": { "OPS": 97.33333333333333,
                for category, info_list in cat_dict.items():
                    for item in info_list:
                        for stat, value in item['stats'].items():  # { "stats": { "OPS": 97.33333333333333,
                            this_stat = dict()
                            this_stat['category'] = category
                            this_stat['node'] = nodeid
                            this_stat['stat_type'] = stat
                            this_stat['stat_value'] = value
                            this_stat['timestamp'] = item['timestamp']
                            resp_list.append(this_stat)
            return resp_list

        splitmethod = method.split("_")
        words = len(splitmethod)

        # does it end in "_list"?
        if method != "alerts_list" and method != "directory_quota_list":
            if splitmethod[words - 1] == "list" or method == "filesystems_get_capacity":
                for key, value_dict in raw_resp.items():
                    newkey = key.split("I")[0].lower() + "_id"
                    value_dict[newkey] = key
                    resp_list.append(value_dict)
                # older weka versions lack a "mode" key in the hosts-list
                if method == "hosts_list":
                    for value_dict in resp_list:
                        if "mode" not in value_dict:
                            if value_dict["drives_dedicated_cores"] != 0:
                                value_dict["mode"] = "backend"
                            else:
                                value_dict["mode"] = "client"
                return resp_list

        # ignore other method types for now.
        return raw_resp

    # end of _format_response()

    # log into the api  # assumes that you got an UNAUTHORIZED (401)
    def _login(self):
        login_message_id = self.unique_id()
        log.debug("logging in")

        if self.authorization is not None:  # maybe should see if _tokens != None also?
            # try to refresh the auth since we have prior authorization
            params = dict(refresh_token=self._tokens["refresh_token"])
            login_request = self.format_request(login_message_id, "user_refresh_token", params)
            log.debug("reauthorizing with host {}".format(self._host))
        else:
            # try default login info - we have no tokens.
            params = dict(username="admin", password="admin")
            login_request = self.format_request(login_message_id, "user_login", params)
            log.debug(f"default login with host {self._host} {login_request}")

        exception_to_raise = None

        try:
            api_endpoint = f"{self._scheme}://{self._host}:{self._port}{self._path}"
            log.debug(f"trying {api_endpoint}")
            response = self.http_conn.request('POST', api_endpoint, headers=self.headers,
                                              body=json.dumps(login_request).encode('utf-8'))
        # MaxRetryError occurs when we can't talk to the host at all
        except urllib3.exceptions.MaxRetryError as exc:
            # https failed, try http - http would never produce an ssl error
            if isinstance(exc.reason, urllib3.exceptions.SSLError):
                log.debug(f"SSLError detected: {exc.reason}")
                exception_to_raise = wekalib.exceptions.SSLError(exc.reason)
            # NewConnectionError occurs when we can't establish a new connection... determine why
            elif isinstance(exc.reason, urllib3.exceptions.NewConnectionError):
                log.debug(f"***************NewConnectionError caught {type(exc.reason)}")
                if isinstance(exc.reason, urllib3.exceptions.ConnectTimeoutError):  # timed out/didn't respond
                    log.debug(f"########## ConnectTimeoutError {str(exc.reason)}")
                    exception_to_raise = wekalib.exceptions.NewConnectionError("Host unreachable")
                elif isinstance(exc.reason, urllib3.exceptions.RequestError):  # Not sure... not resolvable?
                    log.debug(f"########## RequestError")
                    exception_to_raise = wekalib.exceptions.LoginError("Login failed")
                else:
                    exception_to_raise = wekalib.exceptions.NameNotResolvable(self._host)
            else:
                # not a new connection error, so report it
                log.debug(f"*********MaxRetryError: {exc.url} - {exc.reason};;;;;{type(exc.reason)}")
                # track = traceback.format_exc()
                # print(track)
                exception_to_raise = wekalib.exceptions.CommunicationError("Login attempt failed")
        # misc errors
        except Exception as exc:
            log.critical(f"{exc}")
            exception_to_raise = wekalib.exceptions.APIError("Unknown Error occurred")

        if exception_to_raise is not None:
            raise exception_to_raise

        response_body = response.data.decode('utf-8')

        if response.status != 200:
            # host rejected tokens
            raise wekalib.exceptions.LoginError(f"Login Failure on {self._host}, ({response.status}) {response_body}")
            # raise wekalib.exceptions.HTTPError(self._host, response.status, response_body)

        response_object = json.loads(response_body)

        if "error" in response_object:
            if response_object['error']['code'] == -32601:  # is it in STEM mode?
                log.info(f"{self._host} appears to be in STEM mode")
                raise wekalib.exceptions.STEMModeError(f"{self._host} appears to be in STEM mode")
            else:
                raise wekalib.exceptions.LoginError(
                    f"Unexpected error loggin into {self._host}, ({response.status}) {response_body}")

        try:
            self._tokens = response_object["result"]
        except KeyError:
            raise wekalib.exceptions.LoginError(f"Login Failure on {self._host}, ({response.status}) {response_body}")
        if self._tokens != {}:
            self.authorization = '{0} {1}'.format(self._tokens['token_type'], self._tokens['access_token'])
        else:
            self.authorization = None
            self._tokens = None
            log.critical("Login failed - no tokens returned")
            raise wekalib.exceptions.LoginError("Login failed (bad userid/password)")

        self.headers["Authorization"] = self.authorization
        log.debug("login to {} successful".format(self._host))

        # end of _login()

    # re-implemented with urllib3
    def weka_api_command(self, method, parms):

        api_exception = None
        api_endpoint = f"{self._scheme}://{self._host}:{self._port}{self._path}"
        message_id = self.unique_id()
        request = self.format_request(message_id, method, parms)
        log.debug(f"trying {api_endpoint}")
        try:
            response = self.http_conn.request('POST', api_endpoint, headers=self.headers,
                                              body=json.dumps(request).encode('utf-8'))
        except urllib3.exceptions.MaxRetryError as exc:
            # https failed, try http - http would never produce an ssl error
            if isinstance(exc.reason, urllib3.exceptions.SSLError):
                log.debug(f"SSLError detected")
                api_exception = wekalib.exceptions.SSLError(exc)
            elif isinstance(exc.reason, urllib3.exceptions.NewConnectionError):
                log.critical(f"NewConnectionError caught")
                api_exception = wekalib.exceptions.CommunicationError(
                    f"Cannot re-establish communication with {self._host}")
            else:
                log.critical(f"MaxRetryError: {exc.reason}")
                api_exception = wekalib.exceptions.CommunicationError(
                    f"MaxRetries exceeded on {self._host}: {exc.reason}")
        except urllib3.exceptions.ReadTimeoutError as exc:
            log.error(f"Read Timeout on {self._host}, {method}")
            api_exception = wekalib.exceptions.TimeoutError(f"Read request timeout on {self._host}")
            # raise
        except Exception as exc:
            log.debug(traceback.format_exc())
            api_exception = wekalib.exceptions.APIError("MiscException ({exc})")

        if api_exception is not None:
            raise api_exception

        # log.info('Response Code: {}'.format(response.status))  # ie: 200, 501, etc
        # log.info('Response Body: {}'.format(resp_body))

        if response.status == httpclient.UNAUTHORIZED:  # not logged in?
            log.debug("need to login")
            self._login()
            return self.weka_api_command(method, parms)  # recurse - try again

        if response.status in (httpclient.OK, httpclient.CREATED, httpclient.ACCEPTED):
            response_object = json.loads(response.data.decode('utf-8'))
            if 'error' in response_object:
                log.error("bad response from {}".format(self._host))
                raise wekalib.exceptions.IOStopped(response_object['error']['message'])
            log.debug(f"good response from {self._host}")
            return self._format_response(method, response_object)
        elif response.status == httpclient.MOVED_PERMANENTLY:
            oldhost = self._host
            self._scheme, self._host, self._port, self._path = self._parse_url(response.getheader('Location'))
            log.debug("redirection: {} moved to {}".format(oldhost, self._host))
        else:
            log.error(f"Other error on {self._host}, status={response.status}, reason={response.reason}")
            raise wekalib.exceptions.HTTPError(self._host, response.status, response.reason)

        # should only get here if MOVED_PERMANENTLY?
        resp_dict = json.loads(response.data.decode('utf-8'))
        log.debug(f"exiting from {self._host}**************************************")
        return self._format_response(method, resp_dict)


# main is for testing
def main():
    logger = getLogger()
    logger.setLevel(DEBUG)
    # log.setLevel(DEBUG)
    FORMAT = "%(filename)s:%(lineno)s:%(funcName)s():%(message)s"
    # create handler to log to stderr
    console_handler = StreamHandler()
    console_handler.setFormatter(Formatter(FORMAT))
    logger.addHandler(console_handler)

    api_connection = WekaApi('172.20.0.128')

    # parms={u'category': u'ops', u'stat': u'OPS', u'interval': u'1m', u'per_node': True}
    # print( parms)
    # print( json.dumps( api_connection.weka_api_command( "status" ) , indent=4, sort_keys=True))
    # print( json.dumps( api_connection.weka_api_command( "hosts_list" ) , indent=4, sort_keys=True))
    print(json.dumps(api_connection.weka_api_command("nodes_list"), indent=4, sort_keys=True))
    # print( json.dumps( api_connection.weka_api_command( "filesystems_list" ) , indent=4, sort_keys=True))
    # print( json.dumps( api_connection.weka_api_command( "filesystems_get_capacity" ) , indent=4, sort_keys=True))

    # more examples:
    # print( json.dumps(api_connection.weka_api_command( "stats_show", category='ops', stat='OPS', interval='1m', per_node=True ), indent=4, sort_keys=True) )
    # print( api_connection.weka_api_command( "status", fred="mary" ) )   # should produce error; for checking errorhandling

    # ======================================================================


if __name__ == "__main__":
    sys.exit(main())
