# 
# exceptions.py - exception definitions for wekalib
#


class APIError(Exception):
    """ base class """
    pass


class NewConnectionError(APIError):
    """ failed to open a connection to the API """
    pass


class LoginError(APIError):
    """ failed to log into the host/cluster """
    pass


class STEMModeError(APIError):
    """ failed to log into the host/cluster """
    pass


class CommunicationError(APIError):
    """ an existing connection has failed (host or cluster or both?) """
    pass


class HTTPError(APIError):
    """ API returned a bad return code """

    def __init__(self, host, code, message):
        self.host = host
        self.code = code
        self.message = message
        APIError.__init__(self, "%s: (%s) %s" % (host, code, message))


class AuthFileError(APIError):
    """ auth file was not found, or is unreadable/parsable """
    pass


class IOStopped(APIError):
    """ Cluster reports that io has been stopped (weka cluster stop-io) """
    pass


class TimeoutError(APIError):
    """ request to cluster timed out """
    pass


class NameNotResolvable(APIError):
    """ raised when we can't resolve a name """
    pass


class SSLError(APIError):
    """ raised when we can't resolve a name """
    pass
