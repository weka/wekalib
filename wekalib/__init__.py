from . import _version
__version__ = _version.get_versions()['version']

from logging import getLogger, NullHandler
log = getLogger('wekaapi')
log.addHandler(NullHandler())


from .wekaapi import WekaApi
from .wekacluster import WekaCluster
from .signals import signal_handling
from .exceptions import (
    APIError,
    NewConnectionError,
    LoginError,
    STEMModeError,
    CommunicationError,
    HTTPError,
    AuthFileError,
    IOStopped,
    TimeoutError,
    NameNotResolvable,
    SSLError
)

from . import _version
__version__ = _version.get_versions()['version']
