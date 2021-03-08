#
# signals module - handle typical signals
#

# author: Vince Fleming, vince@weka.io

import signal
import sys
from logging import getLogger

log = getLogger(__name__)


class signal_handling():
    def __init__(self, graceful_terminate=None, opaque_obj=None):
        global func
        global obj
        func = graceful_terminate
        obj = opaque_obj
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        signal.signal(signal.SIGINT, self.sigint_handler)
        signal.signal(signal.SIGHUP, self.sighup_handler)
        log.debug("signal_handling: signal handlers set")

    @staticmethod
    def sigterm_handler(signal, frame):
        log.critical('SIGTERM received, exiting')
        if func is not None:
            func(obj)
        sys.exit(0)

    @staticmethod
    def sigint_handler(signal, frame):
        log.critical('SIGINT received, exiting')
        if func is not None:
            func(obj)
        sys.exit(1)

    @staticmethod
    def sighup_handler(signal, frame):
        log.critical('SIGHUP received, exiting')
        if func is not None:
            func(obj)
        sys.exit(0)
