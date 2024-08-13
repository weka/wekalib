#
# beacon.py - collect WEKA beacons
#
# WEKA STEM mode sends multicast beacons, and here we collect them so we can auto-detect hosts that need configuration
#

import socket
import threading
import time
from copy import copy
import logging

log = logging.getLogger(__name__)


class Beacons:
    def __init__(self, udp_ip="", port=14080, max_age=120):
        self.beacons = dict()
        self.beacon_thread = None
        self.terminate = True
        self.ip = udp_ip
        self.port = port
        self.MAX_AGE = max_age
        self.MAGIC = "WEKABCN1"
        self.I_AM_HERE = 0x9a

    def start_beacon_listener(self):
        self.beacon_thread = threading.Thread(target=self.collect_beacons)
        self.beacon_thread.start()

    def stop_beacon_listener(self):
        self.terminate = True
        self.beacon_thread.join()

    def get_beacons_by_ip(self):
        self._remove_stale_beacons()
        value = dict()
        for ipaddr, host  in copy(self.beacons).items():
            value[ipaddr] = host[0]
        return dict(sorted(value.items()))

    def get_beacons_by_hostname(self):
        self._remove_stale_beacons()
        value = dict()
        for ipaddr, host  in copy(self.beacons).items():
            if host[0] not in value:
                value[host[0]] = [ipaddr]
            else:
                value[host[0]].append(ipaddr)
        return dict(sorted(value.items()))

    def add_beacon(self, ip, hostname):
        self.beacons[ip] = (hostname, time.time())

    def collect_beacons(self):
        start_time = time.time()

        # Create a UDP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        sock.bind((self.ip, self.port))

        log.info(f"Listening for UDP packets on {self.ip}:{self.port}")

        self.terminate = False
        while not self.terminate:
            # Receive data from the socket
            data, addr = sock.recvfrom(1024)

            magic = data[:8].decode("utf-8")
            if magic != self.MAGIC:
                continue    # ignore the packet

            packet_size = data[8]
            i_am_here = data[10]
            if i_am_here != self.I_AM_HERE:
                continue
            hostname_length = data[11]
            #print(f"length of hostname is {hostname_length}")
            hostname = data[12:12+hostname_length].decode("utf-8")
            host_address, port = addr
            log.debug(f"hostname is {hostname} addr is {host_address}:{port}")
            log.debug(f"time running: {int(time.time() - start_time)}")
            self.add_beacon(host_address, hostname)
            #self.beacons[host_address] = (hostname, time.time())

    def _remove_stale_beacons(self):
        # time out old entries
        now = time.time()
        for ipaddr, host in copy(self.beacons).items():
            hostname, timestamp = host
            if now - timestamp > self.MAX_AGE:
                log.info(f"deleting beacon {ipaddr}/{hostname}")
                del self.beacons[ipaddr]


if __name__ == '__main__':
    def display_tables():
        def print_tables(beacon_dict):
            for beacon, value in beacon_dict.items():
                print(f"{beacon}:{value}")

        hostlist = beacons.get_beacons_by_ip()
        print("BY IP")
        print_tables(hostlist)
        hostlist = beacons.get_beacons_by_hostname()
        print("BY HOST")
        print_tables(hostlist)
        print()

    logging.basicConfig()
    log.setLevel(logging.INFO)

    beacons = Beacons(max_age=30)

    print("STARTING THREAD")
    beacons.start_beacon_listener()

    time.sleep(30)
    display_tables()
    time.sleep(30)
    display_tables()
    time.sleep(30)
    display_tables()
    time.sleep(30)
    display_tables()
    time.sleep(30)
    display_tables()
    print("STOPPING THREAD")
    beacons.stop_beacon_listener()
