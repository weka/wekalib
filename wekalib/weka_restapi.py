from logging import getLogger, DEBUG, StreamHandler, Formatter
import requests

http = "http://"


class WekaAPIClient:
    def __init__(self, hostname):
        self.hostname = hostname
        self.api_key = None
        self.token_type = None
        self.base_url = f"http://{self.hostname}:14000/api/v2"
        self.weka_version = None
        self.mcb = False

    def login(self, user, password):
        """ returns auth-tokens """
        url = self.base_url + "/login"
        body = {"username": user, "password": password, "org": "root"}
        response = requests.post(url, data=body, timeout=0.2)
        if response.status_code != 200:
            raise Exception("Invalid username or password")
        answer = response.json()['data']
        auth = dict()
        auth['access_token'] = answer['access_token']
        auth['token_type'] = answer['token_type']
        auth['refresh_token'] = answer['refresh_token']
        self.token_type = auth['token_type']
        self.api_key = auth['access_token']
        cluster_data = self.get_cluster()
        self.weka_version = cluster_data['data']['release'].split('.')  # so we can tell api vers
        # < 4.1.x uses the "old" terms - hosts, nodes, etc.   4.1.x and above use servers, processes, etc
        self.api_vers = 1 if int(self.weka_version[0]) < 4 or int(self.weka_version[0]) == 4 and int(
            self.weka_version[1]) == 0 else 2
        return auth

    def get_containers(self):
        # This returns clients as well as backends
        method = "/containers"
        url = self.base_url + method
        headers = {"Authorization": f"{self.token_type} {self.api_key}"}
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            raise Exception(f"ERROR {response.status_code} connecting to cluster")
        return response.json()

    # return a set of unique hostnames, eliminating dupes caused by multiple containers
    # supports a parameter to filter out non-backends
    def get_hosts(self, backends_only=False, clients_only=False):
        containers = self.get_containers()
        hostlist = containers['data']

        #Set comprehension to test for mode and eliminate duplicates
        hostnames = {host['hostname'] for host in hostlist
                         if (host['mode'] == 'backend' and backends_only) or
                            (host['mode'] == 'client' and clients_only) or
                            (not backends_only and not clients_only)
                    }
        return hostnames


    # add this method to return a list of hosts/base containers
    def get_base_containers(self):
        hosts = self.get_containers()
        hostlist = hosts['data']
        for host in hostlist:
            # get rid of clients and down hosts
            # print(f"host = {host['hostname']}, mode= {host['mode']}, state={host['state']}, status={host['status']}")
            if host['mode'] != 'backend' or host["state"] != "ACTIVE" or host["status"] != "UP":
                # print(f"   removing {host['hostname']}")
                hostlist.remove(host)
            else:
                # it's online, but not the container we want
                if host["mgmt_port"] != 14000:
                    self.mcb = True     # if there are containers not on 14000, it's MCB
                    hostlist.remove(host)  # we only want one per server
        # print(hostlist)
        return hostlist


    def get_cluster(self):
        url = self.base_url + "/cluster"
        headers = {"Authorization": f"{self.token_type} {self.api_key}"}
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            raise Exception(f"ERROR {response.status_code} connecting to cluster")
        return response.json()

    def get_filesystems(self):
        url = self.base_url + "/fileSystems"
        headers = {"Authorization": f"{self.token_type} {self.api_key}"}
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            raise Exception(f"ERROR {response.status_code} connecting to cluster")
        return response.json()

    def update_document(self, document_id, data):
        url = f"{self.base_url}/documents/{document_id}"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        response = requests.put(url, headers=headers, json=data, timeout=60)
        response.raise_for_status()
        return response.json()

    def delete_document(self, document_id):
        url = f"{self.base_url}/documents/{document_id}"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        response = requests.delete(url, headers=headers, timeout=60)
        response.raise_for_status()

# main is for testing
def main():
    logger = getLogger()
    logger.setLevel(DEBUG)

    FORMAT = "%(filename)s:%(lineno)s:%(funcName)s():%(message)s"
    # create handler to log to stderr
    console_handler = StreamHandler()
    console_handler.setFormatter(Formatter(FORMAT))
    logger.addHandler(console_handler)

    # Connect to Weka server
    weka = WekaAPIClient('localhost')
    weka.login('admin', 'Admin777')

    # Get Cluster Status
    print('{0:-^80}'.format('Get Cluster Status'))
    cluster = weka.get_cluster()
    print('Cluster Status: {0}'.format(cluster['data']['status']))

    # Get All Hosts
    print('{0:-^80}'.format('Get All Hosts'))
    hosts = weka.get_hosts()
    for host in hosts:
        print(host)

    # Get Backend Hosts
    print('{0:-^80}'.format('Get Backend Hosts'))
    hosts = weka.get_hosts(backends_only=True)
    for host in hosts:
        print(host)

    # Get Client Hosts
    print('{0:-^80}'.format('Get Client Hosts'))
    hosts = weka.get_hosts(clients_only=True)
    for host in hosts:
        print(host)


    # Get Containers
    print('{0:-^80}'.format('Get Containers'))
    containers = weka.get_containers()
    for container in containers['data']:
        print('{0} {1} {2} {3}'.format(
            container['uid'],
            container['hostname'],
            container['mode'],
            container['container_name']))

    # Get Base Containers
    print('{0:-^80}'.format('Get Base Containers'))
    base_containers=weka.get_base_containers()
    for base_container in base_containers:
        print('{0} {1}'.format(base_container['uid'], base_container['hostname']))

    # Get Filesystems
    print('{0:-^80}'.format('Get FileSystems'))
    filesystems = weka.get_filesystems()
    for filesystem in filesystems['data']:
        print(filesystem['name'])



if __name__ == "__main__":
    exit(main())
