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

    def get_hosts(self):
        # not sure if this returns clients also...
        method = "/hosts"
        url = self.base_url + method
        headers = {"Authorization": f"{self.token_type} {self.api_key}"}
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"ERROR {response.status_code} connecting to cluster")
        return response.json()

    # add this method to return a list of hosts/base containers
    def get_base_containers(self):
        hosts = self.get_hosts()
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
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"ERROR {response.status_code} connecting to cluster")
        return response.json()

    def update_document(self, document_id, data):
        url = f"{self.base_url}/documents/{document_id}"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        response = requests.put(url, headers=headers, json=data)
        response.raise_for_status()
        return response.json()

    def delete_document(self, document_id):
        url = f"{self.base_url}/documents/{document_id}"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        response = requests.delete(url, headers=headers)
        response.raise_for_status()
