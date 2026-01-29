import os
import random
import requests
import  json
from time import sleep

from dotenv import load_dotenv
from tqdm import tqdm

from utils import CITY

load_dotenv()

ZABBIX_URL = os.getenv("ZABBIX_URL", "ZABBIX_URL")
ZABBIX_USER = os.getenv("ZABBIX_USER", "ZABBIX_USER")
ZABBIX_PASSWORD = os.getenv("ZABBIX_PASSWORD", "ZABBIX_PASSWORD")
HOSTS = int(os.getenv("HOST_COUNTS", 1))


class ZabbixManagement:
    
    def __init__(self, zabbix_url: str, username: str, password: str) -> None:
        self.zabbix_url = zabbix_url
        self._username = username
        self._password = password
        
    def get_token(self) -> str:
        payload = {
            "jsonrpc": "2.0",
            "method": "user.login",
            "params": {
                "username": self._username,
                "password": self._password
            },
            "id": 1
        }
        
        response = requests.post(
            self.zabbix_url,
            data=json.dumps(payload),
            headers=self.get_header()
        ).json()
        return response["result"]
    
    def create_host(self, token: str, number_server: int) -> None:
        payload = {
            "jsonrpc": "2.0",
            "method": "host.create",
            "params": {
                "host": f"web_server {number_server}",
                "interfaces": [
                    {
                        "type": 2,
                        "main": 1,
                        "useip": 1,
                        "ip": "172.22.0.100",
                        "dns": "",
                        "port": "161",
                        "details": {
                            "version": 2,
                            "bulk": 1,
                            "community": "public"
                        }
                    }
                ],
                "groups": [
                    {
                        "groupid": "5"
                    }
                ],
                "templates": [
                    {
                        "templateid": "10248"
                    }
                ]
            },
            "id": 1
        }
        
        response = requests.post(
            self.zabbix_url,
            data=json.dumps(payload),
            headers=self.get_header(token)
        )
        return response.json()["result"]["hostids"][0]
    
    def get_interface_id(self, token: str, host_id: str) -> str:
        payload = {
            "jsonrpc": "2.0",
            "method": "host.get",
            "params": {
                "output": "hostid",
                "hostids": host_id,
                "selectInterfaces": ["interfaceid", "ip"]
            },
            "id": 1
        }
        
        response = requests.post(
            ZABBIX_URL, 
            data=json.dumps(payload), 
            headers=self.get_header(token)
        )
        return response.json()["result"][0]["interfaces"][0]["interfaceid"]
        
    def change_ip_address(self, token: str, host_id: str, interface_id: str) -> None:
        payload = {
            "jsonrpc": "2.0",
            "method": "host.update",
            "params": {
                "hostid": host_id,
                "interfaces": [
                    {
                        "interfaceid": interface_id,
                        "ip": self.generate_ip_address()
                    }
                ]
            },
            "id": 1
        }
        requests.post(
            ZABBIX_URL, 
            data=json.dumps(payload), 
            headers=self.get_header(token)
        )
        
    def disable_host(self, token: str, host_id: str) -> None:
        payload = {
            "jsonrpc": "2.0",
            "method": "host.update",
            "params": {
                "hostid": host_id,
                "status": "1"
            },
            "id": 1
        }
        requests.post(
            ZABBIX_URL, 
            data=json.dumps(payload), 
            headers=self.get_header(token)
        )
        
    def add_city(self, token: str, host_id: str, city: str) -> None:
        payload = {
            "jsonrpc": "2.0",
            "method": "host.update",
            "params": {
                "hostid": host_id,
                "description": city
            },
            "id": 1
        }
        requests.post(
            ZABBIX_URL, 
            data=json.dumps(payload), 
            headers=self.get_header(token)
        )

    @staticmethod
    def generate_ip_address() -> str:
        return (
            f'{random.randint(1, 254)}.'
            f'{random.randint(0, 254)}.'
            f'{random.randint(0, 254)}.'
            f'{random.randint(1, 254)}'
        )
        
    @staticmethod
    def get_header(token: str | None = None) -> dict[str, str]:
        if token is None:
            return {
                "Content-Type": "application/json-rpc"
            }
        else:
            return {
                "Content-Type": "application/json-rpc",
                "Authorization": f"Bearer {token}"
            }


if __name__ == "__main__":
    hosts_list = []
    zabbix = ZabbixManagement(
        zabbix_url=ZABBIX_URL,
        username=ZABBIX_USER,
        password=ZABBIX_PASSWORD
    )
    
    token = zabbix.get_token()
    for host in tqdm(range(1, HOSTS + 1), "Загружаем оборудование мониторинга"):
        hosts_list.append(zabbix.create_host(token=token, number_server=host))
        sleep(15)
    for host in tqdm(hosts_list, "Происходит дискаверинг объектов оборудования"):
        interface_id = zabbix.get_interface_id(token=token, host_id=host)
        zabbix.change_ip_address(
            token=token, 
            host_id=host,
            interface_id=interface_id
        )
        zabbix.add_city(
            token=token,
            host_id=host,
            city=random.choice(CITY)
        )
        zabbix.disable_host(token=token, host_id=host)
        sleep(10)
    