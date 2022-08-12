import json
import os
from dataclasses import dataclass
from typing import Optional

import requests
import yaml


@dataclass
class SberCloudConfig:
    login: Optional[str] = None
    password: Optional[str] = None
    domain: Optional[str] = None
    project_id: Optional[str] = None
    iam_url: Optional[str] = None

    @staticmethod
    def from_dict(config_dict: dict) -> 'SberCloudConfig':
        return SberCloudConfig(**config_dict)

    @staticmethod
    def from_yaml(config_path: str) -> 'SberCloudConfig':
        with open(config_path) as file:
            config_dict = yaml.load(file, Loader=yaml.FullLoader)
        return SberCloudConfig.from_dict(config_dict["sbercloud"])

    @staticmethod
    def from_env() -> 'SberCloudConfig':
        config_dict = {
            "domain": os.getenv("SBERCLOUD_DOMAIN"),
            "login": os.getenv("SBERCLOUD_LOGIN"),
            "password": os.getenv("SBERCLOUD_PASSWORD"),
            "project_id": os.getenv("SBERCLOUD_PROJECT_ID"),
            "iam_url": os.getenv("SBERCLOUD_IAM_URL"),
        }
        return SberCloudConfig.from_dict(config_dict)


class SberCloud:
    """
    Manages resources in Sbercloud via API.

    API reference:
    https://docs.sbercloud.ru/terraform/ug/topics/quickstart.html
    https://support.hc.sbercloud.ru/en-us/api/ecs/en-us_topic_0020212668.html
    """
    def __init__(self, config: SberCloudConfig) -> None:
        self.config = config
        self.ecs_url = None
        self.project_id = None
        self.token = None
        self._initialize()
        self.ecs_nodes = self.get_ecs_nodes()

    def _initialize(self) -> None:
        data = {
            'auth': {
                'identity': {
                    'methods': ['password'],
                    'password': {
                        'user': {
                            'domain': {
                                'name': self.config.domain
                            },
                            'name': self.config.login,
                            'password': self.config.password
                        }
                    }
                },
                'scope': {
                    'project': {
                        'id': self.config.project_id
                    }
                }
            }
        }
        response = requests.post(
            f'{self.config.iam_url}/v3/auth/tokens',
            data=json.dumps(data),
            headers={'Content-Type': 'application/json'}
        )
        self.ecs_url = [
            catalog['endpoints'][0]['url']
            for catalog in response.json()['token']['catalog'] if catalog['type'] == 'ecs'
        ][0]
        self.project_id = self.ecs_url.split('/')[-1]
        self.token = response.headers['X-Subject-Token']

    def find_ecs_node_by_ip(self, ip: str, no_cache: bool = False) -> str:
        if not self.ecs_nodes or no_cache:
            self.ecs_nodes = self.get_ecs_nodes()
        nodes_by_ip = [
            node for node in self.ecs_nodes
            if ip in [
                node_ip['addr']
                for node_ips in node['addresses'].values()
                for node_ip in node_ips
            ]
        ]
        assert len(nodes_by_ip) == 1
        return nodes_by_ip[0]['id']

    def get_ecs_nodes(self) -> list[dict]:
        response = requests.get(f'{self.ecs_url}/cloudservers/detail',
                                headers={'X-Auth-Token': self.token}).json()
        return response['servers']

    def start_node(self, node_id: Optional[str] = None, node_ip: Optional[str] = None) -> None:
        data = {
            'os-start': {
                'servers': [
                    {
                        'id': node_id or self.find_ecs_node_by_ip(node_ip)
                    }
                ]
            }
        }
        response = requests.post(
            f'{self.ecs_url}/cloudservers/action',
            data=json.dumps(data),
            headers={'Content-Type': 'application/json', 'X-Auth-Token': self.token}
        )
        assert response.status_code < 300, \
            f'Status:{response.status_code}. Server not started: {response.json()}'

    def stop_node(self, node_id: Optional[str] = None, node_ip: Optional[str] = None,
                  hard: bool = False) -> None:
        data = {
            'os-stop': {
                'type': 'HARD' if hard else 'SOFT',
                'servers': [
                    {
                        'id': node_id or self.find_ecs_node_by_ip(node_ip)
                    }
                ]
            }
        }
        response = requests.post(
            f'{self.ecs_url}/cloudservers/action',
            data=json.dumps(data),
            headers={'Content-Type': 'application/json', 'X-Auth-Token': self.token}
        )
        assert response.status_code < 300, \
            f'Status:{response.status_code}. Server not stopped: {response.json()}'
