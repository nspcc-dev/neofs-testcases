import json
from dataclasses import dataclass

import requests
from yaml import FullLoader
from yaml import load as yaml_load


@dataclass
class SberCloudCtx:
    sber_login: str = None
    sber_password: str = None
    sber_domain: str = None
    sber_project_id: str = None
    sber_iam_url: str = None
    sber_ecss: list = None

    @staticmethod
    def from_dict(sbercloud_dict: dict) -> 'SberCloudCtx':
        return SberCloudCtx(**sbercloud_dict)

    @staticmethod
    def from_yaml(config: str) -> 'SberCloudCtx':
        with open(config) as yaml_file:
            config_from_yaml = yaml_load(yaml_file, Loader=FullLoader)
        return SberCloudCtx.from_dict(config_from_yaml)


class SberCloud:
    def __init__(self, config: str):
        self.sbercloud_config = SberCloudCtx().from_yaml(config)
        self.ecs_url = None
        self.project_id = None
        self.token = None
        self.update_token()
        self.ecss = self.get_ecss()

    def update_token(self):
        data = {
            'auth': {
                'identity': {
                    'methods': ['password'],
                    'password': {
                        'user': {
                            'domain': {
                                'name': self.sbercloud_config.sber_domain
                            },
                            'name': self.sbercloud_config.sber_login,
                            'password': self.sbercloud_config.sber_password
                        }
                    }
                },
                'scope': {
                    'project': {
                        'id': self.sbercloud_config.sber_project_id
                    }
                }
            }
        }
        response = requests.post(f'{self.sbercloud_config.sber_iam_url}/v3/auth/tokens', data=json.dumps(data),
                                 headers={'Content-Type': 'application/json'})
        self.ecs_url = [catalog['endpoints'][0]['url']
                        for catalog in response.json()['token']['catalog'] if catalog['type'] == 'ecs'][0]
        self.project_id = self.ecs_url.split('/')[-1]
        self.token = response.headers['X-Subject-Token']

    def find_esc_by_ip(self, ip: str, update: bool = False) -> str:
        if not self.ecss or update:
            self.ecss = self.get_ecss()
        ecss = [ecs for ecs in self.ecss if ip in [
                ecs_ip['addr'] for ecs_ip in [ecs_ip for ecs_ips in ecs['addresses'].values() for ecs_ip in ecs_ips]]]
        assert len(ecss) == 1
        return ecss[0]['id']

    def get_ecss(self) -> [dict]:
        response = requests.get(f'{self.ecs_url}/cloudservers/detail',
                                headers={'X-Auth-Token': self.token}).json()
        return response['servers']

    def start_node(self, node_id: str = None, node_ip: str = None):
        data = {
            'os-start': {
                'servers': [
                    {
                        'id': node_id or self.find_esc_by_ip(node_ip)
                    }
                ]
            }
        }
        response = requests.post(f'{self.ecs_url}/cloudservers/action',
                                 data=json.dumps(data),
                                 headers={'Content-Type': 'application/json', 'X-Auth-Token': self.token})
        assert response.status_code < 300, f'Status:{response.status_code}. Server not started: {response.json()}'

    def stop_node(self, node_id: str = None, node_ip: str = None, hard: bool = False):
        data = {
            'os-stop': {
                'type': 'HARD' if hard else 'SOFT',
                'servers': [
                    {
                        'id': node_id or self.find_esc_by_ip(node_ip)
                    }

                ]
            }
        }
        response = requests.post(f'{self.ecs_url}/cloudservers/action',
                                 data=json.dumps(data),
                                 headers={'Content-Type': 'application/json', 'X-Auth-Token': self.token})
        assert response.status_code < 300, f'Status:{response.status_code}. Server not stopped: {response.json()}'
