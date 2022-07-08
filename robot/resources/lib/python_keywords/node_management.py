#!/usr/bin/python3

"""
    This module contains keywords for management test stand
    nodes. It assumes that nodes are docker containers.
"""

import random
import re
from dataclasses import dataclass
from typing import List, Tuple

import docker
from common import DEPLOY_PATH, NEOFS_NETMAP_DICT
from robot.api import logger
from robot.api.deco import keyword
from ssh_helper import HostClient

ROBOT_AUTO_KEYWORDS = False


@dataclass
class HealthStatus:
    network_status: str = None
    health_status: str = None

    @staticmethod
    def from_stdout(output: str) -> 'HealthStatus':
        network, health = None, None
        for line in output.split('\n'):
            if 'Network status' in line:
                network = line.split(':')[-1].strip()
            if 'Health status' in line:
                health = line.split(':')[-1].strip()
        return HealthStatus(network, health)


@keyword('Stop Nodes')
def stop_nodes(number: int, nodes: list):
    """
        The function shuts down the given number of randomly
        selected nodes in docker.
        Args:
           number (int): the number of nodes to shut down
           nodes (list): the list of nodes for possible shut down
        Returns:
            (list): the list of nodes which have been shut down
    """
    nodes = random.sample(nodes, number)
    client = docker.APIClient()
    for node in nodes:
        node = node.split('.')[0]
        client.stop(node)
    return nodes


@keyword('Start Nodes')
def start_nodes(nodes: list):
    """
        The function raises the given nodes.
        Args:
           nodes (list): the list of nodes to raise
        Returns:
            (void)
    """
    client = docker.APIClient()
    for node in nodes:
        node = node.split('.')[0]
        client.start(node)


@keyword('Get control endpoint and wallet')
def get_control_endpoint_and_wallet(endpoint_number: str = ''):
    """
        Gets control endpoint for a random or given node

        Args:
            endpoint_number (optional, str): the number of the node
                                        in the form of 's01', 's02', etc.
                                        given in NEOFS_NETMAP_DICT as keys
        Returns:
            (str): the number of the node
            (str): endpoint control for the node
            (str): the wallet of the respective node
    """
    if endpoint_number == '':
        endpoint_num = random.choice(list(NEOFS_NETMAP_DICT.keys()))
        logger.info(f'Random node chosen: {endpoint_num}')
    else:
        endpoint_num = endpoint_number

    endpoint_values = NEOFS_NETMAP_DICT[f'{endpoint_num}']
    endpoint_control = endpoint_values['control']
    wlt = endpoint_values['wallet_path']

    return endpoint_num, endpoint_control, wlt


@keyword('Get Locode')
def get_locode():
    endpoint_values = random.choice(list(NEOFS_NETMAP_DICT.values()))
    locode = endpoint_values['UN-LOCODE']
    logger.info(f'Random locode chosen: {locode}')

    return locode


@keyword('Stop Nodes Remote')
def stop_nodes_remote(client: HostClient, number: int, nodes: list):
    """
        The function shuts down the given number of randomly
        selected nodes in docker.
        Args:
            client (HostClient): client that implements exec command
            number (int): the number of nodes to shut down
            nodes (list): the list of nodes for possible shut down
        Returns:
            (list): the list of nodes which have been shut down
    """
    nodes = random.sample(nodes, number)
    for node in nodes:
        node = node.split('.')[0]
        client.exec(f'docker stop {node}')
    return nodes


@keyword('Start Nodes Remote')
def start_nodes_remote(client: HostClient, nodes: list):
    """
        The function starts nodes in docker.
        Args:
           client (HostClient): client that implements exec command
           nodes (list): the list of nodes for possible shut down
    """
    for node in nodes:
        node = node.split('.')[0]
        client.exec(f'docker start {node}')


@keyword('Healthcheck for node')
def node_healthcheck(client: HostClient, node_name: str) -> HealthStatus:
    """
        The function returns node's health status.
        Args:
            client HostClient: client that implements exec command.
            node_name str: node name to use for netmap snapshot operation
        Returns:
            health status as HealthStatus object.
    """
    if node_name not in NEOFS_NETMAP_DICT:
        raise AssertionError(f'Node {node_name} is not found!')

    node_config = NEOFS_NETMAP_DICT.get(node_name)
    control_url = node_config.get('control')
    host, port = control_url.split(':')
    cmd = f'{DEPLOY_PATH}/vendor/neofs-cli control healthcheck --endpoint {control_url} ' \
          f'--wallet {DEPLOY_PATH}/services/storage/wallet0{port[-1]}.json ' \
          f'--config {DEPLOY_PATH}/services/storage/cli-cfg.yml'
    output = client.exec_with_confirmation(cmd, [''])
    return HealthStatus.from_stdout(output.stdout)


@keyword('Set status for node')
def node_set_status(client: HostClient, node_name: str, status: str):
    """
        The function sets particular status for given node.
        Args:
            client HostClient: client that implements exec command.
            node_name str: node name to use for netmap snapshot operation
            status str: online or offline.
        Returns:
            (void)
    """
    if node_name not in NEOFS_NETMAP_DICT:
        raise AssertionError(f'Node {node_name} is not found!')

    node_config = NEOFS_NETMAP_DICT.get(node_name)
    control_url = node_config.get('control')
    host, port = control_url.split(':')
    cmd = f'{DEPLOY_PATH}/vendor/neofs-cli control set-status --endpoint {control_url} ' \
          f'--wallet {DEPLOY_PATH}/services/storage/wallet0{port[-1]}.json ' \
          f'--config {DEPLOY_PATH}/services/storage/cli-cfg.yml --status {status}'
    client.exec_with_confirmation(cmd, [''])


@keyword('Get netmap snapshot')
def get_netmap_snapshot(client: HostClient, node_name: str = None) -> str:
    """
        The function returns string representation of netmap-snapshot.
        Args:
            client HostClient: client that implements exec command.
            node_name str: node name to use for netmap snapshot operation
        Returns:
            string representation of netmap-snapshot
    """
    node_name = node_name or list(NEOFS_NETMAP_DICT)[0]

    if node_name not in NEOFS_NETMAP_DICT:
        raise AssertionError(f'Node {node_name} is not found!')

    node_config = NEOFS_NETMAP_DICT.get(node_name)
    control_url = node_config.get('control')
    host, port = control_url.split(':')
    cmd = f'{DEPLOY_PATH}/vendor/neofs-cli control netmap-snapshot --endpoint {control_url} ' \
          f'--wallet {DEPLOY_PATH}/services/storage/wallet0{port[-1]}.json ' \
          f'--config {DEPLOY_PATH}/services/storage/cli-cfg.yml'
    output = client.exec_with_confirmation(cmd, [''])
    return output.stdout


@keyword('Shard list for node')
def node_shard_list(client: HostClient, node_name: str) -> List[str]:
    """
        The function returns list of shards for particular node.
        Args:
            client HostClient: client that implements exec command.
            node_name str: node name to use for netmap snapshot operation
        Returns:
            list of shards.
    """
    control_url, port = _url_port_for_node(node_name)
    cmd = f'{DEPLOY_PATH}/vendor/neofs-cli control shards list --endpoint {control_url} ' \
          f'--wallet {DEPLOY_PATH}/services/storage/wallet0{port[-1]}.json ' \
          f'--config {DEPLOY_PATH}/services/storage/cli-cfg.yml'
    output = client.exec_with_confirmation(cmd, [''])
    return re.findall(r'Shard (.*):', output.stdout)


@keyword('Shard list for node')
def node_shard_set_mode(client: HostClient, node_name: str, shard: str, mode: str) -> str:
    """
        The function sets mode for node's particular shard.
        Args:
            client HostClient: client that implements exec command.
            node_name str: node name to use for netmap snapshot operation
        Returns:
            health status as HealthStatus object.
    """
    control_url, port = _url_port_for_node(node_name)
    cmd = f'{DEPLOY_PATH}/vendor/neofs-cli control shards set-mode --endpoint {control_url} ' \
          f'--wallet {DEPLOY_PATH}/services/storage/wallet0{port[-1]}.json ' \
          f'--config {DEPLOY_PATH}/services/storage/cli-cfg.yml --id {shard} --mode {mode}'
    output = client.exec_with_confirmation(cmd, [''])
    return output.stdout


@keyword('Drop object from node {node_name}')
def drop_object(client: HostClient, node_name: str, cid: str, oid: str) -> str:
    """
        The function drops object from particular node.
        Args:
            client HostClient: client that implements exec command.
            node_name str: node name to use for netmap snapshot operation
        Returns:
            health status as HealthStatus object.
    """
    control_url, port = _url_port_for_node(node_name)
    cmd = f'{DEPLOY_PATH}/vendor/neofs-cli control drop-objects --endpoint {control_url} ' \
          f'--wallet {DEPLOY_PATH}/services/storage/wallet0{port[-1]}.json ' \
          f'--config {DEPLOY_PATH}/services/storage/cli-cfg.yml -o {cid}/{oid}'
    output = client.exec_with_confirmation(cmd, [''])
    return output.stdout


def _url_port_for_node(node_name: str) -> Tuple[str, str]:
    """
    Returns control url and port for particular storage node.
    Args:
        node_name: str node bane from NEOFS_NETMAP_DICT

    Returns:
        control url and port as a tuple.
    """
    if node_name not in NEOFS_NETMAP_DICT:
        raise AssertionError(f'Node {node_name} is not found!')

    node_config = NEOFS_NETMAP_DICT.get(node_name)
    control_url = node_config.get('control')
    port = control_url.split(':')[-1]
    return control_url, port
