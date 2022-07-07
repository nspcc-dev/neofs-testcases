#!/usr/bin/python3

"""
    This module contains keywords for management test stand
    nodes. It assumes that nodes are docker containers.
"""

import random
import re
from contextlib import contextmanager
from dataclasses import dataclass
from typing import List

import docker
from common import NEOFS_NETMAP_DICT, STORAGE_NODE_BIN_PATH, STORAGE_NODE_CONFIG_PATH, STORAGE_NODE_PRIVATE_CONTROL_ENDPOINT, STORAGE_NODE_PWD, STORAGE_NODE_USER
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


@contextmanager
def create_ssh_client(node_name: str) -> HostClient:
    if node_name not in NEOFS_NETMAP_DICT:
        raise AssertionError(f'Node {node_name} is not found!')

    node_config = NEOFS_NETMAP_DICT.get(node_name)
    host = node_config.get('control').split(':')[0]
    ssh_client = HostClient(host, STORAGE_NODE_USER, STORAGE_NODE_PWD)

    try:
        yield ssh_client
    finally:
        ssh_client.drop()


@keyword('Stop Nodes')
def stop_nodes(number: int, nodes: list) -> None:
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
def start_nodes(nodes: list) -> None:
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
def stop_nodes_remote(number: int, nodes: list) -> None:
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
    for node in nodes:
        node = node.split('.')[0]
        with create_ssh_client(node) as ssh_client:
            ssh_client.exec(f'docker stop {node}')
    return nodes


@keyword('Start Nodes Remote')
def start_nodes_remote(nodes: list) -> None:
    """
        The function starts nodes in docker.
        Args:
           nodes (list): the list of nodes for possible shut down
    """
    for node in nodes:
        node = node.split('.')[0]
        with create_ssh_client(node) as ssh_client:
            ssh_client.exec(f'docker start {node}')


@keyword('Healthcheck for node')
def node_healthcheck(node_name: str) -> HealthStatus:
    """
        The function returns node's health status.
        Args:
            node_name str: node name to use for netmap snapshot operation
        Returns:
            health status as HealthStatus object.
    """
    with create_ssh_client(node_name) as ssh_client:
        cmd = f'{STORAGE_NODE_BIN_PATH}/neofs-cli control healthcheck ' \
            f'--endpoint {STORAGE_NODE_PRIVATE_CONTROL_ENDPOINT} ' \
            f'--config {STORAGE_NODE_CONFIG_PATH}'
        output = ssh_client.exec_with_confirmation(cmd, [''])
        return HealthStatus.from_stdout(output.stdout)


@keyword('Set status for node')
def node_set_status(node_name: str, status: str):
    """
        The function sets particular status for given node.
        Args:
            node_name str: node name to use for netmap snapshot operation
            status str: online or offline.
        Returns:
            (void)
    """
    with create_ssh_client(node_name) as ssh_client:
        cmd = f'{STORAGE_NODE_BIN_PATH}/neofs-cli control set-status ' \
            f'--endpoint {STORAGE_NODE_PRIVATE_CONTROL_ENDPOINT} ' \
            f'--config {STORAGE_NODE_CONFIG_PATH} --status {status}'
        ssh_client.exec_with_confirmation(cmd, [''])


@keyword('Get netmap snapshot')
def get_netmap_snapshot(node_name: str = None) -> str:
    """
        The function returns string representation of netmap-snapshot.
        Args:
            node_name str: node name to use for netmap snapshot operation
        Returns:
            string representation of netmap-snapshot
    """
    node_name = node_name or list(NEOFS_NETMAP_DICT)[0]

    with create_ssh_client(node_name) as ssh_client:
        cmd = f'{STORAGE_NODE_BIN_PATH}/neofs-cli control netmap-snapshot ' \
            f'--endpoint {STORAGE_NODE_PRIVATE_CONTROL_ENDPOINT} ' \
            f'--config {STORAGE_NODE_CONFIG_PATH}'
        output = ssh_client.exec_with_confirmation(cmd, [''])
        return output.stdout


@keyword('Shard list for node')
def node_shard_list(node_name: str) -> List[str]:
    """
        The function returns list of shards for particular node.
        Args:
            node_name str: node name to use for netmap snapshot operation
        Returns:
            list of shards.
    """
    with create_ssh_client(node_name) as ssh_client:
        cmd = f'{STORAGE_NODE_BIN_PATH}/neofs-cli control shards list ' \
            f'--endpoint {STORAGE_NODE_PRIVATE_CONTROL_ENDPOINT} ' \
            f'--config {STORAGE_NODE_CONFIG_PATH}'
        output = ssh_client.exec_with_confirmation(cmd, [''])
        return re.findall(r'Shard (.*):', output.stdout)


@keyword('Shard list for node')
def node_shard_set_mode(node_name: str, shard: str, mode: str) -> str:
    """
        The function sets mode for node's particular shard.
        Args:
            node_name str: node name to use for netmap snapshot operation
        Returns:
            health status as HealthStatus object.
    """
    with create_ssh_client(node_name) as ssh_client:
        cmd = f'{STORAGE_NODE_BIN_PATH}/neofs-cli control shards set-mode ' \
            f'--endpoint {STORAGE_NODE_PRIVATE_CONTROL_ENDPOINT} ' \
            f'--config {STORAGE_NODE_CONFIG_PATH} --id {shard} --mode {mode}'
        output = ssh_client.exec_with_confirmation(cmd, [''])
        return output.stdout


@keyword('Drop object from node {node_name}')
def drop_object(node_name: str, cid: str, oid: str) -> str:
    """
        The function drops object from particular node.
        Args:
            node_name str: node name to use for netmap snapshot operation
        Returns:
            health status as HealthStatus object.
    """
    with create_ssh_client(node_name) as ssh_client:
        cmd = f'{STORAGE_NODE_BIN_PATH}/neofs-cli control drop-objects ' \
            f'--endpoint {STORAGE_NODE_PRIVATE_CONTROL_ENDPOINT} ' \
            f'--config {STORAGE_NODE_CONFIG_PATH} -o {cid}/{oid}'
        output = ssh_client.exec_with_confirmation(cmd, [''])
        return output.stdout
