#!/usr/bin/python3.9

"""
    This module contains keywords for tests that check management of storage nodes.
"""

import random
import re
import time
from dataclasses import dataclass
from typing import Optional

from common import MAINNET_BLOCK_TIME, NEOFS_NETMAP_DICT, STORAGE_WALLET_PASS
from data_formatters import get_wallet_public_key
from epoch import tick_epoch
from robot.api import logger
from robot.api.deco import keyword
from service_helper import get_storage_service_helper
from utility import robot_time_to_int

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
def stop_nodes(number: int, nodes: list) -> list:
    """
        The function shuts down the given number of randomly
        selected nodes in docker.
        Args:
           number (int): the number of nodes to shut down
           nodes (list): the list of nodes for possible shut down
        Returns:
            (list): the list of nodes which have been shut down
    """
    helper = get_storage_service_helper()
    nodes_to_stop = random.sample(nodes, number)
    for node in nodes_to_stop:
        helper.stop_node(node)
    return nodes_to_stop


@keyword('Start Nodes')
def start_nodes(nodes: list) -> None:
    """
        The function raises the given nodes.
        Args:
           nodes (list): the list of nodes to raise
        Returns:
            (void)
    """
    helper = get_storage_service_helper()
    for node in nodes:
        helper.start(node)


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
    wallet = endpoint_values['wallet_path']

    return endpoint_num, endpoint_control, wallet


@keyword('Get Locode')
def get_locode():
    endpoint_values = random.choice(list(NEOFS_NETMAP_DICT.values()))
    locode = endpoint_values['UN-LOCODE']
    logger.info(f'Random locode chosen: {locode}')

    return locode


@keyword('Healthcheck for node')
def node_healthcheck(node_name: str) -> HealthStatus:
    """
        The function returns node's health status.
        Args:
            node_name str: node name to use for netmap snapshot operation
        Returns:
            health status as HealthStatus object.
    """
    command = "control healthcheck"
    output = _run_control_command(node_name, command)
    return HealthStatus.from_stdout(output)


@keyword('Set status for node')
def node_set_status(node_name: str, status: str, retries: int = 0) -> None:
    """
        The function sets particular status for given node.
        Args:
            node_name str: node name to use for netmap snapshot operation
            status str: online or offline.
            retries (optional, int): number of retry attempts if it didn't work from the first time
        Returns:
            (void)
    """
    command = f"control set-status --status {status}"
    _run_control_command(node_name, command, retries)


@keyword('Get netmap snapshot')
def get_netmap_snapshot(node_name: Optional[str] = None) -> str:
    """
        The function returns string representation of netmap-snapshot.
        Args:
            node_name str: node name to use for netmap snapshot operation
        Returns:
            string representation of netmap-snapshot
    """
    node_name = node_name or list(NEOFS_NETMAP_DICT)[0]
    command = "control netmap-snapshot"
    return _run_control_command(node_name, command)


@keyword('Shard list for node')
def node_shard_list(node_name: str) -> list[str]:
    """
        The function returns list of shards for particular node.
        Args:
            node_name str: node name to use for netmap snapshot operation
        Returns:
            list of shards.
    """
    command = "control shards list"
    output = _run_control_command(node_name, command)
    return re.findall(r'Shard (.*):', output)


@keyword('Shard list for node')
def node_shard_set_mode(node_name: str, shard: str, mode: str) -> str:
    """
        The function sets mode for node's particular shard.
        Args:
            node_name str: node name to use for netmap snapshot operation
        Returns:
            health status as HealthStatus object.
    """
    command = f"control shards set-mode  --id {shard} --mode {mode}"
    return _run_control_command(node_name, command)


@keyword('Drop object from node {node_name}')
def drop_object(node_name: str, cid: str, oid: str) -> str:
    """
        The function drops object from particular node.
        Args:
            node_name str: node name to use for netmap snapshot operation
        Returns:
            health status as HealthStatus object.
    """
    command = f"control drop-objects  -o {cid}/{oid}"
    return _run_control_command(node_name, command)


@keyword('Delete data of node {node_name}')
def delete_node_data(node_name: str) -> None:
    helper = get_storage_service_helper()
    helper.delete_node_data(node_name)
    time.sleep(robot_time_to_int(MAINNET_BLOCK_TIME))


@keyword('Exclude node {node_to_include} from network map')
def exclude_node_from_network_map(node_to_exclude, alive_node):
    node_wallet_path = NEOFS_NETMAP_DICT[node_to_exclude]['wallet_path']
    node_netmap_key = get_wallet_public_key(
        node_wallet_path,
        STORAGE_WALLET_PASS,
        format="base58"
    )

    node_set_status(node_to_exclude, status='offline')

    time.sleep(robot_time_to_int(MAINNET_BLOCK_TIME))
    tick_epoch()

    snapshot = get_netmap_snapshot(node_name=alive_node)
    assert node_netmap_key not in snapshot, f'Expected node with key {node_netmap_key} not in network map'


@keyword('Include node {node_to_include} into network map')
def include_node_to_network_map(node_to_include: str, alive_node: str) -> None:
    node_set_status(node_to_include, status='online')

    time.sleep(robot_time_to_int(MAINNET_BLOCK_TIME))
    tick_epoch()

    check_node_in_map(node_to_include, alive_node)


@keyword('Check node {node_name} in network map')
def check_node_in_map(node_name: str, alive_node: str = None):
    alive_node = alive_node or node_name
    node_wallet_path = NEOFS_NETMAP_DICT[node_name]['wallet_path']
    node_netmap_key = get_wallet_public_key(
        node_wallet_path,
        STORAGE_WALLET_PASS,
        format="base58"
    )

    logger.info(f'Node {node_name} netmap key: {node_netmap_key}')

    snapshot = get_netmap_snapshot(node_name=alive_node)
    assert node_netmap_key in snapshot, f'Expected node with key {node_netmap_key} in network map'


def _run_control_command(node_name: str, command: str, retries: int = 0) -> str:
    helper = get_storage_service_helper()
    for attempt in range(1 + retries):  # original attempt + specified retries
        try:
            return helper.run_control_command(node_name, command)
        except AssertionError as err:
            if attempt < retries:
                logger.warn(f'Command {command} failed with error {err} and will be retried')
                continue
            raise AssertionError(f'Command {command} failed with error {err}') from err
