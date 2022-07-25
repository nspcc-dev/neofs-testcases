#!/usr/bin/python3.9

"""
    This module contains keywords for management test stand
    nodes. It assumes that nodes are docker containers.
"""

import random
import re
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional
from cli_helpers import _cmd_run

import docker
from common import (NEOFS_CLI_EXEC, NEOFS_NETMAP_DICT, STORAGE_CONTROL_ENDPOINT_PRIVATE,
                    STORAGE_NODE_BIN_PATH, STORAGE_NODE_SSH_PASSWORD,
                    STORAGE_NODE_SSH_PRIVATE_KEY_PATH, STORAGE_NODE_SSH_USER, WALLET_CONFIG)
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

    # We use rpc endpoint to determine host address, because control endpoint
    # (if it is private) will be a local address on the host machine
    node_config = NEOFS_NETMAP_DICT.get(node_name)
    host = node_config.get('rpc').split(':')[0]
    ssh_client = HostClient(
        host,
        login=STORAGE_NODE_SSH_USER,
        password=STORAGE_NODE_SSH_PASSWORD,
        private_key_path=STORAGE_NODE_SSH_PRIVATE_KEY_PATH,
    )

    try:
        yield ssh_client
    finally:
        ssh_client.drop()


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
    wallet = endpoint_values['wallet_path']

    return endpoint_num, endpoint_control, wallet


@keyword('Get Locode')
def get_locode():
    endpoint_values = random.choice(list(NEOFS_NETMAP_DICT.values()))
    locode = endpoint_values['UN-LOCODE']
    logger.info(f'Random locode chosen: {locode}')

    return locode


@keyword('Stop Nodes Remote')
def stop_nodes_remote(number: int, nodes: list) -> list:
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
    command = "control healthcheck"
    output = run_control_command(node_name, command)
    return HealthStatus.from_stdout(output)


@keyword('Set status for node')
def node_set_status(node_name: str, status: str) -> None:
    """
        The function sets particular status for given node.
        Args:
            node_name str: node name to use for netmap snapshot operation
            status str: online or offline.
        Returns:
            (void)
    """
    command = f"control set-status --status {status}"
    run_control_command(node_name, command)


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
    return run_control_command(node_name, command)


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
    output = run_control_command(node_name, command)
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
    return run_control_command(node_name, command)


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
    return run_control_command(node_name, command)


def run_control_command(node_name: str, command: str) -> str:
    control_endpoint = NEOFS_NETMAP_DICT[node_name]["control"]
    wallet_path = NEOFS_NETMAP_DICT[node_name]["wallet_path"]

    if not STORAGE_CONTROL_ENDPOINT_PRIVATE:
        cmd = (
            f'{NEOFS_CLI_EXEC} {command} --endpoint {control_endpoint} '
            f'--wallet {wallet_path} --config {WALLET_CONFIG}'
        )
        output = _cmd_run(cmd)
        return output

    # Private control endpoint is accessible only from the host where storage node is running
    # So, we connect to storage node host and run CLI command from there
    with create_ssh_client(node_name) as ssh_client:
        # Copy wallet content on storage node host
        with open(wallet_path, "r") as file:
            wallet = file.read()
        remote_wallet_path = "/tmp/{node_name}-wallet.json"
        ssh_client.exec_with_confirmation(f"echo '{wallet}' > {remote_wallet_path}", [""])

        # Put config on storage node host
        remote_config_path = "/tmp/{node_name}-config.yaml"
        remote_config = 'password: ""'
        ssh_client.exec_with_confirmation(f"echo '{remote_config}' > {remote_config_path}", [""])

        # Execute command
        cmd = (
            f'{STORAGE_NODE_BIN_PATH}/neofs-cli {command} --endpoint {control_endpoint} '
            f'--wallet {remote_wallet_path} --config {remote_config_path}'
        )
        output = ssh_client.exec_with_confirmation(cmd, [""])
        return output.stdout
