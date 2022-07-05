#!/usr/bin/python3

"""
    This module contains keywords for management test stand
    nodes. It assumes that nodes are docker containers.
"""

import random

import docker
from common import NEOFS_NETMAP_DICT
from robot.api import logger
from robot.api.deco import keyword

ROBOT_AUTO_KEYWORDS = False


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
