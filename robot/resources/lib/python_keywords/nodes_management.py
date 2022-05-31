#!/usr/bin/python3

"""
    This module contains keywords for management test stand
    nodes. It assumes that nodes are docker containers.
"""

import random

import docker
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
