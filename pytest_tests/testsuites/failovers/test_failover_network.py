import logging
from random import choices
from time import sleep

import allure
import pytest
from common import (STORAGE_NODE_SSH_PASSWORD, STORAGE_NODE_SSH_PRIVATE_KEY_PATH,
                    STORAGE_NODE_SSH_USER)
from failover_utils import wait_all_storage_node_returned, wait_object_replication_on_nodes
from iptables_helper import IpTablesHelper
from python_keywords.container import create_container
from python_keywords.neofs_verbs import get_object, put_object
from python_keywords.utility_keywords import generate_file, get_file_hash
from ssh_helper import HostClient
from wellknown_acl import PUBLIC_ACL

logger = logging.getLogger('NeoLogger')
STORAGE_NODE_COMMUNICATION_PORT = '8080'
STORAGE_NODE_COMMUNICATION_PORT_TLS = '8082'
PORTS_TO_BLOCK = [STORAGE_NODE_COMMUNICATION_PORT, STORAGE_NODE_COMMUNICATION_PORT_TLS]
blocked_hosts = []


@pytest.fixture(autouse=True)
@allure.step('Restore network')
def restore_network():
    yield

    not_empty = len(blocked_hosts) != 0
    for host in list(blocked_hosts):
        with allure.step(f'Start storage node {host}'):
            client = HostClient(ip=host, login=STORAGE_NODE_SSH_USER,
                                password=STORAGE_NODE_SSH_PASSWORD,
                                private_key_path=STORAGE_NODE_SSH_PRIVATE_KEY_PATH)
            with client.create_ssh_connection():
                IpTablesHelper.restore_input_traffic_to_port(client, PORTS_TO_BLOCK)
        blocked_hosts.remove(host)
    if not_empty:
        wait_all_storage_node_returned()


@allure.title('Block Storage node traffic')
@pytest.mark.failover
@pytest.mark.failover_net
def test_block_storage_node_traffic(prepare_wallet_and_deposit, cloud_infrastructure_check):
    """
    Block storage nodes traffic using iptables and wait for replication for objects.
    """
    wallet = prepare_wallet_and_deposit
    placement_rule = 'REP 2 IN X CBF 2 SELECT 2 FROM * AS X'
    excluded_nodes = []
    wakeup_node_timeout = 10  # timeout to let nodes detect that traffic has blocked
    nodes_to_block_count = 2

    source_file_path = generate_file()
    cid = create_container(wallet, rule=placement_rule, basic_acl=PUBLIC_ACL)
    oid = put_object(wallet, source_file_path, cid)
    nodes = wait_object_replication_on_nodes(wallet, cid, oid, 2)

    logger.info(f'Nodes are {nodes}')
    random_nodes = [(node, node.split(':')[0]) for node in nodes]
    if nodes_to_block_count > len(nodes):
        random_nodes = [(node, node.split(':')[0]) for node in choices(nodes, k=2)]

    for random_node, random_node_ip in random_nodes:
        client = HostClient(ip=random_node_ip, login=STORAGE_NODE_SSH_USER,
                            password=STORAGE_NODE_SSH_PASSWORD,
                            private_key_path=STORAGE_NODE_SSH_PRIVATE_KEY_PATH)

        with allure.step(f'Block incoming traffic for node {random_node} on port {PORTS_TO_BLOCK}'):
            with client.create_ssh_connection():
                IpTablesHelper.drop_input_traffic_to_port(client, PORTS_TO_BLOCK)
            blocked_hosts.append(random_node_ip)
            excluded_nodes.append(random_node)
            sleep(wakeup_node_timeout)

        new_nodes = wait_object_replication_on_nodes(wallet, cid, oid, 2, excluded_nodes=excluded_nodes)

        assert random_node not in new_nodes

        got_file_path = get_object(wallet, cid, oid, endpoint=new_nodes[0])
        assert get_file_hash(source_file_path) == get_file_hash(got_file_path)

    for random_node, random_node_ip in random_nodes:
        client = HostClient(ip=random_node_ip, login=STORAGE_NODE_SSH_USER,
                            password=STORAGE_NODE_SSH_PASSWORD,
                            private_key_path=STORAGE_NODE_SSH_PRIVATE_KEY_PATH)

        with allure.step(f'Unblock incoming traffic for node {random_node} on port {PORTS_TO_BLOCK}'):
            with client.create_ssh_connection():
                IpTablesHelper.restore_input_traffic_to_port(client, PORTS_TO_BLOCK)
            blocked_hosts.remove(random_node_ip)
        sleep(wakeup_node_timeout)

    new_nodes = wait_object_replication_on_nodes(wallet, cid, oid, 2)

    got_file_path = get_object(wallet, cid, oid, endpoint=new_nodes[0])
    assert get_file_hash(source_file_path) == get_file_hash(got_file_path)
