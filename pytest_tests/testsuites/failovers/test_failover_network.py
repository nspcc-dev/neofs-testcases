import logging
from random import choices
from time import sleep

import allure
import pytest
from failover_utils import wait_all_storage_node_returned, wait_object_replication_on_nodes
from file_helper import generate_file, get_file_hash
from iptables_helper import IpTablesHelper
from neofs_testlib.hosting import Hosting
from python_keywords.container import create_container
from python_keywords.neofs_verbs import get_object, put_object
from wellknown_acl import PUBLIC_ACL

logger = logging.getLogger("NeoLogger")
STORAGE_NODE_COMMUNICATION_PORT = "8080"
STORAGE_NODE_COMMUNICATION_PORT_TLS = "8082"
PORTS_TO_BLOCK = [STORAGE_NODE_COMMUNICATION_PORT, STORAGE_NODE_COMMUNICATION_PORT_TLS]
blocked_hosts = []


@pytest.fixture(autouse=True)
@allure.step("Restore network")
def restore_network(hosting: Hosting):
    yield

    not_empty = len(blocked_hosts) != 0
    for host_address in list(blocked_hosts):
        with allure.step(f"Restore network at host {host_address}"):
            host = hosting.get_host_by_address(host_address)
            IpTablesHelper.restore_input_traffic_to_port(host.get_shell(), PORTS_TO_BLOCK)
        blocked_hosts.remove(host)
    if not_empty:
        wait_all_storage_node_returned(hosting)


@allure.title("Block Storage node traffic")
@pytest.mark.failover
@pytest.mark.failover_net
def test_block_storage_node_traffic(
    prepare_wallet_and_deposit, client_shell, require_multiple_hosts, hosting: Hosting
):
    """
    Block storage nodes traffic using iptables and wait for replication for objects.
    """
    wallet = prepare_wallet_and_deposit
    placement_rule = "REP 2 IN X CBF 2 SELECT 2 FROM * AS X"
    wakeup_node_timeout = 10  # timeout to let nodes detect that traffic has blocked
    nodes_to_block_count = 2

    source_file_path = generate_file()
    cid = create_container(wallet, shell=client_shell, rule=placement_rule, basic_acl=PUBLIC_ACL)
    oid = put_object(wallet, source_file_path, cid, shell=client_shell)

    # TODO: we need to refactor wait_object_replication_on_nodes so that it returns
    # storage node names rather than endpoints
    node_endpoints = wait_object_replication_on_nodes(wallet, cid, oid, 2, shell=client_shell)

    logger.info(f"Nodes are {node_endpoints}")
    node_endpoints_to_block = node_endpoints
    if nodes_to_block_count > len(node_endpoints):
        # TODO: the intent of this logic is not clear, need to revisit
        node_endpoints_to_block = choices(node_endpoints, k=2)

    excluded_nodes = []
    for node_endpoint in node_endpoints_to_block:
        host_address = node_endpoint.split(":")[0]
        host = hosting.get_host_by_address(host_address)

        with allure.step(f"Block incoming traffic at host {host_address} on port {PORTS_TO_BLOCK}"):
            blocked_hosts.append(host_address)
            excluded_nodes.append(node_endpoint)
            IpTablesHelper.drop_input_traffic_to_port(host.get_shell(), PORTS_TO_BLOCK)
            sleep(wakeup_node_timeout)

        with allure.step(f"Check object is not stored on node {node_endpoint}"):
            new_nodes = wait_object_replication_on_nodes(
                wallet, cid, oid, 2, shell=client_shell, excluded_nodes=excluded_nodes
            )
            assert node_endpoint not in new_nodes

        with allure.step(f"Check object data is not corrupted"):
            got_file_path = get_object(wallet, cid, oid, endpoint=new_nodes[0], shell=client_shell)
            assert get_file_hash(source_file_path) == get_file_hash(got_file_path)

    for node_endpoint in node_endpoints_to_block:
        host_address = node_endpoint.split(":")[0]
        host = hosting.get_host_by_address(host_address)

        with allure.step(
            f"Unblock incoming traffic at host {host_address} on port {PORTS_TO_BLOCK}"
        ):
            IpTablesHelper.restore_input_traffic_to_port(host.get_shell(), PORTS_TO_BLOCK)
            blocked_hosts.remove(host_address)
            sleep(wakeup_node_timeout)

    with allure.step(f"Check object data is not corrupted"):
        new_nodes = wait_object_replication_on_nodes(wallet, cid, oid, 2, shell=client_shell)

        got_file_path = get_object(wallet, cid, oid, shell=client_shell, endpoint=new_nodes[0])
        assert get_file_hash(source_file_path) == get_file_hash(got_file_path)
