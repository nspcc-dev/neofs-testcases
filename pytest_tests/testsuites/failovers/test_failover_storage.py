import logging

import allure
import pytest
from common import (
    STORAGE_NODE_SSH_PASSWORD,
    STORAGE_NODE_SSH_PRIVATE_KEY_PATH,
    STORAGE_NODE_SSH_USER,
)
from failover_utils import wait_all_storage_node_returned, wait_object_replication_on_nodes
from file_helper import generate_file, get_file_hash
from neofs_testlib.hosting import Hosting
from python_keywords.container import create_container
from python_keywords.neofs_verbs import get_object, put_object
from ssh_helper import HostClient
from wellknown_acl import PUBLIC_ACL

logger = logging.getLogger("NeoLogger")
stopped_hosts = []


@pytest.fixture(autouse=True)
@allure.step("Return all storage nodes")
def return_all_storage_nodes_fixture(hosting: Hosting):
    yield
    return_all_storage_nodes(hosting)


def panic_reboot_host(ip: str = None):
    ssh = HostClient(
        ip=ip,
        login=STORAGE_NODE_SSH_USER,
        password=STORAGE_NODE_SSH_PASSWORD,
        private_key_path=STORAGE_NODE_SSH_PRIVATE_KEY_PATH,
    )
    ssh.exec('sudo sh -c "echo 1 > /proc/sys/kernel/sysrq"')
    ssh_stdin, _, _ = ssh.ssh_client.exec_command(
        'sudo sh -c "echo b > /proc/sysrq-trigger"', timeout=1
    )
    ssh_stdin.close()


def return_all_storage_nodes(hosting: Hosting) -> None:
    for host_address in list(stopped_hosts):
        with allure.step(f"Start host {host_address}"):
            host = hosting.get_host_by_address(host_address)
            host.start_host()
        stopped_hosts.remove(host_address)

    wait_all_storage_node_returned(hosting)


@allure.title("Lost and returned nodes")
@pytest.mark.parametrize("hard_reboot", [True, False])
@pytest.mark.failover
def test_lost_storage_node(
    prepare_wallet_and_deposit,
    client_shell,
    hosting: Hosting,
    hard_reboot: bool,
):
    wallet = prepare_wallet_and_deposit
    placement_rule = "REP 2 IN X CBF 2 SELECT 2 FROM * AS X"
    source_file_path = generate_file()
    cid = create_container(wallet, shell=client_shell, rule=placement_rule, basic_acl=PUBLIC_ACL)
    oid = put_object(wallet, source_file_path, cid, shell=client_shell)
    nodes = wait_object_replication_on_nodes(wallet, cid, oid, 2, shell=client_shell)

    new_nodes = []
    for node in nodes:
        host = hosting.get_host_by_service(node)
        stopped_hosts.append(host.config.address)
        with allure.step(f"Stop storage node {node}"):
            host.stop_host("hard" if hard_reboot else "soft")
        new_nodes = wait_object_replication_on_nodes(wallet, cid, oid, 2, shell=client_shell, excluded_nodes=[node])

    assert not [node for node in nodes if node in new_nodes]
    got_file_path = get_object(wallet, cid, oid, shell=client_shell, endpoint=new_nodes[0])
    assert get_file_hash(source_file_path) == get_file_hash(got_file_path)

    with allure.step(f"Return storage nodes"):
        return_all_storage_nodes(hosting)

    new_nodes = wait_object_replication_on_nodes(wallet, cid, oid, 2, shell=client_shell)

    got_file_path = get_object(wallet, cid, oid, shell=client_shell, endpoint=new_nodes[0])
    assert get_file_hash(source_file_path) == get_file_hash(got_file_path)


@allure.title("Panic storage node(s)")
@pytest.mark.parametrize("sequence", [True, False])
@pytest.mark.failover_panic
@pytest.mark.failover
def test_panic_storage_node(
    prepare_wallet_and_deposit, client_shell, cloud_infrastructure_check, sequence: bool
):
    wallet = prepare_wallet_and_deposit
    placement_rule = "REP 2 IN X CBF 2 SELECT 2 FROM * AS X"
    source_file_path = generate_file()
    cid = create_container(wallet, shell=client_shell, rule=placement_rule, basic_acl=PUBLIC_ACL)
    oid = put_object(wallet, source_file_path, cid, shell=client_shell)

    nodes = wait_object_replication_on_nodes(wallet, cid, oid, 2, shell=client_shell)
    new_nodes: list[str] = []
    allure.attach("\n".join(nodes), "Current nodes with object", allure.attachment_type.TEXT)
    for node in nodes:
        with allure.step(f"Hard reboot host {node} via magic SysRq option"):
            panic_reboot_host(ip=node.split(":")[-2])
            if sequence:
                try:
                    new_nodes = wait_object_replication_on_nodes(
                        wallet, cid, oid, 2, shell=client_shell, excluded_nodes=[node]
                    )
                except AssertionError:
                    new_nodes = wait_object_replication_on_nodes(
                        wallet, cid, oid, 2, shell=client_shell
                    )

                allure.attach(
                    "\n".join(new_nodes),
                    f"Nodes with object after {node} fail",
                    allure.attachment_type.TEXT,
                )

    if not sequence:
        new_nodes = wait_object_replication_on_nodes(wallet, cid, oid, 2, shell=client_shell)
        allure.attach(
            "\n".join(new_nodes), "Nodes with object after nodes fail", allure.attachment_type.TEXT
        )

    got_file_path = get_object(wallet, cid, oid, shell=client_shell, endpoint=new_nodes[0])
    assert get_file_hash(source_file_path) == get_file_hash(got_file_path)
