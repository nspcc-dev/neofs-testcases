import logging

import allure
import pytest
from failover_utils import wait_all_storage_node_returned, wait_object_replication_on_nodes
from file_helper import generate_file, get_file_hash
from neofs_testlib.hosting import Host, Hosting
from neofs_testlib.shell import CommandOptions
from python_keywords.container import create_container
from python_keywords.neofs_verbs import get_object, put_object
from wellknown_acl import PUBLIC_ACL

logger = logging.getLogger("NeoLogger")
stopped_hosts = []


@pytest.fixture(autouse=True)
@allure.step("Return all stopped hosts")
def after_run_return_all_stopped_hosts(hosting: Hosting):
    yield
    return_stopped_hosts(hosting)


def panic_reboot_host(host: Host) -> None:
    shell = host.get_shell()
    shell.exec('sudo sh -c "echo 1 > /proc/sys/kernel/sysrq"')

    options = CommandOptions(close_stdin=True, timeout=1, check=False)
    shell.exec('sudo sh -c "echo b > /proc/sysrq-trigger"', options)


def return_stopped_hosts(hosting: Hosting) -> None:
    for host_address in list(stopped_hosts):
        with allure.step(f"Start host {host_address}"):
            host = hosting.get_host_by_address(host_address)
            host.start_host()
        stopped_hosts.remove(host_address)

    wait_all_storage_node_returned(hosting)


@allure.title("Lose and return storage node's host")
@pytest.mark.parametrize("hard_reboot", [True, False])
@pytest.mark.failover
def test_lose_storage_node_host(
    prepare_wallet_and_deposit,
    client_shell,
    hosting: Hosting,
    hard_reboot: bool,
    require_multiple_hosts,
):
    wallet = prepare_wallet_and_deposit
    placement_rule = "REP 2 IN X CBF 2 SELECT 2 FROM * AS X"
    source_file_path = generate_file()
    cid = create_container(wallet, shell=client_shell, rule=placement_rule, basic_acl=PUBLIC_ACL)
    oid = put_object(wallet, source_file_path, cid, shell=client_shell)
    node_endpoints = wait_object_replication_on_nodes(wallet, cid, oid, 2, shell=client_shell)

    for node_endpoint in node_endpoints:
        host_address = node_endpoint.split(":")[0]
        host = hosting.get_host_by_address(host_address)
        stopped_hosts.append(host.config.address)

        with allure.step(f"Stop host {host_address}"):
            host.stop_host("hard" if hard_reboot else "soft")

        new_nodes = wait_object_replication_on_nodes(
            wallet, cid, oid, 2, shell=client_shell, excluded_nodes=[node_endpoint]
        )
    assert all(old_node not in new_nodes for old_node in node_endpoints)

    with allure.step("Check object data is not corrupted"):
        got_file_path = get_object(wallet, cid, oid, endpoint=new_nodes[0], shell=client_shell)
        assert get_file_hash(source_file_path) == get_file_hash(got_file_path)

    with allure.step(f"Return all hosts"):
        return_stopped_hosts(hosting)

    with allure.step("Check object data is not corrupted"):
        new_nodes = wait_object_replication_on_nodes(wallet, cid, oid, 2, shell=client_shell)
        got_file_path = get_object(wallet, cid, oid, shell=client_shell, endpoint=new_nodes[0])
        assert get_file_hash(source_file_path) == get_file_hash(got_file_path)


@allure.title("Panic storage node's host")
@pytest.mark.parametrize("sequence", [True, False])
@pytest.mark.failover_panic
@pytest.mark.failover
def test_panic_storage_node_host(
    prepare_wallet_and_deposit,
    client_shell,
    hosting: Hosting,
    require_multiple_hosts,
    sequence: bool,
):
    wallet = prepare_wallet_and_deposit
    placement_rule = "REP 2 IN X CBF 2 SELECT 2 FROM * AS X"
    source_file_path = generate_file()
    cid = create_container(wallet, shell=client_shell, rule=placement_rule, basic_acl=PUBLIC_ACL)
    oid = put_object(wallet, source_file_path, cid, shell=client_shell)

    node_endpoints = wait_object_replication_on_nodes(wallet, cid, oid, 2, shell=client_shell)
    allure.attach(
        "\n".join(node_endpoints),
        "Current nodes with object",
        allure.attachment_type.TEXT,
    )

    new_nodes: list[str] = []
    for node_endpoint in node_endpoints:
        host_address = node_endpoint.split(":")[0]

        with allure.step(f"Hard reboot host {node_endpoint} via magic SysRq option"):
            host = hosting.get_host_by_address(host_address)
            panic_reboot_host(host)
            if sequence:
                try:
                    new_nodes = wait_object_replication_on_nodes(
                        wallet, cid, oid, 2, shell=client_shell, excluded_nodes=[node_endpoint]
                    )
                except AssertionError:
                    new_nodes = wait_object_replication_on_nodes(
                        wallet, cid, oid, 2, shell=client_shell
                    )

                allure.attach(
                    "\n".join(new_nodes),
                    f"Nodes with object after {node_endpoint} fail",
                    allure.attachment_type.TEXT,
                )

    if not sequence:
        new_nodes = wait_object_replication_on_nodes(wallet, cid, oid, 2, shell=client_shell)
        allure.attach(
            "\n".join(new_nodes), "Nodes with object after nodes fail", allure.attachment_type.TEXT
        )

    got_file_path = get_object(wallet, cid, oid, shell=client_shell, endpoint=new_nodes[0])
    assert get_file_hash(source_file_path) == get_file_hash(got_file_path)
