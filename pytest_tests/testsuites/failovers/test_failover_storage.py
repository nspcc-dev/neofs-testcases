import logging
import os
from time import sleep

import allure
import pytest
from common import STORAGE_NODE_PWD, STORAGE_NODE_USER
from python_keywords.container import create_container
from python_keywords.neofs_verbs import get_object, put_object
from python_keywords.utility_keywords import get_file_hash
from sbercloud_helper import SberCloud
from ssh_helper import HostClient, HostIsNotAvailable
from storage_policy import get_nodes_with_object
from wellknown_acl import PUBLIC_ACL

logger = logging.getLogger('NeoLogger')
stopped_hosts = []


@pytest.fixture(scope='session')
def sbercloud_client():
    yield SberCloud(f'{os.getcwd()}/configuration/sbercloud.yaml')


@pytest.fixture(scope='session', autouse=True)
def return_all_storage_nodes_fixture(sbercloud_client):
    yield
    return_all_storage_nodes(sbercloud_client)


@allure.title('Hard reboot host via magic SysRq option')
def panic_reboot_host(ip: str = None):
    ssh = HostClient(ip=ip, login=STORAGE_NODE_USER, password=STORAGE_NODE_PWD)
    ssh.create_connection(attempts=1)
    ssh.exec('echo 1 > /proc/sys/kernel/sysrq')
    with pytest.raises(HostIsNotAvailable):
        ssh.exec('echo b > /proc/sysrq-trigger', timeout=1)


def return_all_storage_nodes(sbercloud_client: SberCloud):
    for host in stopped_hosts:
        sbercloud_client.start_node(node_ip=host.split(':')[-2])
        stopped_hosts.remove(host)


def wait_object_replication(wallet, cid, oid, expected_copies: int) -> [str]:
    sleep_interval, attempts = 10, 12
    nodes = []
    for __attempt in range(attempts):
        nodes = get_nodes_with_object(wallet, cid, oid)
        if len(nodes) == expected_copies:
            return nodes
        sleep(sleep_interval)
    raise AssertionError(f'Expected {expected_copies} copies of object, but found {len(nodes)} ')


@allure.title('Lost and return nodes')
@pytest.mark.parametrize('hard_reboot', [True, False])
def test_lost_storage_node(prepare_wallet_and_deposit, generate_file, sbercloud_client: SberCloud, hard_reboot: bool):
    wallet, _ = prepare_wallet_and_deposit
    placement_rule = 'REP 2 IN X CBF 2 SELECT 2 FROM * AS X'
    cid = create_container(wallet, rule=placement_rule, basic_acl=PUBLIC_ACL)
    oid = put_object(wallet, generate_file, cid)
    nodes = wait_object_replication(wallet, cid, oid, 2)

    new_nodes = []
    for node in nodes:
        with allure.step(f'Stop storage node {node}'):
            sbercloud_client.stop_node(node_ip=node.split(':')[-2], hard=hard_reboot)
        new_nodes = wait_object_replication(wallet, cid, oid, 2)

    assert not [node for node in nodes if node in new_nodes]
    got_file_path = get_object(wallet, cid, oid)
    assert get_file_hash(generate_file) == get_file_hash(got_file_path)

    with allure.step(f'Return storage nodes'):
        return_all_storage_nodes(sbercloud_client)

    wait_object_replication(wallet, cid, oid, 2)

    got_file_path = get_object(wallet, cid, oid)
    assert get_file_hash(generate_file) == get_file_hash(got_file_path)


@allure.title('Panic storage node(s)')
@pytest.mark.parametrize('sequence', [True, False])
def test_panic_storage_node(prepare_wallet_and_deposit, generate_file, sequence: bool):
    wallet, _ = prepare_wallet_and_deposit
    placement_rule = 'REP 2 IN X CBF 2 SELECT 2 FROM * AS X'
    cid = create_container(wallet, rule=placement_rule, basic_acl=PUBLIC_ACL)
    oid = put_object(wallet, generate_file, cid)

    with allure.step(f'Return storage nodes'):
        nodes = wait_object_replication(wallet, cid, oid, 2)
        for node in nodes:
            panic_reboot_host(ip=node.split(':')[-2])
            if sequence:
                wait_object_replication(wallet, cid, oid, 2)

        if not sequence:
            wait_object_replication(wallet, cid, oid, 2)

        got_file_path = get_object(wallet, cid, oid)
        assert get_file_hash(generate_file) == get_file_hash(got_file_path)
