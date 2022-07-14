import logging
import os
from time import sleep

import allure
import pytest
from common import NEOFS_NETMAP_DICT
from python_keywords.container import create_container
from python_keywords.neofs_verbs import get_object, put_object
from python_keywords.node_management import node_healthcheck
from python_keywords.utility_keywords import generate_file, get_file_hash
from sbercloud_helper import SberCloud
from ssh_helper import HostClient, HostIsNotAvailable
from storage_policy import get_nodes_with_object
from wellknown_acl import PUBLIC_ACL

logger = logging.getLogger('NeoLogger')
stopped_hosts = []


@pytest.fixture(scope='session')
def free_storage_check():
    if os.getenv('FREE_STORAGE', default='False').lower() not in ('true', '1'):
        pytest.skip('Test only works on SberCloud infrastructure')
    yield


@pytest.fixture(scope='session')
def sbercloud_client():
    with allure.step('Connect to SberCloud'):
        try:
            yield SberCloud(f'{os.getcwd()}/configuration/sbercloud.yaml')
        except Exception:
            pytest.fail('SberCloud infrastructure not available')


@pytest.fixture(autouse=True)
def return_all_storage_nodes_fixture(sbercloud_client):
    yield
    return_all_storage_nodes(sbercloud_client)


def panic_reboot_host(ip: str = None):
    ssh = HostClient(ip=ip, login="root", private_key_path=f"{os.getcwd()}/configuration/id_rsa")
    ssh.exec('echo 1 > /proc/sys/kernel/sysrq')
    with pytest.raises(HostIsNotAvailable):
        ssh.exec('echo b > /proc/sysrq-trigger', timeout=1)


def return_all_storage_nodes(sbercloud_client: SberCloud):
    for host in stopped_hosts:
        with allure.step(f'Start storage node {host}'):
            sbercloud_client.start_node(node_ip=host.split(':')[-2])
    wait_all_storage_node_returned()
    stopped_hosts.clear()


def is_all_storage_node_returned() -> bool:
    with allure.step('Run health check for all storage nodes'):
        for node_name in NEOFS_NETMAP_DICT.keys():
            try:
                health_check = node_healthcheck(node_name)
            except (AssertionError, HostIsNotAvailable, TimeoutError):
                return False
            if health_check.health_status != 'READY' or health_check.network_status != 'ONLINE':
                return False
    return True


def wait_all_storage_node_returned():
    sleep_interval, attempts = 10, 12
    for __attempt in range(attempts):
        if is_all_storage_node_returned():
            return
        sleep(sleep_interval)
    raise AssertionError('Storage node(s) is broken')


def wait_object_replication(wallet, cid, oid, expected_copies: int, excluded_nodes: [str] = None) -> [str]:
    excluded_nodes = excluded_nodes or []
    sleep_interval, attempts = 10, 12
    nodes = []
    for __attempt in range(attempts):
        nodes = [node for node in get_nodes_with_object(wallet, cid, oid) if node not in excluded_nodes]
        if len(nodes) == expected_copies:
            return nodes
        sleep(sleep_interval)
    raise AssertionError(f'Expected {expected_copies} copies of object, but found {len(nodes)} ')


@allure.title('Lost and return nodes')
@pytest.mark.parametrize('hard_reboot', [True, False])
@pytest.mark.failover
def test_lost_storage_node(prepare_wallet_and_deposit, sbercloud_client: SberCloud,
                           free_storage_check, hard_reboot: bool):
    wallet = prepare_wallet_and_deposit
    placement_rule = 'REP 2 IN X CBF 2 SELECT 2 FROM * AS X'
    source_file_path = generate_file()
    cid = create_container(wallet, rule=placement_rule, basic_acl=PUBLIC_ACL)
    oid = put_object(wallet, source_file_path, cid)
    nodes = wait_object_replication(wallet, cid, oid, 2)

    new_nodes = []
    for node in nodes:
        stopped_hosts.append(node)
        with allure.step(f'Stop storage node {node}'):
            sbercloud_client.stop_node(node_ip=node.split(':')[-2], hard=hard_reboot)
        new_nodes = wait_object_replication(wallet, cid, oid, 2, excluded_nodes=[node])

    assert not [node for node in nodes if node in new_nodes]
    got_file_path = get_object(wallet, cid, oid, endpoint=new_nodes[0])
    assert get_file_hash(source_file_path) == get_file_hash(got_file_path)

    with allure.step(f'Return storage nodes'):
        return_all_storage_nodes(sbercloud_client)

    new_nodes = wait_object_replication(wallet, cid, oid, 2)

    got_file_path = get_object(wallet, cid, oid, endpoint=new_nodes[0])
    assert get_file_hash(source_file_path) == get_file_hash(got_file_path)


@allure.title('Panic storage node(s)')
@pytest.mark.parametrize('sequence', [True, False])
@pytest.mark.failover
def test_panic_storage_node(prepare_wallet_and_deposit, free_storage_check, sequence: bool):
    wallet = prepare_wallet_and_deposit
    placement_rule = 'REP 2 IN X CBF 2 SELECT 2 FROM * AS X'
    source_file_path = generate_file()
    cid = create_container(wallet, rule=placement_rule, basic_acl=PUBLIC_ACL)
    oid = put_object(wallet, source_file_path, cid)

    nodes = wait_object_replication(wallet, cid, oid, 2)
    allure.attach('\n'.join(nodes), 'Current nodes with object', allure.attachment_type.TEXT)
    for node in nodes:
        with allure.step(f'Hard reboot host {node} via magic SysRq option'):
            panic_reboot_host(ip=node.split(':')[-2])
            if sequence:
                new_nodes = wait_object_replication(wallet, cid, oid, 2, excluded_nodes=[node])
                allure.attach('\n'.join(new_nodes), f'Nodes with object after {node} fail',
                                   allure.attachment_type.TEXT)

    if not sequence:
        new_nodes = wait_object_replication(wallet, cid, oid, 2, excluded_nodes=nodes)
        allure.attach('\n'.join(new_nodes), 'Nodes with object after nodes fail', allure.attachment_type.TEXT)

    got_file_path = get_object(wallet, cid, oid, endpoint=new_nodes[0])
    assert get_file_hash(source_file_path) == get_file_hash(got_file_path)
