import logging

import allure
import pytest

from common import (STORAGE_NODE_SSH_PRIVATE_KEY_PATH, STORAGE_NODE_SSH_USER,
                    STORAGE_NODE_SSH_PASSWORD)
from python_keywords.container import create_container
from python_keywords.neofs_verbs import get_object, put_object
from python_keywords.utility_keywords import generate_file, get_file_hash
from sbercloud_helper import SberCloud, SberCloudConfig
from ssh_helper import HostClient, HostIsNotAvailable
from wellknown_acl import PUBLIC_ACL
from .failover_utils import wait_all_storage_node_returned, wait_object_replication_on_nodes

logger = logging.getLogger('NeoLogger')
stopped_hosts = []


@pytest.fixture(scope='session')
def sbercloud_client():
    with allure.step('Connect to SberCloud'):
        try:
            config = SberCloudConfig.from_env()
            yield SberCloud(config)
        except Exception as err:
            pytest.fail(f'SberCloud infrastructure not available. Error\n{err}')


@pytest.fixture(autouse=True)
@allure.step('Return all storage nodes')
def return_all_storage_nodes_fixture(sbercloud_client):
    yield
    return_all_storage_nodes(sbercloud_client)


def panic_reboot_host(ip: str = None):
    ssh = HostClient(ip=ip, login=STORAGE_NODE_SSH_USER,
                     password=STORAGE_NODE_SSH_PASSWORD,
                     private_key_path=STORAGE_NODE_SSH_PRIVATE_KEY_PATH)
    ssh.exec('sudo sh -c "echo 1 > /proc/sys/kernel/sysrq"')
    with pytest.raises(HostIsNotAvailable):
        ssh.exec('sudo sh -c "echo b > /proc/sysrq-trigger"', timeout=1)


def return_all_storage_nodes(sbercloud_client: SberCloud) -> None:
    for host in list(stopped_hosts):
        with allure.step(f'Start storage node {host}'):
            sbercloud_client.start_node(node_ip=host.split(':')[-2])
        stopped_hosts.remove(host)

    wait_all_storage_node_returned()


@allure.title('Lost and return nodes')
@pytest.mark.parametrize('hard_reboot', [True, False])
@pytest.mark.failover
def test_lost_storage_node(prepare_wallet_and_deposit, sbercloud_client: SberCloud,
                           cloud_infrastructure_check, hard_reboot: bool):
    wallet = prepare_wallet_and_deposit
    placement_rule = 'REP 2 IN X CBF 2 SELECT 2 FROM * AS X'
    source_file_path = generate_file()
    cid = create_container(wallet, rule=placement_rule, basic_acl=PUBLIC_ACL)
    oid = put_object(wallet, source_file_path, cid)
    nodes = wait_object_replication_on_nodes(wallet, cid, oid, 2)

    new_nodes = []
    for node in nodes:
        stopped_hosts.append(node)
        with allure.step(f'Stop storage node {node}'):
            sbercloud_client.stop_node(node_ip=node.split(':')[-2], hard=hard_reboot)
        new_nodes = wait_object_replication_on_nodes(wallet, cid, oid, 2, excluded_nodes=[node])

    assert not [node for node in nodes if node in new_nodes]
    got_file_path = get_object(wallet, cid, oid, endpoint=new_nodes[0])
    assert get_file_hash(source_file_path) == get_file_hash(got_file_path)

    with allure.step(f'Return storage nodes'):
        return_all_storage_nodes(sbercloud_client)

    new_nodes = wait_object_replication_on_nodes(wallet, cid, oid, 2)

    got_file_path = get_object(wallet, cid, oid, endpoint=new_nodes[0])
    assert get_file_hash(source_file_path) == get_file_hash(got_file_path)


@allure.title('Panic storage node(s)')
@pytest.mark.parametrize('sequence', [True, False])
@pytest.mark.failover_panic
@pytest.mark.failover
def test_panic_storage_node(prepare_wallet_and_deposit, cloud_infrastructure_check,
                            sequence: bool):
    wallet = prepare_wallet_and_deposit
    placement_rule = 'REP 2 IN X CBF 2 SELECT 2 FROM * AS X'
    source_file_path = generate_file()
    cid = create_container(wallet, rule=placement_rule, basic_acl=PUBLIC_ACL)
    oid = put_object(wallet, source_file_path, cid)

    nodes = wait_object_replication_on_nodes(wallet, cid, oid, 2)
    allure.attach('\n'.join(nodes), 'Current nodes with object', allure.attachment_type.TEXT)
    for node in nodes:
        with allure.step(f'Hard reboot host {node} via magic SysRq option'):
            panic_reboot_host(ip=node.split(':')[-2])
            if sequence:
                try:
                    new_nodes = wait_object_replication_on_nodes(wallet, cid, oid, 2, excluded_nodes=[node])
                except AssertionError:
                    new_nodes = wait_object_replication_on_nodes(wallet, cid, oid, 2)

                allure.attach('\n'.join(new_nodes), f'Nodes with object after {node} fail',
                              allure.attachment_type.TEXT)

    if not sequence:
        new_nodes = wait_object_replication_on_nodes(wallet, cid, oid, 2)
        allure.attach('\n'.join(new_nodes), 'Nodes with object after nodes fail', allure.attachment_type.TEXT)

    got_file_path = get_object(wallet, cid, oid, endpoint=new_nodes[0])
    assert get_file_hash(source_file_path) == get_file_hash(got_file_path)
