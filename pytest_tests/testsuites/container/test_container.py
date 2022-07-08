import json
from time import sleep

import allure
import pytest

from contract_keywords import tick_epoch
from python_keywords.container import create_container, get_container, list_containers, delete_container
from utility import placement_policy_from_container


@pytest.mark.parametrize('name', ['', 'test-container'], ids=['No name', 'Set particular name'])
@pytest.mark.sanity
@pytest.mark.container
def test_container_creation(prepare_wallet_and_deposit, name):
    wallet = prepare_wallet_and_deposit
    msg = f'with name {name}' if name else 'without name'
    allure.dynamic.title(f'User can create container {msg}')

    with open(wallet) as fp:
        json_wallet = json.load(fp)

    placement_rule = 'REP 2 IN X CBF 1 SELECT 2 FROM * AS X'
    info_to_check = {'basic ACL: 0x1c8c8ccc (private)',
                     f'owner ID: {json_wallet.get("accounts")[0].get("address")}'}
    if name:
        info_to_check.add(f'attribute: Name={name}')
        name = f' --name {name}'

    cid = create_container(wallet, rule=placement_rule, options=name)
    info_to_check.add(f'container ID: {cid}')

    containers = list_containers(wallet)
    assert cid in containers, f'Expected container {cid} in containers: {containers}'

    get_output = get_container(wallet, cid, flag='')

    with allure.step('Check container has correct information'):
        got_policy = placement_policy_from_container(get_output)
        assert got_policy == placement_rule.replace('\'', ''), \
            f'Expected \n{placement_rule} and got policy \n{got_policy} are the same'

        for info in info_to_check:
            assert info in get_output, f'Expected info {info} in output:\n{get_output}'

    with allure.step('Delete container and check it was deleted'):
        delete_container(wallet, cid)
        tick_epoch()
        wait_for_container_deletion(wallet, cid)


@allure.step('Wait for container deletion')
def wait_for_container_deletion(wallet: str, cid: str):
    attempts, sleep_interval = 10, 5
    for _ in range(attempts):
        try:
            get_container(wallet, cid)
            sleep(sleep_interval)
            continue
        except Exception as err:
            if 'container not found' not in str(err):
                raise AssertionError(f'Expected "container not found" in error, got\n{err}')
            return
    raise AssertionError(f'Expected container deleted during {attempts * sleep_interval} sec.')
