import json
from time import sleep

import allure
import pytest

from epoch import tick_epoch
from grpc_responses import CONTAINER_NOT_FOUND, error_matches_status
from python_keywords.container import (create_container, delete_container, get_container,
                                       list_containers)
from utility import placement_policy_from_container
from wellknown_acl import PRIVATE_ACL_F


@pytest.mark.parametrize('name', ['', 'test-container'], ids=['No name', 'Set particular name'])
@pytest.mark.sanity
@pytest.mark.container
def test_container_creation(prepare_wallet_and_deposit, name):
    scenario_title = f'with name {name}' if name else 'without name'
    allure.dynamic.title(f'User can create container {scenario_title}')

    wallet = prepare_wallet_and_deposit
    with open(wallet) as file:
        json_wallet = json.load(file)

    placement_rule = 'REP 2 IN X CBF 1 SELECT 2 FROM * AS X'
    options = f"--name {name}" if name else ""
    cid = create_container(wallet, rule=placement_rule, options=options)

    containers = list_containers(wallet)
    assert cid in containers, f'Expected container {cid} in containers: {containers}'

    container_info: str = get_container(wallet, cid, flag='')
    container_info = container_info.casefold() # To ignore case when comparing with expected values

    info_to_check = {
        f'basic ACL: {PRIVATE_ACL_F} (private)',
        f'owner ID: {json_wallet.get("accounts")[0].get("address")}',
        f'container ID: {cid}',
    }
    if name:
        info_to_check.add(f'Name={name}')

    with allure.step('Check container has correct information'):
        expected_policy = placement_rule.casefold()
        actual_policy = placement_policy_from_container(container_info)
        assert actual_policy == expected_policy, \
            f'Expected policy\n{expected_policy} but got policy\n{actual_policy}'

        for info in info_to_check:
            expected_info = info.casefold()
            assert expected_info in container_info, \
                f'Expected {expected_info} in container info:\n{container_info}'

    with allure.step('Delete container and check it was deleted'):
        delete_container(wallet, cid)
        tick_epoch()
        wait_for_container_deletion(wallet, cid)


@allure.step('Wait for container deletion')
def wait_for_container_deletion(wallet: str, cid: str) -> None:
    attempts, sleep_interval = 10, 5
    for _ in range(attempts):
        try:
            get_container(wallet, cid)
            sleep(sleep_interval)
            continue
        except Exception as err:
            if error_matches_status(err, CONTAINER_NOT_FOUND):
                return
            raise AssertionError(f'Expected "{CONTAINER_NOT_FOUND}" error, got\n{err}')

    raise AssertionError(f'Container was not deleted within {attempts * sleep_interval} sec')
