import logging
from time import sleep

import allure
import pytest
from container import create_container
from epoch import tick_epoch
from tombstone import verify_head_tombstone
from python_keywords.neofs_verbs import (delete_object, get_object, get_range,
                                         get_range_hash, head_object,
                                         put_object, search_object)
from python_keywords.storage_policy import get_simple_object_copies
from python_keywords.utility_keywords import generate_file, get_file_hash

logger = logging.getLogger('NeoLogger')

CLEANUP_TIMEOUT = 10


@allure.title('Test native object API')
@pytest.mark.sanity
@pytest.mark.grpc_api
def test_object_api(prepare_wallet_and_deposit):
    wallet = prepare_wallet_and_deposit
    cid = create_container(wallet)
    wallet_cid = {'wallet': wallet, 'cid': cid}
    file_usr_header = {'key1': 1, 'key2': 'abc'}
    file_usr_header_oth = {'key1': 2}
    range_cut = '0:10'
    oids = []

    file_path = generate_file()
    file_hash = get_file_hash(file_path)

    search_object(**wallet_cid, expected_objects_list=oids)

    with allure.step('Put objects'):
        oids.append(put_object(wallet=wallet, path=file_path, cid=cid))
        oids.append(put_object(wallet=wallet, path=file_path, cid=cid, user_headers=file_usr_header))
        oids.append(put_object(wallet=wallet, path=file_path, cid=cid, user_headers=file_usr_header_oth))

    with allure.step('Validate storage policy for objects'):
        for oid_to_check in oids:
            assert get_simple_object_copies(wallet=wallet, cid=cid, oid=oid_to_check) == 2, 'Expected 2 copies'

    with allure.step('Get objects and compare hashes'):
        for oid_to_check in oids:
            got_file_path = get_object(wallet=wallet, cid=cid, oid=oid_to_check)
            got_file_hash = get_file_hash(got_file_path)
            assert file_hash == got_file_hash

    with allure.step('Get range/range hash'):
        get_range_hash(**wallet_cid, oid=oids[0], bearer_token='', range_cut=range_cut)
        get_range_hash(**wallet_cid, oid=oids[1], bearer_token='', range_cut=range_cut)
        get_range(**wallet_cid, oid=oids[1], bearer='', range_cut=range_cut)

    with allure.step('Search objects'):
        search_object(**wallet_cid, expected_objects_list=oids)
        search_object(**wallet_cid, filters=file_usr_header, expected_objects_list=oids[1:2])
        search_object(**wallet_cid, filters=file_usr_header_oth, expected_objects_list=oids[2:3])

    with allure.step('Head object and validate'):
        head_object(**wallet_cid, oid=oids[0])
        head_info = head_object(**wallet_cid, oid=oids[1])
        check_header_is_presented(head_info, file_usr_header)

    with allure.step('Delete objects'):
        tombstone_s = delete_object(**wallet_cid, oid=oids[0])
        tombstone_h = delete_object(**wallet_cid, oid=oids[1])

    verify_head_tombstone(wallet_path=wallet, cid=cid, oid_ts=tombstone_s, oid=oids[0])
    verify_head_tombstone(wallet_path=wallet, cid=cid, oid_ts=tombstone_h, oid=oids[1])

    tick_epoch()
    sleep(CLEANUP_TIMEOUT)

    with allure.step('Get objects and check errors'):
        get_object_and_check_error(**wallet_cid, oid=oids[0], err_msg='object already removed')
        get_object_and_check_error(**wallet_cid, oid=oids[1], err_msg='object already removed')


def get_object_and_check_error(wallet: str, cid: str, oid: str, err_msg: str):
    try:
        get_object(wallet=wallet, cid=cid, oid=oid)
        raise AssertionError(f'Expected object {oid} removed, but it is not')
    except Exception as err:
        logger.info(f'Error is {err}')
        assert err_msg in str(err), f'Expected message {err_msg} in error: {err}'


def check_header_is_presented(head_info: dict, object_header: dict):
    for key_to_check, val_to_check in object_header.items():
        assert key_to_check in head_info['header']['attributes'], f'Key {key_to_check} is found in {head_object}'
        assert head_info['header']['attributes'].get(key_to_check) == str(
            val_to_check), f'Value {val_to_check} is equal'
