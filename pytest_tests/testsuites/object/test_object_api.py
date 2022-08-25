import logging
from time import sleep

import allure
import pytest
from common import COMPLEX_OBJ_SIZE, SIMPLE_OBJ_SIZE
from container import create_container
from epoch import get_epoch, tick_epoch
from grpc_responses import OBJECT_ALREADY_REMOVED, OBJECT_NOT_FOUND, error_matches_status
from python_keywords.neofs_verbs import (delete_object, get_object, get_range, get_range_hash, head_object, put_object,
                                         search_object)
from python_keywords.storage_policy import get_simple_object_copies
from python_keywords.utility_keywords import generate_file, get_file_hash
from tombstone import verify_head_tombstone
from utility import get_file_content, wait_for_gc_pass_on_storage_nodes

logger = logging.getLogger('NeoLogger')

CLEANUP_TIMEOUT = 10


@allure.title('Test native object API')
@pytest.mark.sanity
@pytest.mark.grpc_api
@pytest.mark.parametrize('object_size', [SIMPLE_OBJ_SIZE, COMPLEX_OBJ_SIZE], ids=['simple object', 'complex object'])
def test_object_api(prepare_wallet_and_deposit, request, object_size):
    """
    Test common gRPC API for object (put/get/head/get_range_hash/get_range/search/delete).
    """
    wallet = prepare_wallet_and_deposit
    cid = create_container(wallet)
    wallet_cid = {'wallet': wallet, 'cid': cid}
    file_usr_header = {'key1': 1, 'key2': 'abc', 'common_key': 'common_value'}
    file_usr_header_oth = {'key1': 2, 'common_key': 'common_value'}
    common_header = {'common_key': 'common_value'}
    range_len = 10
    range_cut = f'0:{range_len}'
    oids = []

    allure.dynamic.title(f'Test native object API for {request.node.callspec.id}')
    file_path = generate_file(object_size)
    file_hash = get_file_hash(file_path)

    search_object(**wallet_cid, expected_objects_list=oids)

    with allure.step('Put objects'):
        oids.append(put_object(wallet=wallet, path=file_path, cid=cid))
        oids.append(put_object(wallet=wallet, path=file_path, cid=cid, attributes=file_usr_header))
        oids.append(put_object(wallet=wallet, path=file_path, cid=cid, attributes=file_usr_header_oth))

    with allure.step('Validate storage policy for objects'):
        for oid_to_check in oids:
            assert get_simple_object_copies(wallet=wallet, cid=cid, oid=oid_to_check) == 2, 'Expected 2 copies'

    with allure.step('Get objects and compare hashes'):
        for oid_to_check in oids:
            got_file_path = get_object(wallet=wallet, cid=cid, oid=oid_to_check)
            got_file_hash = get_file_hash(got_file_path)
            assert file_hash == got_file_hash

    with allure.step('Get range/range hash'):
        range_hash = get_range_hash(**wallet_cid, oid=oids[0], bearer_token='', range_cut=range_cut)
        assert get_file_hash(file_path, range_len) == range_hash, \
            f'Expected range hash to match {range_cut} slice of file payload'

        range_hash = get_range_hash(**wallet_cid, oid=oids[1], bearer_token='', range_cut=range_cut)
        assert get_file_hash(file_path, range_len) == range_hash, \
            f'Expected range hash to match {range_cut} slice of file payload'

        _, range_content = get_range(**wallet_cid, oid=oids[1], bearer='', range_cut=range_cut)
        assert get_file_content(file_path, content_len=range_len, mode='rb') == range_content, \
            f'Expected range content to match {range_cut} slice of file payload'

    with allure.step('Search objects'):
        search_object(**wallet_cid, expected_objects_list=oids)
        search_object(**wallet_cid, filters=file_usr_header, expected_objects_list=oids[1:2])
        search_object(**wallet_cid, filters=file_usr_header_oth, expected_objects_list=oids[2:3])
        search_object(**wallet_cid, filters=common_header, expected_objects_list=oids[1:3])

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
        get_object_and_check_error(**wallet_cid, oid=oids[0], error_pattern=OBJECT_ALREADY_REMOVED)
        get_object_and_check_error(**wallet_cid, oid=oids[1], error_pattern=OBJECT_ALREADY_REMOVED)


@allure.title('Test object life time')
@pytest.mark.sanity
@pytest.mark.grpc_api
@pytest.mark.parametrize('object_size', [SIMPLE_OBJ_SIZE, COMPLEX_OBJ_SIZE], ids=['simple object', 'complex object'])
def test_object_api_lifetime(prepare_wallet_and_deposit, request, object_size):
    """
    Test object deleted after expiration epoch.
    """
    wallet = prepare_wallet_and_deposit
    cid = create_container(wallet)

    allure.dynamic.title(f'Test object life time for {request.node.callspec.id}')

    file_path = generate_file(object_size)
    file_hash = get_file_hash(file_path)
    epoch = get_epoch()

    oid = put_object(wallet, file_path, cid, expire_at=epoch + 1)
    got_file = get_object(wallet, cid, oid)
    assert get_file_hash(got_file) == file_hash

    with allure.step('Tick two epochs'):
        for _ in range(2):
            tick_epoch()

    # Wait for GC, because object with expiration is counted as alive until GC removes it
    wait_for_gc_pass_on_storage_nodes()

    with allure.step('Check object deleted because it expires-on epoch'):
        with pytest.raises(Exception, match=OBJECT_NOT_FOUND):
            get_object(wallet, cid, oid)


def get_object_and_check_error(wallet: str, cid: str, oid: str, error_pattern: str) -> None:
    try:
        get_object(wallet=wallet, cid=cid, oid=oid)
        raise AssertionError(f'Expected object {oid} removed, but it is not')
    except Exception as err:
        logger.info(f'Error is {err}')
        assert error_matches_status(err, error_pattern), f'Expected {err} to match {error_pattern}'


def check_header_is_presented(head_info: dict, object_header: dict):
    for key_to_check, val_to_check in object_header.items():
        assert key_to_check in head_info['header']['attributes'], f'Key {key_to_check} is found in {head_object}'
        assert head_info['header']['attributes'].get(key_to_check) == str(
            val_to_check), f'Value {val_to_check} is equal'
