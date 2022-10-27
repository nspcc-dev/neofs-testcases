import logging
import os
import random
from distutils.command.upload import upload
from time import sleep

import allure
import pytest
from common import COMPLEX_OBJ_SIZE, SIMPLE_OBJ_SIZE
from container import create_container
from epoch import get_epoch, tick_epoch
from file_helper import generate_file, get_file_content, get_file_hash
from grpc_responses import OBJECT_ALREADY_REMOVED, OBJECT_NOT_FOUND, error_matches_status
from neofs_testlib.shell import Shell
from python_keywords.neofs_verbs import (
    delete_object,
    get_object,
    get_range,
    get_range_hash,
    head_object,
    put_object,
    search_object,
)
from python_keywords.storage_policy import get_complex_object_copies, get_simple_object_copies
from tombstone import verify_head_tombstone
from utility import wait_for_gc_pass_on_storage_nodes

logger = logging.getLogger("NeoLogger")

CLEANUP_TIMEOUT = 10
COMMON_ATTRIBUTE = {"common_key": "common_value"}
# Will upload object for each attribute set
OBJECT_ATTRIBUTES = [
    None,
    {"key1": 1, "key2": "abc", "common_key": "common_value"},
    {"key1": 2, "common_key": "common_value"},
]

# Config for Range tests
RANGES_COUNT = 4  # by quorters
RANGE_OFFSET = 0
RANGE_LEN = 10
RANGE_CUT = f"{RANGE_OFFSET}:{RANGE_LEN}"
# Used for static ranges found with issues
STATIC_RANGES = []


class storage_object_info:
    def __init__(self) -> None:
        pass


@pytest.fixture()
def generate_ranges(upload_files: list):
    file_size = upload_files[0].size

    range_step = file_size / RANGES_COUNT

    ranges = []

    for i in range(0, RANGES_COUNT):
        ranges.append((int(range_step * i), int(range_step * (i + 1))))

    range_min = range_step // 2
    range_max = int(range_step)
    for i in range(RANGES_COUNT):
        range_lenght = random.randint(range_min, range_max)
        range_start = random.randint(ranges[i][0], ranges[i][1])

        ranges.append((range_start, min(range_start + range_lenght, file_size)))

    ranges.extend(STATIC_RANGES)
    return ranges


@pytest.fixture(
    params=[SIMPLE_OBJ_SIZE, COMPLEX_OBJ_SIZE],
    ids=["simple object", "complex object"],
    # Scope session to upload/delete each files set only once
    scope="session",
)
def upload_files(prepare_wallet_and_deposit, client_shell, container, request):
    wallet = prepare_wallet_and_deposit
    cid = container
    file_path = generate_file(request.param)
    file_hash = get_file_hash(file_path)

    storage_objects = []

    with allure.step("Put objects"):
        for attributes in OBJECT_ATTRIBUTES:
            storage_object = storage_object_info()
            storage_object.size = request.param
            storage_object.cid = cid
            storage_object.wallet = wallet
            storage_object.file_path = file_path
            storage_object.file_hash = file_hash
            storage_object.attributes = attributes
            storage_object.oid = put_object(
                wallet=wallet,
                path=file_path,
                cid=cid,
                shell=client_shell,
                attributes=storage_object.attributes,
            )

            storage_objects.append(storage_object)

    yield storage_objects

    # Teardown after all tests with current param are done
    delete_objects(storage_objects, client_shell)


def delete_objects(storage_objects: list, client_shell: Shell):
    with allure.step("Delete objects"):
        for storage_object in storage_objects:
            storage_object.tombstone = delete_object(
                storage_object.wallet, storage_object.cid, storage_object.oid, client_shell
            )
            verify_head_tombstone(
                wallet_path=storage_object.wallet,
                cid=storage_object.cid,
                oid_ts=storage_object.tombstone,
                oid=storage_object.oid,
                shell=client_shell,
            )

    tick_epoch(shell=client_shell)
    sleep(CLEANUP_TIMEOUT)

    with allure.step("Get objects and check errors"):
        for storage_object in storage_objects:
            get_object_and_check_error(
                storage_object.wallet,
                storage_object.cid,
                storage_object.oid,
                error_pattern=OBJECT_ALREADY_REMOVED,
                shell=client_shell,
            )


@pytest.fixture(scope="session")
def container(prepare_wallet_and_deposit, client_shell):
    wallet = prepare_wallet_and_deposit
    cid = create_container(wallet, shell=client_shell)

    with allure.step("Check container is empty"):
        search_object(wallet, cid, expected_objects_list=[], shell=client_shell)

    return cid


@allure.title("Verify object storage policy by native API")
@pytest.mark.sanity
@pytest.mark.grpc_api
def test_object_storage_policies(client_shell, request, upload_files: list):
    """
    Validate object storage policy
    """
    allure.dynamic.title(
        f"Validate object storage policy by native API for {request.node.callspec.id}"
    )

    with allure.step("Validate storage policy for objects"):
        for storage_object in upload_files:
            if storage_object.size == SIMPLE_OBJ_SIZE:
                copies = get_simple_object_copies(
                    storage_object.wallet,
                    storage_object.cid,
                    storage_object.oid,
                    shell=client_shell,
                )
            else:
                copies = get_complex_object_copies(
                    storage_object.wallet,
                    storage_object.cid,
                    storage_object.oid,
                    shell=client_shell,
                )
            assert copies == 2, "Expected 2 copies"


@allure.title("Validate get object native API")
@pytest.mark.sanity
@pytest.mark.grpc_api
def test_get_object_api(client_shell, request, upload_files):
    """
    Validate get object native API
    """
    allure.dynamic.title(f"Validate get object native API for {request.node.callspec.id}")

    with allure.step("Get objects and compare hashes"):
        for storage_object in upload_files:
            file_path = get_object(
                storage_object.wallet, storage_object.cid, storage_object.oid, client_shell
            )
            file_hash = get_file_hash(file_path)
            assert storage_object.file_hash == file_hash


@allure.title("Validate head object native API")
@pytest.mark.sanity
@pytest.mark.grpc_api
def test_head_object_api(client_shell, request, upload_files):
    """
    Validate head object native API
    """
    allure.dynamic.title(f"Validate head object by native API for {request.node.callspec.id}")

    storage_object_1 = upload_files[0]
    storage_object_2 = upload_files[1]

    with allure.step("Head object and validate"):
        head_object(
            storage_object_1.wallet, storage_object_1.cid, storage_object_1.oid, shell=client_shell
        )
        head_info = head_object(
            storage_object_2.wallet, storage_object_2.cid, storage_object_2.oid, shell=client_shell
        )
        check_header_is_presented(head_info, storage_object_2.attributes)


@allure.title("Validate object search by native API")
@pytest.mark.sanity
@pytest.mark.grpc_api
def test_search_object_api(client_shell, request, upload_files):
    """
    Validate object search by native API
    """
    allure.dynamic.title(f"Validate object search by native API for {request.node.callspec.id}")

    oids = [storage_object.oid for storage_object in upload_files]
    wallet = upload_files[0].wallet
    cid = upload_files[0].cid

    test_table = [
        (OBJECT_ATTRIBUTES[1], oids[1:2]),
        (OBJECT_ATTRIBUTES[2], oids[2:3]),
        (COMMON_ATTRIBUTE, oids[1:3]),
    ]

    with allure.step("Search objects"):
        # Search with no attributes
        result = search_object(wallet, cid, shell=client_shell, expected_objects_list=oids)

        assert sorted(result) == sorted(oids)

        # search by test table
        for filter, expected_oids in test_table:
            result = search_object(
                wallet, cid, shell=client_shell, filters=filter, expected_objects_list=expected_oids
            )
            assert sorted(result) == sorted(expected_oids)


@allure.title("Validate native object API get_range_hash")
@pytest.mark.sanity
@pytest.mark.grpc_api
def test_object_get_range(client_shell, request, upload_files, generate_ranges):
    """
    Validate get_range_hash for object by common gRPC API
    """
    allure.dynamic.title(f"Validate native object API for {request.node.callspec.id}")

    logging.info(f"Ranges used in test {generate_ranges}")

    wallet = upload_files[0].wallet
    cid = upload_files[0].cid
    oids = [storage_object.oid for storage_object in upload_files[:2]]
    file_path = upload_files[0].file_path

    for range_start, range_stop in generate_ranges:
        range_len = range_stop - range_start
        range_cut = f"{range_start}:{range_len}"
        with allure.step(f"Get range hash ({range_cut})"):
            for oid in oids:
                range_hash = get_range_hash(
                    wallet, cid, oid, shell=client_shell, range_cut=range_cut
                )
                assert (
                    get_file_hash(file_path, range_len, range_start) == range_hash
                ), f"Expected range hash to match {range_cut} slice of file payload"

        with allure.step(f"Get range ({range_cut})"):
            _, range_content = get_range(wallet, cid, oid, shell=client_shell, range_cut=range_cut)
            assert (
                get_file_content(file_path, content_len=range_len, mode="rb", offset=range_start)
                == range_content
            ), f"Expected range content to match {range_cut} slice of file payload"


@allure.title("Test object life time")
@pytest.mark.sanity
@pytest.mark.grpc_api
@pytest.mark.parametrize(
    "object_size", [SIMPLE_OBJ_SIZE, COMPLEX_OBJ_SIZE], ids=["simple object", "complex object"]
)
def test_object_api_lifetime(prepare_wallet_and_deposit, client_shell, request, object_size):
    """
    Test object deleted after expiration epoch.
    """
    wallet = prepare_wallet_and_deposit
    cid = create_container(wallet, shell=client_shell)

    allure.dynamic.title(f"Test object life time for {request.node.callspec.id}")

    file_path = generate_file(object_size)
    file_hash = get_file_hash(file_path)
    epoch = get_epoch(shell=client_shell)

    oid = put_object(wallet, file_path, cid, shell=client_shell, expire_at=epoch + 1)
    got_file = get_object(wallet, cid, oid, shell=client_shell)
    assert get_file_hash(got_file) == file_hash

    with allure.step("Tick two epochs"):
        for _ in range(2):
            tick_epoch(shell=client_shell)

    # Wait for GC, because object with expiration is counted as alive until GC removes it
    wait_for_gc_pass_on_storage_nodes()

    with allure.step("Check object deleted because it expires-on epoch"):
        with pytest.raises(Exception, match=OBJECT_NOT_FOUND):
            get_object(wallet, cid, oid, shell=client_shell)


def get_object_and_check_error(
    wallet: str, cid: str, oid: str, error_pattern: str, shell: Shell
) -> None:
    try:
        get_object(wallet=wallet, cid=cid, oid=oid, shell=shell)
        raise AssertionError(f"Expected object {oid} removed, but it is not")
    except Exception as err:
        logger.info(f"Error is {err}")
        assert error_matches_status(err, error_pattern), f"Expected {err} to match {error_pattern}"


def check_header_is_presented(head_info: dict, object_header: dict):
    for key_to_check, val_to_check in object_header.items():
        assert (
            key_to_check in head_info["header"]["attributes"]
        ), f"Key {key_to_check} is found in {head_object}"
        assert head_info["header"]["attributes"].get(key_to_check) == str(
            val_to_check
        ), f"Value {val_to_check} is equal"
