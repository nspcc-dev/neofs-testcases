import logging
import random
import sys

import allure
import pytest
from common import COMPLEX_OBJ_SIZE, SIMPLE_OBJ_SIZE
from container import create_container
from file_helper import generate_file, get_file_content, get_file_hash
from grpc_responses import OUT_OF_RANGE
from neofs_testlib.shell import Shell
from pytest import FixtureRequest
from python_keywords.neofs_verbs import (
    get_netmap_netinfo,
    get_object,
    get_range,
    get_range_hash,
    head_object,
    put_object,
    search_object,
)
from python_keywords.storage_policy import get_complex_object_copies, get_simple_object_copies

from helpers.storage_object_info import StorageObjectInfo
from steps.storage_object import delete_objects

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
RANGES_COUNT = 4  # by quarters
RANGE_MIN_LEN = 10
RANGE_MAX_LEN = 500
# Used for static ranges found with issues
STATIC_RANGES = {
    SIMPLE_OBJ_SIZE: [],
    COMPLEX_OBJ_SIZE: [],
}


def generate_ranges(file_size: int, max_object_size: int) -> list[(int, int)]:
    file_range_step = file_size / RANGES_COUNT

    file_ranges = []
    file_ranges_to_test = []

    for i in range(0, RANGES_COUNT):
        file_ranges.append((int(file_range_step * i), int(file_range_step * (i + 1))))

    # For simple object we can read all file ranges without too much time for testing
    if file_size == SIMPLE_OBJ_SIZE:
        file_ranges_to_test.extend(file_ranges)
    # For complex object we need to fetch multiple child objects from different nodes.
    if file_size == COMPLEX_OBJ_SIZE:
        assert (
            file_size >= RANGE_MAX_LEN + max_object_size
        ), f"Complex object size should be at least {max_object_size + RANGE_MAX_LEN}. Current: {file_size}"
        file_ranges_to_test.append((RANGE_MAX_LEN, RANGE_MAX_LEN + max_object_size))

    # Special cases to read some bytes from start and some bytes from end of object
    file_ranges_to_test.append((0, RANGE_MIN_LEN))
    file_ranges_to_test.append((file_size - RANGE_MIN_LEN, file_size))

    for start, end in file_ranges:
        range_length = random.randint(RANGE_MIN_LEN, RANGE_MAX_LEN)
        range_start = random.randint(start, end)

        file_ranges_to_test.append((range_start, min(range_start + range_length, file_size)))

    file_ranges_to_test.extend(STATIC_RANGES[file_size])

    return file_ranges_to_test


@pytest.fixture(
    params=[SIMPLE_OBJ_SIZE, COMPLEX_OBJ_SIZE],
    ids=["simple object", "complex object"],
    # Scope session to upload/delete each files set only once
    scope="module",
)
def storage_objects(
    prepare_wallet_and_deposit: str, client_shell: Shell, request: FixtureRequest
) -> list[StorageObjectInfo]:
    wallet = prepare_wallet_and_deposit
    # Separate containers for complex/simple objects to avoid side-effects
    cid = create_container(wallet, shell=client_shell)

    file_path = generate_file(request.param)
    file_hash = get_file_hash(file_path)

    storage_objects = []

    with allure.step("Put objects"):
        # We need to upload objects multiple times with different attributes
        for attributes in OBJECT_ATTRIBUTES:
            storage_object_id = put_object(
                wallet=wallet,
                path=file_path,
                cid=cid,
                shell=client_shell,
                attributes=attributes,
            )

            storage_object = StorageObjectInfo(cid, storage_object_id)
            storage_object.size = request.param
            storage_object.wallet_file_path = wallet
            storage_object.file_path = file_path
            storage_object.file_hash = file_hash
            storage_object.attributes = attributes

            storage_objects.append(storage_object)

    yield storage_objects

    # Teardown after all tests done with current param
    delete_objects(storage_objects, client_shell)


@allure.title("Validate object storage policy by native API")
@pytest.mark.sanity
@pytest.mark.grpc_api
def test_object_storage_policies(
    client_shell: Shell, request: FixtureRequest, storage_objects: list[StorageObjectInfo]
):
    """
    Validate object storage policy
    """
    allure.dynamic.title(
        f"Validate object storage policy by native API for {request.node.callspec.id}"
    )

    with allure.step("Validate storage policy for objects"):
        for storage_object in storage_objects:
            if storage_object.size == SIMPLE_OBJ_SIZE:
                copies = get_simple_object_copies(
                    storage_object.wallet_file_path,
                    storage_object.cid,
                    storage_object.oid,
                    shell=client_shell,
                )
            else:
                copies = get_complex_object_copies(
                    storage_object.wallet_file_path,
                    storage_object.cid,
                    storage_object.oid,
                    shell=client_shell,
                )
            assert copies == 2, "Expected 2 copies"


@allure.title("Validate get object native API")
@pytest.mark.sanity
@pytest.mark.grpc_api
def test_get_object_api(
    client_shell: Shell, request: FixtureRequest, storage_objects: list[StorageObjectInfo]
):
    """
    Validate get object native API
    """
    allure.dynamic.title(f"Validate get object native API for {request.node.callspec.id}")

    with allure.step("Get objects and compare hashes"):
        for storage_object in storage_objects:
            file_path = get_object(
                storage_object.wallet_file_path,
                storage_object.cid,
                storage_object.oid,
                client_shell,
            )
            file_hash = get_file_hash(file_path)
            assert storage_object.file_hash == file_hash


@allure.title("Validate head object native API")
@pytest.mark.sanity
@pytest.mark.grpc_api
def test_head_object_api(
    client_shell: Shell, request: FixtureRequest, storage_objects: list[StorageObjectInfo]
):
    """
    Validate head object native API
    """
    allure.dynamic.title(f"Validate head object by native API for {request.node.callspec.id}")

    storage_object_1 = storage_objects[0]
    storage_object_2 = storage_objects[1]

    with allure.step("Head object and validate"):
        head_object(
            storage_object_1.wallet_file_path,
            storage_object_1.cid,
            storage_object_1.oid,
            shell=client_shell,
        )
        head_info = head_object(
            storage_object_2.wallet_file_path,
            storage_object_2.cid,
            storage_object_2.oid,
            shell=client_shell,
        )
        check_header_is_presented(head_info, storage_object_2.attributes)


@allure.title("Validate object search by native API")
@pytest.mark.sanity
@pytest.mark.grpc_api
def test_search_object_api(
    client_shell: Shell, request: FixtureRequest, storage_objects: list[StorageObjectInfo]
):
    """
    Validate object search by native API
    """
    allure.dynamic.title(f"Validate object search by native API for {request.node.callspec.id}")

    oids = [storage_object.oid for storage_object in storage_objects]
    wallet = storage_objects[0].wallet_file_path
    cid = storage_objects[0].cid

    test_table = [
        (OBJECT_ATTRIBUTES[1], oids[1:2]),
        (OBJECT_ATTRIBUTES[2], oids[2:3]),
        (COMMON_ATTRIBUTE, oids[1:3]),
    ]

    with allure.step("Search objects"):
        # Search with no attributes
        result = search_object(
            wallet, cid, shell=client_shell, expected_objects_list=oids, root=True
        )
        assert sorted(oids) == sorted(result)

        # search by test table
        for filter, expected_oids in test_table:
            result = search_object(
                wallet,
                cid,
                shell=client_shell,
                filters=filter,
                expected_objects_list=expected_oids,
                root=True,
            )
            assert sorted(expected_oids) == sorted(result)


@allure.title("Validate object search with removed items")
@pytest.mark.sanity
@pytest.mark.grpc_api
@pytest.mark.parametrize(
    "object_size", [SIMPLE_OBJ_SIZE, COMPLEX_OBJ_SIZE], ids=["simple object", "complex object"]
)
def test_object_search_should_return_tombstone_items(
    prepare_wallet_and_deposit: str, client_shell: Shell, request: FixtureRequest, object_size: int
):
    """
    Validate object search with removed items
    """
    allure.dynamic.title(
        f"Validate object search with removed items for {request.node.callspec.id}"
    )

    wallet = prepare_wallet_and_deposit
    cid = create_container(wallet, shell=client_shell)

    with allure.step("Upload file"):
        file_path = generate_file(object_size)
        file_hash = get_file_hash(file_path)

        storage_object = StorageObjectInfo(
            cid=cid,
            oid=put_object(wallet, file_path, cid, shell=client_shell),
            size=object_size,
            wallet_file_path=wallet,
            file_path=file_path,
            file_hash=file_hash,
        )

    with allure.step("Search object"):
        # Root Search object should return root object oid
        result = search_object(wallet, cid, shell=client_shell, root=True)
        assert result == [storage_object.oid]

    with allure.step("Delete file"):
        delete_objects([storage_object], client_shell)

    with allure.step("Search deleted object with --root"):
        # Root Search object should return nothing
        result = search_object(wallet, cid, shell=client_shell, root=True)
        assert len(result) == 0

    with allure.step("Search deleted object with --phy should return only tombstones"):
        # Physical Search object should return only tombstones
        result = search_object(wallet, cid, shell=client_shell, phy=True)
        assert (
            storage_object.tombstone in result
        ), f"Search result should contain tombstone of removed object"
        assert (
            storage_object.oid not in result
        ), f"Search result should not contain ObjectId of removed object"
        for tombstone_oid in result:
            header = head_object(wallet, cid, tombstone_oid, shell=client_shell)["header"]
            object_type = header["objectType"]
            assert (
                object_type == "TOMBSTONE"
            ), f"Object wasn't deleted properly. Found object {tombstone_oid} with type {object_type}"


@allure.title("Validate native object API get_range_hash")
@pytest.mark.sanity
@pytest.mark.grpc_api
def test_object_get_range_hash(
    client_shell: Shell, request: FixtureRequest, storage_objects: list[StorageObjectInfo]
):
    """
    Validate get_range_hash for object by common gRPC API
    """
    allure.dynamic.title(
        f"Validate native get_range_hash object API for {request.node.callspec.id}"
    )

    wallet = storage_objects[0].wallet_file_path
    cid = storage_objects[0].cid
    oids = [storage_object.oid for storage_object in storage_objects[:2]]
    file_path = storage_objects[0].file_path
    net_info = get_netmap_netinfo(wallet, client_shell)
    max_object_size = net_info["maximum_object_size"]

    file_ranges_to_test = generate_ranges(storage_objects[0].size, max_object_size)
    logging.info(f"Ranges used in test {file_ranges_to_test}")

    for range_start, range_end in file_ranges_to_test:
        range_len = range_end - range_start
        range_cut = f"{range_start}:{range_len}"
        with allure.step(f"Get range hash ({range_cut})"):
            for oid in oids:
                range_hash = get_range_hash(
                    wallet, cid, oid, shell=client_shell, range_cut=range_cut
                )
                assert (
                    get_file_hash(file_path, range_len, range_start) == range_hash
                ), f"Expected range hash to match {range_cut} slice of file payload"


@allure.title("Validate native object API get_range")
@pytest.mark.sanity
@pytest.mark.grpc_api
def test_object_get_range(
    client_shell: Shell, request: FixtureRequest, storage_objects: list[StorageObjectInfo]
):
    """
    Validate get_range for object by common gRPC API
    """
    allure.dynamic.title(f"Validate native get_range object API for {request.node.callspec.id}")

    wallet = storage_objects[0].wallet_file_path
    cid = storage_objects[0].cid
    oids = [storage_object.oid for storage_object in storage_objects[:2]]
    file_path = storage_objects[0].file_path
    net_info = get_netmap_netinfo(wallet, client_shell)
    max_object_size = net_info["maximum_object_size"]

    file_ranges_to_test = generate_ranges(storage_objects[0].size, max_object_size)
    logging.info(f"Ranges used in test {file_ranges_to_test}")

    for range_start, range_end in file_ranges_to_test:
        range_len = range_end - range_start
        range_cut = f"{range_start}:{range_len}"
        with allure.step(f"Get range ({range_cut})"):
            for oid in oids:
                _, range_content = get_range(
                    wallet, cid, oid, shell=client_shell, range_cut=range_cut
                )
                assert (
                    get_file_content(
                        file_path, content_len=range_len, mode="rb", offset=range_start
                    )
                    == range_content
                ), f"Expected range content to match {range_cut} slice of file payload"


@allure.title("Validate native object API get_range negative cases")
@pytest.mark.sanity
@pytest.mark.grpc_api
def test_object_get_range_negatives(
    client_shell: Shell,
    request: FixtureRequest,
    storage_objects: list[StorageObjectInfo],
):
    """
    Validate get_range negative for object by common gRPC API
    """
    allure.dynamic.title(
        f"Validate native get_range negative object API for {request.node.callspec.id}"
    )

    wallet = storage_objects[0].wallet_file_path
    cid = storage_objects[0].cid
    oids = [storage_object.oid for storage_object in storage_objects[:2]]
    file_size = storage_objects[0].size

    assert (
        RANGE_MIN_LEN < file_size
    ), f"Incorrect test setup. File size ({file_size}) is less than RANGE_MIN_LEN ({RANGE_MIN_LEN})"

    file_ranges_to_test = [
        # Offset is bigger than the file size, the length is small.
        (file_size + 1, RANGE_MIN_LEN),
        # Offset is ok, but offset+length is too big.
        (file_size - RANGE_MIN_LEN, RANGE_MIN_LEN * 2),
        # Offset is ok, and length is very-very big (e.g. MaxUint64) so that offset+length is wrapped and still "valid".
        (RANGE_MIN_LEN, sys.maxsize * 2 + 1),
    ]

    for range_start, range_len in file_ranges_to_test:
        range_cut = f"{range_start}:{range_len}"
        with allure.step(f"Get range ({range_cut})"):
            for oid in oids:
                with pytest.raises(Exception, match=OUT_OF_RANGE):
                    get_range(wallet, cid, oid, shell=client_shell, range_cut=range_cut)


@allure.title("Validate native object API get_range_hash negative cases")
@pytest.mark.sanity
@pytest.mark.grpc_api
def test_object_get_range_hash_negatives(
    client_shell: Shell,
    request: FixtureRequest,
    storage_objects: list[StorageObjectInfo],
):
    """
    Validate get_range_hash negative for object by common gRPC API
    """
    allure.dynamic.title(
        f"Validate native get_range_hash negative object API for {request.node.callspec.id}"
    )

    wallet = storage_objects[0].wallet_file_path
    cid = storage_objects[0].cid
    oids = [storage_object.oid for storage_object in storage_objects[:2]]
    file_size = storage_objects[0].size

    assert (
        RANGE_MIN_LEN < file_size
    ), f"Incorrect test setup. File size ({file_size}) is less than RANGE_MIN_LEN ({RANGE_MIN_LEN})"

    file_ranges_to_test = [
        # Offset is bigger than the file size, the length is small.
        (file_size + 1, RANGE_MIN_LEN),
        # Offset is ok, but offset+length is too big.
        (file_size - RANGE_MIN_LEN, RANGE_MIN_LEN * 2),
        # Offset is ok, and length is very-very big (e.g. MaxUint64) so that offset+length is wrapped and still "valid".
        (RANGE_MIN_LEN, sys.maxsize * 2 + 1),
    ]

    for range_start, range_len in file_ranges_to_test:
        range_cut = f"{range_start}:{range_len}"
        with allure.step(f"Get range ({range_cut})"):
            for oid in oids:
                with pytest.raises(Exception, match=OUT_OF_RANGE):
                    get_range_hash(wallet, cid, oid, shell=client_shell, range_cut=range_cut)


def check_header_is_presented(head_info: dict, object_header: dict) -> None:
    for key_to_check, val_to_check in object_header.items():
        assert (
            key_to_check in head_info["header"]["attributes"]
        ), f"Key {key_to_check} is found in {head_object}"
        assert head_info["header"]["attributes"].get(key_to_check) == str(
            val_to_check
        ), f"Value {val_to_check} is equal"
