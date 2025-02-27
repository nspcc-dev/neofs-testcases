import logging
import os
import random
import sys

import allure
import pytest
from helpers.common import TEST_FILES_DIR, get_assets_dir_path
from helpers.complex_object_actions import (
    get_complex_object_copies,
    get_complex_object_split_ranges,
    get_link_object,
    get_object_chunks,
    get_simple_object_copies,
)
from helpers.container import create_container, delete_container
from helpers.file_helper import generate_file, get_file_content, get_file_hash
from helpers.grpc_responses import (
    INVALID_LENGTH_SPECIFIER,
    INVALID_OFFSET_SPECIFIER,
    INVALID_RANGE_OVERFLOW,
    INVALID_RANGE_ZERO_LENGTH,
    INVALID_SEARCH_QUERY,
    LINK_OBJECT_FOUND,
    OBJECT_ALREADY_REMOVED,
    OBJECT_HEADER_LENGTH_LIMIT,
    OBJECT_NOT_FOUND,
    OUT_OF_RANGE,
)
from helpers.neofs_verbs import (
    NEOFS_API_HEADER_LIMIT,
    delete_object,
    get_object,
    get_object_from_random_node,
    get_range,
    get_range_hash,
    head_object,
    put_object_to_random_node,
    search_object,
)
from helpers.storage_object_info import StorageObjectInfo, delete_objects
from helpers.test_control import expect_not_raises
from helpers.utility import wait_for_gc_pass_on_storage_nodes
from neofs_env.neofs_env_test_base import NeofsEnvTestBase
from neofs_testlib.env.env import NeoFSEnv, NodeWallet
from neofs_testlib.shell import Shell
from pytest import FixtureRequest
from pytest_lazy_fixtures import lf

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
STATIC_RANGES = {}
OBJECT_NUMERIC_VALUES = [-(2**64) - 1, -1, 0, 1, 10, 2**64 + 1]
NUMERIC_VALUE_ATTR_NAME = "numeric_value"


def _id_should_be_in_result(id_: int, result: list[int]):
    assert id_ in result, f"{id_} not in result, while it should be"


def _id_should_not_be_in_result(id_: int, result: list[int]):
    assert id_ not in result, f"{id_} in result, while it should not be"


def generate_ranges(
    storage_object: StorageObjectInfo, max_object_size: int, shell: Shell, neofs_env: NeoFSEnv
) -> list[(int, int)]:
    file_range_step = storage_object.size / RANGES_COUNT

    file_ranges = []
    file_ranges_to_test = []

    for i in range(0, RANGES_COUNT):
        file_ranges.append((int(file_range_step * i), int(file_range_step)))

    # For simple object we can read all file ranges without too much time for testing
    if storage_object.size < max_object_size:
        file_ranges_to_test.extend(file_ranges)
    # For complex object we need to fetch multiple child objects from different nodes.
    else:
        assert storage_object.size >= RANGE_MAX_LEN + max_object_size, (
            f"Complex object size should be at least {max_object_size + RANGE_MAX_LEN}. Current: {storage_object.size}"
        )
        file_ranges_to_test.append((RANGE_MAX_LEN, max_object_size - RANGE_MAX_LEN))
        file_ranges_to_test.extend(get_complex_object_split_ranges(storage_object, shell, neofs_env))

    # Special cases to read some bytes from start and some bytes from end of object
    file_ranges_to_test.append((0, RANGE_MIN_LEN))
    file_ranges_to_test.append((storage_object.size - RANGE_MIN_LEN, RANGE_MIN_LEN))

    for offset, length in file_ranges:
        range_length = random.randint(RANGE_MIN_LEN, RANGE_MAX_LEN)
        range_start = random.randint(offset, offset + length - 1)

        file_ranges_to_test.append((range_start, min(range_length, storage_object.size - range_start)))

    file_ranges_to_test.extend(STATIC_RANGES.get(storage_object.size, []))

    return file_ranges_to_test


@pytest.fixture(
    params=[lf("simple_object_size"), lf("complex_object_size")],
    ids=["simple object", "complex object"],
    # Scope session to upload/delete each files set only once
    scope="function",
)
def storage_objects(
    default_wallet: str, client_shell: Shell, neofs_env: NeoFSEnv, request: FixtureRequest
) -> list[StorageObjectInfo]:
    wallet = default_wallet
    # Separate containers for complex/simple objects to avoid side-effects
    cid = create_container(wallet.path, shell=client_shell, endpoint=neofs_env.sn_rpc)

    file_path = generate_file(request.param)
    file_hash = get_file_hash(file_path)

    storage_objects = []

    with allure.step("Put objects"):
        # We need to upload objects multiple times with different attributes
        for attributes in OBJECT_ATTRIBUTES:
            storage_object_id = put_object_to_random_node(
                wallet=wallet.path,
                path=file_path,
                cid=cid,
                shell=client_shell,
                neofs_env=neofs_env,
                attributes=attributes,
            )

            storage_object = StorageObjectInfo(cid, storage_object_id)
            storage_object.size = request.param
            storage_object.wallet_file_path = wallet.path
            storage_object.file_path = file_path
            storage_object.file_hash = file_hash
            storage_object.attributes = attributes

            storage_objects.append(storage_object)

    yield storage_objects

    # Teardown after all tests done with current param
    with expect_not_raises():
        delete_objects(storage_objects, client_shell, neofs_env)


@pytest.fixture
def container(default_wallet: NodeWallet, client_shell: Shell, neofs_env: NeoFSEnv) -> str:
    cid = create_container(default_wallet.path, shell=client_shell, endpoint=neofs_env.sn_rpc)
    yield cid
    delete_container(default_wallet.path, cid, shell=client_shell, endpoint=neofs_env.sn_rpc)


class TestObjectApi(NeofsEnvTestBase):
    @pytest.mark.sanity
    @allure.title("Validate object storage policy by native API")
    def test_object_storage_policies(
        self, request: FixtureRequest, storage_objects: list[StorageObjectInfo], simple_object_size
    ):
        """
        Validate object storage policy
        """
        allure.dynamic.title(f"Validate object storage policy by native API for {request.node.callspec.id}")

        with allure.step("Validate storage policy for objects"):
            for storage_object in storage_objects:
                if storage_object.size == simple_object_size:
                    copies = get_simple_object_copies(
                        storage_object.wallet_file_path,
                        storage_object.cid,
                        storage_object.oid,
                        shell=self.shell,
                        nodes=self.neofs_env.storage_nodes,
                    )
                else:
                    copies = get_complex_object_copies(
                        storage_object.wallet_file_path,
                        storage_object.cid,
                        storage_object.oid,
                        shell=self.shell,
                        nodes=self.neofs_env.storage_nodes,
                    )
                assert copies == 2, "Expected 2 copies"

    @allure.title("Validate get object native API")
    def test_get_object_api(self, request: FixtureRequest, storage_objects: list[StorageObjectInfo]):
        """
        Validate get object native API
        """
        allure.dynamic.title(f"Validate get object native API for {request.node.callspec.id}")

        with allure.step("Get objects and compare hashes"):
            for storage_object in storage_objects:
                file_path = get_object_from_random_node(
                    storage_object.wallet_file_path,
                    storage_object.cid,
                    storage_object.oid,
                    self.shell,
                    neofs_env=self.neofs_env,
                )
                file_hash = get_file_hash(file_path)
                assert storage_object.file_hash == file_hash

    @allure.title("Validate head object native API")
    def test_head_object_api(self, request: FixtureRequest, storage_objects: list[StorageObjectInfo]):
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
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            head_info = head_object(
                storage_object_2.wallet_file_path,
                storage_object_2.cid,
                storage_object_2.oid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            self.check_header_is_presented(head_info, storage_object_2.attributes)

    @allure.title("Validate object search by native API")
    def test_search_object_api(self, request: FixtureRequest, storage_objects: list[StorageObjectInfo]):
        """
        Validate object search by native API
        """
        allure.dynamic.title(f"Validate object search by native API for {request.node.callspec.id}")

        oids = [storage_object.oid for storage_object in storage_objects]
        wallet = storage_objects[0].wallet_file_path
        cid = storage_objects[0].cid

        def _generate_filters_expressions(attrib_dict: dict[str, str]):
            return [f"{filter_key} EQ {filter_val}" for filter_key, filter_val in attrib_dict.items()]

        test_table = [
            (_generate_filters_expressions(OBJECT_ATTRIBUTES[1]), oids[1:2]),
            (_generate_filters_expressions(OBJECT_ATTRIBUTES[2]), oids[2:3]),
            (_generate_filters_expressions(COMMON_ATTRIBUTE), oids[1:3]),
        ]

        with allure.step("Search objects"):
            # Search with no attributes
            result = search_object(
                wallet,
                cid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                expected_objects_list=oids,
                root=True,
            )
            assert sorted(oids) == sorted(result)

            # search by test table
            for _filter, expected_oids in test_table:
                result = search_object(
                    wallet,
                    cid,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    filters=_filter,
                    expected_objects_list=expected_oids,
                    root=True,
                )
                assert sorted(expected_oids) == sorted(result)

    @pytest.mark.parametrize("operator", ["GT", "GE", "LT", "LE"])
    @pytest.mark.parametrize(
        "object_size",
        [lf("simple_object_size"), lf("complex_object_size")],
        ids=["simple object", "complex object"],
    )
    def test_object_search_with_numeric_queries(
        self, default_wallet: NodeWallet, container: str, object_size: int, operator: str
    ):
        objects = []
        for numeric_value in OBJECT_NUMERIC_VALUES:
            file_path = generate_file(object_size)

            objects.append(
                {
                    NUMERIC_VALUE_ATTR_NAME: numeric_value,
                    "id": put_object_to_random_node(
                        default_wallet.path,
                        file_path,
                        container,
                        shell=self.shell,
                        neofs_env=self.neofs_env,
                        attributes={NUMERIC_VALUE_ATTR_NAME: numeric_value},
                    ),
                }
            )

        for numeric_value in OBJECT_NUMERIC_VALUES:
            result = search_object(
                default_wallet.path,
                container,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                filters=[f"{NUMERIC_VALUE_ATTR_NAME} {operator} {numeric_value}"],
                root=True,
            )

            for obj in objects:
                if operator == "GT":
                    if obj[NUMERIC_VALUE_ATTR_NAME] > numeric_value:
                        _id_should_be_in_result(obj["id"], result)
                    else:
                        _id_should_not_be_in_result(obj["id"], result)
                elif operator == "GE":
                    if obj[NUMERIC_VALUE_ATTR_NAME] >= numeric_value:
                        _id_should_be_in_result(obj["id"], result)
                    else:
                        _id_should_not_be_in_result(obj["id"], result)
                elif operator == "LT":
                    if obj[NUMERIC_VALUE_ATTR_NAME] < numeric_value:
                        _id_should_be_in_result(obj["id"], result)
                    else:
                        _id_should_not_be_in_result(obj["id"], result)
                elif operator == "LE":
                    if obj[NUMERIC_VALUE_ATTR_NAME] <= numeric_value:
                        _id_should_be_in_result(obj["id"], result)
                    else:
                        _id_should_not_be_in_result(obj["id"], result)

    @pytest.mark.parametrize(
        "filters",
        [
            "non_existent_attr GT abc",
            "non_existent_attr GT 99-32",
            "non_existent_attr LT 9.1",
        ],
    )
    def test_object_search_with_numeric_operators_invalid_filters(
        self, default_wallet: NodeWallet, container: str, filters: str
    ):
        with pytest.raises(Exception, match=INVALID_SEARCH_QUERY):
            search_object(
                default_wallet.path,
                container,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                filters=[filters],
                root=True,
            )

    def test_object_search_with_attr_as_number(
        self, default_wallet: NodeWallet, container: str, simple_object_size: int
    ):
        file_path = generate_file(simple_object_size)
        oid = put_object_to_random_node(
            default_wallet.path,
            file_path,
            container,
            shell=self.shell,
            neofs_env=self.neofs_env,
            attributes={100: 200},
        )
        result = search_object(
            default_wallet.path,
            container,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            filters=["100 GE 200"],
            root=True,
        )

        assert oid in result, "Object was not found, while it should be"

    def test_object_search_numeric_with_attr_as_string(
        self, default_wallet: NodeWallet, container: str, simple_object_size: int
    ):
        file_path = generate_file(simple_object_size)
        string_attr = "cool_string_attribute"
        oid = put_object_to_random_node(
            default_wallet.path,
            file_path,
            container,
            shell=self.shell,
            neofs_env=self.neofs_env,
            attributes={string_attr: "xyz"},
        )
        result = search_object(
            default_wallet.path,
            container,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            filters=[f"{string_attr} GT 0"],
            root=True,
        )

        assert oid not in result, "Object was found, while it should not be"

    @allure.title("Validate object search with removed items")
    @pytest.mark.parametrize(
        "object_size",
        [lf("simple_object_size"), lf("complex_object_size")],
        ids=["simple object", "complex object"],
    )
    def test_object_search_should_return_tombstone_items(
        self, default_wallet: NodeWallet, request: FixtureRequest, object_size: int
    ):
        """
        Validate object search with removed items
        """
        allure.dynamic.title(f"Validate object search with removed items for {request.node.callspec.id}")

        wallet = default_wallet
        cid = create_container(wallet.path, self.shell, self.neofs_env.sn_rpc)

        with allure.step("Upload file"):
            file_path = generate_file(object_size)
            file_hash = get_file_hash(file_path)

            storage_object = StorageObjectInfo(
                cid=cid,
                oid=put_object_to_random_node(wallet.path, file_path, cid, self.shell, neofs_env=self.neofs_env),
                size=object_size,
                wallet_file_path=wallet.path,
                file_path=file_path,
                file_hash=file_hash,
            )

        with allure.step("Search object"):
            # Root Search object should return root object oid
            result = search_object(wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc, root=True)
            assert result == [storage_object.oid]

        with allure.step("Delete file"):
            delete_objects([storage_object], self.shell, self.neofs_env)

        with allure.step("Search deleted object with --root"):
            # Root Search object should return nothing
            result = search_object(wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc, root=True)
            assert len(result) == 0

        with allure.step("Search deleted object with --phy should return only tombstones"):
            # Physical Search object should return only tombstones
            result = search_object(wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc, phy=True)
            assert storage_object.tombstone in result, "Search result should contain tombstone of removed object"
            assert storage_object.oid not in result, "Search result should not contain ObjectId of removed object"
            for tombstone_oid in result:
                header = head_object(
                    wallet.path,
                    cid,
                    tombstone_oid,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                )["header"]
                object_type = header["objectType"]
                assert object_type == "TOMBSTONE", (
                    f"Object wasn't deleted properly. Found object {tombstone_oid} with type {object_type}"
                )

    @allure.title("Validate objects search by common prefix")
    def test_search_object_api_common_prefix(self, default_wallet: NodeWallet, simple_object_size: int, container: str):
        FILEPATH_ATTR_NAME = "FilePath"
        NUMBER_OF_OBJECTS = 5
        wallet = default_wallet

        objects = {}
        for _ in range(NUMBER_OF_OBJECTS):
            file_path = generate_file(simple_object_size)

            with allure.step("Put objects"):
                objects[file_path] = put_object_to_random_node(
                    wallet=wallet.path,
                    path=file_path,
                    cid=container,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                    attributes={FILEPATH_ATTR_NAME: file_path},
                )
        all_oids = sorted(objects.values())

        for common_prefix, expected_oids in (
            ("/", all_oids),
            (os.path.join(get_assets_dir_path()), all_oids),
            (os.path.join(get_assets_dir_path(), TEST_FILES_DIR), all_oids),
            (file_path, [objects[file_path]]),
        ):
            with allure.step(f"Search objects by path: {common_prefix}"):
                search_object(
                    wallet.path,
                    container,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    filters=[f"{FILEPATH_ATTR_NAME} COMMON_PREFIX {common_prefix}"],
                    expected_objects_list=expected_oids,
                    root=True,
                    fail_on_assert=True,
                )

        for common_prefix in (f"{file_path}/o123/COMMON_PREFIX", "?", "213"):
            with allure.step(f"Search objects by path: {common_prefix}"):
                with pytest.raises(AssertionError):
                    search_object(
                        wallet.path,
                        container,
                        shell=self.shell,
                        endpoint=self.neofs_env.sn_rpc,
                        filters=[f"{FILEPATH_ATTR_NAME} COMMON_PREFIX {common_prefix}"],
                        expected_objects_list=expected_oids,
                        root=True,
                        fail_on_assert=True,
                    )

    @allure.title("Validate native object API get_range_hash")
    def test_object_get_range_hash(
        self, request: FixtureRequest, storage_objects: list[StorageObjectInfo], max_object_size
    ):
        """
        Validate get_range_hash for object by native gRPC API
        """
        allure.dynamic.title(f"Validate native get_range_hash object API for {request.node.callspec.id}")

        wallet = storage_objects[0].wallet_file_path
        cid = storage_objects[0].cid
        oids = [storage_object.oid for storage_object in storage_objects[:2]]
        file_path = storage_objects[0].file_path

        file_ranges_to_test = generate_ranges(storage_objects[0], max_object_size, self.shell, self.neofs_env)
        logging.info(f"Ranges used in test {file_ranges_to_test}")

        for range_start, range_len in file_ranges_to_test:
            range_cut = f"{range_start}:{range_len}"
            with allure.step(f"Get range hash ({range_cut})"):
                for oid in oids:
                    range_hash = get_range_hash(
                        wallet,
                        cid,
                        oid,
                        shell=self.shell,
                        endpoint=self.neofs_env.sn_rpc,
                        range_cut=range_cut,
                    )
                    assert get_file_hash(file_path, range_len, range_start) == range_hash, (
                        f"Expected range hash to match {range_cut} slice of file payload"
                    )

        if self.neofs_env.get_binary_version(self.neofs_env.neofs_node_path) > "0.44.2":
            with allure.step("Verify zero payload ranges"):
                range_hash = get_range_hash(
                    wallet,
                    cid,
                    oid,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    range_cut="0:0",
                )
                assert get_file_hash(file_path) == range_hash, "Expected range hash to match full file payload"

                with pytest.raises(Exception, match=r".*zero length with non-zero offset.*"):
                    get_range_hash(
                        wallet,
                        cid,
                        oid,
                        shell=self.shell,
                        endpoint=self.neofs_env.sn_rpc,
                        range_cut="5:0",
                    )

    @allure.title("Validate native object API get_range")
    def test_object_get_range(self, request: FixtureRequest, storage_objects: list[StorageObjectInfo], max_object_size):
        """
        Validate get_range for object by native gRPC API
        """
        allure.dynamic.title(f"Validate native get_range object API for {request.node.callspec.id}")

        wallet = storage_objects[0].wallet_file_path
        cid = storage_objects[0].cid
        oids = [storage_object.oid for storage_object in storage_objects[:2]]
        file_path = storage_objects[0].file_path

        file_ranges_to_test = generate_ranges(storage_objects[0], max_object_size, self.shell, self.neofs_env)
        logging.info(f"Ranges used in test {file_ranges_to_test}")

        for range_start, range_len in file_ranges_to_test:
            range_cut = f"{range_start}:{range_len}"
            with allure.step(f"Get range ({range_cut})"):
                for oid in oids:
                    _, range_content = get_range(
                        wallet,
                        cid,
                        oid,
                        shell=self.shell,
                        endpoint=self.neofs_env.sn_rpc,
                        range_cut=range_cut,
                    )
                    assert (
                        get_file_content(file_path, content_len=range_len, mode="rb", offset=range_start)
                        == range_content
                    ), f"Expected range content to match {range_cut} slice of file payload"

        if self.neofs_env.get_binary_version(self.neofs_env.neofs_node_path) > "0.44.2":
            with allure.step("Verify zero payload ranges"):
                _, range_content = get_range(
                    wallet,
                    cid,
                    oid,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    range_cut="0:0",
                )
                assert get_file_content(file_path, mode="rb") == range_content, (
                    "Expected range content to match full file payload"
                )

                with pytest.raises(Exception, match=r".*zero length with non-zero offset.*"):
                    get_range(
                        wallet,
                        cid,
                        oid,
                        shell=self.shell,
                        endpoint=self.neofs_env.sn_rpc,
                        range_cut="5:0",
                    )

    @allure.title("Validate native object API get_range for a complex object")
    def test_object_get_range_complex(self, default_wallet: NodeWallet, container: str, complex_object_size: int):
        """
        Validate get_range for object by native gRPC API for a complex object
        """
        four_chunked_size = complex_object_size
        file_path = generate_file(four_chunked_size)
        oid = put_object_to_random_node(
            default_wallet.path,
            file_path,
            container,
            shell=self.shell,
            neofs_env=self.neofs_env,
        )

        file_ranges_to_test = []

        parts = get_object_chunks(default_wallet.path, container, oid, self.shell, self.neofs_env)

        # range is inside one child
        file_ranges_to_test.append((0, parts[0][1] - 1))
        # range matches child
        file_ranges_to_test.append((parts[0][1], parts[1][1]))
        # range requires more than one child and includes the first child
        file_ranges_to_test.append((0, parts[0][1] + parts[1][1] - 1))
        # range requires more than one child and includes the last child
        file_ranges_to_test.append((parts[0][1] + 1, complex_object_size - parts[0][1] - 1))
        # range requires more than one child and does not include the first and the last child
        file_ranges_to_test.append((parts[0][1] + 1, complex_object_size - parts[0][1] - parts[-1][1] - 1))
        # range requires more than two children and includes the first and the last child
        file_ranges_to_test.append((0, complex_object_size - 1))

        logging.info(f"Ranges used in test {file_ranges_to_test}")

        for range_start, range_len in file_ranges_to_test:
            range_cut = f"{range_start}:{range_len}"
            with allure.step(f"Get range ({range_cut})"):
                _, range_content = get_range(
                    default_wallet.path,
                    container,
                    oid,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    range_cut=range_cut,
                )
                assert (
                    get_file_content(file_path, content_len=range_len, mode="rb", offset=range_start) == range_content
                ), f"Expected range content to match {range_cut} slice of file payload"

    @allure.title("Validate native object API get_range negative cases")
    def test_object_get_range_negatives(
        self,
        request: FixtureRequest,
        storage_objects: list[StorageObjectInfo],
    ):
        """
        Validate get_range negative for object by native gRPC API
        """
        allure.dynamic.title(f"Validate native get_range negative object API for {request.node.callspec.id}")

        wallet = storage_objects[0].wallet_file_path
        cid = storage_objects[0].cid
        oids = [storage_object.oid for storage_object in storage_objects[:2]]
        file_size = storage_objects[0].size

        assert RANGE_MIN_LEN < file_size, (
            f"Incorrect test setup. File size ({file_size}) is less than RANGE_MIN_LEN ({RANGE_MIN_LEN})"
        )

        file_ranges_to_test: list[tuple(int, int, str)] = [
            # Offset is bigger than the file size, the length is small.
            (file_size + 1, RANGE_MIN_LEN, OUT_OF_RANGE),
            # Offset is ok, but offset+length is too big.
            (file_size - RANGE_MIN_LEN, RANGE_MIN_LEN * 2, OUT_OF_RANGE),
            # Offset is ok, and length is very-very big (e.g. MaxUint64) so that offset+length is wrapped and still "valid".
            (RANGE_MIN_LEN, sys.maxsize * 2 + 1, INVALID_RANGE_OVERFLOW),
            # Length is zero
            (10, 0, INVALID_RANGE_ZERO_LENGTH),
            # Negative values
            (-1, 1, INVALID_OFFSET_SPECIFIER),
            (10, -5, INVALID_LENGTH_SPECIFIER),
        ]

        for range_start, range_len, expected_error in file_ranges_to_test:
            range_cut = f"{range_start}:{range_len}"
            expected_error = expected_error.format(range=range_cut) if "{range}" in expected_error else expected_error
            with allure.step(f"Get range ({range_cut})"):
                for oid in oids:
                    with pytest.raises(Exception, match=expected_error):
                        get_range(
                            wallet,
                            cid,
                            oid,
                            shell=self.shell,
                            endpoint=self.neofs_env.sn_rpc,
                            range_cut=range_cut,
                        )

    @allure.title("Validate native object API get_range_hash negative cases")
    def test_object_get_range_hash_negatives(
        self,
        request: FixtureRequest,
        storage_objects: list[StorageObjectInfo],
    ):
        """
        Validate get_range_hash negative for object by native gRPC API
        """
        allure.dynamic.title(f"Validate native get_range_hash negative object API for {request.node.callspec.id}")

        wallet = storage_objects[0].wallet_file_path
        cid = storage_objects[0].cid
        oids = [storage_object.oid for storage_object in storage_objects[:2]]
        file_size = storage_objects[0].size

        assert RANGE_MIN_LEN < file_size, (
            f"Incorrect test setup. File size ({file_size}) is less than RANGE_MIN_LEN ({RANGE_MIN_LEN})"
        )

        file_ranges_to_test: list[tuple(int, int, str)] = [
            # Offset is bigger than the file size, the length is small.
            (file_size + 1, RANGE_MIN_LEN, OUT_OF_RANGE),
            # Offset is ok, but offset+length is too big.
            (file_size - RANGE_MIN_LEN, RANGE_MIN_LEN * 2, OUT_OF_RANGE),
            # Offset is ok, and length is very-very big (e.g. MaxUint64) so that offset+length is wrapped and still "valid".
            (RANGE_MIN_LEN, sys.maxsize * 2 + 1, INVALID_RANGE_OVERFLOW),
            # Length is zero
            (10, 0, INVALID_RANGE_ZERO_LENGTH),
            # Negative values
            (-1, 1, INVALID_OFFSET_SPECIFIER),
            (10, -5, INVALID_LENGTH_SPECIFIER),
        ]

        for range_start, range_len, expected_error in file_ranges_to_test:
            range_cut = f"{range_start}:{range_len}"
            expected_error = expected_error.format(range=range_cut) if "{range}" in expected_error else expected_error
            with allure.step(f"Get range hash ({range_cut})"):
                for oid in oids:
                    with pytest.raises(Exception, match=expected_error):
                        get_range_hash(
                            wallet,
                            cid,
                            oid,
                            shell=self.shell,
                            endpoint=self.neofs_env.sn_rpc,
                            range_cut=range_cut,
                        )

    def test_put_object_header_limitation(self, default_wallet: NodeWallet, container: str, simple_object_size: int):
        file_path = generate_file(simple_object_size)
        attr_key = "a" * (NEOFS_API_HEADER_LIMIT // 2)
        attr_val = "b" * (NEOFS_API_HEADER_LIMIT // 2)
        with pytest.raises(Exception, match=OBJECT_HEADER_LENGTH_LIMIT):
            put_object_to_random_node(
                default_wallet.path,
                file_path,
                container,
                shell=self.shell,
                neofs_env=self.neofs_env,
                attributes={attr_key: attr_val},
            )

    @allure.title("Finished objects (with link object found) cannot be deleted")
    def test_object_parts_cannot_be_deleted(self, default_wallet: NodeWallet, container: str, complex_object_size: int):
        file_path = generate_file(complex_object_size)
        oid = put_object_to_random_node(
            default_wallet.path,
            file_path,
            container,
            shell=self.shell,
            neofs_env=self.neofs_env,
        )

        link_oid = get_link_object(
            default_wallet.path, container, oid, shell=self.shell, nodes=self.neofs_env.storage_nodes
        )

        with allure.step(f"Trying to delete link object {link_oid}"):
            with pytest.raises(Exception, match=LINK_OBJECT_FOUND):
                delete_object(
                    default_wallet.path,
                    container,
                    link_oid,
                    self.shell,
                    self.neofs_env.sn_rpc,
                )

        with allure.step("Trying to delete children"):
            parts = get_object_chunks(default_wallet.path, container, oid, self.shell, self.neofs_env)
            for part in parts:
                with pytest.raises(Exception, match=LINK_OBJECT_FOUND):
                    delete_object(
                        default_wallet.path,
                        container,
                        part[0],
                        self.shell,
                        self.neofs_env.sn_rpc,
                    )

    @allure.title("Big object parts are removed after deletion")
    def test_object_parts_are_unavailable_after_deletion(
        self, default_wallet: NodeWallet, container: str, complex_object_size: int
    ):
        with allure.step("Upload big object"):
            file_path = generate_file(complex_object_size)
            oid = put_object_to_random_node(
                default_wallet.path,
                file_path,
                container,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )
        with allure.step("Get object parts"):
            parts = get_object_chunks(default_wallet.path, container, oid, self.shell, self.neofs_env)

        with allure.step("Delete big object"):
            delete_object(
                default_wallet.path,
                container,
                oid,
                self.shell,
                self.neofs_env.sn_rpc,
            )

        with allure.step("Check big object is deleted"):
            with pytest.raises(Exception, match=OBJECT_ALREADY_REMOVED):
                get_object_from_random_node(default_wallet.path, container, oid, self.shell, neofs_env=self.neofs_env)

        with allure.step("Try to get object parts"):
            for part in parts:
                with pytest.raises(Exception, match=OBJECT_ALREADY_REMOVED):
                    get_object(
                        default_wallet.path,
                        container,
                        part[0],
                        self.shell,
                        self.neofs_env.sn_rpc,
                    )

    @allure.title("Big object parts are removed after expiration")
    def test_object_parts_are_unavailable_after_expiration(
        self, default_wallet: NodeWallet, container: str, complex_object_size: int
    ):
        with allure.step("Get current epoch"):
            epoch = self.get_epoch()

        with allure.step("Upload big object"):
            file_path = generate_file(complex_object_size)
            oid = put_object_to_random_node(
                default_wallet.path,
                file_path,
                container,
                shell=self.shell,
                neofs_env=self.neofs_env,
                expire_at=epoch + 1,
            )
        with allure.step("Get object parts"):
            parts = get_object_chunks(default_wallet.path, container, oid, self.shell, self.neofs_env)

        with allure.step("Tick two epochs"):
            self.tick_epochs_and_wait(2)

        wait_for_gc_pass_on_storage_nodes()

        with allure.step("Check big object is deleted"):
            with pytest.raises(Exception, match=OBJECT_NOT_FOUND):
                get_object_from_random_node(default_wallet.path, container, oid, self.shell, neofs_env=self.neofs_env)

        with allure.step("Try to get object parts"):
            for part in parts:
                with pytest.raises(Exception, match=OBJECT_NOT_FOUND):
                    get_object(
                        default_wallet.path,
                        container,
                        part[0],
                        self.shell,
                        self.neofs_env.sn_rpc,
                    )

    def check_header_is_presented(self, head_info: dict, object_header: dict) -> None:
        for key_to_check, val_to_check in object_header.items():
            assert key_to_check in head_info["header"]["attributes"], f"Key {key_to_check} is found in {head_object}"
            assert head_info["header"]["attributes"].get(key_to_check) == str(val_to_check), (
                f"Value {val_to_check} is equal"
            )
