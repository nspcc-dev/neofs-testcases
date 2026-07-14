import logging
import os

import allure
import pytest
from helpers.complex_object_actions import get_link_object
from helpers.container import (
    create_container,
)
from helpers.file_helper import generate_file
from helpers.neofs_verbs import delete_object, lock_object, put_object_to_random_node
from helpers.rest_gate import (
    attr_into_header,
    get_object_by_attr_and_verify_hashes,
    get_via_rest_gate,
    head_via_rest_gate,
    head_via_rest_gate_by_attribute,
    try_to_get_object_and_expect_error,
    upload_via_rest_gate,
)
from helpers.storage_object_info import StorageObjectInfo
from helpers.wellknown_acl import PUBLIC_ACL
from pytest import FixtureRequest
from rest_gw.rest_base import TestNeofsRestBase

logger = logging.getLogger("NeoLogger")

OBJECT_TYPE_HEADER = "X-Object-Type"


@pytest.mark.sanity
class Test_rest_headers(TestNeofsRestBase):
    PLACEMENT_RULE = "REP 2 IN X CBF 1 SELECT 4 FROM * AS X"
    obj1_keys = ["Writer", "Chapter1", "Chapter2"]
    obj2_keys = ["Writer", "Ch$pter1", "Chapter2"]
    values = ["Leo Tolstoy", "peace", "w$r"]
    OBJECT_ATTRIBUTES = [
        {obj1_keys[0]: values[0], obj1_keys[1]: values[1], obj1_keys[2]: values[2]},
        {obj2_keys[0]: values[0], obj2_keys[1]: values[1], obj2_keys[2]: values[2]},
    ]

    @pytest.fixture(scope="class", autouse=True)
    @allure.title("[Class/Autouse]: Prepare wallet and deposit")
    def prepare_wallet(self, default_wallet):
        Test_rest_headers.wallet = default_wallet

    @pytest.fixture(
        params=[
            pytest.param("simple_object_size", id="simple object", marks=pytest.mark.simple),
            pytest.param("complex_object_size", id="complex object", marks=pytest.mark.complex),
        ],
        scope="class",
    )
    def storage_objects_with_attributes(self, request: FixtureRequest, gw_endpoint) -> list[StorageObjectInfo]:
        storage_objects = []
        wallet = self.wallet.path
        cid = create_container(
            wallet=self.wallet.path,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            rule=self.PLACEMENT_RULE,
            basic_acl=PUBLIC_ACL,
        )
        file_path = generate_file(self.neofs_env.get_object_size(request.param))
        for attributes in self.OBJECT_ATTRIBUTES:
            storage_object_id = upload_via_rest_gate(
                cid=cid,
                path=file_path,
                endpoint=gw_endpoint,
                headers=attr_into_header(attributes),
            )
            storage_object = StorageObjectInfo(cid, storage_object_id)
            storage_object.size = os.path.getsize(file_path)
            storage_object.wallet_file_path = wallet
            storage_object.file_path = file_path
            storage_object.attributes = attributes

            storage_objects.append(storage_object)

        yield storage_objects

    @allure.title("Get object1 by attribute")
    def test_object1_can_be_get_by_attr(self, storage_objects_with_attributes: list[StorageObjectInfo], gw_endpoint):
        """
        Test to get object#1 by attribute and comapre hashes

        Steps:
        1. Download object#1 with attributes [Chapter2=w$r] and compare hashes
        """

        storage_object_1 = storage_objects_with_attributes[0]

        with allure.step(
            f"Download object#1 via wget with attributes Chapter2: {storage_object_1.attributes['Chapter2']} and compare hashes"
        ):
            get_object_by_attr_and_verify_hashes(
                oid=storage_object_1.oid,
                file_name=storage_object_1.file_path,
                cid=storage_object_1.cid,
                attrs={"Chapter2": storage_object_1.attributes["Chapter2"]},
                endpoint=gw_endpoint,
            )

    @allure.title("Test get object2 with different attributes, then delete object2 and get object1")
    def test_object2_can_be_get_by_attr(self, storage_objects_with_attributes: list[StorageObjectInfo], gw_endpoint):
        """
        Test to get object2 with different attributes, then delete object2 and get object1 using 1st attribute. Note: obj1 and obj2 have the same attribute#1,
        and when obj2 is deleted you can get obj1 by 1st attribute

        Steps:
        1. Download object#2 with attributes [Chapter2=w$r] and compare hashes
        2. Download object#2 with attributes [Ch$pter1=peace] and compare hashes
        3. Delete object#2
        4. Download object#1 with attributes [Writer=Leo Tolstoy] and compare hashes
        """
        storage_object_1 = storage_objects_with_attributes[0]
        storage_object_2 = storage_objects_with_attributes[1]

        with allure.step(
            f"Download object#2 via wget with attributes [Chapter2={storage_object_2.attributes['Chapter2']}] / [Ch$pter1={storage_object_2.attributes['Ch$pter1']}]  and compare hashes"
        ):
            selected_attributes_object2 = [
                {"Chapter2": storage_object_2.attributes["Chapter2"]},
                {"Ch$pter1": storage_object_2.attributes["Ch$pter1"]},
            ]
            for attributes in selected_attributes_object2:
                get_object_by_attr_and_verify_hashes(
                    oid=storage_object_2.oid,
                    file_name=storage_object_2.file_path,
                    cid=storage_object_2.cid,
                    attrs=attributes,
                    endpoint=gw_endpoint,
                )
        with allure.step("Delete object#2 and verify is the container deleted"):
            delete_object(
                wallet=self.wallet.path,
                cid=storage_object_2.cid,
                oid=storage_object_2.oid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            error_pattern = "404 Not Found"
            try_to_get_object_and_expect_error(
                cid=storage_object_2.cid,
                oid=storage_object_2.oid,
                error_pattern=error_pattern,
                endpoint=gw_endpoint,
            )
            storage_objects_with_attributes.remove(storage_object_2)

        with allure.step(
            f"Download object#1 with attributes [Writer={storage_object_1.attributes['Writer']}] and compare hashes"
        ):
            key_value_pair = {"Writer": storage_object_1.attributes["Writer"]}
            get_object_by_attr_and_verify_hashes(
                oid=storage_object_1.oid,
                file_name=storage_object_1.file_path,
                cid=storage_object_1.cid,
                attrs=key_value_pair,
                endpoint=gw_endpoint,
            )


@pytest.mark.sanity
class Test_rest_object_type(TestNeofsRestBase):
    """Verify the ``X-Object-Type`` response header returned by the REST gateway.

    The gateway exposes the NeoFS object type (REGULAR/TOMBSTONE/LOCK/LINK) via the
    ``X-Object-Type`` header on object GET, HEAD and get-by-attribute responses.
    """

    PLACEMENT_RULE = "REP 2 IN X CBF 1 SELECT 4 FROM * AS X"
    LOCK_LIFETIME = 5

    @pytest.fixture(scope="class", autouse=True)
    @allure.title("[Class/Autouse]: Prepare wallet and deposit")
    def prepare_wallet(self, default_wallet):
        Test_rest_object_type.wallet = default_wallet

    @pytest.fixture(scope="class")
    def container(self) -> str:
        return create_container(
            wallet=self.wallet.path,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            rule=self.PLACEMENT_RULE,
            basic_acl=PUBLIC_ACL,
        )

    @allure.title("REGULAR object type in X-Object-Type header ({object_size})")
    @pytest.mark.parametrize(
        "object_size",
        [
            pytest.param("simple_object_size", id="simple object", marks=pytest.mark.simple),
            pytest.param("complex_object_size", id="complex object", marks=pytest.mark.complex),
        ],
    )
    def test_regular_object_type(self, container: str, gw_endpoint: str, object_size: str):
        """
        Test that a regular object reports the REGULAR type via X-Object-Type.

        Steps:
        1. Upload an object via REST gate (with a user attribute).
        2. HEAD the object by id and verify X-Object-Type == REGULAR.
        3. GET the object by id and verify X-Object-Type == REGULAR.
        4. HEAD the object by attribute and verify X-Object-Type == REGULAR.
        """
        attributes = {"FileName": "regular_object"}
        file_path = generate_file(self.neofs_env.get_object_size(object_size))

        with allure.step("Upload object via REST gate"):
            oid = upload_via_rest_gate(
                cid=container,
                path=file_path,
                endpoint=gw_endpoint,
                headers=attr_into_header(attributes),
            )

        with allure.step("HEAD object by id and verify X-Object-Type is REGULAR"):
            resp = head_via_rest_gate(cid=container, oid=oid, endpoint=gw_endpoint)
            assert resp.headers.get(OBJECT_TYPE_HEADER) == "REGULAR", (
                f"Expected {OBJECT_TYPE_HEADER}=REGULAR on HEAD, got {resp.headers.get(OBJECT_TYPE_HEADER)}"
            )

        with allure.step("GET object by id and verify X-Object-Type is REGULAR"):
            resp = get_via_rest_gate(cid=container, oid=oid, endpoint=gw_endpoint, return_response=True)
            assert resp.headers.get(OBJECT_TYPE_HEADER) == "REGULAR", (
                f"Expected {OBJECT_TYPE_HEADER}=REGULAR on GET, got {resp.headers.get(OBJECT_TYPE_HEADER)}"
            )

        with allure.step("HEAD object by attribute and verify X-Object-Type is REGULAR"):
            resp = head_via_rest_gate_by_attribute(cid=container, attribute=attributes, endpoint=gw_endpoint)
            assert resp.headers.get(OBJECT_TYPE_HEADER) == "REGULAR", (
                f"Expected {OBJECT_TYPE_HEADER}=REGULAR on HEAD by attribute, got {resp.headers.get(OBJECT_TYPE_HEADER)}"
            )

    @allure.title("LOCK object type in X-Object-Type header")
    @pytest.mark.simple
    def test_lock_object_type(self, container: str, gw_endpoint: str):
        """
        Test that a lock object reports the LOCK type via X-Object-Type.

        Steps:
        1. Upload a regular object.
        2. Lock it, obtaining the lock object id.
        3. HEAD the lock object by id and verify X-Object-Type == LOCK.
        """
        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))

        with allure.step("Upload object and lock it"):
            oid = put_object_to_random_node(
                wallet=self.wallet.path,
                path=file_path,
                cid=container,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )
            lock_oid = lock_object(
                wallet=self.wallet.path,
                cid=container,
                oid=oid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                lifetime=self.LOCK_LIFETIME,
            )

        with allure.step("HEAD lock object by id and verify X-Object-Type is LOCK"):
            resp = head_via_rest_gate(cid=container, oid=lock_oid, endpoint=gw_endpoint)
            assert resp.headers.get(OBJECT_TYPE_HEADER) == "LOCK", (
                f"Expected {OBJECT_TYPE_HEADER}=LOCK, got {resp.headers.get(OBJECT_TYPE_HEADER)}"
            )

    @allure.title("TOMBSTONE object type in X-Object-Type header")
    @pytest.mark.simple
    def test_tombstone_object_type(self, container: str, gw_endpoint: str):
        """
        Test that a tombstone object reports the TOMBSTONE type via X-Object-Type.

        Steps:
        1. Upload a regular object.
        2. Delete it, obtaining the tombstone object id.
        3. HEAD the tombstone object by id and verify X-Object-Type == TOMBSTONE.
        """
        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))

        with allure.step("Upload object and delete it"):
            oid = put_object_to_random_node(
                wallet=self.wallet.path,
                path=file_path,
                cid=container,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )
            tombstone_oid = delete_object(
                wallet=self.wallet.path,
                cid=container,
                oid=oid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("HEAD tombstone object by id and verify X-Object-Type is TOMBSTONE"):
            resp = head_via_rest_gate(cid=container, oid=tombstone_oid, endpoint=gw_endpoint)
            assert resp.headers.get(OBJECT_TYPE_HEADER) == "TOMBSTONE", (
                f"Expected {OBJECT_TYPE_HEADER}=TOMBSTONE, got {resp.headers.get(OBJECT_TYPE_HEADER)}"
            )

    @allure.title("LINK object type in X-Object-Type header")
    @pytest.mark.complex
    def test_link_object_type(self, container: str, gw_endpoint: str):
        """
        Test that a link object reports the LINK type via X-Object-Type.

        Steps:
        1. Upload a complex (split) object.
        2. Resolve the link object id of the complex object.
        3. HEAD the link object by id and verify X-Object-Type == LINK.
        """
        file_path = generate_file(self.neofs_env.get_object_size("complex_object_size"))

        with allure.step("Upload complex object and resolve its link object"):
            oid = put_object_to_random_node(
                wallet=self.wallet.path,
                path=file_path,
                cid=container,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )
            link_oid = get_link_object(
                wallet=self.wallet.path,
                cid=container,
                oid=oid,
                neofs_env=self.neofs_env,
            )

        with allure.step("HEAD link object by id and verify X-Object-Type is LINK"):
            resp = head_via_rest_gate(cid=container, oid=link_oid, endpoint=gw_endpoint)
            assert resp.headers.get(OBJECT_TYPE_HEADER) == "LINK", (
                f"Expected {OBJECT_TYPE_HEADER}=LINK, got {resp.headers.get(OBJECT_TYPE_HEADER)}"
            )
