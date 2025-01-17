import logging
import os

import allure
import pytest
from helpers.container import (
    create_container,
    delete_container,
    list_containers,
    wait_for_container_deletion,
)
from helpers.file_helper import generate_file
from helpers.neofs_verbs import delete_object
from helpers.rest_gate import (
    attr_into_str_header,
    attr_into_str_header_curl,
    get_object_by_attr_and_verify_hashes,
    try_to_get_object_and_expect_error,
    try_to_get_object_via_passed_request_and_expect_error,
    upload_via_rest_gate,
    upload_via_rest_gate_curl,
)
from helpers.storage_object_info import StorageObjectInfo
from helpers.wellknown_acl import PUBLIC_ACL
from neofs_env.neofs_env_test_base import NeofsEnvTestBase
from pytest import FixtureRequest
from pytest_lazy_fixtures import lf

logger = logging.getLogger("NeoLogger")


@pytest.mark.sanity
class Test_rest_headers(NeofsEnvTestBase):
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
            lf("simple_object_size"),
            lf("complex_object_size"),
        ],
        ids=["simple object", "complex object"],
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
        file_path = generate_file(request.param)
        for attributes in self.OBJECT_ATTRIBUTES:
            storage_object_id = upload_via_rest_gate(
                cid=cid,
                path=file_path,
                endpoint=gw_endpoint,
                headers=attr_into_str_header(attributes),
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

    @allure.title("[Negative] Try to put object and get right after container is deleted")
    def test_negative_put_and_get_object3(self, simple_object_size, gw_endpoint):
        """
        Test to attempt to put object and try to download it right after the container has been deleted

        Steps:
        1. [Negative] Allocate and attempt to put object#3 via http with attributes: [Writer=Leo Tolstoy, Writer=peace, peace=peace]
            Expected: "Error duplication of attributes detected"
        2. Delete container
        3. [Negative] Try to download object with attributes [peace=peace]
            Expected: "HTTP request sent, awaiting response... 404 Not Found"
        """
        cid = create_container(
            wallet=self.wallet.path,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            rule=self.PLACEMENT_RULE,
            basic_acl=PUBLIC_ACL,
        )
        file_path = generate_file(simple_object_size)
        upload_via_rest_gate(
            cid=cid,
            path=file_path,
            endpoint=gw_endpoint,
            headers=attr_into_str_header(self.OBJECT_ATTRIBUTES[0]),
        )

        with allure.step(
            "[Negative] Allocate and attemt to put object#3 via http with attributes: [Writer=Leo Tolstoy, Writer=peace, peace=peace]"
        ):
            file_path_3 = generate_file(simple_object_size)
            attrs_obj3 = {"Writer": "Leo Tolstoy", "peace": "peace"}
            headers = attr_into_str_header_curl(attrs_obj3)
            headers.append(" ".join(attr_into_str_header_curl({"Writer": "peace"})))
            error_pattern = "key duplication error: X-Attribute-Writer"
            upload_via_rest_gate_curl(
                cid=cid,
                filepath=file_path_3,
                endpoint=gw_endpoint,
                headers=headers,
                error_pattern=error_pattern,
            )
        with allure.step("Delete container and verify container deletion"):
            delete_container(
                wallet=self.wallet.path,
                cid=cid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            self.tick_epochs_and_wait(1)
            wait_for_container_deletion(
                self.wallet.path,
                cid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            assert cid not in list_containers(self.wallet.path, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
        with allure.step("[Negative] Try to download (wget) object via wget with attributes [peace=peace]"):
            error_pattern = "404 Not Found"
            try_to_get_object_via_passed_request_and_expect_error(
                cid=cid,
                oid="",
                error_pattern=error_pattern,
                attrs=attrs_obj3,
                http_request_path=None,
                endpoint=gw_endpoint,
            )
