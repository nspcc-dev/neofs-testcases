import logging

import allure
import pytest
from helpers.container import create_container
from helpers.file_helper import generate_file
from helpers.neofs_verbs import put_object_to_random_node
from helpers.rest_gate import (
    get_object_by_attr_and_verify_hashes,
    try_to_get_object_via_passed_request_and_expect_error,
)
from helpers.wellknown_acl import PUBLIC_ACL
from rest_gw.rest_base import TestNeofsRestBase
from rest_gw.rest_utils import get_object_and_verify_hashes

logger = logging.getLogger("NeoLogger")


@pytest.mark.sanity
class Test_rest_object(TestNeofsRestBase):
    PLACEMENT_RULE = "REP 2 IN X CBF 1 SELECT 4 FROM * AS X"

    @pytest.fixture(scope="class", autouse=True)
    @allure.title("[Class/Autouse]: Prepare wallet and deposit")
    def prepare_wallet(self, default_wallet):
        Test_rest_object.wallet = default_wallet

    @pytest.fixture(scope="class")
    def gw_attributes(self):
        return {
            "endpoint": f"http://{self.neofs_env.rest_gw.endpoint}/v1",
            # List of Key=Value attributes
            # REST gateway accepts attributes in the Canonical MIME Header Key.
            "obj_key1": "Chapter1",
            "obj_value1": "peace",
            "obj_key2": "Chapter2",
            "obj_value2": "war",
        }

    @allure.title("Test Put over gRPC, Get over HTTP")
    @pytest.mark.parametrize(
        "object_size",
        ["simple_object_size", "complex_object_size"],
        ids=["simple object", "complex object"],
    )
    def test_object_put_get_attributes(self, object_size: int, gw_attributes):
        """
        Test that object can be put using gRPC interface and get using HTTP.

        Steps:
        1. Create object;
        2. Put objects using gRPC (neofs-cli) with attributes [--attributes chapter1=peace,chapter2=war];
        3. Download object using HTTP gate (https://github.com/nspcc-dev/neofs-http-gw#downloading);
        4. Compare hashes between original and downloaded object;
        5. [Negative] Try to the get object with specified attributes and `by_id` request: [objects/$CID/by_id/chapter1/peace];
        6. Download the object with specified attributes and `by_attribute` request: [objects/$CID/by_attribute/chapter1/peace];
        7. Compare hashes between original and downloaded object;
        8. [Negative] Try to the get object via `by_attribute` request: [objects/$CID/by_attribute/$OID];


        Expected result:
        Hashes must be the same.
        """
        with allure.step("Create public container"):
            cid = create_container(
                self.wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                rule=self.PLACEMENT_RULE,
                basic_acl=PUBLIC_ACL,
            )

        # Generate file
        file_path = generate_file(self.neofs_env.get_object_size(object_size))

        # List of Key=Value attributes
        obj_key1 = gw_attributes["obj_key1"]
        obj_value1 = gw_attributes["obj_value1"]
        obj_key2 = gw_attributes["obj_key2"]
        obj_value2 = gw_attributes["obj_value2"]

        # Prepare for grpc PUT request
        key_value1 = obj_key1 + "=" + obj_value1
        key_value2 = obj_key2 + "=" + obj_value2

        with allure.step("Put objects using gRPC [--attributes chapter1=peace,chapter2=war]"):
            oid = put_object_to_random_node(
                wallet=self.wallet.path,
                path=file_path,
                cid=cid,
                shell=self.shell,
                neofs_env=self.neofs_env,
                attributes=f"{key_value1},{key_value2}",
            )
        with allure.step("Get object and verify hashes [ objects/$CID/by_id/$OID ]"):
            get_object_and_verify_hashes(
                oid=oid,
                file_name=file_path,
                wallet=self.wallet.path,
                cid=cid,
                shell=self.shell,
                nodes=self.neofs_env.storage_nodes,
                endpoint=gw_attributes["endpoint"],
            )
        with allure.step("[Negative] try to get object: [objects/$CID/by_id/chapter1/peace]"):
            attrs = {obj_key1: obj_value1, obj_key2: obj_value2}
            request = f"/objects/{cid}/by_id/{obj_key1}/{obj_value1}"
            expected_err_msg = "Failed to get object via REST gate:"
            try_to_get_object_via_passed_request_and_expect_error(
                cid=cid,
                oid=oid,
                error_pattern=expected_err_msg,
                http_request_path=request,
                attrs=attrs,
                endpoint=gw_attributes["endpoint"],
            )

        with allure.step("Download the object with attribute [objects/$CID/by_attribute/chapter1/peace]"):
            get_object_by_attr_and_verify_hashes(
                oid=oid,
                file_name=file_path,
                cid=cid,
                attrs=attrs,
                endpoint=gw_attributes["endpoint"],
            )
        with allure.step("[Negative] try to get object: objects/$CID/by_attribute/$OID"):
            request = f"/objects/{cid}/by_attribute/{oid}"
            try_to_get_object_via_passed_request_and_expect_error(
                cid=cid,
                oid=oid,
                error_pattern=expected_err_msg,
                http_request_path=request,
                endpoint=gw_attributes["endpoint"],
            )

        with allure.step(
            "[Negative] Try to get object with invalid attribute [objects/$CID/by_attribute/chapter1/war]"
        ):
            with pytest.raises(Exception, match=".*object not found.*"):
                get_object_by_attr_and_verify_hashes(
                    oid=oid,
                    file_name=file_path,
                    cid=cid,
                    attrs={obj_key1: obj_value2},
                    endpoint=gw_attributes["endpoint"],
                )
