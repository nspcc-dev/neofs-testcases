import logging

import allure
import pytest
from common import HTTP_GATE
from container import create_container
from file_helper import generate_file
from python_keywords.http_gate import (
    get_object_and_verify_hashes,
    get_object_by_attr_and_verify_hashes,
    try_to_get_object_via_passed_request_and_expect_error,
)
from python_keywords.neofs_verbs import put_object
from wellknown_acl import PUBLIC_ACL

logger = logging.getLogger("NeoLogger")


@pytest.mark.sanity
@pytest.mark.http_gate
class Test_http_object:
    PLACEMENT_RULE = "REP 2 IN X CBF 1 SELECT 4 FROM * AS X"

    @pytest.fixture(scope="class", autouse=True)
    @allure.title("[Class/Autouse]: Prepare wallet and deposit")
    def prepare_wallet(self, prepare_wallet_and_deposit):
        Test_http_object.wallet = prepare_wallet_and_deposit

    @allure.title("Test Put over gRPC, Get over HTTP")
    def test_object_put_get_attributes(self, client_shell):
        """
        Test that object can be put using gRPC interface and get using HTTP.

        Steps:
        1. Create object;
        2. Put objects using gRPC (neofs-cli) with attributes [--attributes chapter1=peace,chapter2=war];
        3. Download object using HTTP gate (https://github.com/nspcc-dev/neofs-http-gw#downloading);
        4. Compare hashes between original and downloaded object;
        5. [Negative] Try to the get object with specified attributes and `get` request: [get/$CID/chapter1/peace];
        6. Download the object with specified attributes and `get_by_attribute` request: [get_by_attribute/$CID/chapter1/peace];
        7. Compare hashes between original and downloaded object;
        8. [Negative] Try to the get object via `get_by_attribute` request: [get_by_attribute/$CID/$OID];


        Expected result:
        Hashes must be the same.
        """
        with allure.step("Create public container"):
            cid = create_container(
                self.wallet, shell=client_shell, rule=self.PLACEMENT_RULE, basic_acl=PUBLIC_ACL
            )

        # Generate file
        file_path = generate_file()

        # List of Key=Value attributes
        obj_key1 = "chapter1"
        obj_value1 = "peace"
        obj_key2 = "chapter2"
        obj_value2 = "war"

        # Prepare for grpc PUT request
        key_value1 = obj_key1 + "=" + obj_value1
        key_value2 = obj_key2 + "=" + obj_value2

        with allure.step("Put objects using gRPC [--attributes chapter1=peace,chapter2=war]"):
            oid = put_object(
                wallet=self.wallet,
                path=file_path,
                cid=cid,
                shell=client_shell,
                attributes=f"{key_value1},{key_value2}",
            )
        with allure.step("Get object and verify hashes [ get/$CID/$OID ]"):
            get_object_and_verify_hashes(oid, file_path, self.wallet, cid, shell=client_shell)

        with allure.step("[Negative] try to get object: [get/$CID/chapter1/peace]"):
            attrs = {obj_key1: obj_value1, obj_key2: obj_value2}
            request = f"{HTTP_GATE}/get/{cid}/{obj_key1}/{obj_value1}"
            expected_err_msg = "Failed to get object via HTTP gate:"
            try_to_get_object_via_passed_request_and_expect_error(
                cid=cid, oid=oid, error_pattern=expected_err_msg, http_request=request, attrs=attrs
            )

        with allure.step(
            "Download the object with attribute [get_by_attribute/$CID/chapter1/peace]"
        ):
            get_object_by_attr_and_verify_hashes(oid=oid, file_name=file_path, cid=cid, attrs=attrs)

        with allure.step("[Negative] try to get object: get_by_attribute/$CID/$OID"):
            request = f"{HTTP_GATE}/get_by_attribute/{cid}/{oid}"
            try_to_get_object_via_passed_request_and_expect_error(
                cid=cid, oid=oid, error_pattern=expected_err_msg, http_request=request
            )
