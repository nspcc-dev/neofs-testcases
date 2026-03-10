import logging

import allure
import pytest
from helpers.file_helper import generate_file
from helpers.rest_gate import (
    attr_into_header,
    create_container,
    delete_container,
    delete_object,
    get_container_eacl,
    get_container_info,
    get_rest_gateway_address,
    put_container_eacl,
    upload_via_rest_gate,
)
from helpers.wellknown_acl import EACL_PUBLIC_READ_WRITE, PUBLIC_ACL
from neofs_testlib.env.env import NodeWallet
from rest_gw.rest_base import TestNeofsRestBase
from rest_gw.rest_utils import generate_session_token_v2

logger = logging.getLogger("NeoLogger")


@pytest.mark.sanity
class TestRestContainers(TestNeofsRestBase):
    PLACEMENT_RULE = "REP 2 IN X CBF 1 SELECT 4 FROM * AS X"

    @pytest.fixture(scope="class", autouse=True)
    @allure.title("[Class/Autouse]: Prepare wallet and deposit")
    def prepare_wallet(self, default_wallet: NodeWallet):
        TestRestContainers.wallet = default_wallet

    @pytest.mark.parametrize("wallet_connect", [True, False])
    @pytest.mark.simple
    def test_rest_gw_containers_sanity(self, gw_endpoint: str, wallet_connect: bool):
        session_token = generate_session_token_v2(
            gw_endpoint, self.wallet, [{"verbs": ["CONTAINER_PUT"]}], wallet_connect=wallet_connect
        )
        cid = create_container(
            gw_endpoint,
            "rest_gw_container",
            self.PLACEMENT_RULE,
            PUBLIC_ACL,
            session_token,
            wallet_connect=wallet_connect,
        )

        resp = get_container_info(gw_endpoint, cid)

        assert resp["containerId"] == cid, "Invalid containerId"
        assert resp["basicAcl"] == PUBLIC_ACL.lower().strip("0"), "Invalid ACL"
        assert resp["placementPolicy"].replace("\n", " ") == self.PLACEMENT_RULE, "Invalid placementPolicy"
        assert resp["cannedAcl"] == EACL_PUBLIC_READ_WRITE, "Invalid cannedAcl"

        upload_via_rest_gate(
            cid=cid,
            path=generate_file(self.neofs_env.get_object_size("simple_object_size")),
            endpoint=gw_endpoint,
        )

        session_token = generate_session_token_v2(
            gw_endpoint, self.wallet, [{"verbs": ["CONTAINER_DELETE"]}], wallet_connect=wallet_connect
        )
        delete_container(gw_endpoint, cid, session_token, wallet_connect=wallet_connect)

    @allure.title("Test REST EACL numeric filters")
    @pytest.mark.parametrize(
        "match_type,filter_value,test_values",
        [
            ("NUM_LE", "100", {"50": True, "100": True, "150": False}),
            ("NUM_LT", "100", {"50": True, "100": False, "150": False}),
            ("NUM_GT", "100", {"50": False, "100": False, "150": True}),
            ("NUM_GE", "100", {"50": False, "100": True, "150": True}),
        ],
    )
    def test_rest_container_eacl_numeric_filters(
        self, gw_endpoint: str, match_type: str, filter_value: str, test_values: dict
    ):
        rest_gw_address = get_rest_gateway_address(gw_endpoint)

        container_session_token = generate_session_token_v2(
            gw_endpoint, self.wallet, [{"verbs": ["CONTAINER_PUT", "CONTAINER_DELETE"]}], wallet_connect=True
        )
        cid = create_container(
            gw_endpoint,
            f"rest_gw_eacl_{match_type.lower()}",
            self.PLACEMENT_RULE,
            PUBLIC_ACL,
            container_session_token,
            wallet_connect=True,
        )

        session_token = generate_session_token_v2(
            gw_endpoint,
            self.wallet,
            [{"containerID": cid, "verbs": ["CONTAINER_SET_EACL", "OBJECT_PUT", "OBJECT_DELETE"]}],
            targets=[rest_gw_address],
            wallet_connect=True,
        )

        with allure.step(f"Set EACL with {match_type} filter (value={filter_value})"):
            eacl_records = [
                {
                    "action": "DENY",
                    "operation": "DELETE",
                    "filters": [
                        {"headerType": "OBJECT", "key": "MyKey", "matchType": match_type, "value": filter_value}
                    ],
                    "targets": [{"role": "OTHERS", "keys": []}],
                }
            ]

            put_response = put_container_eacl(gw_endpoint, cid, eacl_records, session_token, wallet_connect=True)
            assert put_response.get("success") is True, f"Failed to set EACL with {match_type}: {put_response}"

        with allure.step(f"Verify {match_type} filter was set correctly"):
            eacl_response = get_container_eacl(gw_endpoint, cid)
            assert len(eacl_response["records"]) == 1
            assert eacl_response["records"][0]["filters"][0]["matchType"] == match_type
            assert eacl_response["records"][0]["filters"][0]["value"] == filter_value

        with allure.step(f"Verify {match_type} filter enforcement"):
            for attr_value, should_deny in test_values.items():
                with allure.step(f"Test with MyKey={attr_value} (should_deny={should_deny})"):
                    test_file = generate_file(self.neofs_env.get_object_size("simple_object_size"))
                    oid = upload_via_rest_gate(
                        cid=cid,
                        path=test_file,
                        endpoint=gw_endpoint,
                        headers=attr_into_header({"MyKey": attr_value}),
                        session_token=session_token,
                    )

                    if should_deny:
                        try:
                            delete_object(gw_endpoint, cid, oid, session_token)
                            raise AssertionError(
                                f"DELETE should have been denied for MyKey={attr_value} with {match_type} {filter_value}"
                            )
                        except Exception as e:
                            assert "403" in str(e) or "denied" in str(e).lower(), (
                                f"Expected access denied error, got: {e}"
                            )
                    else:
                        delete_object(gw_endpoint, cid, oid, session_token)

        delete_container(gw_endpoint, cid, container_session_token, wallet_connect=True)

    @allure.title("Test REST EACL string filters")
    @pytest.mark.parametrize(
        "match_type,filter_key,filter_value",
        [
            ("STRING_EQUAL", "FileName", "test.txt"),
            ("STRING_NOT_EQUAL", "FileName", "secret.txt"),
            ("NOT_PRESENT", "RestrictedAttr", None),
        ],
    )
    def test_rest_container_eacl_string_filters(
        self, gw_endpoint: str, match_type: str, filter_key: str, filter_value: str
    ):
        rest_gw_address = get_rest_gateway_address(gw_endpoint)

        container_session_token = generate_session_token_v2(
            gw_endpoint, self.wallet, [{"verbs": ["CONTAINER_PUT", "CONTAINER_DELETE"]}], wallet_connect=True
        )
        cid = create_container(
            gw_endpoint,
            f"rest_gw_eacl_{match_type.lower()}",
            self.PLACEMENT_RULE,
            PUBLIC_ACL,
            container_session_token,
            wallet_connect=True,
        )

        session_token = generate_session_token_v2(
            gw_endpoint,
            self.wallet,
            [{"containerID": cid, "verbs": ["CONTAINER_SET_EACL", "OBJECT_PUT", "OBJECT_DELETE"]}],
            targets=[rest_gw_address],
            wallet_connect=True,
        )

        with allure.step(f"Set EACL with {match_type} filter"):
            filter_dict = {"headerType": "OBJECT", "key": filter_key, "matchType": match_type}
            if filter_value is not None:
                filter_dict["value"] = filter_value

            eacl_records = [
                {
                    "action": "DENY",
                    "operation": "DELETE",
                    "filters": [filter_dict],
                    "targets": [{"role": "OTHERS", "keys": []}],
                }
            ]

            put_response = put_container_eacl(gw_endpoint, cid, eacl_records, session_token, wallet_connect=True)
            assert put_response.get("success") is True, f"Failed to set EACL with {match_type}: {put_response}"

        with allure.step(f"Verify {match_type} filter was set correctly"):
            eacl_response = get_container_eacl(gw_endpoint, cid)
            assert eacl_response["records"][0]["filters"][0]["matchType"] == match_type
            if filter_value is not None:
                assert eacl_response["records"][0]["filters"][0]["value"] == filter_value

        with allure.step(f"Verify {match_type} filter enforcement"):
            test_file = generate_file(self.neofs_env.get_object_size("simple_object_size"))

            if match_type == "STRING_EQUAL":
                with allure.step(f"Upload object with {filter_key}={filter_value} (should deny DELETE)"):
                    oid_match = upload_via_rest_gate(
                        cid=cid,
                        path=test_file,
                        endpoint=gw_endpoint,
                        headers=attr_into_header({filter_key: filter_value}),
                        session_token=session_token,
                    )

                    try:
                        delete_object(gw_endpoint, cid, oid_match, session_token)
                        raise AssertionError(f"DELETE should have been denied for matching {filter_key}")
                    except Exception as e:
                        assert "403" in str(e) or "denied" in str(e).lower(), f"Expected access denied error, got: {e}"

                with allure.step(f"Upload object with {filter_key}=other.txt (should allow DELETE)"):
                    oid_nomatch = upload_via_rest_gate(
                        cid=cid,
                        path=test_file,
                        endpoint=gw_endpoint,
                        headers=attr_into_header({filter_key: "other.txt"}),
                        session_token=session_token,
                    )
                    delete_object(gw_endpoint, cid, oid_nomatch, session_token)

            elif match_type == "STRING_NOT_EQUAL":
                with allure.step(f"Upload object with {filter_key}=allowed.txt (should deny DELETE)"):
                    oid_nomatch = upload_via_rest_gate(
                        cid=cid,
                        path=test_file,
                        endpoint=gw_endpoint,
                        headers=attr_into_header({filter_key: "allowed.txt"}),
                        session_token=session_token,
                    )

                    try:
                        delete_object(gw_endpoint, cid, oid_nomatch, session_token)
                        raise AssertionError(f"DELETE should have been denied for non-matching {filter_key}")
                    except Exception as e:
                        assert "403" in str(e) or "denied" in str(e).lower(), f"Expected access denied error, got: {e}"

                with allure.step(f"Upload object with {filter_key}={filter_value} (should allow DELETE)"):
                    oid_match = upload_via_rest_gate(
                        cid=cid,
                        path=test_file,
                        endpoint=gw_endpoint,
                        headers=attr_into_header({filter_key: filter_value}),
                        session_token=session_token,
                    )
                    delete_object(gw_endpoint, cid, oid_match, session_token)

            elif match_type == "NOT_PRESENT":
                with allure.step(f"Upload object without {filter_key} (should deny DELETE)"):
                    oid_no_attr = upload_via_rest_gate(
                        cid=cid, path=test_file, endpoint=gw_endpoint, session_token=session_token
                    )

                    try:
                        delete_object(gw_endpoint, cid, oid_no_attr, session_token)
                        raise AssertionError(f"DELETE should have been denied when {filter_key} is not present")
                    except Exception as e:
                        assert "403" in str(e) or "denied" in str(e).lower(), f"Expected access denied error, got: {e}"

                with allure.step(f"Upload object with {filter_key}=value (should allow DELETE)"):
                    oid_with_attr = upload_via_rest_gate(
                        cid=cid,
                        path=test_file,
                        endpoint=gw_endpoint,
                        headers=attr_into_header({filter_key: "value"}),
                        session_token=session_token,
                    )
                    delete_object(gw_endpoint, cid, oid_with_attr, session_token)

        delete_container(gw_endpoint, cid, container_session_token, wallet_connect=True)

    @allure.title("Test REST EACL invalid numeric filter")
    def test_rest_container_eacl_invalid_numeric_filter(self, gw_endpoint: str):
        rest_gw_address = get_rest_gateway_address(gw_endpoint)

        container_session_token = generate_session_token_v2(
            gw_endpoint, self.wallet, [{"verbs": ["CONTAINER_PUT", "CONTAINER_DELETE"]}], wallet_connect=True
        )
        cid = create_container(
            gw_endpoint,
            "rest_gw_eacl_invalid",
            self.PLACEMENT_RULE,
            PUBLIC_ACL,
            container_session_token,
            wallet_connect=True,
        )

        session_token = generate_session_token_v2(
            gw_endpoint,
            self.wallet,
            [{"containerID": cid, "verbs": ["CONTAINER_SET_EACL"]}],
            targets=[rest_gw_address],
            wallet_connect=True,
        )

        with allure.step("Attempt to set EACL with invalid numeric value"):
            eacl_records = [
                {
                    "action": "DENY",
                    "operation": "DELETE",
                    "filters": [{"headerType": "OBJECT", "key": "MyKey", "matchType": "NUM_GE", "value": "123a"}],
                    "targets": [{"role": "OTHERS", "keys": []}],
                }
            ]

            put_response = put_container_eacl(
                gw_endpoint,
                cid,
                eacl_records,
                session_token,
                wallet_connect=True,
                expect_error=True,
                expected_error_message="is not a valid numeric value",
            )

            assert put_response.get("status_code") == 400, "Expected status code 400 for invalid numeric value"

        delete_container(gw_endpoint, cid, container_session_token, wallet_connect=True)
