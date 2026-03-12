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
    get_via_rest_gate,
    put_container_eacl,
    upload_via_rest_gate,
)
from helpers.wellknown_acl import EACL_PUBLIC_READ_WRITE, PUBLIC_ACL
from neofs_testlib.env.env import NodeWallet
from neofs_testlib.utils.wallet import get_last_address_from_wallet
from rest_gw.rest_base import TestNeofsRestBase
from rest_gw.rest_utils import generate_session_token_v2

logger = logging.getLogger("NeoLogger")


@pytest.mark.sanity
class TestRestContainers(TestNeofsRestBase):
    PLACEMENT_RULE = "REP 2 IN X CBF 1 SELECT 4 FROM * AS X"

    @pytest.fixture(scope="class", autouse=True)
    @allure.title("[Class/Autouse]: Prepare wallet and deposit")
    def prepare_wallet(self, default_wallet: NodeWallet, user_wallet: NodeWallet, stranger_wallet: NodeWallet):
        TestRestContainers.wallet = default_wallet
        TestRestContainers.user_wallet = user_wallet
        TestRestContainers.stranger_wallet = stranger_wallet

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

    @allure.title("Test REST EACL account-based targets")
    def test_rest_container_eacl_account_targets(self, gw_endpoint: str):
        """
        Test eACL enforcement using account addresses as targets.

        Steps:
        1. Create container
        2. Set eACL: ALLOW DELETE for user_wallet account, DENY DELETE for OTHERS
        3. Upload two objects as owner
        4. Verify user_wallet (explicitly allowed account) can delete an object
        5. Verify stranger_wallet (falls under OTHERS, denied) cannot delete an object
        """
        rest_gw_address = get_rest_gateway_address(gw_endpoint)
        user_address = get_last_address_from_wallet(self.user_wallet.path, self.user_wallet.password)

        container_session_token = generate_session_token_v2(
            gw_endpoint, self.wallet, [{"verbs": ["CONTAINER_PUT", "CONTAINER_DELETE"]}], wallet_connect=True
        )
        cid = create_container(
            gw_endpoint,
            "rest_gw_eacl_account_targets",
            self.PLACEMENT_RULE,
            PUBLIC_ACL,
            container_session_token,
            wallet_connect=True,
        )

        owner_session_token = generate_session_token_v2(
            gw_endpoint,
            self.wallet,
            [{"containerID": cid, "verbs": ["CONTAINER_SET_EACL", "OBJECT_PUT", "OBJECT_DELETE"]}],
            targets=[rest_gw_address],
            wallet_connect=True,
        )

        with allure.step("Set eACL: ALLOW DELETE for user_wallet account, DENY DELETE for OTHERS"):
            eacl_records = [
                {
                    "action": "ALLOW",
                    "operation": "DELETE",
                    "filters": [],
                    "targets": [{"accounts": [user_address]}],
                },
                {
                    "action": "DENY",
                    "operation": "DELETE",
                    "filters": [],
                    "targets": [{"role": "OTHERS", "keys": []}],
                },
            ]
            put_response = put_container_eacl(gw_endpoint, cid, eacl_records, owner_session_token, wallet_connect=True)
            assert put_response.get("success") is True, f"Failed to set account-based eACL: {put_response}"

        with allure.step("Verify eACL records were persisted"):
            eacl_response = get_container_eacl(gw_endpoint, cid)
            assert len(eacl_response["records"]) == 2
            allow_record = eacl_response["records"][0]
            assert allow_record["action"] == "ALLOW"
            assert allow_record["targets"][0]["accounts"] == [user_address]
            deny_record = eacl_response["records"][1]
            assert deny_record["action"] == "DENY"
            assert deny_record["targets"][0]["role"] == "OTHERS"

        with allure.step("Upload two objects as owner"):
            test_file = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oid_for_user = upload_via_rest_gate(
                cid=cid, path=test_file, endpoint=gw_endpoint, session_token=owner_session_token
            )
            oid_for_stranger = upload_via_rest_gate(
                cid=cid, path=test_file, endpoint=gw_endpoint, session_token=owner_session_token
            )

        with allure.step("user_wallet (in ALLOW account list) can delete"):
            user_session_token = generate_session_token_v2(
                gw_endpoint,
                self.user_wallet,
                [{"containerID": cid, "verbs": ["OBJECT_DELETE"]}],
                targets=[rest_gw_address],
            )
            delete_object(gw_endpoint, cid, oid_for_user, user_session_token)

        with allure.step("stranger_wallet (OTHERS, denied) cannot delete"):
            stranger_session_token = generate_session_token_v2(
                gw_endpoint,
                self.stranger_wallet,
                [{"containerID": cid, "verbs": ["OBJECT_DELETE"]}],
                targets=[rest_gw_address],
            )
            try:
                delete_object(gw_endpoint, cid, oid_for_stranger, stranger_session_token)
                raise AssertionError("DELETE should have been denied for stranger account (OTHERS)")
            except AssertionError:
                raise
            except Exception as e:
                assert "403" in str(e) or "denied" in str(e).lower(), f"Expected access denied error, got: {e}"

        delete_container(gw_endpoint, cid, container_session_token, wallet_connect=True)

    @allure.title("Test REST EACL account-based targets - multiple accounts in one target")
    def test_rest_container_eacl_account_target_multiple_accounts(self, gw_endpoint: str):
        """
        Test that the accounts array inside a single target can list several addresses,
        applying one rule to all of them simultaneously.

        Steps:
        1. Create container
        2. Set eACL: DENY DELETE with [user_wallet, stranger_wallet] in one target record
        3. Upload two objects as owner
        4. Verify user_wallet cannot delete (matched by DENY rule)
        5. Verify stranger_wallet cannot delete (matched by the same DENY rule)
        6. Verify owner can still delete (owner bypasses eACL)
        """
        rest_gw_address = get_rest_gateway_address(gw_endpoint)
        user_address = get_last_address_from_wallet(self.user_wallet.path, self.user_wallet.password)
        stranger_address = get_last_address_from_wallet(self.stranger_wallet.path, self.stranger_wallet.password)

        container_session_token = generate_session_token_v2(
            gw_endpoint, self.wallet, [{"verbs": ["CONTAINER_PUT", "CONTAINER_DELETE"]}], wallet_connect=True
        )
        cid = create_container(
            gw_endpoint,
            "rest_gw_eacl_multi_accounts",
            self.PLACEMENT_RULE,
            PUBLIC_ACL,
            container_session_token,
            wallet_connect=True,
        )

        owner_session_token = generate_session_token_v2(
            gw_endpoint,
            self.wallet,
            [{"containerID": cid, "verbs": ["CONTAINER_SET_EACL", "OBJECT_PUT", "OBJECT_DELETE"]}],
            targets=[rest_gw_address],
            wallet_connect=True,
        )

        with allure.step("Set eACL: DENY DELETE with both accounts in one target"):
            eacl_records = [
                {
                    "action": "DENY",
                    "operation": "DELETE",
                    "filters": [],
                    "targets": [{"accounts": [user_address, stranger_address]}],
                },
            ]
            put_response = put_container_eacl(gw_endpoint, cid, eacl_records, owner_session_token, wallet_connect=True)
            assert put_response.get("success") is True, f"Failed to set multi-account eACL: {put_response}"

        with allure.step("Verify both addresses are stored in a single target record"):
            eacl_response = get_container_eacl(gw_endpoint, cid)
            assert len(eacl_response["records"]) == 1
            stored_accounts = eacl_response["records"][0]["targets"][0]["accounts"]
            assert user_address in stored_accounts
            assert stranger_address in stored_accounts

        with allure.step("Upload two objects as owner"):
            test_file = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oid_for_user = upload_via_rest_gate(
                cid=cid, path=test_file, endpoint=gw_endpoint, session_token=owner_session_token
            )
            oid_for_stranger = upload_via_rest_gate(
                cid=cid, path=test_file, endpoint=gw_endpoint, session_token=owner_session_token
            )

        with allure.step("user_wallet (in DENY accounts list) cannot delete"):
            user_session_token = generate_session_token_v2(
                gw_endpoint,
                self.user_wallet,
                [{"containerID": cid, "verbs": ["OBJECT_DELETE"]}],
                targets=[rest_gw_address],
            )
            try:
                delete_object(gw_endpoint, cid, oid_for_user, user_session_token)
                raise AssertionError("DELETE should have been denied for user account")
            except AssertionError:
                raise
            except Exception as e:
                assert "403" in str(e) or "denied" in str(e).lower(), f"Expected access denied error, got: {e}"

        with allure.step("stranger_wallet (in same DENY accounts list) cannot delete"):
            stranger_session_token = generate_session_token_v2(
                gw_endpoint,
                self.stranger_wallet,
                [{"containerID": cid, "verbs": ["OBJECT_DELETE"]}],
                targets=[rest_gw_address],
            )
            try:
                delete_object(gw_endpoint, cid, oid_for_stranger, stranger_session_token)
                raise AssertionError("DELETE should have been denied for stranger account")
            except AssertionError:
                raise
            except Exception as e:
                assert "403" in str(e) or "denied" in str(e).lower(), f"Expected access denied error, got: {e}"

        with allure.step("Owner (not in DENY list, bypasses eACL) can delete"):
            delete_object(gw_endpoint, cid, oid_for_user, owner_session_token)

        delete_container(gw_endpoint, cid, container_session_token, wallet_connect=True)

    @allure.title("Test REST EACL account-based targets - GET operation")
    def test_rest_container_eacl_account_target_get_operation(self, gw_endpoint: str):
        """
        Test account-based eACL enforcement for GET (read) operations.

        Steps:
        1. Create container and upload an object as owner
        2. Set eACL: ALLOW GET for user_wallet account, DENY GET for OTHERS
        3. Verify user_wallet can GET the object
        4. Verify stranger_wallet (falls under OTHERS) cannot GET the object (403)
        """
        rest_gw_address = get_rest_gateway_address(gw_endpoint)
        user_address = get_last_address_from_wallet(self.user_wallet.path, self.user_wallet.password)

        container_session_token = generate_session_token_v2(
            gw_endpoint, self.wallet, [{"verbs": ["CONTAINER_PUT", "CONTAINER_DELETE"]}], wallet_connect=True
        )
        cid = create_container(
            gw_endpoint,
            "rest_gw_eacl_account_get",
            self.PLACEMENT_RULE,
            PUBLIC_ACL,
            container_session_token,
            wallet_connect=True,
        )

        owner_session_token = generate_session_token_v2(
            gw_endpoint,
            self.wallet,
            [{"containerID": cid, "verbs": ["CONTAINER_SET_EACL", "OBJECT_PUT"]}],
            targets=[rest_gw_address],
            wallet_connect=True,
        )

        with allure.step("Upload an object as owner"):
            test_file = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oid = upload_via_rest_gate(cid=cid, path=test_file, endpoint=gw_endpoint, session_token=owner_session_token)

        with allure.step("Set eACL: ALLOW GET for user_wallet account, DENY GET for OTHERS"):
            eacl_records = [
                {
                    "action": "ALLOW",
                    "operation": "GET",
                    "filters": [],
                    "targets": [{"accounts": [user_address]}],
                },
                {
                    "action": "DENY",
                    "operation": "GET",
                    "filters": [],
                    "targets": [{"role": "OTHERS", "keys": []}],
                },
            ]
            put_response = put_container_eacl(gw_endpoint, cid, eacl_records, owner_session_token, wallet_connect=True)
            assert put_response.get("success") is True, f"Failed to set GET eACL: {put_response}"

        with allure.step("user_wallet (in ALLOW account list) can GET the object"):
            user_get_token = generate_session_token_v2(
                gw_endpoint,
                self.user_wallet,
                [{"containerID": cid, "verbs": ["OBJECT_GET"]}],
                targets=[rest_gw_address],
            )
            resp = get_via_rest_gate(cid, oid, gw_endpoint, session_token=user_get_token, return_response=True)
            assert resp.ok, f"user_wallet should be able to GET the object, got: {resp.status_code}"

        with allure.step("stranger_wallet (OTHERS, denied) cannot GET the object"):
            stranger_get_token = generate_session_token_v2(
                gw_endpoint,
                self.stranger_wallet,
                [{"containerID": cid, "verbs": ["OBJECT_GET"]}],
                targets=[rest_gw_address],
            )
            resp = get_via_rest_gate(cid, oid, gw_endpoint, session_token=stranger_get_token, expect_error=True)
            assert not resp.ok, "stranger_wallet should be denied GET (OTHERS)"
            assert resp.status_code == 403, f"Expected 403, got: {resp.status_code}"

        delete_container(gw_endpoint, cid, container_session_token, wallet_connect=True)

    @allure.title("Test REST EACL account-based targets - overwrite replaces account restrictions")
    def test_rest_container_eacl_account_targets_overwrite(self, gw_endpoint: str):
        """
        Test that replacing (overwriting) the eACL table correctly removes the old account
        restrictions and activates the new ones, verifying eACL mutability for account targets.

        Steps:
        1. Create container and upload four objects as owner
        2. Set eACL v1: DENY DELETE for user_wallet account only
        3. Verify user_wallet cannot delete, stranger_wallet can delete (falls through to basic ACL)
        4. Overwrite eACL with v2: DENY DELETE for stranger_wallet account only
        5. Verify stranger_wallet cannot delete, user_wallet can now delete
        """
        rest_gw_address = get_rest_gateway_address(gw_endpoint)
        user_address = get_last_address_from_wallet(self.user_wallet.path, self.user_wallet.password)
        stranger_address = get_last_address_from_wallet(self.stranger_wallet.path, self.stranger_wallet.password)

        container_session_token = generate_session_token_v2(
            gw_endpoint, self.wallet, [{"verbs": ["CONTAINER_PUT", "CONTAINER_DELETE"]}], wallet_connect=True
        )
        cid = create_container(
            gw_endpoint,
            "rest_gw_eacl_account_overwrite",
            self.PLACEMENT_RULE,
            PUBLIC_ACL,
            container_session_token,
            wallet_connect=True,
        )

        owner_session_token = generate_session_token_v2(
            gw_endpoint,
            self.wallet,
            [{"containerID": cid, "verbs": ["CONTAINER_SET_EACL", "OBJECT_PUT", "OBJECT_DELETE"]}],
            targets=[rest_gw_address],
            wallet_connect=True,
        )
        user_delete_token = generate_session_token_v2(
            gw_endpoint,
            self.user_wallet,
            [{"containerID": cid, "verbs": ["OBJECT_DELETE"]}],
            targets=[rest_gw_address],
        )
        stranger_delete_token = generate_session_token_v2(
            gw_endpoint,
            self.stranger_wallet,
            [{"containerID": cid, "verbs": ["OBJECT_DELETE"]}],
            targets=[rest_gw_address],
        )

        with allure.step("Upload four objects as owner"):
            test_file = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oids = [
                upload_via_rest_gate(cid=cid, path=test_file, endpoint=gw_endpoint, session_token=owner_session_token)
                for _ in range(4)
            ]

        with allure.step("Set eACL v1: DENY DELETE for user_wallet only"):
            eacl_v1 = [
                {
                    "action": "DENY",
                    "operation": "DELETE",
                    "filters": [],
                    "targets": [{"accounts": [user_address]}],
                },
            ]
            put_response = put_container_eacl(gw_endpoint, cid, eacl_v1, owner_session_token, wallet_connect=True)
            assert put_response.get("success") is True, f"Failed to set eACL v1: {put_response}"

        with allure.step("eACL v1: user_wallet (denied) cannot delete"):
            try:
                delete_object(gw_endpoint, cid, oids[0], user_delete_token)
                raise AssertionError("DELETE should have been denied for user_wallet under eACL v1")
            except AssertionError:
                raise
            except Exception as e:
                assert "403" in str(e) or "denied" in str(e).lower(), f"Expected access denied error, got: {e}"

        with allure.step("eACL v1: stranger_wallet (not in any rule, falls through to basic ACL) can delete"):
            delete_object(gw_endpoint, cid, oids[1], stranger_delete_token)

        with allure.step("Overwrite eACL with v2: DENY DELETE for stranger_wallet only"):
            eacl_v2 = [
                {
                    "action": "DENY",
                    "operation": "DELETE",
                    "filters": [],
                    "targets": [{"accounts": [stranger_address]}],
                },
            ]
            put_response = put_container_eacl(gw_endpoint, cid, eacl_v2, owner_session_token, wallet_connect=True)
            assert put_response.get("success") is True, f"Failed to overwrite eACL v2: {put_response}"

        with allure.step("Verify eACL was replaced: only stranger_wallet address remains in records"):
            eacl_response = get_container_eacl(gw_endpoint, cid)
            assert len(eacl_response["records"]) == 1
            assert eacl_response["records"][0]["targets"][0]["accounts"] == [stranger_address]

        with allure.step("eACL v2: user_wallet (no longer denied) can delete"):
            delete_object(gw_endpoint, cid, oids[2], user_delete_token)

        with allure.step("eACL v2: stranger_wallet (now denied) cannot delete"):
            try:
                delete_object(gw_endpoint, cid, oids[3], stranger_delete_token)
                raise AssertionError("DELETE should have been denied for stranger_wallet under eACL v2")
            except AssertionError:
                raise
            except Exception as e:
                assert "403" in str(e) or "denied" in str(e).lower(), f"Expected access denied error, got: {e}"

        delete_container(gw_endpoint, cid, container_session_token, wallet_connect=True)

    @allure.title("Test REST EACL owner doesn't bypass account-based rules")
    def test_rest_container_eacl_owner_no_bypass(self, gw_endpoint: str):
        """
        Test that the owner does NOT bypass eACL rules when explicitly targeted in account-based eACL.
        This verifies that when the owner account is listed in a DENY rule or not listed in an ALLOW rule,
        the owner's operations are correctly restricted.

        Steps:
        1. Create container as owner
        2. Test scenario 1: DENY DELETE for owner account
           - Set eACL with DENY DELETE targeting owner's account
           - Upload object as owner
           - Verify owner cannot delete the object (403)
        3. Test scenario 2: ALLOW PUT for non-owner account only, DENY PUT for owner and OTHERS
           - Set eACL with ALLOW PUT for user_wallet + DENY PUT for owner account + DENY PUT for OTHERS
           - Verify owner (explicitly in DENY PUT rule, role USER) cannot PUT new objects (403)
           - Verify user_wallet can PUT objects (explicitly allowed)
        """
        rest_gw_address = get_rest_gateway_address(gw_endpoint)
        owner_address = get_last_address_from_wallet(self.wallet.path, self.wallet.password)
        user_address = get_last_address_from_wallet(self.user_wallet.path, self.user_wallet.password)

        container_session_token = generate_session_token_v2(
            gw_endpoint, self.wallet, [{"verbs": ["CONTAINER_PUT", "CONTAINER_DELETE"]}], wallet_connect=True
        )
        cid = create_container(
            gw_endpoint,
            "rest_gw_eacl_owner_no_bypass",
            self.PLACEMENT_RULE,
            PUBLIC_ACL,
            container_session_token,
            wallet_connect=True,
        )

        # Scenario 1: DENY DELETE for owner account
        with allure.step("Scenario 1: Set eACL with DENY DELETE targeting owner account"):
            owner_session_token = generate_session_token_v2(
                gw_endpoint,
                self.wallet,
                [{"containerID": cid, "verbs": ["CONTAINER_SET_EACL", "OBJECT_PUT", "OBJECT_DELETE"]}],
                targets=[rest_gw_address],
                wallet_connect=True,
            )

            eacl_records = [
                {
                    "action": "DENY",
                    "operation": "DELETE",
                    "filters": [],
                    "targets": [{"accounts": [owner_address]}],
                },
            ]
            put_response = put_container_eacl(gw_endpoint, cid, eacl_records, owner_session_token, wallet_connect=True)
            assert put_response.get("success") is True, f"Failed to set DENY DELETE eACL for owner: {put_response}"

        with allure.step("Verify eACL was set with owner account in DENY DELETE"):
            eacl_response = get_container_eacl(gw_endpoint, cid)
            assert len(eacl_response["records"]) == 1
            assert eacl_response["records"][0]["action"] == "DENY"
            assert eacl_response["records"][0]["operation"] == "DELETE"
            assert eacl_response["records"][0]["targets"][0]["accounts"] == [owner_address]

        with allure.step("Upload object as owner"):
            test_file = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oid_deny_delete = upload_via_rest_gate(
                cid=cid, path=test_file, endpoint=gw_endpoint, session_token=owner_session_token
            )

        with allure.step("Owner (in DENY DELETE list) cannot delete the object"):
            try:
                delete_object(gw_endpoint, cid, oid_deny_delete, owner_session_token)
                raise AssertionError("DELETE should have been denied for owner account when explicitly in DENY rule")
            except AssertionError:
                raise
            except Exception as e:
                assert "403" in str(e) or "denied" in str(e).lower(), f"Expected access denied error, got: {e}"

        # Scenario 2: ALLOW PUT for non-owner account only
        with allure.step("Scenario 2: Set eACL with ALLOW PUT for user_wallet, DENY PUT for owner and OTHERS"):
            owner_set_eacl_token = generate_session_token_v2(
                gw_endpoint,
                self.wallet,
                [{"containerID": cid, "verbs": ["CONTAINER_SET_EACL"]}],
                targets=[rest_gw_address],
                wallet_connect=True,
            )

            eacl_records = [
                {
                    "action": "ALLOW",
                    "operation": "PUT",
                    "filters": [],
                    "targets": [{"accounts": [user_address]}],
                },
                {
                    "action": "DENY",
                    "operation": "PUT",
                    "filters": [],
                    "targets": [{"accounts": [owner_address]}],
                },
                {
                    "action": "DENY",
                    "operation": "PUT",
                    "filters": [],
                    "targets": [{"role": "OTHERS", "keys": []}],
                },
            ]
            put_response = put_container_eacl(gw_endpoint, cid, eacl_records, owner_set_eacl_token, wallet_connect=True)
            assert put_response.get("success") is True, f"Failed to set ALLOW PUT eACL for user only: {put_response}"

        with allure.step("Verify eACL was set with user_wallet in ALLOW PUT, owner and OTHERS in DENY PUT"):
            eacl_response = get_container_eacl(gw_endpoint, cid)
            assert len(eacl_response["records"]) == 3
            assert eacl_response["records"][0]["action"] == "ALLOW"
            assert eacl_response["records"][0]["operation"] == "PUT"
            assert eacl_response["records"][0]["targets"][0]["accounts"] == [user_address]
            assert eacl_response["records"][1]["action"] == "DENY"
            assert eacl_response["records"][1]["operation"] == "PUT"
            assert eacl_response["records"][1]["targets"][0]["accounts"] == [owner_address]
            assert eacl_response["records"][2]["action"] == "DENY"
            assert eacl_response["records"][2]["operation"] == "PUT"
            assert eacl_response["records"][2]["targets"][0]["role"] == "OTHERS"

        with allure.step("Owner (explicitly in DENY PUT list) cannot upload new object"):
            owner_put_token = generate_session_token_v2(
                gw_endpoint,
                self.wallet,
                [{"containerID": cid, "verbs": ["OBJECT_PUT"]}],
                targets=[rest_gw_address],
            )
            test_file = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            try:
                upload_via_rest_gate(cid=cid, path=test_file, endpoint=gw_endpoint, session_token=owner_put_token)
                raise AssertionError("PUT should have been denied for owner (explicitly in DENY PUT rule)")
            except AssertionError:
                raise
            except Exception as e:
                assert "403" in str(e) or "denied" in str(e).lower(), f"Expected access denied error, got: {e}"

        with allure.step("user_wallet (in ALLOW PUT list) can upload new object"):
            user_put_token = generate_session_token_v2(
                gw_endpoint,
                self.user_wallet,
                [{"containerID": cid, "verbs": ["OBJECT_PUT"]}],
                targets=[rest_gw_address],
            )
            test_file = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oid_user_put = upload_via_rest_gate(
                cid=cid, path=test_file, endpoint=gw_endpoint, session_token=user_put_token
            )
            assert oid_user_put, "user_wallet should be able to upload object when in ALLOW PUT rule"

        delete_container(gw_endpoint, cid, container_session_token, wallet_connect=True)
