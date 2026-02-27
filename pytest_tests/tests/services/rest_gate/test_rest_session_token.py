import logging
import time
import uuid

import allure
import pytest
from helpers.file_helper import generate_file
from helpers.nns import get_contract_hashes, register_nns_domain_with_record
from helpers.rest_gate import (
    create_container,
    delete_container,
    delete_object,
    get_container_eacl,
    get_via_rest_gate,
    head_via_rest_gate,
    searchv2,
    upload_via_rest_gate,
)
from helpers.wellknown_acl import PUBLIC_ACL
from neofs_testlib.env.env import NodeWallet
from neofs_testlib.utils.wallet import get_last_address_from_wallet
from rest_gw.rest_base import TestNeofsRestBase
from rest_gw.rest_utils import generate_session_token_v2

logger = logging.getLogger("NeoLogger")


def unique_container_name() -> str:
    return f"rest_session_{uuid.uuid4()}"


class TestRestSessionTokenV2(TestNeofsRestBase):
    PLACEMENT_RULE = "REP 2 IN X CBF 1 SELECT 4 FROM * AS X"

    @pytest.fixture(scope="class", autouse=True)
    @allure.title("[Class/Autouse]: Prepare wallet")
    def prepare_wallet(self, default_wallet: NodeWallet, user_wallet: NodeWallet, stranger_wallet: NodeWallet):
        TestRestSessionTokenV2.owner_wallet = default_wallet
        TestRestSessionTokenV2.user_wallet = user_wallet
        TestRestSessionTokenV2.stranger_wallet = stranger_wallet

    @allure.title("Test V2 Session Token - Object Operations via REST")
    @pytest.mark.parametrize(
        "object_size,wallet_connect",
        [
            pytest.param("simple_object_size", False, id="simple-deterministic", marks=pytest.mark.simple),
            # pytest.param("simple_object_size", True, id="simple-walletconnect", marks=pytest.mark.simple),
        ],
    )
    def test_rest_v2_session_token_object_operations(self, gw_endpoint: str, object_size: str, wallet_connect: bool):
        """
        Test object operations with V2 session token via REST API.

        Steps:
        1. Create container
        2. Create V2 session token with all object verbs
        3. Upload objects using session token
        4. Get object using session token
        5. Head object using session token
        6. Search objects using session token
        7. Get object range using session token
        8. Get object range hash using session token
        9. Delete object using session token
        """
        with allure.step("Create container"):
            container_token = generate_session_token_v2(gw_endpoint, self.owner_wallet, [{"verbs": ["CONTAINER_PUT"]}])
            cid = create_container(
                gw_endpoint,
                unique_container_name(),
                self.PLACEMENT_RULE,
                PUBLIC_ACL,
                container_token,
            )

        with allure.step("Create V2 Session Token for object operations"):
            contexts = [
                {
                    "containerID": cid,
                    "verbs": [
                        "OBJECT_PUT",
                        "OBJECT_GET",
                        "OBJECT_HEAD",
                        "OBJECT_DELETE",
                        "OBJECT_SEARCH",
                        "OBJECT_RANGE",
                    ],
                }
            ]
            rest_gw_address = get_last_address_from_wallet(
                self.neofs_env.rest_gw.wallet.path, self.neofs_env.rest_gw.wallet.password
            )
            session_token = generate_session_token_v2(
                gw_endpoint, self.owner_wallet, contexts, targets=[rest_gw_address], wallet_connect=wallet_connect
            )

        with allure.step("Upload objects with session token"):
            file_path = generate_file(self.neofs_env.get_object_size(object_size))
            oid = upload_via_rest_gate(cid, file_path, gw_endpoint, session_token=session_token)
            assert oid, "Object ID should be returned"

        with allure.step("Get object with session token"):
            got_file_path = get_via_rest_gate(cid, oid, gw_endpoint, session_token=session_token)
            assert got_file_path, "File should be downloaded"

        with allure.step("Head object with session token"):
            resp = head_via_rest_gate(cid, oid, gw_endpoint, session_token=session_token)
            assert resp.ok, f"Failed to head object: {resp.text}"
            assert resp.headers["X-Object-Id"] == oid

        with allure.step("Search objects with session token"):
            search_results = searchv2(gw_endpoint, cid, session_token=session_token, filters=[])
            assert "objects" in search_results, "Search results should contain objects"

        with allure.step("Get object range with session token"):
            resp = get_via_rest_gate(
                cid,
                oid,
                gw_endpoint,
                session_token=session_token,
                headers={"Range": "bytes=0-99"},
                return_response=True,
            )
            assert resp.ok, f"Failed to get object range: {resp.text}"
            assert resp.status_code == 206 or resp.status_code == 200, "Should return partial content or full content"

        with allure.step("Delete object with session token"):
            delete_object(gw_endpoint, cid, oid, session_token)

        with allure.step("Verify object was deleted"):
            try:
                get_via_rest_gate(cid, oid, gw_endpoint)
                raise AssertionError("Object should not exist after deletion")
            except Exception as e:
                assert "Failed to get object" in str(e), f"Expected object deletion error, got: {e}"

    @allure.title("Test V2 Session Token - Wildcard Container")
    @pytest.mark.simple
    def test_rest_v2_session_token_wildcard_container(self, gw_endpoint: str):
        """
        Test V2 session token with wildcard container (omit containerID) allows operations on any container.

        Steps:
        1. Create V2 session token with wildcard container and object verbs
        2. Create two containers
        3. Upload objects to both containers using the same token
        4. Verify token works on both containers
        """
        with allure.step("Create V2 Session Token with wildcard container"):
            contexts = [{"verbs": ["OBJECT_PUT", "OBJECT_GET", "OBJECT_HEAD"]}]
            rest_gw_address = get_last_address_from_wallet(
                self.neofs_env.rest_gw.wallet.path, self.neofs_env.rest_gw.wallet.password
            )
            session_token = generate_session_token_v2(
                gw_endpoint, self.owner_wallet, contexts, targets=[rest_gw_address]
            )

        with allure.step("Create first container and upload object"):
            container_token = generate_session_token_v2(gw_endpoint, self.owner_wallet, [{"verbs": ["CONTAINER_PUT"]}])
            cid1 = create_container(
                gw_endpoint, unique_container_name(), self.PLACEMENT_RULE, PUBLIC_ACL, container_token
            )
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oid1 = upload_via_rest_gate(cid1, file_path, gw_endpoint, session_token=session_token)
            assert oid1, "Object ID should be returned"

        with allure.step("Verify GET on first container"):
            resp = get_via_rest_gate(cid1, oid1, gw_endpoint, session_token=session_token, return_response=True)
            assert resp.ok, f"Failed to get object from first container: {resp.text}"

        with allure.step("Create second container and upload object"):
            cid2 = create_container(
                gw_endpoint, unique_container_name(), self.PLACEMENT_RULE, PUBLIC_ACL, container_token
            )
            oid2 = upload_via_rest_gate(cid2, file_path, gw_endpoint, session_token=session_token)
            assert oid2, "Object ID should be returned"

        with allure.step("Verify GET on second container"):
            resp = get_via_rest_gate(cid2, oid2, gw_endpoint, session_token=session_token, return_response=True)
            assert resp.ok, f"Failed to get object from second container: {resp.text}"

    @allure.title("Test V2 Session Token - Multiple Contexts")
    @pytest.mark.simple
    def test_rest_v2_session_token_multiple_contexts(self, gw_endpoint: str):
        """
        Test V2 session token with multiple container contexts.

        Steps:
        1. Create two containers
        2. Create V2 session token for uploading to both containers
        3. Upload objects to both containers using token
        4. Create V2 session token with GET contexts for both containers
        5. Verify GET token works on both containers
        """
        with allure.step("Create containers"):
            container_token = generate_session_token_v2(gw_endpoint, self.owner_wallet, [{"verbs": ["CONTAINER_PUT"]}])
            cid1 = create_container(
                gw_endpoint, unique_container_name(), self.PLACEMENT_RULE, PUBLIC_ACL, container_token
            )
            cid2 = create_container(
                gw_endpoint, unique_container_name(), self.PLACEMENT_RULE, PUBLIC_ACL, container_token
            )
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))

        with allure.step("Create session token for uploads"):
            rest_gw_address = get_last_address_from_wallet(
                self.neofs_env.rest_gw.wallet.path, self.neofs_env.rest_gw.wallet.password
            )
            upload_contexts = [
                {"containerID": cid1, "verbs": ["OBJECT_PUT"]},
                {"containerID": cid2, "verbs": ["OBJECT_PUT"]},
            ]
            upload_token = generate_session_token_v2(
                gw_endpoint, self.owner_wallet, upload_contexts, targets=[rest_gw_address]
            )

        with allure.step("Upload objects with session token"):
            oid1 = upload_via_rest_gate(cid1, file_path, gw_endpoint, session_token=upload_token)
            oid2 = upload_via_rest_gate(cid2, file_path, gw_endpoint, session_token=upload_token)

        with allure.step("Create V2 Session Token with multiple contexts"):
            contexts = [
                {"containerID": cid1, "verbs": ["OBJECT_GET", "OBJECT_HEAD"]},
                {"containerID": cid2, "verbs": ["OBJECT_GET", "OBJECT_HEAD"]},
            ]
            rest_gw_address = get_last_address_from_wallet(
                self.neofs_env.rest_gw.wallet.path, self.neofs_env.rest_gw.wallet.password
            )
            session_token = generate_session_token_v2(
                gw_endpoint, self.owner_wallet, contexts, targets=[rest_gw_address]
            )

        with allure.step("Verify operations on first container"):
            resp = get_via_rest_gate(cid1, oid1, gw_endpoint, session_token=session_token, return_response=True)
            assert resp.ok, f"Failed to get object from first container: {resp.text}"

        with allure.step("Verify operations on second container"):
            resp = get_via_rest_gate(cid2, oid2, gw_endpoint, session_token=session_token, return_response=True)
            assert resp.ok, f"Failed to get object from second container: {resp.text}"

    @allure.title("Test V2 Session Token - Token Expiration")
    @pytest.mark.simple
    def test_rest_v2_session_token_expiration(self, gw_endpoint: str):
        """
        Test V2 session token expiration.

        Steps:
        1. Create container and upload object
        2. Create V2 session token with short lifetime (10 seconds)
        3. Verify token works initially
        4. Wait for expiration
        5. Verify token is rejected after expiration
        """
        with allure.step("Create container"):
            container_token = generate_session_token_v2(gw_endpoint, self.owner_wallet, [{"verbs": ["CONTAINER_PUT"]}])
            cid = create_container(
                gw_endpoint, unique_container_name(), self.PLACEMENT_RULE, PUBLIC_ACL, container_token
            )
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))

        with allure.step("Upload object with session token"):
            rest_gw_address = get_last_address_from_wallet(
                self.neofs_env.rest_gw.wallet.path, self.neofs_env.rest_gw.wallet.password
            )
            upload_token = generate_session_token_v2(
                gw_endpoint,
                self.owner_wallet,
                [{"containerID": cid, "verbs": ["OBJECT_PUT"]}],
                targets=[rest_gw_address],
            )
            oid = upload_via_rest_gate(cid, file_path, gw_endpoint, session_token=upload_token)

        with allure.step("Create V2 Session Token with short lifetime"):
            contexts = [{"containerID": cid, "verbs": ["OBJECT_GET", "OBJECT_HEAD"]}]
            session_token = generate_session_token_v2(
                gw_endpoint, self.owner_wallet, contexts, targets=[rest_gw_address], lifetime=30
            )

        with allure.step("Verify token works before expiration"):
            resp = get_via_rest_gate(cid, oid, gw_endpoint, session_token=session_token, return_response=True)
            assert resp.ok, f"Token should work before expiration: {resp.text}"

        with allure.step("Wait for token expiration"):
            time.sleep(32)

        with allure.step("Verify token is rejected after expiration"):
            resp = get_via_rest_gate(cid, oid, gw_endpoint, session_token=session_token, expect_error=True)
            assert not resp.ok, "Token should be rejected after expiration"
            assert "expired" in resp.text.lower() or "session" in resp.text.lower()

    @allure.title("Test V2 Session Token - Unauthorized Container")
    @pytest.mark.simple
    def test_rest_v2_session_token_unauthorized_container(self, gw_endpoint: str):
        """
        Test that V2 session token rejects operations on unauthorized containers.

        Steps:
        1. Create two containers
        2. Upload objects with session token
        3. Create V2 session token for only first container
        4. Verify token works for first container
        5. Verify token is rejected for second container
        """
        with allure.step("Create containers"):
            container_token = generate_session_token_v2(gw_endpoint, self.owner_wallet, [{"verbs": ["CONTAINER_PUT"]}])
            cid1 = create_container(
                gw_endpoint, unique_container_name(), self.PLACEMENT_RULE, PUBLIC_ACL, container_token
            )
            cid2 = create_container(
                gw_endpoint, unique_container_name(), self.PLACEMENT_RULE, PUBLIC_ACL, container_token
            )
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))

        with allure.step("Upload objects with session token"):
            rest_gw_address = get_last_address_from_wallet(
                self.neofs_env.rest_gw.wallet.path, self.neofs_env.rest_gw.wallet.password
            )
            upload_token = generate_session_token_v2(
                gw_endpoint, self.owner_wallet, [{"verbs": ["OBJECT_PUT"]}], targets=[rest_gw_address]
            )
            oid1 = upload_via_rest_gate(cid1, file_path, gw_endpoint, session_token=upload_token)
            oid2 = upload_via_rest_gate(cid2, file_path, gw_endpoint, session_token=upload_token)

        with allure.step("Create V2 Session Token for only first container"):
            contexts = [{"containerID": cid1, "verbs": ["OBJECT_GET", "OBJECT_HEAD"]}]
            session_token = generate_session_token_v2(
                gw_endpoint, self.owner_wallet, contexts, targets=[rest_gw_address]
            )

        with allure.step("Verify token works for first container"):
            resp = get_via_rest_gate(cid1, oid1, gw_endpoint, session_token=session_token, return_response=True)
            assert resp.ok, f"Token should work for authorized container: {resp.text}"

        with allure.step("Verify token is rejected for second container"):
            resp = get_via_rest_gate(cid2, oid2, gw_endpoint, session_token=session_token, expect_error=True)
            assert not resp.ok, "Token should be rejected for unauthorized container"

    @allure.title("Test V2 Session Token - Unauthorized Verbs")
    @pytest.mark.simple
    def test_rest_v2_session_token_unauthorized_verbs(self, gw_endpoint: str):
        """
        Test that operations with unauthorized verbs are rejected.

        Steps:
        1. Create container
        2. Upload object with session token
        3. Create V2 session token with only OBJECT_GET verb
        4. Verify GET operation works
        5. Verify PUT operation is rejected
        """
        with allure.step("Create container"):
            container_token = generate_session_token_v2(gw_endpoint, self.owner_wallet, [{"verbs": ["CONTAINER_PUT"]}])
            cid = create_container(
                gw_endpoint, unique_container_name(), self.PLACEMENT_RULE, PUBLIC_ACL, container_token
            )
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))

        with allure.step("Upload object with session token"):
            rest_gw_address = get_last_address_from_wallet(
                self.neofs_env.rest_gw.wallet.path, self.neofs_env.rest_gw.wallet.password
            )
            upload_token = generate_session_token_v2(
                gw_endpoint,
                self.owner_wallet,
                [{"containerID": cid, "verbs": ["OBJECT_PUT"]}],
                targets=[rest_gw_address],
            )
            oid = upload_via_rest_gate(cid, file_path, gw_endpoint, session_token=upload_token)

        with allure.step("Create V2 Session Token with only GET verb"):
            contexts = [{"containerID": cid, "verbs": ["OBJECT_GET"]}]
            session_token = generate_session_token_v2(
                gw_endpoint, self.owner_wallet, contexts, targets=[rest_gw_address]
            )

        with allure.step("Verify GET operation works"):
            resp = get_via_rest_gate(cid, oid, gw_endpoint, session_token=session_token, return_response=True)
            assert resp.ok, f"GET should work: {resp.text}"

        with allure.step("Verify PUT operation is rejected"):
            new_oid = upload_via_rest_gate(
                cid, file_path, gw_endpoint, session_token=session_token, error_pattern="invalid"
            )
            assert not new_oid, "PUT should be rejected"

    @allure.title("Test V2 Session Token - Delegation Chain")
    @pytest.mark.simple
    def test_rest_v2_session_token_delegation(self, gw_endpoint: str):
        """
        Test V2 session token delegation using origin token.

        Steps:
        1. Create container and upload object
        2. Create original V2 session token
        3. Create delegated token using origin parameter
        4. Verify delegated token works
        """
        with allure.step("Create container"):
            container_token = generate_session_token_v2(gw_endpoint, self.owner_wallet, [{"verbs": ["CONTAINER_PUT"]}])
            cid = create_container(
                gw_endpoint, unique_container_name(), self.PLACEMENT_RULE, PUBLIC_ACL, container_token
            )
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))

        with allure.step("Get addresses"):
            rest_gw_address = get_last_address_from_wallet(
                self.neofs_env.rest_gw.wallet.path, self.neofs_env.rest_gw.wallet.password
            )

        with allure.step("Upload object with session token"):
            upload_token = generate_session_token_v2(
                gw_endpoint,
                self.owner_wallet,
                [{"containerID": cid, "verbs": ["OBJECT_PUT"]}],
                targets=[rest_gw_address],
            )
            oid = upload_via_rest_gate(cid, file_path, gw_endpoint, session_token=upload_token)

        with allure.step("Create original V2 Session Token"):
            contexts = [{"containerID": cid, "verbs": ["OBJECT_GET", "OBJECT_HEAD"]}]
            original_token = generate_session_token_v2(
                gw_endpoint, self.owner_wallet, contexts, targets=[rest_gw_address]
            )

        with allure.step("Create delegated token"):
            reduced_contexts = [{"containerID": cid, "verbs": ["OBJECT_GET"]}]
            delegated_token = generate_session_token_v2(
                gw_endpoint, self.user_wallet, reduced_contexts, targets=[rest_gw_address], origin=original_token
            )

        with allure.step("Verify delegated token works"):
            resp = get_via_rest_gate(cid, oid, gw_endpoint, session_token=delegated_token, return_response=True)
            assert resp.ok, f"Delegated token should work: {resp.text}"

    @allure.title("Test V2 Session Token - Final Flag Prevents Delegation")
    @pytest.mark.simple
    def test_rest_v2_session_token_final_flag(self, gw_endpoint: str):
        """
        Test that V2 session token with final flag cannot be used as origin.

        Steps:
        1. Create container
        2. Create V2 session token with final=True
        3. Attempt to create delegated token (should fail)
        """
        with allure.step("Create container"):
            container_token = generate_session_token_v2(gw_endpoint, self.owner_wallet, [{"verbs": ["CONTAINER_PUT"]}])
            cid = create_container(
                gw_endpoint, unique_container_name(), self.PLACEMENT_RULE, PUBLIC_ACL, container_token
            )

        with allure.step("Get addresses"):
            user_address = get_last_address_from_wallet(self.user_wallet.path, self.user_wallet.password)

        with allure.step("Create V2 Session Token with final flag"):
            contexts = [{"containerID": cid, "verbs": ["OBJECT_GET", "OBJECT_HEAD"]}]
            final_token = generate_session_token_v2(
                gw_endpoint, self.owner_wallet, contexts, targets=[user_address], final=True
            )

        with allure.step("Attempt to create delegated token (should fail)"):
            with pytest.raises(Exception):
                generate_session_token_v2(
                    gw_endpoint, self.user_wallet, contexts, targets=[user_address], origin=final_token
                )

    @allure.title("Test V2 Session Token - Cannot Extend Verbs in Delegation")
    @pytest.mark.simple
    def test_rest_v2_session_token_cannot_extend_verbs(self, gw_endpoint: str):
        """
        Test that delegated token cannot have more verbs than original.

        Steps:
        1. Create container
        2. Create original token with limited verbs (GET, HEAD)
        3. Attempt to create delegated token with more verbs (GET, HEAD, PUT)
        4. Verify delegation fails
        """
        with allure.step("Create container"):
            container_token = generate_session_token_v2(gw_endpoint, self.owner_wallet, [{"verbs": ["CONTAINER_PUT"]}])
            cid = create_container(
                gw_endpoint, unique_container_name(), self.PLACEMENT_RULE, PUBLIC_ACL, container_token
            )

        with allure.step("Get addresses"):
            user_address = get_last_address_from_wallet(self.user_wallet.path, self.user_wallet.password)

        with allure.step("Create original token with limited verbs"):
            contexts = [{"containerID": cid, "verbs": ["OBJECT_GET", "OBJECT_HEAD"]}]
            original_token = generate_session_token_v2(gw_endpoint, self.owner_wallet, contexts, targets=[user_address])

        with allure.step("Attempt to create delegated token with more verbs"):
            extended_contexts = [{"containerID": cid, "verbs": ["OBJECT_GET", "OBJECT_HEAD", "OBJECT_PUT"]}]
            with pytest.raises(Exception):
                generate_session_token_v2(
                    gw_endpoint,
                    self.user_wallet,
                    extended_contexts,
                    targets=[user_address],
                    lifetime=900,
                    origin=original_token,
                )

    @allure.title("Test V2 Session Token - Cannot Extend Containers in Delegation")
    @pytest.mark.simple
    def test_rest_v2_session_token_cannot_extend_containers(self, gw_endpoint: str):
        """
        Test that delegated token cannot add containers beyond original.

        Steps:
        1. Create two containers
        2. Create original token for first container only
        3. Attempt to create delegated token for both containers
        4. Verify delegation fails
        """
        with allure.step("Create two containers"):
            container_token = generate_session_token_v2(gw_endpoint, self.owner_wallet, [{"verbs": ["CONTAINER_PUT"]}])
            cid1 = create_container(
                gw_endpoint, unique_container_name(), self.PLACEMENT_RULE, PUBLIC_ACL, container_token
            )
            cid2 = create_container(
                gw_endpoint, unique_container_name(), self.PLACEMENT_RULE, PUBLIC_ACL, container_token
            )

        with allure.step("Get addresses"):
            user_address = get_last_address_from_wallet(self.user_wallet.path, self.user_wallet.password)

        with allure.step("Create original token for first container only"):
            contexts = [{"containerID": cid1, "verbs": ["OBJECT_GET", "OBJECT_HEAD"]}]
            original_token = generate_session_token_v2(gw_endpoint, self.owner_wallet, contexts, targets=[user_address])

        with allure.step("Attempt to create delegated token with additional container"):
            extended_contexts = [
                {"containerID": cid1, "verbs": ["OBJECT_GET", "OBJECT_HEAD"]},
                {"containerID": cid2, "verbs": ["OBJECT_GET", "OBJECT_HEAD"]},
            ]
            with pytest.raises(Exception):
                generate_session_token_v2(
                    gw_endpoint,
                    self.user_wallet,
                    extended_contexts,
                    targets=[user_address],
                    lifetime=900,
                    origin=original_token,
                )

    @allure.title("Test V2 Session Token - Cannot Extend Wildcard from Specific Container")
    @pytest.mark.simple
    def test_rest_v2_session_token_cannot_extend_to_wildcard(self, gw_endpoint: str):
        """
        Test that delegated token cannot extend specific container to wildcard.

        Steps:
        1. Create container
        2. Create original token for specific container
        3. Attempt to create delegated token with wildcard container
        4. Verify delegation fails
        """
        with allure.step("Create container"):
            container_token = generate_session_token_v2(gw_endpoint, self.owner_wallet, [{"verbs": ["CONTAINER_PUT"]}])
            cid = create_container(
                gw_endpoint, unique_container_name(), self.PLACEMENT_RULE, PUBLIC_ACL, container_token
            )

        with allure.step("Get addresses"):
            user_address = get_last_address_from_wallet(self.user_wallet.path, self.user_wallet.password)

        with allure.step("Create original token for specific container"):
            contexts = [{"containerID": cid, "verbs": ["OBJECT_GET", "OBJECT_HEAD"]}]
            original_token = generate_session_token_v2(gw_endpoint, self.owner_wallet, contexts, targets=[user_address])

        with allure.step("Attempt to create delegated token with wildcard"):
            wildcard_contexts = [{"verbs": ["OBJECT_GET", "OBJECT_HEAD"]}]
            with pytest.raises(Exception):
                generate_session_token_v2(
                    gw_endpoint,
                    self.user_wallet,
                    wildcard_contexts,
                    targets=[user_address],
                    lifetime=900,
                    origin=original_token,
                )

    @allure.title("Test V2 Session Token - Cannot Extend Lifetime in Delegation")
    @pytest.mark.simple
    def test_rest_v2_session_token_cannot_extend_lifetime(self, gw_endpoint: str):
        """
        Test that delegated token cannot have longer lifetime than original.

        Steps:
        1. Create container
        2. Create original token with lifetime=100s
        3. Attempt to create delegated token with lifetime=200s
        4. Verify delegation fails
        """
        with allure.step("Create container"):
            container_token = generate_session_token_v2(gw_endpoint, self.owner_wallet, [{"verbs": ["CONTAINER_PUT"]}])
            cid = create_container(
                gw_endpoint, unique_container_name(), self.PLACEMENT_RULE, PUBLIC_ACL, container_token
            )

        with allure.step("Get addresses"):
            user_address = get_last_address_from_wallet(self.user_wallet.path, self.user_wallet.password)

        with allure.step("Create original token with short lifetime"):
            contexts = [{"containerID": cid, "verbs": ["OBJECT_GET", "OBJECT_HEAD"]}]
            original_token = generate_session_token_v2(
                gw_endpoint, self.owner_wallet, contexts, targets=[user_address], lifetime=100
            )

        with allure.step("Attempt to create delegated token with longer lifetime"):
            with pytest.raises(Exception):
                generate_session_token_v2(
                    gw_endpoint, self.user_wallet, contexts, targets=[user_address], lifetime=200, origin=original_token
                )

    @allure.title("Test V2 Session Token - Wrong Verb Type")
    @pytest.mark.simple
    def test_rest_v2_session_token_wrong_verb_type(self, gw_endpoint: str):
        """
        Test that container verbs cannot be used for object operations.

        Steps:
        1. Create container
        2. Create token with CONTAINER_PUT, CONTAINER_DELETE verbs
        3. Attempt to use it for PUT object operation
        4. Verify operation is rejected
        """
        with allure.step("Create container"):
            container_token = generate_session_token_v2(gw_endpoint, self.owner_wallet, [{"verbs": ["CONTAINER_PUT"]}])
            cid = create_container(
                gw_endpoint, unique_container_name(), self.PLACEMENT_RULE, PUBLIC_ACL, container_token
            )

        with allure.step("Create V2 Session Token with container verbs only"):
            rest_gw_address = get_last_address_from_wallet(
                self.neofs_env.rest_gw.wallet.path, self.neofs_env.rest_gw.wallet.password
            )
            contexts = [{"verbs": ["CONTAINER_PUT", "CONTAINER_DELETE"]}]
            session_token = generate_session_token_v2(
                gw_endpoint, self.owner_wallet, contexts, targets=[rest_gw_address]
            )

        with allure.step("Attempt to use token for object PUT operation"):
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            result = upload_via_rest_gate(
                cid, file_path, gw_endpoint, session_token=session_token, error_pattern="invalid"
            )
            assert not result, "Object PUT with container-only token should fail"

    @allure.title("Test V2 Session Token with NNS")
    @pytest.mark.simple
    def test_rest_v2_session_token_nns(self, gw_endpoint: str):
        """
        Test V2 session token with NNS

        Steps:
        1. Register NNS domain name for REST GW
        2. Add NNS record for REST GW
        3. Create V2 session token with NNS subject for REST GW
        4. Upload object using REST GW with session token
        5. Verify REST GW can perform operations using the token
        """
        owner_wallet = self.owner_wallet
        neofs_env = self.neofs_env

        with allure.step("Refill gas for owner wallet"):
            neofs_env.neofs_adm().fschain.refill_gas(
                rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
                alphabet_wallets=neofs_env.alphabet_wallets_dir,
                storage_wallet=owner_wallet.path,
                gas=200,
                wallet_address=owner_wallet.address,
            )

        with allure.step("Get contract hashes"):
            contracts_hashes = get_contract_hashes(neofs_env)

        with allure.step("Get REST GW address"):
            rest_gw_address = get_last_address_from_wallet(
                neofs_env.rest_gw.wallet.path, neofs_env.rest_gw.wallet.password
            )

        with allure.step("Register NNS domain for REST GW wallet"):
            rest_gw_domain = "restgwdomain.neofs"
            register_nns_domain_with_record(
                neofs_env=neofs_env,
                wallet=owner_wallet,
                domain=rest_gw_domain,
                contracts_hashes=contracts_hashes,
                neo_address=rest_gw_address,
            )

        with allure.step("Verify domain is registered"):
            raw_dumped_names = neofs_env.neofs_adm().fschain.dump_names(f"http://{neofs_env.fschain_rpc}").stdout
            assert rest_gw_domain in raw_dumped_names, f"Domain {rest_gw_domain} not found"

        with allure.step("Create container"):
            container_token = generate_session_token_v2(gw_endpoint, owner_wallet, [{"verbs": ["CONTAINER_PUT"]}])
            cid = create_container(
                gw_endpoint, unique_container_name(), self.PLACEMENT_RULE, PUBLIC_ACL, container_token
            )
            file_path = generate_file(neofs_env.get_object_size("simple_object_size"))

        with allure.step("Upload and get object with session token wihth NNS domain"):
            session_token = generate_session_token_v2(
                gw_endpoint,
                owner_wallet,
                [{"containerID": cid, "verbs": ["OBJECT_PUT", "OBJECT_GET"]}],
                targets=[rest_gw_domain],
            )
            oid = upload_via_rest_gate(cid, file_path, gw_endpoint, session_token=session_token)
            resp = get_via_rest_gate(cid, oid, gw_endpoint, session_token=session_token, return_response=True)
            assert resp.ok, f"GET should work with NNS subject: {resp.text}"

    @allure.title("Test V2 Session Token with NNS Subjects - Delegation")
    @pytest.mark.simple
    def test_rest_v2_session_token_nns_delegation(self, gw_endpoint: str):
        """
        Test V2 session token with NNS subjects using delegation pattern.

        Steps:
        1. Register NNS domain names for owner and user wallets
        2. Create original token with user's NNS domain as target
        3. Create delegated token from user to their own NNS domain
        4. Verify delegated token works via REST API
        """
        owner_wallet = self.owner_wallet
        neofs_env = self.neofs_env

        with allure.step("Refill gas for owner wallet"):
            neofs_env.neofs_adm().fschain.refill_gas(
                rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
                alphabet_wallets=neofs_env.alphabet_wallets_dir,
                storage_wallet=owner_wallet.path,
                gas=200,
                wallet_address=owner_wallet.address,
            )

        with allure.step("Get contract hashes"):
            contracts_hashes = get_contract_hashes(neofs_env)

        with allure.step("Register NNS domains"):
            owner_domain = "ownerdomain.neofs"

            register_nns_domain_with_record(
                neofs_env=neofs_env,
                wallet=owner_wallet,
                domain=owner_domain,
                contracts_hashes=contracts_hashes,
                neo_address=owner_wallet.address,
            )

        with allure.step("Verify domains are registered"):
            raw_dumped_names = neofs_env.neofs_adm().fschain.dump_names(f"http://{neofs_env.fschain_rpc}").stdout
            assert owner_domain in raw_dumped_names

        with allure.step("Create container"):
            container_token = generate_session_token_v2(gw_endpoint, owner_wallet, [{"verbs": ["CONTAINER_PUT"]}])
            cid = create_container(
                gw_endpoint, unique_container_name(), self.PLACEMENT_RULE, PUBLIC_ACL, container_token
            )
            file_path = generate_file(neofs_env.get_object_size("simple_object_size"))

        with allure.step("Upload object with session token"):
            rest_gw_address = get_last_address_from_wallet(
                neofs_env.rest_gw.wallet.path, neofs_env.rest_gw.wallet.password
            )
            upload_token = generate_session_token_v2(
                gw_endpoint, owner_wallet, [{"containerID": cid, "verbs": ["OBJECT_PUT"]}], targets=[rest_gw_address]
            )
            oid = upload_via_rest_gate(cid, file_path, gw_endpoint, session_token=upload_token)

        with allure.step("Create original token with REST GW NNS domain as target"):
            contexts = [{"containerID": cid, "verbs": ["OBJECT_GET", "OBJECT_HEAD"]}]
            original_token = generate_session_token_v2(gw_endpoint, owner_wallet, contexts, targets=[owner_domain])

        with allure.step("Create delegated token for REST GW with reduced permissions"):
            reduced_contexts = [{"containerID": cid, "verbs": ["OBJECT_GET"]}]
            delegated_token = generate_session_token_v2(
                gw_endpoint, owner_wallet, reduced_contexts, targets=[owner_domain], origin=original_token
            )

        with allure.step("Verify delegated token works via REST"):
            resp = get_via_rest_gate(cid, oid, gw_endpoint, session_token=delegated_token, return_response=True)
            assert resp.ok, f"Delegated token with NNS subjects should work: {resp.text}"

    @allure.title("Test V2 Session Token - Container SET_EACL Operation")
    @pytest.mark.simple
    def test_rest_v2_session_token_container_set_eacl(self, gw_endpoint: str):
        """
        Test container SET_EACL operation with V2 session token.

        Steps:
        1. Create container
        2. Create V2 session token with CONTAINER_SET_EACL verb
        3. Attempt to set eACL using session token
        4. Verify operation works (or is properly authorized)
        """
        with allure.step("Create container"):
            container_token = generate_session_token_v2(gw_endpoint, self.owner_wallet, [{"verbs": ["CONTAINER_PUT"]}])
            cid = create_container(
                gw_endpoint, unique_container_name(), self.PLACEMENT_RULE, PUBLIC_ACL, container_token
            )

        with allure.step("Create V2 Session Token with CONTAINER_SET_EACL verb"):
            contexts = [{"verbs": ["CONTAINER_SET_EACL"]}]
            session_token = generate_session_token_v2(gw_endpoint, self.owner_wallet, contexts)

        with allure.step("Test token authorization for SETEACL"):
            try:
                get_container_eacl(gw_endpoint, cid, session_token)
            except Exception as e:
                assert "404" in str(e) or "Failed to get container eacl" in str(e), (
                    f"Token should be valid for eACL operations: {e}"
                )

    @allure.title("Test V2 Session Token - Mixed Container and Object Verbs")
    @pytest.mark.simple
    def test_rest_v2_session_token_mixed_verbs(self, gw_endpoint: str):
        """
        Test session token with both container and object verbs in different contexts.

        Steps:
        1. Create token with container context (CONTAINER_PUT, CONTAINER_DELETE)
        2. Create token with object context (OBJECT_PUT, OBJECT_GET)
        3. Verify container operations work
        4. Verify object operations work
        """
        with allure.step("Create V2 Session Token with mixed contexts"):
            rest_gw_address = get_last_address_from_wallet(
                self.neofs_env.rest_gw.wallet.path, self.neofs_env.rest_gw.wallet.password
            )
            contexts = [
                {"verbs": ["CONTAINER_PUT", "CONTAINER_DELETE"]},
                {"verbs": ["OBJECT_PUT", "OBJECT_GET", "OBJECT_HEAD"]},
            ]
            session_token = generate_session_token_v2(
                gw_endpoint, self.owner_wallet, contexts, targets=[rest_gw_address]
            )

        with allure.step("Test container creation with token"):
            cid = create_container(gw_endpoint, unique_container_name(), self.PLACEMENT_RULE, PUBLIC_ACL, session_token)

        with allure.step("Test object upload with token"):
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oid = upload_via_rest_gate(cid, file_path, gw_endpoint, session_token=session_token)
            assert oid, "Object upload should work"

        with allure.step("Test object get with token"):
            resp = get_via_rest_gate(cid, oid, gw_endpoint, session_token=session_token, return_response=True)
            assert resp.ok, "Object GET should work"

        with allure.step("Test container deletion with token"):
            delete_container(gw_endpoint, cid, session_token)
