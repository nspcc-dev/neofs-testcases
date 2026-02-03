import logging
import time

import allure
import pytest
from helpers.container import create_container, delete_container, list_containers
from helpers.file_helper import generate_file
from helpers.grpc_responses import (
    EXPIRED_SESSION_TOKEN,
    INVALID_V2_SESSION_TOKEN,
    SESSION_TOKEN_DOESNOT_AUTHORIZE,
    SESSION_VALIDATION_FAILED,
)
from helpers.neofs_verbs import (
    delete_object,
    get_object,
    head_object,
    put_object,
    put_object_to_random_node,
    search_object,
)
from helpers.nns import get_contract_hashes, register_nns_domain_with_record
from helpers.session_token import create_session_token_v2
from helpers.utility import parse_version
from neofs_env.neofs_env_test_base import TestNeofsBase
from neofs_testlib.utils.wallet import get_last_address_from_wallet

logger = logging.getLogger("NeoLogger")


class TestSessionTokenV2(TestNeofsBase):
    @pytest.fixture(autouse=True)
    def check_node_version(self):
        if parse_version(self.neofs_env.get_binary_version(self.neofs_env.neofs_node_path)) <= parse_version("0.50.2"):
            pytest.skip("V2 session token tests require fresh neofs-node")

    def _create_session_token_with_delegation(
        self,
        owner_wallet,
        user_wallet,
        contexts,
        use_delegation,
        owner_lifetime=1000,
        user_lifetime=900,
    ):
        """
        Helper method to create session token with optional delegation pattern.

        Args:
            owner_wallet: Wallet of the token owner
            user_wallet: Wallet of the delegated user
            contexts: List of context strings for the token
            use_delegation: If True, creates delegated token chain; if False, creates direct token
            owner_lifetime: Lifetime for owner's token (default: 1000)
            user_lifetime: Lifetime for user's delegated token (default: 900)

        Returns:
            The session token to be used (either owner's token or delegated token)
        """
        owner_address = get_last_address_from_wallet(owner_wallet.path, owner_wallet.password)
        subject_address = get_last_address_from_wallet(user_wallet.path, user_wallet.password)

        if use_delegation:
            owner_token = create_session_token_v2(
                shell=self.shell,
                owner_wallet=owner_wallet,
                rpc_endpoint=self.neofs_env.sn_rpc,
                lifetime=owner_lifetime,
                subjects=[subject_address],
                contexts=contexts,
            )
            session_token = create_session_token_v2(
                shell=self.shell,
                owner_wallet=user_wallet,
                rpc_endpoint=self.neofs_env.sn_rpc,
                lifetime=user_lifetime,
                subjects=[subject_address],
                contexts=contexts,
                origin=owner_token,
            )
        else:
            session_token = create_session_token_v2(
                shell=self.shell,
                owner_wallet=owner_wallet,
                rpc_endpoint=self.neofs_env.sn_rpc,
                lifetime=owner_lifetime,
                subjects=[owner_address],
                contexts=contexts,
            )

        return session_token

    @allure.title("Test V2 Session Token with Object Operations")
    @pytest.mark.parametrize(
        "object_size,use_delegation",
        [
            pytest.param("simple_object_size", False, id="simple object owner-direct", marks=pytest.mark.simple),
            pytest.param("simple_object_size", True, id="simple object delegation", marks=pytest.mark.simple),
            pytest.param("complex_object_size", False, id="complex object owner-direct", marks=pytest.mark.complex),
            pytest.param("complex_object_size", True, id="complex object delegation", marks=pytest.mark.complex),
        ],
    )
    def test_v2_session_token_object_operations(self, default_wallet, user_wallet, object_size, use_delegation):
        """
        Test how operations over objects are executed with a V2 session token

        Steps:
        1. Create a container
        2. Put objects with session token
        3. Get, head, search objects with session token
        4. Delete object with session token
        """

        with allure.step("Init wallets"):
            owner_wallet = default_wallet
            operator_wallet = owner_wallet if not use_delegation else user_wallet

        with allure.step("Create Container"):
            cid = create_container(
                owner_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Put initial object"):
            file_path = generate_file(self.neofs_env.get_object_size(object_size))
            oid = put_object_to_random_node(
                wallet=owner_wallet.path,
                path=file_path,
                cid=cid,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step("Create V2 Session Token with object operations"):
            verbs = "HEAD,DELETE,GET,SEARCH,PUT"
            contexts = [f"{cid}:{verbs}"]

            session_token = self._create_session_token_with_delegation(
                owner_wallet=owner_wallet,
                user_wallet=user_wallet,
                contexts=contexts,
                use_delegation=use_delegation,
            )

        with allure.step("Verify HEAD operation with session token"):
            head_object(
                wallet=operator_wallet.path,
                cid=cid,
                oid=oid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                session=session_token,
            )

        with allure.step("Verify GET operation with session token"):
            get_object(
                wallet=operator_wallet.path,
                cid=cid,
                oid=oid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                session=session_token,
            )

        with allure.step("Verify PUT operation with session token"):
            put_object(
                wallet=operator_wallet.path,
                path=file_path,
                cid=cid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                session=session_token,
            )

        with allure.step("Verify SEARCH operation with session token"):
            search_result = search_object(
                wallet=operator_wallet.path,
                cid=cid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                session=session_token,
                root=True,
            )
            assert oid in search_result, f"Object {oid} not found in search results"

        with allure.step("Verify DELETE operation with session token"):
            delete_object(
                wallet=operator_wallet.path,
                cid=cid,
                oid=oid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                session=session_token,
            )

    @allure.title("Test V2 Session Token with Container Operations")
    @pytest.mark.parametrize("use_delegation", [False, True], ids=["owner-direct", "delegation"])
    @pytest.mark.simple
    def test_v2_session_token_container_operations(self, default_wallet, user_wallet, use_delegation):
        """
        Test container operations with V2 session token

        Steps:
        1. Create V2 session token with CONTAINERPUT and CONTAINERDELETE contexts
        2. Create container using session token
        3. Delete container using session token
        """

        with allure.step("Init wallets"):
            owner_wallet = default_wallet
            operator_wallet = owner_wallet if not use_delegation else user_wallet

        with allure.step("Create V2 Session Token with container operations"):
            contexts = ["0:CONTAINERPUT,CONTAINERDELETE"]

            session_token = self._create_session_token_with_delegation(
                owner_wallet=owner_wallet,
                user_wallet=user_wallet,
                contexts=contexts,
                use_delegation=use_delegation,
            )

        with allure.step("Create container with V2 session token"):
            cid = create_container(
                operator_wallet.path,
                session_token=session_token,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                wait_for_creation=False,
            )

        with allure.step("Verify container was created and belongs to owner"):
            containers = list_containers(owner_wallet.path, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
            assert cid in containers, f"Container {cid} not found in owner's containers"

        with allure.step("Delete container with V2 session token"):
            delete_container(
                wallet=operator_wallet.path,
                cid=cid,
                session_token=session_token,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                await_mode=True,
            )

        with allure.step("Verify container was deleted"):
            containers = list_containers(owner_wallet.path, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
            assert cid not in containers, f"Container {cid} should have been deleted"

    @allure.title("Test V2 Session Token with Multiple Subjects")
    @pytest.mark.simple
    def test_v2_session_token_multiple_subjects(self, default_wallet, user_wallet, stranger_wallet):
        """
        Test V2 session token with multiple subjects

        Steps:
        1. Create V2 session token with two subjects
        2. Verify both subjects can perform operations
        3. Verify unauthorized wallet cannot use the token
        """

        with allure.step("Init wallets"):
            owner_wallet = default_wallet
            subject1_address = get_last_address_from_wallet(user_wallet.path, user_wallet.password)
            subject2_address = get_last_address_from_wallet(stranger_wallet.path, stranger_wallet.password)

        with allure.step("Create Container"):
            cid = create_container(
                owner_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Put object"):
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oid = put_object_to_random_node(
                wallet=owner_wallet.path,
                path=file_path,
                cid=cid,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step("Create V2 Session Token with multiple subjects"):
            contexts = [f"{cid}:GET,HEAD"]

            owner_token = create_session_token_v2(
                shell=self.shell,
                owner_wallet=owner_wallet,
                rpc_endpoint=self.neofs_env.sn_rpc,
                lifetime=1000,
                subjects=[subject1_address, subject2_address],
                contexts=contexts,
            )

        with allure.step("First subject creates delegated token"):
            user_token = create_session_token_v2(
                shell=self.shell,
                owner_wallet=user_wallet,
                rpc_endpoint=self.neofs_env.sn_rpc,
                lifetime=900,
                subjects=[subject1_address],
                contexts=contexts,
                origin=owner_token,
            )

        with allure.step("Second subject creates delegated token"):
            stranger_token = create_session_token_v2(
                shell=self.shell,
                owner_wallet=stranger_wallet,
                rpc_endpoint=self.neofs_env.sn_rpc,
                lifetime=900,
                subjects=[subject2_address],
                contexts=contexts,
                origin=owner_token,
            )

        with allure.step("Verify first subject can use their delegated token"):
            head_object(
                wallet=user_wallet.path,
                cid=cid,
                oid=oid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                session=user_token,
            )

        with allure.step("Verify second subject can use their delegated token"):
            head_object(
                wallet=stranger_wallet.path,
                cid=cid,
                oid=oid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                session=stranger_token,
            )

    @allure.title("Test V2 Session Token with Multiple Contexts")
    @pytest.mark.simple
    def test_v2_session_token_multiple_contexts(self, default_wallet, user_wallet):
        """
        Test V2 session token with multiple contexts

        Steps:
        1. Create two containers with objects
        2. Create V2 session token with contexts for both containers
        3. Verify operations work on both containers
        """

        with allure.step("Init wallets"):
            owner_wallet = default_wallet
            subject_address = get_last_address_from_wallet(user_wallet.path, user_wallet.password)

        with allure.step("Create first container and object"):
            cid1 = create_container(
                owner_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oid1 = put_object_to_random_node(
                wallet=owner_wallet.path,
                path=file_path,
                cid=cid1,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step("Create second container and object"):
            cid2 = create_container(
                owner_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            oid2 = put_object_to_random_node(
                wallet=owner_wallet.path,
                path=file_path,
                cid=cid2,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step("Create V2 Session Token with multiple contexts"):
            contexts = sorted(
                [
                    f"{cid1}:HEAD,GET",
                    f"{cid2}:GET,HEAD",
                ]
            )

            owner_token = create_session_token_v2(
                shell=self.shell,
                owner_wallet=owner_wallet,
                rpc_endpoint=self.neofs_env.sn_rpc,
                lifetime=1000,
                subjects=[subject_address],
                contexts=contexts,
            )

        with allure.step("User creates delegated token from owner's token"):
            session_token = create_session_token_v2(
                shell=self.shell,
                owner_wallet=user_wallet,
                rpc_endpoint=self.neofs_env.sn_rpc,
                lifetime=900,
                subjects=[subject_address],
                contexts=contexts,
                origin=owner_token,
            )

        with allure.step("Verify operations on first container with session token"):
            head_object(
                wallet=user_wallet.path,
                cid=cid1,
                oid=oid1,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                session=session_token,
            )

        with allure.step("Verify operations on second container with session token"):
            head_object(
                wallet=user_wallet.path,
                cid=cid2,
                oid=oid2,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                session=session_token,
            )

    @allure.title("Test V2 Session Token Expiration")
    @pytest.mark.parametrize("use_delegation", [False, True], ids=["owner-direct", "delegation"])
    @pytest.mark.simple
    def test_v2_session_token_expiration(self, default_wallet, user_wallet, use_delegation):
        """
        Test V2 session token expiration

        Steps:
        1. Create V2 session token with short lifetime
        2. Verify token works initially
        3. Wait for expiration
        4. Verify token is rejected
        """

        with allure.step("Init wallets"):
            owner_wallet = default_wallet
            operator_wallet = owner_wallet if not use_delegation else user_wallet

        with allure.step("Create Container"):
            cid = create_container(
                owner_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Put object"):
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oid = put_object_to_random_node(
                wallet=owner_wallet.path,
                path=file_path,
                cid=cid,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step("Create V2 Session Token with short lifetime"):
            contexts = [f"{cid}:GET,HEAD"]
            short_lifetime = 10

            session_token = self._create_session_token_with_delegation(
                owner_wallet=owner_wallet,
                user_wallet=user_wallet,
                contexts=contexts,
                use_delegation=use_delegation,
                owner_lifetime=short_lifetime,
                user_lifetime=short_lifetime - 2,
            )

        with allure.step("Verify session token works before expiration"):
            head_object(
                wallet=operator_wallet.path,
                cid=cid,
                oid=oid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                session=session_token,
            )

        with allure.step("Wait for token expiration"):
            time.sleep(12)

        with allure.step("Verify session token is rejected after expiration"):
            with pytest.raises(RuntimeError, match=EXPIRED_SESSION_TOKEN):
                head_object(
                    wallet=operator_wallet.path,
                    cid=cid,
                    oid=oid,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    session=session_token,
                )

    @allure.title("Test V2 Session Token with Wildcard Container")
    @pytest.mark.simple
    def test_v2_session_token_wildcard_container(self, default_wallet, user_wallet):
        """
        Test V2 session token with wildcard container

        Steps:
        1. Create V2 session token with wildcard container (0)
        2. Create multiple containers
        3. Verify token works on all containers
        """

        with allure.step("Init wallets"):
            owner_wallet = default_wallet
            owner_address = get_last_address_from_wallet(owner_wallet.path, owner_wallet.password)

        with allure.step("Create V2 Session Token with wildcard container"):
            contexts = ["0:HEAD,PUT,GET"]

            session_token = create_session_token_v2(
                shell=self.shell,
                owner_wallet=owner_wallet,
                rpc_endpoint=self.neofs_env.sn_rpc,
                lifetime=100,
                subjects=[owner_address],
                contexts=contexts,
            )

        with allure.step("Create first container and put object"):
            cid1 = create_container(
                owner_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oid1 = put_object(
                wallet=owner_wallet.path,
                path=file_path,
                cid=cid1,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                session=session_token,
            )

        with allure.step("Verify GET on first container"):
            get_object(
                wallet=owner_wallet.path,
                cid=cid1,
                oid=oid1,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                session=session_token,
            )

        with allure.step("Create second container and put object"):
            cid2 = create_container(
                owner_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            oid2 = put_object(
                wallet=owner_wallet.path,
                path=file_path,
                cid=cid2,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                session=session_token,
            )

        with allure.step("Verify GET on second container"):
            get_object(
                wallet=owner_wallet.path,
                cid=cid2,
                oid=oid2,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                session=session_token,
            )

    @allure.title("Test V2 Session Token with Token Delegation")
    @pytest.mark.simple
    def test_v2_session_token_delegation(self, default_wallet, user_wallet, stranger_wallet):
        """
        Test V2 session token delegation using origin flag

        Steps:
        1. Create original V2 session token
        2. Create delegated token using --origin flag
        3. Verify delegated token works
        4. Test final flag preventing further delegation
        """

        with allure.step("Init wallets"):
            owner_wallet = default_wallet
            user_address = get_last_address_from_wallet(user_wallet.path, user_wallet.password)
            stranger_address = get_last_address_from_wallet(stranger_wallet.path, stranger_wallet.password)

        with allure.step("Create Container and object"):
            cid = create_container(
                owner_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oid = put_object_to_random_node(
                wallet=owner_wallet.path,
                path=file_path,
                cid=cid,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step("Create original V2 Session Token"):
            contexts = [f"{cid}:GET,HEAD"]

            original_token = create_session_token_v2(
                shell=self.shell,
                owner_wallet=owner_wallet,
                rpc_endpoint=self.neofs_env.sn_rpc,
                lifetime=1000,
                subjects=[user_address],
                contexts=contexts,
            )

        with allure.step("Create first delegated token for user"):
            user_token = create_session_token_v2(
                shell=self.shell,
                owner_wallet=user_wallet,
                rpc_endpoint=self.neofs_env.sn_rpc,
                lifetime=900,
                subjects=[user_address],
                contexts=contexts,
                origin=original_token,
            )

        with allure.step("Verify user's delegated token works"):
            head_object(
                wallet=user_wallet.path,
                cid=cid,
                oid=oid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                session=user_token,
            )

        with allure.step("Create second delegated token from user to stranger"):
            stranger_delegated_token = create_session_token_v2(
                shell=self.shell,
                owner_wallet=user_wallet,
                rpc_endpoint=self.neofs_env.sn_rpc,
                lifetime=800,
                subjects=[stranger_address],
                contexts=contexts,
                origin=original_token,
            )

        with allure.step("Stranger creates their own token from user's delegation"):
            stranger_token = create_session_token_v2(
                shell=self.shell,
                owner_wallet=stranger_wallet,
                rpc_endpoint=self.neofs_env.sn_rpc,
                lifetime=700,
                subjects=[stranger_address],
                contexts=contexts,
                origin=stranger_delegated_token,
            )

        with allure.step("Verify stranger's delegated token works"):
            head_object(
                wallet=stranger_wallet.path,
                cid=cid,
                oid=oid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                session=stranger_token,
            )

    @allure.title("Test V2 Session Token with Final Flag")
    @pytest.mark.simple
    def test_v2_session_token_final_flag(self, default_wallet, user_wallet):
        """
        Test V2 session token with final flag preventing further delegation

        Steps:
        1. Create V2 session token with final=True
        2. Verify token works
        3. Attempt to create delegated token (should fail or be invalid)
        """

        with allure.step("Init wallets"):
            owner_wallet = default_wallet
            user_address = get_last_address_from_wallet(user_wallet.path, user_wallet.password)

        with allure.step("Create Container and object"):
            cid = create_container(
                owner_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oid = put_object_to_random_node(
                wallet=owner_wallet.path,
                path=file_path,
                cid=cid,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step("Create V2 Session Token with final flag"):
            contexts = [f"{cid}:GET,HEAD"]

            final_token = create_session_token_v2(
                shell=self.shell,
                owner_wallet=owner_wallet,
                rpc_endpoint=self.neofs_env.sn_rpc,
                lifetime=1000,
                subjects=[user_address],
                contexts=contexts,
                final=True,
            )

        with allure.step("Create delegated token for user to perform operations"):
            with pytest.raises(Exception, match=".*final token cannot be used as origin.*"):
                create_session_token_v2(
                    shell=self.shell,
                    owner_wallet=user_wallet,
                    rpc_endpoint=self.neofs_env.sn_rpc,
                    lifetime=900,
                    subjects=[user_address],
                    contexts=contexts,
                    origin=final_token,
                )

        with allure.step("Create delegated token with force=True (should succeed but be invalid)"):
            forced_token = create_session_token_v2(
                shell=self.shell,
                owner_wallet=user_wallet,
                rpc_endpoint=self.neofs_env.sn_rpc,
                lifetime=900,
                subjects=[user_address],
                contexts=contexts,
                origin=final_token,
                force=True,
            )

        with allure.step("Verify forced token is rejected when used"):
            with pytest.raises(Exception, match=SESSION_VALIDATION_FAILED):
                head_object(
                    wallet=user_wallet.path,
                    cid=cid,
                    oid=oid,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    session=forced_token,
                )

        with allure.step("Verify original token still works"):
            head_object(
                wallet=owner_wallet.path,
                cid=cid,
                oid=oid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                session=final_token,
            )

    @allure.title("Test V2 Session Token Unrelated Container")
    @pytest.mark.parametrize("use_delegation", [False, True], ids=["owner-direct", "delegation"])
    @pytest.mark.simple
    def test_v2_session_token_unrelated_container(self, default_wallet, user_wallet, use_delegation):
        """
        Test that V2 session token rejects operations on containers not in context

        V2 tokens work at container level, not object level.
        This test verifies that tokens only work for authorized containers.

        Steps:
        1. Create two containers with objects
        2. Create V2 session token for only first container
        3. Verify token works for first container
        4. Verify token is rejected for second container
        """

        with allure.step("Init wallets"):
            owner_wallet = default_wallet
            operator_wallet = owner_wallet if not use_delegation else user_wallet

        with allure.step("Create first container"):
            cid1 = create_container(
                owner_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Create second container"):
            cid2 = create_container(
                owner_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Put objects in both containers"):
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oid1 = put_object_to_random_node(
                wallet=owner_wallet.path,
                path=file_path,
                cid=cid1,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )
            oid2 = put_object_to_random_node(
                wallet=owner_wallet.path,
                path=file_path,
                cid=cid2,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step("Create V2 Session Token for only first container"):
            contexts = [f"{cid1}:GET,HEAD"]

            session_token = self._create_session_token_with_delegation(
                owner_wallet=owner_wallet,
                user_wallet=user_wallet,
                contexts=contexts,
                use_delegation=use_delegation,
            )

        with allure.step("Verify session token works for first container"):
            head_object(
                wallet=operator_wallet.path,
                cid=cid1,
                oid=oid1,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                session=session_token,
            )

        with allure.step("Verify session token is rejected for second container"):
            with pytest.raises(Exception, match=SESSION_VALIDATION_FAILED):
                head_object(
                    wallet=operator_wallet.path,
                    cid=cid2,
                    oid=oid2,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    session=session_token,
                )

    @allure.title("Test V2 Session Token Wrong Verb")
    @pytest.mark.parametrize("use_delegation", [False, True], ids=["owner-direct", "delegation"])
    @pytest.mark.simple
    def test_v2_session_token_wrong_verb(self, default_wallet, user_wallet, use_delegation):
        """
        Test that V2 session token rejects operations with verbs not in context

        Steps:
        1. Create V2 session token with only HEAD verb
        2. Verify HEAD operation works
        3. Verify GET operation is rejected
        """

        with allure.step("Init wallets"):
            owner_wallet = default_wallet
            operator_wallet = owner_wallet if not use_delegation else user_wallet

        with allure.step("Create Container and object"):
            cid = create_container(
                owner_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oid = put_object_to_random_node(
                wallet=owner_wallet.path,
                path=file_path,
                cid=cid,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step("Create V2 Session Token with only HEAD verb"):
            contexts = [f"{cid}:HEAD"]

            session_token = self._create_session_token_with_delegation(
                owner_wallet=owner_wallet,
                user_wallet=user_wallet,
                contexts=contexts,
                use_delegation=use_delegation,
            )

        with allure.step("Verify HEAD operation works with session token"):
            head_object(
                wallet=operator_wallet.path,
                cid=cid,
                oid=oid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                session=session_token,
            )

        with allure.step("Verify GET operation is rejected with session token"):
            with pytest.raises(Exception, match=SESSION_VALIDATION_FAILED):
                get_object(
                    wallet=operator_wallet.path,
                    cid=cid,
                    oid=oid,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    session=session_token,
                )

    @allure.title("Test V2 Session Token with NNS SN key subjects")
    @pytest.mark.simple
    def test_v2_session_token_nns_sn_keys(self, default_wallet):
        """
        Test V2 session token with NNS SN key subject

        Steps:
        1. Register NNS domain name for the SN1
        2. Add NNS record for the SN1
        3. Create V2 session token with NNS subject for SN1 from SN0
        4. Verify SN1 can perform operations using the token
        """

        owner_wallet = default_wallet
        neofs_env = self.neofs_env
        sn0_endpoint = self.neofs_env.storage_nodes[0].endpoint
        sn1_wallet = self.neofs_env.storage_nodes[1].wallet
        sn1_endpoint = self.neofs_env.storage_nodes[1].endpoint

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

        with allure.step("Register NNS domain for the SN1 wallet"):
            sn1_domain = "ownerdomain.neofs"
            register_nns_domain_with_record(
                neofs_env=neofs_env,
                wallet=owner_wallet,
                domain=sn1_domain,
                contracts_hashes=contracts_hashes,
                neo_address=sn1_wallet.address,
            )

        with allure.step("Verify domain is registered"):
            raw_dumped_names = neofs_env.neofs_adm().fschain.dump_names(f"http://{neofs_env.fschain_rpc}").stdout
            assert sn1_domain in raw_dumped_names, f"Domain {sn1_domain} not found"

        with allure.step("Create Container"):
            cid = create_container(
                owner_wallet.path,
                shell=self.shell,
                endpoint=neofs_env.sn_rpc,
            )

        with allure.step("Put object"):
            file_path = generate_file(neofs_env.get_object_size("simple_object_size"))
            oid = put_object_to_random_node(
                wallet=owner_wallet.path,
                path=file_path,
                cid=cid,
                shell=self.shell,
                neofs_env=neofs_env,
            )

        with allure.step("Create V2 Session Token with NNS subject for SN1 from SN0"):
            contexts = [f"{cid}:GET,HEAD"]

            session_token = create_session_token_v2(
                shell=self.shell,
                owner_wallet=owner_wallet,
                rpc_endpoint=sn0_endpoint,
                lifetime=1000,
                subject_nns=[sn1_domain],
                contexts=contexts,
            )

        with allure.step("Verify SN1 can perform HEAD operation with NNS subject token"):
            head_object(
                wallet=owner_wallet.path,
                cid=cid,
                oid=oid,
                shell=self.shell,
                endpoint=sn1_endpoint,
                session=session_token,
            )

        with allure.step("Verify SN1 can perform GET operation with NNS subject token"):
            get_object(
                wallet=owner_wallet.path,
                cid=cid,
                oid=oid,
                shell=self.shell,
                endpoint=sn1_endpoint,
                session=session_token,
            )

    @allure.title("Test V2 Session Token with NNS Subjects - Delegation")
    @pytest.mark.simple
    def test_v2_session_token_nns_delegation(self, default_wallet, user_wallet):
        """
        Test V2 session token with NNS subjects using delegation pattern

        Steps:
        1. Register two NNS domain names for two different wallets
        2. Add NNS records for both domains
        3. Create V2 session token with both NNS subjects
        4. Both subjects create delegated tokens
        5. Verify both subjects can perform operations using their delegated tokens
        """

        with allure.step("Init wallets"):
            owner_wallet = default_wallet
            subject_wallet = user_wallet
            neofs_env = self.neofs_env

        with allure.step("Refill gas for both wallets"):
            for wallet in [owner_wallet, subject_wallet]:
                neofs_env.neofs_adm().fschain.refill_gas(
                    rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
                    alphabet_wallets=neofs_env.alphabet_wallets_dir,
                    storage_wallet=wallet.path,
                    gas=200,
                    wallet_address=wallet.address,
                )

        with allure.step("Get contract hashes"):
            contracts_hashes = get_contract_hashes(neofs_env)

        with allure.step("Register NNS domain for subject wallet"):
            subject_domain = "subjectdomain.neofs"
            register_nns_domain_with_record(
                neofs_env=neofs_env,
                wallet=subject_wallet,
                domain=subject_domain,
                contracts_hashes=contracts_hashes,
            )

        with allure.step("Verify both domains are registered"):
            raw_dumped_names = neofs_env.neofs_adm().fschain.dump_names(f"http://{neofs_env.fschain_rpc}").stdout
            assert subject_domain in raw_dumped_names, f"Domain {subject_domain} not found"

        with allure.step("Create Container"):
            cid = create_container(
                owner_wallet.path,
                shell=self.shell,
                endpoint=neofs_env.sn_rpc,
            )

        with allure.step("Put object"):
            file_path = generate_file(neofs_env.get_object_size("simple_object_size"))
            oid = put_object_to_random_node(
                wallet=owner_wallet.path,
                path=file_path,
                cid=cid,
                shell=self.shell,
                neofs_env=neofs_env,
            )

        with allure.step("Create V2 Session Token with NNS subject"):
            contexts = [f"{cid}:GET,HEAD"]

            owner_token = create_session_token_v2(
                shell=self.shell,
                owner_wallet=owner_wallet,
                rpc_endpoint=neofs_env.sn_rpc,
                lifetime=1000,
                subject_nns=[subject_domain],
                contexts=contexts,
            )

        with allure.step("Subject creates delegated token"):
            subject_delegated_token = create_session_token_v2(
                shell=self.shell,
                owner_wallet=subject_wallet,
                rpc_endpoint=neofs_env.sn_rpc,
                lifetime=900,
                subject_nns=[subject_domain],
                contexts=contexts,
                origin=owner_token,
            )

        with allure.step("Verify subject can use their delegated token with NNS subject"):
            head_object(
                wallet=subject_wallet.path,
                cid=cid,
                oid=oid,
                shell=self.shell,
                endpoint=neofs_env.sn_rpc,
                session=subject_delegated_token,
            )

    @allure.title("Test V2 Session Token - Cannot Extend CID Context to Wildcard")
    @pytest.mark.simple
    def test_v2_session_token_cannot_extend_to_wildcard(self, default_wallet, user_wallet):
        """
        Test that delegated token cannot extend specific CID context to wildcard (0)

        Steps:
        1. Create owner token with specific container ID context
        2. Attempt to create delegated token with wildcard (0) context
        3. Verify the delegated token creation fails or is rejected
        """

        with allure.step("Init wallets"):
            owner_wallet = default_wallet
            subject_address = get_last_address_from_wallet(user_wallet.path, user_wallet.password)

        with allure.step("Create Container"):
            cid = create_container(
                owner_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Create owner token with specific CID context"):
            contexts = [f"{cid}:GET,HEAD"]

            owner_token = create_session_token_v2(
                shell=self.shell,
                owner_wallet=owner_wallet,
                rpc_endpoint=self.neofs_env.sn_rpc,
                lifetime=1000,
                subjects=[subject_address],
                contexts=contexts,
            )

        with allure.step("Attempt to create delegated token with wildcard context"):
            wildcard_contexts = ["0:GET,HEAD"]

            with pytest.raises(Exception, match=".*context.*"):
                create_session_token_v2(
                    shell=self.shell,
                    owner_wallet=user_wallet,
                    rpc_endpoint=self.neofs_env.sn_rpc,
                    lifetime=900,
                    subjects=[subject_address],
                    contexts=wildcard_contexts,
                    origin=owner_token,
                )

        with allure.step("Create delegated token with wildcard and force=True (should succeed but be invalid)"):
            forced_wildcard_token = create_session_token_v2(
                shell=self.shell,
                owner_wallet=user_wallet,
                rpc_endpoint=self.neofs_env.sn_rpc,
                lifetime=900,
                subjects=[subject_address],
                contexts=wildcard_contexts,
                origin=owner_token,
                force=True,
            )

        with allure.step("Verify forced wildcard token is rejected when used"):
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            cid2 = create_container(
                owner_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

            with pytest.raises(Exception, match=INVALID_V2_SESSION_TOKEN):
                put_object(
                    wallet=user_wallet.path,
                    path=file_path,
                    cid=cid2,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    session=forced_wildcard_token,
                )

    @allure.title("Test V2 Session Token - Cannot Use Unauthorized Verbs")
    @pytest.mark.parametrize("use_delegation", [False, True], ids=["owner-direct", "delegation"])
    @pytest.mark.simple
    def test_v2_session_token_unauthorized_verbs(self, default_wallet, user_wallet, use_delegation):
        """
        Test that operations with unauthorized verbs are rejected

        Steps:
        1. Create V2 session token with only GET verb
        2. Attempt PUT operation (should fail)
        3. Attempt DELETE operation (should fail)
        """

        with allure.step("Init wallets"):
            owner_wallet = default_wallet
            operator_wallet = owner_wallet if not use_delegation else user_wallet

        with allure.step("Create Container"):
            cid = create_container(
                owner_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Put object"):
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oid = put_object_to_random_node(
                wallet=owner_wallet.path,
                path=file_path,
                cid=cid,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step("Create V2 Session Token with only GET verb"):
            contexts = [f"{cid}:GET"]

            session_token = self._create_session_token_with_delegation(
                owner_wallet=owner_wallet,
                user_wallet=user_wallet,
                contexts=contexts,
                use_delegation=use_delegation,
            )

        with allure.step("Verify GET operation works"):
            get_object(
                wallet=operator_wallet.path,
                cid=cid,
                oid=oid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                session=session_token,
            )

        with allure.step("Verify PUT operation is rejected"):
            with pytest.raises(Exception, match=SESSION_TOKEN_DOESNOT_AUTHORIZE):
                put_object(
                    wallet=operator_wallet.path,
                    path=file_path,
                    cid=cid,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    session=session_token,
                )

        with allure.step("Verify DELETE operation is rejected"):
            with pytest.raises(Exception, match=SESSION_TOKEN_DOESNOT_AUTHORIZE):
                delete_object(
                    wallet=operator_wallet.path,
                    cid=cid,
                    oid=oid,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    session=session_token,
                )

    @allure.title("Test V2 Session Token - Cannot Delegate More Verbs Than Original")
    @pytest.mark.simple
    def test_v2_session_token_cannot_delegate_more_verbs(self, default_wallet, user_wallet):
        """
        Test that delegated token cannot have more verbs than the original token

        Steps:
        1. Create owner token with GET,HEAD verbs
        2. Attempt to create delegated token with GET,HEAD,PUT verbs
        3. Verify the delegation fails or delegated token is rejected
        """

        with allure.step("Init wallets"):
            owner_wallet = default_wallet
            subject_address = get_last_address_from_wallet(user_wallet.path, user_wallet.password)

        with allure.step("Create Container"):
            cid = create_container(
                owner_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Put object"):
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            put_object_to_random_node(
                wallet=owner_wallet.path,
                path=file_path,
                cid=cid,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step("Create owner token with limited verbs (GET,HEAD)"):
            contexts = [f"{cid}:GET,HEAD"]

            owner_token = create_session_token_v2(
                shell=self.shell,
                owner_wallet=owner_wallet,
                rpc_endpoint=self.neofs_env.sn_rpc,
                lifetime=1000,
                subjects=[subject_address],
                contexts=contexts,
            )

        with allure.step("Attempt to create delegated token with more verbs (GET,HEAD,PUT)"):
            extended_contexts = [f"{cid}:GET,HEAD,PUT"]

            with pytest.raises(Exception, match=".*verb.*|.*context.*"):
                create_session_token_v2(
                    shell=self.shell,
                    owner_wallet=user_wallet,
                    rpc_endpoint=self.neofs_env.sn_rpc,
                    lifetime=900,
                    subjects=[subject_address],
                    contexts=extended_contexts,
                    origin=owner_token,
                )

        with allure.step("Create delegated token with more verbs and force=True (should succeed but be invalid)"):
            forced_extended_token = create_session_token_v2(
                shell=self.shell,
                owner_wallet=user_wallet,
                rpc_endpoint=self.neofs_env.sn_rpc,
                lifetime=900,
                subjects=[subject_address],
                contexts=extended_contexts,
                origin=owner_token,
                force=True,
            )

        with allure.step("Verify forced token is rejected for PUT operation"):
            with pytest.raises(Exception, match=INVALID_V2_SESSION_TOKEN):
                put_object(
                    wallet=user_wallet.path,
                    path=file_path,
                    cid=cid,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    session=forced_extended_token,
                )

    @allure.title("Test V2 Session Token - Cannot Delegate Longer Lifetime")
    @pytest.mark.simple
    def test_v2_session_token_cannot_delegate_longer_lifetime(self, default_wallet, user_wallet):
        """
        Test that delegated token cannot have longer lifetime than the original token

        Steps:
        1. Create owner token with lifetime=100
        2. Attempt to create delegated token with lifetime=200
        3. Verify the delegation fails or delegated token is rejected
        """

        with allure.step("Init wallets"):
            owner_wallet = default_wallet
            subject_address = get_last_address_from_wallet(user_wallet.path, user_wallet.password)

        with allure.step("Create Container"):
            cid = create_container(
                owner_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Create owner token with short lifetime"):
            contexts = [f"{cid}:GET,HEAD"]
            short_lifetime = 100

            owner_token = create_session_token_v2(
                shell=self.shell,
                owner_wallet=owner_wallet,
                rpc_endpoint=self.neofs_env.sn_rpc,
                lifetime=short_lifetime,
                subjects=[subject_address],
                contexts=contexts,
            )

        with allure.step("Attempt to create delegated token with longer lifetime"):
            longer_lifetime = 200

            with pytest.raises(Exception, match=".*lifetime.*|.*expir.*"):
                create_session_token_v2(
                    shell=self.shell,
                    owner_wallet=user_wallet,
                    rpc_endpoint=self.neofs_env.sn_rpc,
                    lifetime=longer_lifetime,
                    subjects=[subject_address],
                    contexts=contexts,
                    origin=owner_token,
                )

        with allure.step("Create delegated token with longer lifetime and force=True (should succeed but be invalid)"):
            forced_longer_token = create_session_token_v2(
                shell=self.shell,
                owner_wallet=user_wallet,
                rpc_endpoint=self.neofs_env.sn_rpc,
                lifetime=longer_lifetime,
                subjects=[subject_address],
                contexts=contexts,
                origin=owner_token,
                force=True,
            )

        with allure.step("Verify forced token is rejected when used"):
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oid = put_object_to_random_node(
                wallet=owner_wallet.path,
                path=file_path,
                cid=cid,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

            with pytest.raises(Exception, match=SESSION_VALIDATION_FAILED):
                head_object(
                    wallet=user_wallet.path,
                    cid=cid,
                    oid=oid,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    session=forced_longer_token,
                )

    @allure.title("Test V2 Session Token - Cannot Delegate Different Container")
    @pytest.mark.simple
    def test_v2_session_token_cannot_delegate_different_container(self, default_wallet, user_wallet):
        """
        Test that delegated token cannot specify different container than original

        Steps:
        1. Create two containers
        2. Create owner token for first container
        3. Attempt to create delegated token for second container
        4. Verify the delegation fails or delegated token is rejected
        """

        with allure.step("Init wallets"):
            owner_wallet = default_wallet
            subject_address = get_last_address_from_wallet(user_wallet.path, user_wallet.password)

        with allure.step("Create first container"):
            cid1 = create_container(
                owner_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Create second container"):
            cid2 = create_container(
                owner_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Create owner token for first container"):
            contexts = [f"{cid1}:GET,HEAD"]

            owner_token = create_session_token_v2(
                shell=self.shell,
                owner_wallet=owner_wallet,
                rpc_endpoint=self.neofs_env.sn_rpc,
                lifetime=1000,
                subjects=[subject_address],
                contexts=contexts,
            )

        with allure.step("Attempt to create delegated token for second container"):
            different_contexts = [f"{cid2}:GET,HEAD"]

            with pytest.raises(Exception, match=".*container.*|.*context.*"):
                create_session_token_v2(
                    shell=self.shell,
                    owner_wallet=user_wallet,
                    rpc_endpoint=self.neofs_env.sn_rpc,
                    lifetime=900,
                    subjects=[subject_address],
                    contexts=different_contexts,
                    origin=owner_token,
                )

        with allure.step(
            "Create delegated token for different container with force=True (should succeed but be invalid)"
        ):
            forced_different_token = create_session_token_v2(
                shell=self.shell,
                owner_wallet=user_wallet,
                rpc_endpoint=self.neofs_env.sn_rpc,
                lifetime=900,
                subjects=[subject_address],
                contexts=different_contexts,
                origin=owner_token,
                force=True,
            )

        with allure.step("Verify forced token is rejected for second container"):
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oid2 = put_object_to_random_node(
                wallet=owner_wallet.path,
                path=file_path,
                cid=cid2,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

            with pytest.raises(Exception, match=SESSION_VALIDATION_FAILED):
                head_object(
                    wallet=user_wallet.path,
                    cid=cid2,
                    oid=oid2,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    session=forced_different_token,
                )

    @allure.title("Test V2 Session Token - Cannot Use Token with Wrong Subject")
    @pytest.mark.parametrize("use_delegation", [False, True], ids=["owner-direct", "delegation"])
    @pytest.mark.simple
    def test_v2_session_token_wrong_subject(self, default_wallet, user_wallet, stranger_wallet, use_delegation):
        """
        Test that token cannot be used by wallet not in subjects list

        Steps:
        1. Create V2 session token for user_wallet
        2. Attempt to use token with stranger_wallet
        3. Verify operation is rejected
        """

        with allure.step("Init wallets"):
            owner_wallet = default_wallet

        with allure.step("Create Container"):
            cid = create_container(
                owner_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Put object"):
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oid = put_object_to_random_node(
                wallet=owner_wallet.path,
                path=file_path,
                cid=cid,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step("Create V2 Session Token for user_wallet only"):
            contexts = [f"{cid}:GET,HEAD"]

            session_token = self._create_session_token_with_delegation(
                owner_wallet=owner_wallet,
                user_wallet=user_wallet,
                contexts=contexts,
                use_delegation=use_delegation,
            )

        operator_wallet = user_wallet if use_delegation else owner_wallet

        with allure.step("Verify token works for authorized subject"):
            head_object(
                wallet=operator_wallet.path,
                cid=cid,
                oid=oid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                session=session_token,
            )

        with allure.step("Verify token is rejected for unauthorized wallet"):
            with pytest.raises(Exception, match=SESSION_VALIDATION_FAILED):
                head_object(
                    wallet=stranger_wallet.path,
                    cid=cid,
                    oid=oid,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    session=session_token,
                )

    @allure.title("Test V2 Session Token - Cannot Add Additional Containers in Delegation")
    @pytest.mark.simple
    def test_v2_session_token_cannot_add_containers(self, default_wallet, user_wallet):
        """
        Test that delegated token cannot add additional containers beyond original

        Steps:
        1. Create owner token with one container
        2. Create second container
        3. Attempt to create delegated token with both containers
        4. Verify the delegation fails or operations on second container fail
        """

        with allure.step("Init wallets"):
            owner_wallet = default_wallet
            subject_address = get_last_address_from_wallet(user_wallet.path, user_wallet.password)

        with allure.step("Create first container"):
            cid1 = create_container(
                owner_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Create second container"):
            cid2 = create_container(
                owner_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Create owner token for first container only"):
            contexts = [f"{cid1}:GET,HEAD"]

            owner_token = create_session_token_v2(
                shell=self.shell,
                owner_wallet=owner_wallet,
                rpc_endpoint=self.neofs_env.sn_rpc,
                lifetime=1000,
                subjects=[subject_address],
                contexts=contexts,
            )

        with allure.step("Attempt to create delegated token with additional container"):
            extended_contexts = sorted([f"{cid1}:GET,HEAD", f"{cid2}:GET,HEAD"])

            with pytest.raises(Exception, match=".*container.*|.*context.*"):
                create_session_token_v2(
                    shell=self.shell,
                    owner_wallet=user_wallet,
                    rpc_endpoint=self.neofs_env.sn_rpc,
                    lifetime=900,
                    subjects=[subject_address],
                    contexts=extended_contexts,
                    origin=owner_token,
                )

        with allure.step(
            "Create delegated token with additional container and force=True (should succeed but be invalid)"
        ):
            forced_extended_token = create_session_token_v2(
                shell=self.shell,
                owner_wallet=user_wallet,
                rpc_endpoint=self.neofs_env.sn_rpc,
                lifetime=900,
                subjects=[subject_address],
                contexts=extended_contexts,
                origin=owner_token,
                force=True,
            )

        with allure.step("Verify forced token is rejected for second container"):
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oid2 = put_object_to_random_node(
                wallet=owner_wallet.path,
                path=file_path,
                cid=cid2,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

            with pytest.raises(Exception, match=SESSION_VALIDATION_FAILED):
                head_object(
                    wallet=user_wallet.path,
                    cid=cid2,
                    oid=oid2,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    session=forced_extended_token,
                )

    @allure.title("Test V2 Session Token - Cannot Use Container Verbs on Object Operations")
    @pytest.mark.simple
    def test_v2_session_token_wrong_verb_type(self, default_wallet):
        """
        Test that container verbs cannot be used for object operations

        Steps:
        1. Create token with CONTAINERPUT,CONTAINERDELETE verbs and wildcard container
        2. Attempt to use it for PUT object operation
        3. Verify operation is rejected
        """

        with allure.step("Init wallet"):
            owner_wallet = default_wallet
            owner_address = get_last_address_from_wallet(owner_wallet.path, owner_wallet.password)

        with allure.step("Create Container"):
            cid = create_container(
                owner_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Create V2 Session Token with container verbs"):
            contexts = ["0:CONTAINERPUT,CONTAINERDELETE"]

            session_token = create_session_token_v2(
                shell=self.shell,
                owner_wallet=owner_wallet,
                rpc_endpoint=self.neofs_env.sn_rpc,
                lifetime=1000,
                subjects=[owner_address],
                contexts=contexts,
            )

        with allure.step("Attempt to use token for object PUT operation"):
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))

            with pytest.raises(Exception, match=SESSION_TOKEN_DOESNOT_AUTHORIZE):
                put_object(
                    wallet=owner_wallet.path,
                    path=file_path,
                    cid=cid,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    session=session_token,
                )
