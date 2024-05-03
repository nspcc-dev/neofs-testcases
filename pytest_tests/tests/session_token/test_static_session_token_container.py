import allure
import pytest
from helpers.acl import (
    EACLAccess,
    EACLOperation,
    EACLRole,
    EACLRule,
    create_eacl,
    set_eacl,
    wait_for_cache_expired,
)
from helpers.container import create_container, delete_container, get_container, list_containers
from helpers.file_helper import generate_file
from helpers.grpc_responses import (
    CONTAINER_DELETION_TIMED_OUT,
    EACL_TIMED_OUT,
    NOT_SESSION_CONTAINER_OWNER,
    SESSION_NOT_ISSUED_BY_OWNER,
)
from helpers.object_access import can_put_object
from helpers.session_token import ContainerVerb, get_container_signed_token
from helpers.wellknown_acl import PUBLIC_ACL
from neofs_env.neofs_env_test_base import NeofsEnvTestBase
from neofs_testlib.env.env import NodeWallet
from neofs_testlib.shell import Shell


@pytest.mark.static_session_container
class TestSessionTokenContainer(NeofsEnvTestBase):
    @pytest.fixture(scope="module")
    def static_sessions(
        self,
        owner_wallet: NodeWallet,
        user_wallet: NodeWallet,
        client_shell: Shell,
        temp_directory: str,
    ) -> dict[ContainerVerb, str]:
        """
        Returns dict with static session token file paths for all verbs with default lifetime
        """
        return self.static_session_token(owner_wallet, user_wallet, client_shell, temp_directory)

    def static_session_token(
        self,
        owner_wallet: NodeWallet,
        user_wallet: NodeWallet,
        client_shell: Shell,
        temp_directory: str,
    ) -> dict[ContainerVerb, str]:
        return {
            verb: get_container_signed_token(owner_wallet, user_wallet, verb, client_shell, temp_directory)
            for verb in ContainerVerb
        }

    def test_static_session_token_container_create(
        self,
        owner_wallet: NodeWallet,
        user_wallet: NodeWallet,
        static_sessions: dict[ContainerVerb, str],
    ):
        """
        Validate static session with create operation
        """
        with allure.step("Create container with static session token"):
            cid = create_container(
                user_wallet.path,
                session_token=static_sessions[ContainerVerb.CREATE],
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                wait_for_creation=False,
            )

        container_info: dict[str, str] = get_container(
            owner_wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc
        )
        assert container_info["ownerID"] == owner_wallet.address

        assert cid not in list_containers(user_wallet.path, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
        assert cid in list_containers(owner_wallet.path, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

    def test_static_session_token_container_create_with_other_verb(
        self,
        user_wallet: NodeWallet,
        static_sessions: dict[ContainerVerb, str],
    ):
        """
        Validate static session without create operation
        """
        with allure.step("Try create container with static session token without PUT rule"):
            for verb in [verb for verb in ContainerVerb if verb != ContainerVerb.CREATE]:
                with pytest.raises(Exception):
                    create_container(
                        user_wallet.path,
                        session_token=static_sessions[verb],
                        shell=self.shell,
                        endpoint=self.neofs_env.sn_rpc,
                        wait_for_creation=False,
                    )

    def test_static_session_token_container_create_with_other_wallet(
        self,
        stranger_wallet: NodeWallet,
        static_sessions: dict[ContainerVerb, str],
    ):
        """
        Validate static session with create operation for other wallet
        """
        with allure.step("Try create container with static session token without PUT rule"):
            with pytest.raises(Exception):
                create_container(
                    stranger_wallet.path,
                    session_token=static_sessions[ContainerVerb.CREATE],
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    wait_for_creation=False,
                )

    def test_static_session_token_container_delete(
        self,
        owner_wallet: NodeWallet,
        user_wallet: NodeWallet,
        static_sessions: dict[ContainerVerb, str],
    ):
        """
        Validate static session with delete operation
        """
        with allure.step("Create container"):
            cid = create_container(
                owner_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                wait_for_creation=False,
            )
        with allure.step("Delete container with static session token"):
            delete_container(
                wallet=user_wallet.path,
                cid=cid,
                session_token=static_sessions[ContainerVerb.DELETE],
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                await_mode=True,
            )

        assert cid not in list_containers(owner_wallet.path, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

    @pytest.mark.trusted_party_proved
    @allure.title("Not owner user can NOT delete container")
    def test_not_owner_user_can_not_delete_container(
        self,
        owner_wallet: NodeWallet,
        user_wallet: NodeWallet,
        stranger_wallet: NodeWallet,
        scammer_wallet: NodeWallet,
        static_sessions: dict[ContainerVerb, str],
        temp_directory: str,
        not_owner_wallet,
    ):
        with allure.step("Create container"):
            cid = create_container(
                owner_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        user_token = self.static_session_token(owner_wallet, user_wallet, self.shell, temp_directory)
        stranger_token = self.static_session_token(user_wallet, stranger_wallet, self.shell, temp_directory)

        with allure.step("Try to delete container using stranger token"):
            with pytest.raises(RuntimeError, match=NOT_SESSION_CONTAINER_OWNER):
                delete_container(
                    wallet=user_wallet.path,
                    cid=cid,
                    session_token=stranger_token[ContainerVerb.DELETE],
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    await_mode=True,
                )

        with allure.step("Try to force delete container using stranger token"):
            with pytest.raises(RuntimeError, match=SESSION_NOT_ISSUED_BY_OWNER):
                delete_container(
                    wallet=user_wallet.path,
                    cid=cid,
                    session_token=stranger_token[ContainerVerb.DELETE],
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    await_mode=True,
                    force=True,
                )

    @pytest.mark.trusted_party_proved
    @allure.title("Not trusted party user can NOT delete container")
    def test_not_trusted_party_user_can_not_delete_container(
        self,
        owner_wallet: NodeWallet,
        user_wallet: NodeWallet,
        stranger_wallet: NodeWallet,
        scammer_wallet: NodeWallet,
        static_sessions: dict[ContainerVerb, str],
        temp_directory: str,
        not_owner_wallet,
    ):
        with allure.step("Create container"):
            cid = create_container(
                owner_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        user_token = self.static_session_token(owner_wallet, user_wallet, self.shell, temp_directory)
        stranger_token = self.static_session_token(user_wallet, stranger_wallet, self.shell, temp_directory)

        with allure.step("Try to delete container using scammer token"):
            with pytest.raises(RuntimeError, match=CONTAINER_DELETION_TIMED_OUT):
                delete_container(
                    wallet=scammer_wallet.path,
                    cid=cid,
                    session_token=user_token[ContainerVerb.DELETE],
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    await_mode=True,
                )

            with pytest.raises(RuntimeError, match=NOT_SESSION_CONTAINER_OWNER):
                delete_container(
                    wallet=scammer_wallet.path,
                    cid=cid,
                    session_token=stranger_token[ContainerVerb.DELETE],
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    await_mode=True,
                )

        with allure.step("Try to force delete container using scammer token"):
            with pytest.raises(RuntimeError, match=CONTAINER_DELETION_TIMED_OUT):
                delete_container(
                    wallet=scammer_wallet.path,
                    cid=cid,
                    session_token=user_token[ContainerVerb.DELETE],
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    await_mode=True,
                    force=True,
                )

            with pytest.raises(RuntimeError, match=SESSION_NOT_ISSUED_BY_OWNER):
                delete_container(
                    wallet=scammer_wallet.path,
                    cid=cid,
                    session_token=stranger_token[ContainerVerb.DELETE],
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    await_mode=True,
                    force=True,
                )

    def test_static_session_token_container_set_eacl(
        self,
        owner_wallet: NodeWallet,
        user_wallet: NodeWallet,
        stranger_wallet: NodeWallet,
        static_sessions: dict[ContainerVerb, str],
        simple_object_size,
    ):
        """
        Validate static session with set eacl operation
        """
        with allure.step("Create container"):
            cid = create_container(
                owner_wallet.path,
                basic_acl=PUBLIC_ACL,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
        file_path = generate_file(simple_object_size)
        assert can_put_object(stranger_wallet.path, cid, file_path, self.shell, neofs_env=self.neofs_env)

        with allure.step("Deny all operations for other via eACL"):
            eacl_deny = [EACLRule(access=EACLAccess.DENY, role=EACLRole.OTHERS, operation=op) for op in EACLOperation]
            set_eacl(
                user_wallet.path,
                cid,
                create_eacl(cid, eacl_deny, shell=self.shell),
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                session_token=static_sessions[ContainerVerb.SETEACL],
            )
            wait_for_cache_expired()

        assert not can_put_object(stranger_wallet.path, cid, file_path, self.shell, neofs_env=self.neofs_env)

    @pytest.mark.trusted_party_proved
    @allure.title("Not owner and not trusted party can NOT set eacl")
    def test_static_session_token_container_set_eacl_only_trusted_party_proved_by_the_container_owner(
        self,
        owner_wallet: NodeWallet,
        user_wallet: NodeWallet,
        stranger_wallet: NodeWallet,
        scammer_wallet: NodeWallet,
        static_sessions: dict[ContainerVerb, str],
        temp_directory: str,
        not_owner_wallet,
    ):
        with allure.step("Create container"):
            cid = create_container(
                owner_wallet.path,
                basic_acl=PUBLIC_ACL,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        user_token = self.static_session_token(owner_wallet, user_wallet, self.shell, temp_directory)
        stranger_token = self.static_session_token(user_wallet, stranger_wallet, self.shell, temp_directory)

        new_eacl = [EACLRule(access=EACLAccess.DENY, role=EACLRole.OTHERS, operation=op) for op in EACLOperation]

        with allure.step("Try to deny all operations for other via eACL"):
            with pytest.raises(RuntimeError, match=NOT_SESSION_CONTAINER_OWNER):
                set_eacl(
                    user_wallet.path,
                    cid,
                    create_eacl(cid, new_eacl, shell=self.shell),
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    session_token=stranger_token[ContainerVerb.SETEACL],
                )

        with allure.step("Try to deny all operations for other via eACL using scammer wallet"):
            with allure.step("Using user token"):
                with pytest.raises(RuntimeError, match=EACL_TIMED_OUT):
                    set_eacl(
                        scammer_wallet.path,
                        cid,
                        create_eacl(cid, new_eacl, shell=self.shell),
                        shell=self.shell,
                        endpoint=self.neofs_env.sn_rpc,
                        session_token=user_token[ContainerVerb.SETEACL],
                    )
            with allure.step("Using scammer token"):
                with pytest.raises(RuntimeError, match=NOT_SESSION_CONTAINER_OWNER):
                    set_eacl(
                        scammer_wallet.path,
                        cid,
                        create_eacl(cid, new_eacl, shell=self.shell),
                        shell=self.shell,
                        endpoint=self.neofs_env.sn_rpc,
                        session_token=stranger_token[ContainerVerb.SETEACL],
                    )
