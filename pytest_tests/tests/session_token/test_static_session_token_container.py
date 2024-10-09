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
    CONTAINER_CREATION_TIMED_OUT,
    CONTAINER_DELETION_TIMED_OUT,
    EACL_TIMED_OUT,
    INVALID_EXP,
    INVALID_IAT,
    INVALID_NBF,
    INVALID_TOKEN_FORMAT,
    INVALID_VERB,
    NOT_SESSION_CONTAINER_OWNER,
    SESSION_NOT_ISSUED_BY_OWNER,
)
from helpers.neofs_verbs import put_object_to_random_node
from helpers.object_access import can_put_object
from helpers.session_token import (
    ContainerVerb,
    Lifetime,
    ObjectVerb,
    generate_container_session_token,
    get_container_signed_token,
    get_object_signed_token,
    sign_session_token,
)
from helpers.storage_object_info import StorageObjectInfo
from helpers.wellknown_acl import PUBLIC_ACL
from neofs_env.neofs_env_test_base import NeofsEnvTestBase
from neofs_testlib.env.env import NodeWallet
from neofs_testlib.shell import Shell


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

    def test_static_session_token_container_create_invalid_lifetime(
        self,
        owner_wallet: NodeWallet,
        user_wallet: NodeWallet,
        temp_directory: str,
    ):
        epoch = self.get_epoch()

        with allure.step(
            'IAt is bigger than current epoch should lead to a 1024 error with "token should not be issued yet" message'
        ):
            session_token_file = generate_container_session_token(
                owner_wallet=owner_wallet,
                session_wallet=user_wallet,
                verb=ContainerVerb.CREATE,
                tokens_dir=temp_directory,
                lifetime=Lifetime(exp=epoch + 2, nbf=epoch, iat=epoch + 2),
            )
            signed_token = sign_session_token(self.shell, session_token_file, owner_wallet)

            with allure.step("Try to create container with invalid IAt"):
                with pytest.raises(Exception, match=INVALID_IAT):
                    create_container(
                        user_wallet.path,
                        session_token=signed_token,
                        shell=self.shell,
                        endpoint=self.neofs_env.sn_rpc,
                        wait_for_creation=False,
                    )

        with allure.step(
            'NBf is bigger than current epoch should lead to a 1024 error with "token "token is not valid yet" message'
        ):
            session_token_file = generate_container_session_token(
                owner_wallet=owner_wallet,
                session_wallet=user_wallet,
                verb=ContainerVerb.CREATE,
                tokens_dir=temp_directory,
                lifetime=Lifetime(exp=epoch + 2, nbf=epoch + 2, iat=epoch - 1),
            )
            signed_token = sign_session_token(self.shell, session_token_file, owner_wallet)

            with allure.step("Try to create container with invalid NBf"):
                with pytest.raises(Exception, match=INVALID_NBF):
                    create_container(
                        user_wallet.path,
                        session_token=signed_token,
                        shell=self.shell,
                        endpoint=self.neofs_env.sn_rpc,
                        wait_for_creation=False,
                    )

        with allure.step("Exp is smaller than current epoch should lead to a 4097 error"):
            session_token_file = generate_container_session_token(
                owner_wallet=owner_wallet,
                session_wallet=user_wallet,
                verb=ContainerVerb.CREATE,
                tokens_dir=temp_directory,
                lifetime=Lifetime(exp=epoch - 1, nbf=epoch + 1, iat=epoch - 1),
            )
            signed_token = sign_session_token(self.shell, session_token_file, owner_wallet)

            with allure.step("Try to create container with invalid Exp"):
                with pytest.raises(Exception, match=INVALID_EXP):
                    create_container(
                        user_wallet.path,
                        session_token=signed_token,
                        shell=self.shell,
                        endpoint=self.neofs_env.sn_rpc,
                        wait_for_creation=False,
                    )

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

    def test_static_session_token_container_create_signed_with_wrong_wallet(
        self, owner_wallet: NodeWallet, user_wallet: NodeWallet, stranger_wallet: NodeWallet, temp_directory: str
    ):
        if self.neofs_env.storage_nodes[0]._get_version() <= "0.43.0":
            pytest.skip("This test runs only on post 0.43.0 neofs-node version")
        session_token_file = generate_container_session_token(
            owner_wallet=user_wallet,
            session_wallet=user_wallet,
            verb=ContainerVerb.CREATE,
            tokens_dir=temp_directory,
        )
        container_token = sign_session_token(self.shell, session_token_file, stranger_wallet)

        with pytest.raises(Exception, match=CONTAINER_CREATION_TIMED_OUT):
            create_container(
                user_wallet.path,
                session_token=container_token,
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

    def test_static_session_token_container_delete_with_other_verb(
        self,
        owner_wallet: NodeWallet,
        user_wallet: NodeWallet,
        static_sessions: dict[ContainerVerb, str],
    ):
        """
        Validate static session without delete operation
        """
        with allure.step("Create container"):
            cid = create_container(
                owner_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                wait_for_creation=False,
            )
        with allure.step("Try to delete container with static session token without DELETE rule"):
            for verb in [verb for verb in ContainerVerb if verb != ContainerVerb.DELETE]:
                with pytest.raises(RuntimeError, match=INVALID_VERB):
                    delete_container(
                        wallet=user_wallet.path,
                        cid=cid,
                        session_token=static_sessions[verb],
                        shell=self.shell,
                        endpoint=self.neofs_env.sn_rpc,
                        await_mode=False,
                    )
                assert cid in list_containers(owner_wallet.path, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

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

        self.static_session_token(owner_wallet, user_wallet, self.shell, temp_directory)
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

    def test_static_session_token_container_set_eacl_with_other_verb(
        self,
        owner_wallet: NodeWallet,
        user_wallet: NodeWallet,
        stranger_wallet: NodeWallet,
        static_sessions: dict[ContainerVerb, str],
        simple_object_size,
    ):
        """
        Validate static session without seteacl operation
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

        with allure.step("Try to seteacl with static session token without seteacl rule"):
            for verb in [verb for verb in ContainerVerb if verb != ContainerVerb.SETEACL]:
                with pytest.raises(RuntimeError, match=INVALID_VERB):
                    eacl_deny = [
                        EACLRule(access=EACLAccess.DENY, role=EACLRole.OTHERS, operation=op) for op in EACLOperation
                    ]
                    set_eacl(
                        user_wallet.path,
                        cid,
                        create_eacl(cid, eacl_deny, shell=self.shell),
                        shell=self.shell,
                        endpoint=self.neofs_env.sn_rpc,
                        session_token=static_sessions[verb],
                    )
                assert can_put_object(stranger_wallet.path, cid, file_path, self.shell, neofs_env=self.neofs_env)

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

    def test_use_object_session_token_for_container_operation(
        self,
        owner_wallet: NodeWallet,
        user_wallet: NodeWallet,
        temp_directory: str,
        simple_object_size,
    ):
        with allure.step("Prepare object session token"):
            cid = create_container(owner_wallet.path, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

            storage_objects = []

            with allure.step("Put object"):
                file_path = generate_file(simple_object_size)

                storage_object_id = put_object_to_random_node(
                    wallet=owner_wallet.path,
                    path=file_path,
                    cid=cid,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                )

                storage_object = StorageObjectInfo(cid, storage_object_id)
                storage_object.size = simple_object_size
                storage_object.wallet_file_path = owner_wallet.path
                storage_object.file_path = file_path
                storage_objects.append(storage_object)

            object_token = get_object_signed_token(
                owner_wallet,
                user_wallet,
                cid,
                storage_objects,
                ObjectVerb.PUT,
                self.shell,
                temp_directory,
            )

        with allure.step("Try to create container with object static session token"):
            with pytest.raises(RuntimeError, match=INVALID_TOKEN_FORMAT):
                create_container(
                    user_wallet.path,
                    session_token=object_token,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    wait_for_creation=False,
                )
