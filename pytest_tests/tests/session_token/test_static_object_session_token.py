import logging

import allure
import neofs_env.neofs_epoch as neofs_epoch
import pytest
from helpers.container import create_container
from helpers.file_helper import generate_file
from helpers.grpc_responses import (
    EXPIRED_SESSION_TOKEN,
    INVALID_SESSION_TOKEN_OWNER,
    MALFORMED_REQUEST,
    OBJECT_ACCESS_DENIED,
    OBJECT_NOT_FOUND,
)
from helpers.neofs_verbs import (
    delete_object,
    get_object,
    get_object_from_random_node,
    get_range,
    get_range_hash,
    head_object,
    put_object_to_random_node,
    search_object,
)
from helpers.session_token import (
    INVALID_SIGNATURE,
    UNRELATED_CONTAINER,
    UNRELATED_KEY,
    UNRELATED_OBJECT,
    WRONG_VERB,
    Lifetime,
    ObjectVerb,
    generate_object_session_token,
    get_object_signed_token,
    sign_session_token,
)
from helpers.storage_object_info import StorageObjectInfo, delete_objects
from helpers.test_control import expect_not_raises
from neofs_env.neofs_env_test_base import NeofsEnvTestBase
from neofs_testlib.env.env import NeoFSEnv, NodeWallet
from neofs_testlib.shell import Shell
from pytest import FixtureRequest
from pytest_lazy_fixtures import lf

logger = logging.getLogger("NeoLogger")

RANGE_OFFSET_FOR_COMPLEX_OBJECT = 200


@pytest.fixture(scope="module")
def storage_containers(owner_wallet: NodeWallet, client_shell: Shell, neofs_env: NeoFSEnv) -> list[str]:
    cid = create_container(owner_wallet.path, shell=client_shell, endpoint=neofs_env.sn_rpc)
    other_cid = create_container(owner_wallet.path, shell=client_shell, endpoint=neofs_env.sn_rpc)
    yield [cid, other_cid]


@pytest.fixture(
    params=[lf("simple_object_size"), lf("complex_object_size")],
    ids=["simple object", "complex object"],
    # Scope module to upload/delete each files set only once
    scope="module",
)
def storage_objects(
    owner_wallet: NodeWallet,
    client_shell: Shell,
    storage_containers: list[str],
    neofs_env: NeoFSEnv,
    request: FixtureRequest,
) -> list[StorageObjectInfo]:
    file_path = generate_file(request.param)
    storage_objects = []

    with allure.step("Put objects"):
        # upload couple objects
        for _ in range(3):
            storage_object_id = put_object_to_random_node(
                wallet=owner_wallet.path,
                path=file_path,
                cid=storage_containers[0],
                shell=client_shell,
                neofs_env=neofs_env,
            )

            storage_object = StorageObjectInfo(storage_containers[0], storage_object_id)
            storage_object.size = request.param
            storage_object.wallet_file_path = owner_wallet.path
            storage_object.file_path = file_path
            storage_objects.append(storage_object)

    yield storage_objects

    # Teardown after all tests done with current param
    delete_objects(storage_objects, client_shell, neofs_env)


@allure.step("Get ranges for test")
def get_ranges(storage_object: StorageObjectInfo, max_object_size: int, shell: Shell, endpoint: str) -> list[str]:
    """
    Returns ranges to test range/hash methods via static session
    """
    object_size = storage_object.size

    if object_size > max_object_size:
        assert object_size >= max_object_size + RANGE_OFFSET_FOR_COMPLEX_OBJECT
        return [
            "0:10",
            f"{object_size - 10}:10",
            f"{max_object_size - RANGE_OFFSET_FOR_COMPLEX_OBJECT}:{RANGE_OFFSET_FOR_COMPLEX_OBJECT * 2}",
        ]
    else:
        return ["0:10", f"{object_size - 10}:10"]


@pytest.fixture(scope="module")
def static_sessions(
    owner_wallet: NodeWallet,
    user_wallet: NodeWallet,
    storage_containers: list[str],
    storage_objects: list[StorageObjectInfo],
    client_shell: Shell,
    temp_directory: str,
) -> dict[ObjectVerb, str]:
    """
    Returns dict with static session token file paths for all verbs with default lifetime with
    valid container and first two objects
    """
    return {
        verb: get_object_signed_token(
            owner_wallet,
            user_wallet,
            storage_containers[0],
            storage_objects[0:2],
            verb,
            client_shell,
            temp_directory,
        )
        for verb in ObjectVerb
    }


class TestObjectStaticSession(NeofsEnvTestBase):
    @allure.title("Validate static session with read operations")
    @pytest.mark.parametrize(
        "method_under_test,verb",
        [
            (head_object, ObjectVerb.HEAD),
            (get_object, ObjectVerb.GET),
        ],
    )
    def test_static_session_read(
        self,
        user_wallet: NodeWallet,
        storage_objects: list[StorageObjectInfo],
        static_sessions: dict[ObjectVerb, str],
        method_under_test,
        verb: ObjectVerb,
        request: FixtureRequest,
    ):
        """
        Validate static session with read operations
        """
        allure.dynamic.title(f"Validate static session with read operations for {request.node.callspec.id}")

        for node in self.neofs_env.storage_nodes:
            for storage_object in storage_objects[0:2]:
                method_under_test(
                    wallet=user_wallet.path,
                    cid=storage_object.cid,
                    oid=storage_object.oid,
                    shell=self.shell,
                    endpoint=node.endpoint,
                    session=static_sessions[verb],
                )

    @allure.title("Validate static session with range operations")
    @pytest.mark.parametrize(
        "method_under_test,verb",
        [(get_range, ObjectVerb.RANGE), (get_range_hash, ObjectVerb.RANGEHASH)],
    )
    def test_static_session_range(
        self,
        user_wallet: NodeWallet,
        storage_objects: list[StorageObjectInfo],
        static_sessions: dict[ObjectVerb, str],
        method_under_test,
        verb: ObjectVerb,
        request: FixtureRequest,
        max_object_size,
    ):
        """
        Validate static session with range operations
        """
        allure.dynamic.title(f"Validate static session with range operations for {request.node.callspec.id}")
        storage_object = storage_objects[0]
        ranges_to_test = get_ranges(storage_object, max_object_size, self.shell, self.neofs_env.sn_rpc)

        for range_to_test in ranges_to_test:
            with allure.step(f"Check range {range_to_test}"):
                with expect_not_raises():
                    method_under_test(
                        user_wallet.path,
                        storage_object.cid,
                        storage_object.oid,
                        shell=self.shell,
                        endpoint=self.neofs_env.sn_rpc,
                        session=static_sessions[verb],
                        range_cut=range_to_test,
                    )

    @allure.title("Validate static session with search operation")
    def test_static_session_search(
        self,
        user_wallet: NodeWallet,
        storage_objects: list[StorageObjectInfo],
        static_sessions: dict[ObjectVerb, str],
        request: FixtureRequest,
    ):
        """
        Validate static session with search operations
        """
        allure.dynamic.title(f"Validate static session with search for {request.node.callspec.id}")

        cid = storage_objects[0].cid
        expected_object_ids = [storage_object.oid for storage_object in storage_objects]
        actual_object_ids = search_object(
            user_wallet.path,
            cid,
            self.shell,
            endpoint=self.neofs_env.sn_rpc,
            session=static_sessions[ObjectVerb.SEARCH],
            root=True,
        )
        assert set(expected_object_ids) == set(actual_object_ids)

    @allure.title("Validate static session with object id not in session")
    def test_static_session_unrelated_object(
        self,
        user_wallet: NodeWallet,
        storage_objects: list[StorageObjectInfo],
        static_sessions: dict[ObjectVerb, str],
        request: FixtureRequest,
    ):
        """
        Validate static session with object id not in session
        """
        allure.dynamic.title(f"Validate static session with object id not in session for {request.node.callspec.id}")
        with pytest.raises(Exception, match=UNRELATED_OBJECT):
            head_object(
                user_wallet.path,
                storage_objects[2].cid,
                storage_objects[2].oid,
                self.shell,
                self.neofs_env.sn_rpc,
                session=static_sessions[ObjectVerb.HEAD],
            )

    @allure.title("Validate static session with user id not in session")
    def test_static_session_head_unrelated_user(
        self,
        stranger_wallet: NodeWallet,
        storage_objects: list[StorageObjectInfo],
        static_sessions: dict[ObjectVerb, str],
        request: FixtureRequest,
    ):
        """
        Validate static session with user id not in session
        """
        allure.dynamic.title(f"Validate static session with user id not in session for {request.node.callspec.id}")
        storage_object = storage_objects[0]

        with pytest.raises(Exception, match=UNRELATED_KEY):
            head_object(
                stranger_wallet.path,
                storage_object.cid,
                storage_object.oid,
                self.shell,
                self.neofs_env.sn_rpc,
                session=static_sessions[ObjectVerb.HEAD],
            )

    @allure.title("Validate static session with wrong verb in session")
    def test_static_session_head_wrong_verb(
        self,
        user_wallet: NodeWallet,
        storage_objects: list[StorageObjectInfo],
        static_sessions: dict[ObjectVerb, str],
        request: FixtureRequest,
    ):
        """
        Validate static session with wrong verb in session
        """
        allure.dynamic.title(f"Validate static session with wrong verb in session for {request.node.callspec.id}")
        storage_object = storage_objects[0]

        with pytest.raises(Exception, match=WRONG_VERB):
            get_object(
                user_wallet.path,
                storage_object.cid,
                storage_object.oid,
                self.shell,
                self.neofs_env.sn_rpc,
                session=static_sessions[ObjectVerb.HEAD],
            )

    @allure.title("Validate static session with container id not in session")
    def test_static_session_unrelated_container(
        self,
        user_wallet: NodeWallet,
        storage_objects: list[StorageObjectInfo],
        storage_containers: list[str],
        static_sessions: dict[ObjectVerb, str],
        request: FixtureRequest,
    ):
        """
        Validate static session with container id not in session
        """
        allure.dynamic.title(f"Validate static session with container id not in session for {request.node.callspec.id}")
        storage_object = storage_objects[0]

        with pytest.raises(Exception, match=UNRELATED_CONTAINER):
            get_object_from_random_node(
                user_wallet.path,
                storage_containers[1],
                storage_object.oid,
                self.shell,
                neofs_env=self.neofs_env,
                session=static_sessions[ObjectVerb.GET],
            )

    @allure.title("Validate static session which signed by another wallet")
    def test_static_session_signed_by_other(
        self,
        owner_wallet: NodeWallet,
        user_wallet: NodeWallet,
        stranger_wallet: NodeWallet,
        storage_containers: list[str],
        storage_objects: list[StorageObjectInfo],
        temp_directory: str,
        request: FixtureRequest,
    ):
        """
        Validate static session which signed by another wallet
        """
        allure.dynamic.title(f"Validate static session which signed by another wallet for {request.node.callspec.id}")
        storage_object = storage_objects[0]

        session_token_file = generate_object_session_token(
            owner_wallet,
            user_wallet,
            [storage_object.oid],
            storage_containers[0],
            ObjectVerb.HEAD,
            temp_directory,
        )
        signed_token_file = sign_session_token(self.shell, session_token_file, stranger_wallet)
        expected_error = INVALID_SESSION_TOKEN_OWNER
        with pytest.raises(Exception, match=expected_error):
            head_object(
                user_wallet.path,
                storage_object.cid,
                storage_object.oid,
                self.shell,
                self.neofs_env.sn_rpc,
                session=signed_token_file,
            )

    @allure.title("Validate static session which signed for another container")
    def test_static_session_signed_for_other_container(
        self,
        owner_wallet: NodeWallet,
        user_wallet: NodeWallet,
        storage_containers: list[str],
        storage_objects: list[StorageObjectInfo],
        temp_directory: str,
        request: FixtureRequest,
    ):
        """
        Validate static session which signed for another container
        """
        allure.dynamic.title(
            f"Validate static session which signed for another container for {request.node.callspec.id}"
        )
        storage_object = storage_objects[0]
        container = storage_containers[1]

        session_token_file = generate_object_session_token(
            owner_wallet,
            user_wallet,
            [storage_object.oid],
            container,
            ObjectVerb.HEAD,
            temp_directory,
        )
        signed_token_file = sign_session_token(self.shell, session_token_file, owner_wallet)
        with pytest.raises(Exception, match=OBJECT_NOT_FOUND):
            head_object(
                user_wallet.path,
                container,
                storage_object.oid,
                self.shell,
                self.neofs_env.sn_rpc,
                session=signed_token_file,
            )

    @allure.title("Validate static session which wasn't signed")
    def test_static_session_without_sign(
        self,
        owner_wallet: NodeWallet,
        user_wallet: NodeWallet,
        storage_containers: list[str],
        storage_objects: list[StorageObjectInfo],
        temp_directory: str,
        request: FixtureRequest,
    ):
        """
        Validate static session which wasn't signed
        """
        allure.dynamic.title(f"Validate static session which wasn't signed for {request.node.callspec.id}")
        storage_object = storage_objects[0]

        session_token_file = generate_object_session_token(
            owner_wallet,
            user_wallet,
            [storage_object.oid],
            storage_containers[0],
            ObjectVerb.HEAD,
            temp_directory,
        )
        with pytest.raises(Exception, match=INVALID_SIGNATURE):
            head_object(
                user_wallet.path,
                storage_object.cid,
                storage_object.oid,
                self.shell,
                self.neofs_env.sn_rpc,
                session=session_token_file,
            )

    @allure.title("Validate static session which expires at next epoch")
    def test_static_session_expiration_at_next(
        self,
        owner_wallet: NodeWallet,
        user_wallet: NodeWallet,
        storage_containers: list[str],
        storage_objects: list[StorageObjectInfo],
        temp_directory: str,
        request: FixtureRequest,
    ):
        """
        Validate static session which expires at next epoch
        """
        allure.dynamic.title(f"Validate static session which expires at next epoch for {request.node.callspec.id}")
        epoch = neofs_epoch.ensure_fresh_epoch(self.neofs_env)

        container = storage_containers[0]
        object_id = storage_objects[0].oid
        expiration = Lifetime(epoch + 1, epoch, epoch)

        token_expire_at_next_epoch = get_object_signed_token(
            owner_wallet,
            user_wallet,
            container,
            storage_objects,
            ObjectVerb.HEAD,
            self.shell,
            temp_directory,
            expiration,
        )

        head_object(
            user_wallet.path,
            container,
            object_id,
            self.shell,
            self.neofs_env.sn_rpc,
            session=token_expire_at_next_epoch,
        )

        self.tick_epochs_and_wait(2)

        with pytest.raises(Exception, match=EXPIRED_SESSION_TOKEN):
            head_object(
                user_wallet.path,
                container,
                object_id,
                self.shell,
                self.neofs_env.sn_rpc,
                session=token_expire_at_next_epoch,
            )

    @allure.title("Validate static session which is valid starting from next epoch")
    def test_static_session_start_at_next(
        self,
        owner_wallet: NodeWallet,
        user_wallet: NodeWallet,
        storage_containers: list[str],
        storage_objects: list[StorageObjectInfo],
        temp_directory: str,
        request: FixtureRequest,
    ):
        """
        Validate static session which is valid starting from next epoch
        """
        allure.dynamic.title(
            f"Validate static session which is valid starting from next epoch for {request.node.callspec.id}"
        )
        epoch = neofs_epoch.ensure_fresh_epoch(self.neofs_env)

        container = storage_containers[0]
        object_id = storage_objects[0].oid
        expiration = Lifetime(epoch + 2, epoch + 1, epoch)

        token_start_at_next_epoch = get_object_signed_token(
            owner_wallet,
            user_wallet,
            container,
            storage_objects,
            ObjectVerb.HEAD,
            self.shell,
            temp_directory,
            expiration,
        )

        with pytest.raises(Exception, match=MALFORMED_REQUEST):
            head_object(
                user_wallet.path,
                container,
                object_id,
                self.shell,
                self.neofs_env.sn_rpc,
                session=token_start_at_next_epoch,
            )

        self.tick_epochs_and_wait(1)
        head_object(
            user_wallet.path,
            container,
            object_id,
            self.shell,
            self.neofs_env.sn_rpc,
            session=token_start_at_next_epoch,
        )

        self.tick_epochs_and_wait(2)
        with pytest.raises(Exception, match=EXPIRED_SESSION_TOKEN):
            head_object(
                user_wallet.path,
                container,
                object_id,
                self.shell,
                self.neofs_env.sn_rpc,
                session=token_start_at_next_epoch,
            )

    @allure.title("Validate static session which is already expired")
    def test_static_session_already_expired(
        self,
        owner_wallet: NodeWallet,
        user_wallet: NodeWallet,
        storage_containers: list[str],
        storage_objects: list[StorageObjectInfo],
        temp_directory: str,
        request: FixtureRequest,
    ):
        """
        Validate static session which is already expired
        """
        allure.dynamic.title(f"Validate static session which is already expired for {request.node.callspec.id}")
        epoch = neofs_epoch.ensure_fresh_epoch(self.neofs_env)

        container = storage_containers[0]
        object_id = storage_objects[0].oid
        expiration = Lifetime(epoch - 1, epoch - 2, epoch - 2)

        token_already_expired = get_object_signed_token(
            owner_wallet,
            user_wallet,
            container,
            storage_objects,
            ObjectVerb.HEAD,
            self.shell,
            temp_directory,
            expiration,
        )

        with pytest.raises(Exception, match=EXPIRED_SESSION_TOKEN):
            head_object(
                user_wallet.path,
                container,
                object_id,
                self.shell,
                self.neofs_env.sn_rpc,
                session=token_already_expired,
            )

    @allure.title("Delete verb should be restricted for static session")
    def test_static_session_delete_verb(
        self,
        user_wallet: NodeWallet,
        storage_objects: list[StorageObjectInfo],
        static_sessions: dict[ObjectVerb, str],
        request: FixtureRequest,
    ):
        """
        Delete verb should be restricted for static session
        """
        allure.dynamic.title(f"Delete verb should be restricted for static session for {request.node.callspec.id}")
        storage_object = storage_objects[0]
        with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
            delete_object(
                user_wallet.path,
                storage_object.cid,
                storage_object.oid,
                self.shell,
                endpoint=self.neofs_env.sn_rpc,
                session=static_sessions[ObjectVerb.DELETE],
            )

    @allure.title("Put verb should be restricted for static session")
    def test_static_session_put_verb(
        self,
        user_wallet: NodeWallet,
        storage_objects: list[StorageObjectInfo],
        static_sessions: dict[ObjectVerb, str],
        request: FixtureRequest,
    ):
        """
        Put verb should be restricted for static session
        """
        allure.dynamic.title(f"Put verb should be restricted for static session for {request.node.callspec.id}")
        storage_object = storage_objects[0]
        with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
            put_object_to_random_node(
                user_wallet.path,
                storage_object.file_path,
                storage_object.cid,
                self.shell,
                neofs_env=self.neofs_env,
                session=static_sessions[ObjectVerb.PUT],
            )

    @allure.title("Validate static session which is issued in future epoch")
    def test_static_session_invalid_issued_epoch(
        self,
        owner_wallet: NodeWallet,
        user_wallet: NodeWallet,
        storage_containers: list[str],
        storage_objects: list[StorageObjectInfo],
        temp_directory: str,
        request: FixtureRequest,
    ):
        """
        Validate static session which is issued in future epoch
        """
        allure.dynamic.title(f"Validate static session which is issued in future epoch for {request.node.callspec.id}")
        epoch = neofs_epoch.ensure_fresh_epoch(self.neofs_env)

        container = storage_containers[0]
        object_id = storage_objects[0].oid
        expiration = Lifetime(epoch + 10, 0, epoch + 1)

        token_invalid_issue_time = get_object_signed_token(
            owner_wallet,
            user_wallet,
            container,
            storage_objects,
            ObjectVerb.HEAD,
            self.shell,
            temp_directory,
            expiration,
        )

        with pytest.raises(Exception, match=MALFORMED_REQUEST):
            head_object(
                user_wallet.path,
                container,
                object_id,
                self.shell,
                self.neofs_env.sn_rpc,
                session=token_invalid_issue_time,
            )
