import logging
import re

import allure
import neofs_env.neofs_epoch as neofs_epoch
import pytest
from helpers.common import STORAGE_GC_TIME
from helpers.complex_object_actions import get_link_object, get_nodes_with_object, get_object_chunks
from helpers.container import create_container
from helpers.file_helper import generate_file
from helpers.grpc_responses import (
    LIFETIME_REQUIRED,
    LOCK_NON_REGULAR_OBJECT,
    LOCK_OBJECT_EXPIRATION,
    LOCK_OBJECT_REMOVAL,
    OBJECT_ALREADY_REMOVED,
    OBJECT_IS_LOCKED,
    OBJECT_NOT_FOUND,
)
from helpers.neofs_verbs import delete_object, head_object, lock_object, put_object_to_random_node
from helpers.node_management import delete_node_metadata, drop_object, start_storage_nodes
from helpers.storage_container import StorageContainer, StorageContainerInfo
from helpers.storage_object_info import LockObjectInfo, StorageObjectInfo, delete_objects
from helpers.test_control import expect_not_raises, wait_for_success
from helpers.utility import parse_time, wait_for_gc_pass_on_storage_nodes
from helpers.wallet_helpers import create_wallet
from helpers.wellknown_acl import PUBLIC_ACL
from neofs_env.neofs_env_test_base import TestNeofsBase
from neofs_testlib.env.env import NeoFSEnv, NodeWallet
from neofs_testlib.shell import Shell
from pytest import FixtureRequest

logger = logging.getLogger("NeoLogger")

FIXTURE_LOCK_LIFETIME = 5
FIXTURE_OBJECT_LIFETIME = 10


@pytest.fixture(scope="module")
def user_wallet():
    with allure.step("Create user wallet with container"):
        return create_wallet()


@pytest.fixture(scope="module")
def user_container(user_wallet: NodeWallet, client_shell: Shell, neofs_env: NeoFSEnv):
    container_id = create_container(user_wallet.path, shell=client_shell, endpoint=neofs_env.sn_rpc)
    return StorageContainer(StorageContainerInfo(container_id, user_wallet), client_shell, neofs_env)


@pytest.fixture(
    scope="module",
)
def locked_storage_object(
    user_container: StorageContainer,
    client_shell: Shell,
    neofs_env: NeoFSEnv,
    request: FixtureRequest,
):
    """
    Intention of this fixture is to provide storage object which is NOT expected to be deleted during test act phase
    """
    with allure.step("Creating locked object"):
        current_epoch = neofs_epoch.ensure_fresh_epoch(neofs_env)
        expiration_epoch = current_epoch + FIXTURE_LOCK_LIFETIME

        storage_object = user_container.generate_object(
            neofs_env.get_object_size(request.param), expire_at=current_epoch + FIXTURE_OBJECT_LIFETIME
        )
        lock_object_id = lock_object(
            storage_object.wallet_file_path,
            storage_object.cid,
            storage_object.oid,
            client_shell,
            neofs_env.sn_rpc,
            lifetime=FIXTURE_LOCK_LIFETIME,
        )
        storage_object.locks = [
            LockObjectInfo(storage_object.cid, lock_object_id, FIXTURE_LOCK_LIFETIME, expiration_epoch)
        ]

    yield storage_object

    with allure.step("Delete created locked object"):
        current_epoch = neofs_epoch.get_epoch(neofs_env)
        epoch_diff = expiration_epoch - current_epoch + 1

        if epoch_diff > 0:
            with allure.step(f"Tick {epoch_diff} epochs"):
                for _ in range(epoch_diff):
                    neofs_epoch.tick_epoch_and_wait(neofs_env, current_epoch)
        try:
            delete_object(
                storage_object.wallet_file_path,
                storage_object.cid,
                storage_object.oid,
                client_shell,
                neofs_env.sn_rpc,
            )
        except Exception as ex:
            ex_message = str(ex)
            # It's okay if object already removed
            if not re.search(OBJECT_NOT_FOUND, ex_message) and not re.search(OBJECT_ALREADY_REMOVED, ex_message):
                raise ex
            logger.debug(ex_message)


class TestObjectLockWithGrpc(TestNeofsBase):
    @pytest.fixture()
    def new_locked_storage_object(self, user_container: StorageContainer, request: FixtureRequest) -> StorageObjectInfo:
        """
        Intention of this fixture is to provide new storage object for tests which may delete or corrupt the object or it's complementary objects
        So we need a new one each time we ask for it
        """
        with allure.step("Creating locked object"):
            current_epoch = self.ensure_fresh_epoch()

            storage_object = user_container.generate_object(
                self.neofs_env.get_object_size(request.param), expire_at=current_epoch + FIXTURE_OBJECT_LIFETIME
            )
            lock_object(
                storage_object.wallet_file_path,
                storage_object.cid,
                storage_object.oid,
                self.shell,
                self.neofs_env.sn_rpc,
                lifetime=FIXTURE_LOCK_LIFETIME,
            )

        return storage_object

    @allure.title("Locked object should be protected from deletion")
    @pytest.mark.parametrize(
        "locked_storage_object",
        [
            pytest.param("simple_object_size", id="simple object", marks=pytest.mark.simple),
            pytest.param("complex_object_size", id="complex object", marks=pytest.mark.complex),
        ],
        indirect=True,
    )
    def test_locked_object_cannot_be_deleted(
        self,
        request: FixtureRequest,
        locked_storage_object: StorageObjectInfo,
    ):
        """
        Locked object should be protected from deletion
        """
        allure.dynamic.title(f"Locked object should be protected from deletion for {request.node.callspec.id}")

        with pytest.raises(Exception, match=OBJECT_IS_LOCKED):
            delete_object(
                locked_storage_object.wallet_file_path,
                locked_storage_object.cid,
                locked_storage_object.oid,
                self.shell,
                self.neofs_env.sn_rpc,
            )

    @allure.title("Lock object of a simple object should be protected from deletion")
    # We operate with only lock object here so no complex object needed in this test
    @pytest.mark.parametrize("locked_storage_object", ["simple_object_size"], indirect=True)
    def test_lock_object_itself_cannot_be_deleted(
        self,
        locked_storage_object: StorageObjectInfo,
    ):
        """
        Lock object itself should be protected from deletion
        """

        lock_object = locked_storage_object.locks[0]
        wallet_path = locked_storage_object.wallet_file_path

        with pytest.raises(Exception, match=LOCK_OBJECT_REMOVAL):
            delete_object(
                wallet_path,
                lock_object.cid,
                lock_object.oid,
                self.shell,
                self.neofs_env.sn_rpc,
            )

    @allure.title("Lock object of a simple object cannot be locked")
    # We operate with only lock object here so no complex object needed in this test
    @pytest.mark.parametrize("locked_storage_object", ["simple_object_size"], indirect=True)
    def test_lock_object_cannot_be_locked(
        self,
        locked_storage_object: StorageObjectInfo,
    ):
        """
        Lock object itself cannot be locked
        """

        lock_object_info = locked_storage_object.locks[0]
        wallet_path = locked_storage_object.wallet_file_path

        with pytest.raises(Exception, match=LOCK_NON_REGULAR_OBJECT):
            lock_object(
                wallet_path,
                lock_object_info.cid,
                lock_object_info.oid,
                self.shell,
                self.neofs_env.sn_rpc,
                1,
            )

    @allure.title("Cannot lock simple object without lifetime and expire_at fields")
    # We operate with only lock object here so no complex object needed in this test
    @pytest.mark.parametrize("locked_storage_object", ["simple_object_size"], indirect=True)
    @pytest.mark.parametrize(
        "wrong_lifetime,wrong_expire_at,expected_error",
        [
            (None, None, LIFETIME_REQUIRED),
            (0, 0, LIFETIME_REQUIRED),
            (0, None, LIFETIME_REQUIRED),
            (None, 0, LIFETIME_REQUIRED),
            (-1, None, 'invalid argument "-1" for "--lifetime" flag'),
            (None, -1, 'invalid argument "-1" for "-e, --expire-at" flag'),
        ],
    )
    def test_cannot_lock_object_without_lifetime(
        self,
        locked_storage_object: StorageObjectInfo,
        wrong_lifetime: int,
        wrong_expire_at: int,
        expected_error: str,
    ):
        """
        Cannot lock object without lifetime and expire_at fields
        """
        allure.dynamic.title(
            f"Cannot lock object without lifetime and expire_at fields: (lifetime={wrong_lifetime}, expire-at={wrong_expire_at})"
        )

        lock_object_info = locked_storage_object.locks[0]
        wallet_path = locked_storage_object.wallet_file_path

        with pytest.raises(Exception):
            lock_object(
                wallet_path,
                lock_object_info.cid,
                lock_object_info.oid,
                self.shell,
                self.neofs_env.sn_rpc,
                lifetime=wrong_lifetime,
                expire_at=wrong_expire_at,
            )

    @allure.title("Expired object should be deleted after locks are expired")
    @pytest.mark.parametrize(
        "object_size",
        [
            pytest.param("simple_object_size", id="simple object", marks=pytest.mark.simple),
            pytest.param("complex_object_size", id="complex object", marks=pytest.mark.complex),
        ],
    )
    def test_expired_object_should_be_deleted_after_locks_are_expired(
        self,
        request: FixtureRequest,
        user_container: StorageContainer,
        object_size: int,
    ):
        """
        Expired object should be deleted after locks are expired
        """
        allure.dynamic.title(f"Expired object should be deleted after locks are expired for {request.node.callspec.id}")

        current_epoch = self.ensure_fresh_epoch()
        storage_object = user_container.generate_object(
            self.neofs_env.get_object_size(object_size), expire_at=current_epoch + 1
        )

        with allure.step("Lock object for couple epochs"):
            lock_object(
                storage_object.wallet_file_path,
                storage_object.cid,
                storage_object.oid,
                self.shell,
                self.neofs_env.sn_rpc,
                lifetime=2,
            )
            lock_object(
                storage_object.wallet_file_path,
                storage_object.cid,
                storage_object.oid,
                self.shell,
                self.neofs_env.sn_rpc,
                expire_at=current_epoch + 2,
            )

        with allure.step("Check object is not deleted at expiration time"):
            self.tick_epochs_and_wait(2)
            # Must wait to ensure object is not deleted
            wait_for_gc_pass_on_storage_nodes()
            with expect_not_raises():
                head_object(
                    storage_object.wallet_file_path,
                    storage_object.cid,
                    storage_object.oid,
                    self.shell,
                    self.neofs_env.sn_rpc,
                )

        @wait_for_success(parse_time(STORAGE_GC_TIME))
        def check_object_not_found():
            with pytest.raises(Exception, match=OBJECT_NOT_FOUND):
                head_object(
                    storage_object.wallet_file_path,
                    storage_object.cid,
                    storage_object.oid,
                    self.shell,
                    self.neofs_env.sn_rpc,
                )

        with allure.step("Wait for object to be deleted after third epoch"):
            self.tick_epochs_and_wait(1)
            check_object_not_found()

    @allure.title("Should be possible to lock multiple objects at once")
    @pytest.mark.parametrize(
        "object_size",
        [
            pytest.param("simple_object_size", id="simple object", marks=pytest.mark.simple),
            pytest.param("complex_object_size", id="complex object", marks=pytest.mark.complex),
        ],
    )
    def test_should_be_possible_to_lock_multiple_objects_at_once(
        self,
        request: FixtureRequest,
        user_container: StorageContainer,
        object_size: int,
    ):
        """
        Should be possible to lock multiple objects at once
        """
        allure.dynamic.title(f"Should be possible to lock multiple objects at once for {request.node.callspec.id}")

        current_epoch = self.ensure_fresh_epoch()
        storage_objects: list[StorageObjectInfo] = []

        with allure.step("Generate three objects"):
            for _ in range(3):
                storage_objects.append(
                    user_container.generate_object(
                        self.neofs_env.get_object_size(object_size), expire_at=current_epoch + 5
                    )
                )

        lock_object(
            storage_objects[0].wallet_file_path,
            storage_objects[0].cid,
            ",".join([storage_object.oid for storage_object in storage_objects]),
            self.shell,
            self.neofs_env.sn_rpc,
            expire_at=current_epoch + 1,
        )

        for storage_object in storage_objects:
            with allure.step(f"Try to delete object {storage_object.oid}"):
                with pytest.raises(Exception, match=OBJECT_IS_LOCKED):
                    delete_object(
                        storage_object.wallet_file_path,
                        storage_object.cid,
                        storage_object.oid,
                        self.shell,
                        self.neofs_env.sn_rpc,
                    )

        with allure.step("Tick two epochs"):
            self.tick_epochs_and_wait(2)

        with expect_not_raises():
            delete_objects(storage_objects, self.shell, self.neofs_env)

    @allure.title("Already outdated lock should not be applied")
    @pytest.mark.parametrize(
        "object_size",
        [
            pytest.param("simple_object_size", id="simple object", marks=pytest.mark.simple),
            pytest.param("complex_object_size", id="complex object", marks=pytest.mark.complex),
        ],
    )
    def test_already_outdated_lock_should_not_be_applied(
        self,
        request: FixtureRequest,
        user_container: StorageContainer,
        object_size: int,
    ):
        """
        Already outdated lock should not be applied
        """
        allure.dynamic.title(f"Already outdated lock should not be applied for {request.node.callspec.id}")

        current_epoch = self.ensure_fresh_epoch()

        storage_object = user_container.generate_object(
            self.neofs_env.get_object_size(object_size), expire_at=current_epoch + 1
        )

        expiration_epoch = current_epoch - 1
        with pytest.raises(
            Exception,
            match=LOCK_OBJECT_EXPIRATION.format(expiration_epoch=expiration_epoch, current_epoch=current_epoch),
        ):
            lock_object(
                storage_object.wallet_file_path,
                storage_object.cid,
                storage_object.oid,
                self.shell,
                self.neofs_env.sn_rpc,
                expire_at=expiration_epoch,
            )

    @allure.title("After lock expiration with lifetime user should be able to delete object")
    @pytest.mark.parametrize(
        "object_size",
        [
            pytest.param("simple_object_size", id="simple object", marks=pytest.mark.simple),
            pytest.param("complex_object_size", id="complex object", marks=pytest.mark.complex),
        ],
    )
    @expect_not_raises()
    def test_after_lock_expiration_with_lifetime_user_should_be_able_to_delete_object(
        self,
        request: FixtureRequest,
        user_container: StorageContainer,
        object_size: int,
    ):
        """
        After lock expiration with lifetime user should be able to delete object
        """
        allure.dynamic.title(
            f"After lock expiration with lifetime user should be able to delete object for {request.node.callspec.id}"
        )

        current_epoch = self.ensure_fresh_epoch()
        storage_object = user_container.generate_object(
            self.neofs_env.get_object_size(object_size), expire_at=current_epoch + 5
        )

        lock_object(
            storage_object.wallet_file_path,
            storage_object.cid,
            storage_object.oid,
            self.shell,
            self.neofs_env.sn_rpc,
            lifetime=1,
        )

        self.tick_epochs_and_wait(2)
        with expect_not_raises():
            delete_object(
                storage_object.wallet_file_path,
                storage_object.cid,
                storage_object.oid,
                self.shell,
                self.neofs_env.sn_rpc,
            )

    @allure.title("After lock expiration with expire_at user should be able to delete object")
    @pytest.mark.parametrize(
        "object_size",
        [
            pytest.param("simple_object_size", id="simple object", marks=pytest.mark.simple),
            pytest.param("complex_object_size", id="complex object", marks=pytest.mark.complex),
        ],
    )
    @expect_not_raises()
    def test_after_lock_expiration_with_expire_at_user_should_be_able_to_delete_object(
        self,
        request: FixtureRequest,
        user_container: StorageContainer,
        object_size: int,
    ):
        """
        After lock expiration with expire_at user should be able to delete object
        """
        allure.dynamic.title(
            f"After lock expiration with expire_at user should be able to delete object for {request.node.callspec.id}"
        )

        current_epoch = self.ensure_fresh_epoch()

        storage_object = user_container.generate_object(
            self.neofs_env.get_object_size(object_size), expire_at=current_epoch + 5
        )

        lock_object(
            storage_object.wallet_file_path,
            storage_object.cid,
            storage_object.oid,
            self.shell,
            endpoint=self.neofs_env.sn_rpc,
            expire_at=current_epoch + 1,
        )

        self.tick_epochs_and_wait(2)

        with expect_not_raises():
            delete_object(
                storage_object.wallet_file_path,
                storage_object.cid,
                storage_object.oid,
                self.shell,
                self.neofs_env.sn_rpc,
            )

    @allure.title("Link object of locked complex object can be dropped via control")
    @pytest.mark.parametrize(
        "new_locked_storage_object",
        [
            pytest.param("complex_object_size", id="complex object", marks=pytest.mark.complex),
        ],
        indirect=True,
    )
    def test_link_object_of_locked_complex_object_can_be_dropped(
        self, new_locked_storage_object: StorageObjectInfo, neofs_env: NeoFSEnv
    ):
        link_object_id = get_link_object(
            new_locked_storage_object.wallet_file_path,
            new_locked_storage_object.cid,
            new_locked_storage_object.oid,
            self.shell,
            self.neofs_env.storage_nodes,
        )

        with allure.step(f"Drop link object with id {link_object_id} from nodes"):
            nodes_with_object = get_nodes_with_object(
                new_locked_storage_object.cid,
                link_object_id,
                shell=self.shell,
                nodes=self.neofs_env.storage_nodes,
                neofs_env=neofs_env,
            )
            for node in nodes_with_object:
                with expect_not_raises():
                    drop_object(node, new_locked_storage_object.cid, link_object_id)

    @allure.title("Chunks of locked complex object can be dropped via control")
    @pytest.mark.parametrize(
        "new_locked_storage_object",
        [
            pytest.param("complex_object_size", id="complex object", marks=pytest.mark.complex),
        ],
        indirect=True,
    )
    def test_chunks_of_locked_complex_object_can_be_dropped(
        self, new_locked_storage_object: StorageObjectInfo, neofs_env: NeoFSEnv
    ):
        chunk_objects = get_object_chunks(
            new_locked_storage_object.wallet_file_path,
            new_locked_storage_object.cid,
            new_locked_storage_object.oid,
            self.shell,
            self.neofs_env,
        )

        for chunk in chunk_objects:
            with allure.step(f"Drop chunk object with id {chunk[0]} from nodes"):
                nodes_with_object = get_nodes_with_object(
                    new_locked_storage_object.cid,
                    chunk[0],
                    shell=self.shell,
                    nodes=self.neofs_env.storage_nodes,
                    neofs_env=neofs_env,
                )
                for node in nodes_with_object:
                    with expect_not_raises():
                        drop_object(node, new_locked_storage_object.cid, chunk[0])

    @pytest.mark.parametrize(
        "new_locked_storage_object",
        [
            pytest.param("simple_object_size", id="simple object", marks=pytest.mark.simple),
            pytest.param("complex_object_size", id="complex object", marks=pytest.mark.complex),
        ],
        indirect=True,
    )
    def test_locked_object_can_be_dropped(
        self,
        new_locked_storage_object: StorageObjectInfo,
        request: FixtureRequest,
        neofs_env: NeoFSEnv,
    ):
        allure.dynamic.title(f"Locked {request.node.callspec.id} can be dropped via control")
        nodes_with_object = get_nodes_with_object(
            new_locked_storage_object.cid,
            new_locked_storage_object.oid,
            shell=self.shell,
            nodes=self.neofs_env.storage_nodes,
            neofs_env=neofs_env,
        )

        for node in nodes_with_object:
            with expect_not_raises():
                drop_object(node, new_locked_storage_object.cid, new_locked_storage_object.oid)

    @allure.title("Lock object of a simple object can be dropped via control")
    @pytest.mark.parametrize("locked_storage_object", ["simple_object_size"], indirect=True)
    def test_lock_object_can_be_dropped(self, locked_storage_object: StorageObjectInfo, neofs_env: NeoFSEnv):
        lock_object_info = locked_storage_object.locks[0]

        nodes_with_object = get_nodes_with_object(
            lock_object_info.cid,
            lock_object_info.oid,
            shell=self.shell,
            nodes=self.neofs_env.storage_nodes,
            neofs_env=neofs_env,
        )

        for node in nodes_with_object:
            with expect_not_raises():
                drop_object(node, lock_object_info.cid, lock_object_info.oid)

    @allure.title(
        "The locked object must be protected from deletion after metabase deletion "
        "(metabase resynchronization must be enabled), and after restarting storage nodes"
    )
    @pytest.mark.skip(reason="Unknown issue")
    @pytest.mark.parametrize(
        "new_locked_storage_object",
        [
            pytest.param("simple_object_size", id="simple object", marks=pytest.mark.simple),
            pytest.param("complex_object_size", id="complex object", marks=pytest.mark.complex),
        ],
        indirect=True,
    )
    def test_the_object_lock_should_be_kept_after_metabase_deletion(
        self,
        new_locked_storage_object: StorageObjectInfo,
        enable_metabase_resync_on_start,
        neofs_env: NeoFSEnv,
    ):
        """
        Lock objects should fill metabase on resync_metabase
        """
        with allure.step("Log nodes with object"):
            get_nodes_with_object(
                new_locked_storage_object.cid,
                new_locked_storage_object.oid,
                shell=self.shell,
                nodes=self.neofs_env.storage_nodes,
                neofs_env=neofs_env,
            )

        with allure.step(f"Try to delete object {new_locked_storage_object.oid} before metabase deletion"):
            with pytest.raises(Exception, match=OBJECT_IS_LOCKED):
                delete_object(
                    new_locked_storage_object.wallet_file_path,
                    new_locked_storage_object.cid,
                    new_locked_storage_object.oid,
                    self.shell,
                    self.neofs_env.sn_rpc,
                )

        with allure.step("Log nodes with object"):
            nodes_with_object_after_first_try = get_nodes_with_object(
                new_locked_storage_object.cid,
                new_locked_storage_object.oid,
                shell=self.shell,
                nodes=self.neofs_env.storage_nodes,
                neofs_env=neofs_env,
            )

        with allure.step("Delete metabase files from storage nodes"):
            for node in self.neofs_env.storage_nodes:
                delete_node_metadata(node)

        with allure.step("Start nodes after metabase deletion"):
            start_storage_nodes(self.neofs_env.storage_nodes)

        with allure.step("Log nodes with object"):
            nodes_with_object_after_metabase_deletion = get_nodes_with_object(
                new_locked_storage_object.cid,
                new_locked_storage_object.oid,
                shell=self.shell,
                nodes=self.neofs_env.storage_nodes,
                neofs_env=neofs_env,
            )

        assert len(nodes_with_object_after_metabase_deletion) >= len(nodes_with_object_after_first_try)

        with allure.step(f"Try to delete object {new_locked_storage_object.oid} after metabase deletion"):
            with pytest.raises(Exception, match=OBJECT_IS_LOCKED):
                delete_object(
                    new_locked_storage_object.wallet_file_path,
                    new_locked_storage_object.cid,
                    new_locked_storage_object.oid,
                    self.shell,
                    self.neofs_env.sn_rpc,
                )

    @pytest.mark.simple
    def test_locked_object_removal_from_not_owner_node(self, default_wallet: NodeWallet):
        with allure.step("Create container"):
            wallet = default_wallet
            source_file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            cid = create_container(
                wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                rule="REP 1 CBF 1",
                basic_acl=PUBLIC_ACL,
            )

        with allure.step("Put object"):
            oid = put_object_to_random_node(
                wallet.path, source_file_path, cid, shell=self.shell, neofs_env=self.neofs_env
            )

        with allure.step("Lock object"):
            lock_object(
                wallet.path,
                cid,
                oid,
                self.shell,
                self.neofs_env.sn_rpc,
                lifetime=FIXTURE_LOCK_LIFETIME,
            )

        with allure.step("Get nodes with object"):
            nodes_with_object = get_nodes_with_object(
                cid,
                oid,
                shell=self.shell,
                nodes=self.neofs_env.storage_nodes,
                neofs_env=self.neofs_env,
            )

        with allure.step(f"Try to delete object {oid} from other node"):
            for node in self.neofs_env.storage_nodes:
                if node not in nodes_with_object:
                    with pytest.raises(Exception, match=OBJECT_IS_LOCKED):
                        delete_object(
                            wallet.path,
                            cid,
                            oid,
                            self.shell,
                            node.endpoint,
                        )
