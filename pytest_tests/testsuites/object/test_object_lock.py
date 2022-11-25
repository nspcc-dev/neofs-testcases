import logging
import re

import allure
import pytest
from common import COMPLEX_OBJ_SIZE, SIMPLE_OBJ_SIZE, STORAGE_GC_TIME
from complex_object_actions import get_link_object
from container import create_container
from epoch import ensure_fresh_epoch, get_epoch, tick_epoch
from grpc_responses import (
    LIFETIME_REQUIRED,
    LOCK_NON_REGULAR_OBJECT,
    LOCK_OBJECT_EXPIRATION,
    LOCK_OBJECT_REMOVAL,
    OBJECT_ALREADY_REMOVED,
    OBJECT_IS_LOCKED,
    OBJECT_NOT_FOUND,
)
from neofs_testlib.shell import Shell
from pytest import FixtureRequest
from python_keywords.neofs_verbs import delete_object, head_object, lock_object
from test_control import expect_not_raises, wait_for_success
from utility import parse_time, wait_for_gc_pass_on_storage_nodes

from helpers.container import StorageContainer, StorageContainerInfo
from helpers.storage_object_info import LockObjectInfo, StorageObjectInfo
from helpers.wallet import WalletFactory, WalletFile
from steps.storage_object import delete_objects

logger = logging.getLogger("NeoLogger")

FIXTURE_LOCK_LIFETIME = 5
FIXTURE_OBJECT_LIFETIME = 10


def get_storage_object_chunks(storage_object: StorageObjectInfo, shell: Shell):
    with allure.step(f"Get complex object chunks (f{storage_object.oid})"):
        split_object_id = get_link_object(
            storage_object.wallet_file_path,
            storage_object.cid,
            storage_object.oid,
            shell,
            is_direct=False,
        )
        head = head_object(
            storage_object.wallet_file_path, storage_object.cid, split_object_id, shell
        )

        chunks_object_ids = []
        if "split" in head["header"] and "children" in head["header"]["split"]:
            chunks_object_ids = head["header"]["split"]["children"]
        return chunks_object_ids


@pytest.fixture(
    scope="module",
)
def user_wallet(wallet_factory: WalletFactory):
    with allure.step("Create user wallet with container"):
        wallet_file = wallet_factory.create_wallet()
        return wallet_file


@pytest.fixture(
    scope="module",
)
def user_container(user_wallet: WalletFile, client_shell: Shell):
    container_id = create_container(user_wallet.path, shell=client_shell)
    return StorageContainer(StorageContainerInfo(container_id, user_wallet), client_shell)


@pytest.fixture(
    scope="module",
)
def locked_storage_object(
    user_container: StorageContainer,
    client_shell: Shell,
    request: FixtureRequest,
):
    with allure.step(f"Creating locked object"):
        current_epoch = ensure_fresh_epoch(client_shell)
        expiration_epoch = current_epoch + FIXTURE_LOCK_LIFETIME

        storage_object = user_container.generate_object(
            request.param, expire_at=current_epoch + FIXTURE_OBJECT_LIFETIME
        )
        lock_object_id = lock_object(
            storage_object.wallet_file_path,
            storage_object.cid,
            storage_object.oid,
            client_shell,
            lifetime=FIXTURE_LOCK_LIFETIME,
        )
        storage_object.locks = [
            LockObjectInfo(
                storage_object.cid, lock_object_id, FIXTURE_LOCK_LIFETIME, expiration_epoch
            )
        ]

    yield storage_object

    with allure.step(f"Delete created locked object"):
        current_epoch = get_epoch(client_shell)
        epoch_diff = expiration_epoch - current_epoch + 1

        if epoch_diff > 0:
            with allure.step(f"Tick {epoch_diff} epochs"):
                for _ in range(epoch_diff):
                    tick_epoch(client_shell)
        try:
            delete_object(
                storage_object.wallet_file_path,
                storage_object.cid,
                storage_object.oid,
                client_shell,
            )
        except Exception as ex:
            ex_message = str(ex)
            # It's okay if object already removed
            if not re.search(OBJECT_NOT_FOUND, ex_message) and not re.search(
                OBJECT_ALREADY_REMOVED, ex_message
            ):
                raise ex
            logger.debug(ex_message)


@pytest.mark.sanity
@pytest.mark.grpc_object_lock
class TestObjectLockWithGrpc:
    @allure.title("Locked object should be protected from deletion")
    @pytest.mark.parametrize(
        "locked_storage_object",
        [SIMPLE_OBJ_SIZE, COMPLEX_OBJ_SIZE],
        ids=["simple object", "complex object"],
        indirect=True,
    )
    def test_locked_object_cannot_be_deleted(
        self,
        client_shell: Shell,
        request: FixtureRequest,
        locked_storage_object: StorageObjectInfo,
    ):
        """
        Locked object should be protected from deletion
        """
        allure.dynamic.title(
            f"Locked object should be protected from deletion for {request.node.callspec.id}"
        )

        with pytest.raises(Exception, match=OBJECT_IS_LOCKED):
            delete_object(
                locked_storage_object.wallet_file_path,
                locked_storage_object.cid,
                locked_storage_object.oid,
                client_shell,
            )

    @allure.title("Lock object itself should be protected from deletion")
    # We operate with only lock object here so no complex object needed in this test
    @pytest.mark.parametrize("locked_storage_object", [SIMPLE_OBJ_SIZE], indirect=True)
    def test_lock_object_itself_cannot_be_deleted(
        self,
        client_shell: Shell,
        locked_storage_object: StorageObjectInfo,
    ):
        """
        Lock object itself should be protected from deletion
        """

        lock_object = locked_storage_object.locks[0]
        wallet_path = locked_storage_object.wallet_file_path

        with pytest.raises(Exception, match=LOCK_OBJECT_REMOVAL):
            delete_object(wallet_path, lock_object.cid, lock_object.oid, client_shell)

    @allure.title("Lock object itself cannot be locked")
    # We operate with only lock object here so no complex object needed in this test
    @pytest.mark.parametrize("locked_storage_object", [SIMPLE_OBJ_SIZE], indirect=True)
    def test_lock_object_cannot_be_locked(
        self,
        client_shell: Shell,
        locked_storage_object: StorageObjectInfo,
    ):
        """
        Lock object itself cannot be locked
        """

        lock_object_info = locked_storage_object.locks[0]
        wallet_path = locked_storage_object.wallet_file_path

        with pytest.raises(Exception, match=LOCK_NON_REGULAR_OBJECT):
            lock_object(wallet_path, lock_object_info.cid, lock_object_info.oid, client_shell, 1)

    @allure.title("Cannot lock object without lifetime and expire_at fields")
    # We operate with only lock object here so no complex object needed in this test
    @pytest.mark.parametrize("locked_storage_object", [SIMPLE_OBJ_SIZE], indirect=True)
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
        client_shell: Shell,
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

        with pytest.raises(Exception, match=expected_error):
            lock_object(
                wallet_path,
                lock_object_info.cid,
                lock_object_info.oid,
                client_shell,
                lifetime=wrong_lifetime,
                expire_at=wrong_expire_at,
            )

    @allure.title("Expired object should be deleted after locks are expired")
    @pytest.mark.parametrize(
        "object_size", [SIMPLE_OBJ_SIZE, COMPLEX_OBJ_SIZE], ids=["simple object", "complex object"]
    )
    def test_expired_object_should_be_deleted_after_locks_are_expired(
        self,
        client_shell: Shell,
        request: FixtureRequest,
        user_container: StorageContainer,
        object_size: int,
    ):
        """
        Expired object should be deleted after locks are expired
        """
        allure.dynamic.title(
            f"Expired object should be deleted after locks are expired for {request.node.callspec.id}"
        )

        current_epoch = ensure_fresh_epoch(client_shell)
        storage_object = user_container.generate_object(object_size, expire_at=current_epoch + 1)

        with allure.step("Lock object for couple epochs"):
            lock_object(
                storage_object.wallet_file_path,
                storage_object.cid,
                storage_object.oid,
                client_shell,
                lifetime=3,
            )
            lock_object(
                storage_object.wallet_file_path,
                storage_object.cid,
                storage_object.oid,
                client_shell,
                expire_at=current_epoch + 3,
            )

        with allure.step("Check object is not deleted at expiration time"):
            tick_epoch(client_shell)
            tick_epoch(client_shell)
            # Must wait to ensure object is not deleted
            wait_for_gc_pass_on_storage_nodes()
            with expect_not_raises():
                head_object(
                    storage_object.wallet_file_path,
                    storage_object.cid,
                    storage_object.oid,
                    client_shell,
                )

        @wait_for_success(parse_time(STORAGE_GC_TIME))
        def check_object_not_found():
            with pytest.raises(Exception, match=OBJECT_NOT_FOUND):
                head_object(
                    storage_object.wallet_file_path,
                    storage_object.cid,
                    storage_object.oid,
                    client_shell,
                )

        with allure.step("Wait for object to be deleted after third epoch"):
            tick_epoch(client_shell)
            check_object_not_found()

    @allure.title("Should be possible to lock multiple objects at once")
    @pytest.mark.parametrize(
        "object_size",
        [SIMPLE_OBJ_SIZE, COMPLEX_OBJ_SIZE],
        ids=["simple object", "complex object"],
    )
    def test_should_be_possible_to_lock_multiple_objects_at_once(
        self,
        client_shell: Shell,
        request: FixtureRequest,
        user_container: StorageContainer,
        object_size: int,
    ):
        """
        Should be possible to lock multiple objects at once
        """
        allure.dynamic.title(
            f"Should be possible to lock multiple objects at once for {request.node.callspec.id}"
        )

        current_epoch = ensure_fresh_epoch(client_shell)
        storage_objects: list[StorageObjectInfo] = []

        with allure.step("Generate three objects"):
            for _ in range(3):
                storage_objects.append(
                    user_container.generate_object(object_size, expire_at=current_epoch + 5)
                )

        lock_object(
            storage_objects[0].wallet_file_path,
            storage_objects[0].cid,
            ",".join([storage_object.oid for storage_object in storage_objects]),
            client_shell,
            expire_at=current_epoch + 1,
        )

        for storage_object in storage_objects:
            with allure.step(f"Try to delete object {storage_object.oid}"):
                with pytest.raises(Exception, match=OBJECT_IS_LOCKED):
                    delete_object(
                        storage_object.wallet_file_path,
                        storage_object.cid,
                        storage_object.oid,
                        client_shell,
                    )

        with allure.step("Tick two epochs"):
            tick_epoch(client_shell)
            tick_epoch(client_shell)

        with expect_not_raises():
            delete_objects(storage_objects, client_shell)

    @allure.title("Already outdated lock should not be applied")
    @pytest.mark.parametrize(
        "object_size",
        [SIMPLE_OBJ_SIZE, COMPLEX_OBJ_SIZE],
        ids=["simple object", "complex object"],
    )
    def test_already_outdated_lock_should_not_be_applied(
        self,
        client_shell: Shell,
        request: FixtureRequest,
        user_container: StorageContainer,
        object_size: int,
    ):
        """
        Already outdated lock should not be applied
        """
        allure.dynamic.title(
            f"Already outdated lock should not be applied for {request.node.callspec.id}"
        )

        current_epoch = ensure_fresh_epoch(client_shell)

        storage_object = user_container.generate_object(object_size, expire_at=current_epoch + 1)

        expiration_epoch = current_epoch - 1
        with pytest.raises(
            Exception,
            match=LOCK_OBJECT_EXPIRATION.format(
                expiration_epoch=expiration_epoch, current_epoch=current_epoch
            ),
        ):
            lock_object(
                storage_object.wallet_file_path,
                storage_object.cid,
                storage_object.oid,
                client_shell,
                expire_at=expiration_epoch,
            )

    @allure.title("After lock expiration with lifetime user should be able to delete object")
    @pytest.mark.parametrize(
        "object_size",
        [SIMPLE_OBJ_SIZE, COMPLEX_OBJ_SIZE],
        ids=["simple object", "complex object"],
    )
    @expect_not_raises()
    def test_after_lock_expiration_with_lifetime_user_should_be_able_to_delete_object(
        self,
        client_shell: Shell,
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

        current_epoch = ensure_fresh_epoch(client_shell)
        storage_object = user_container.generate_object(object_size, expire_at=current_epoch + 1)

        lock_object(
            storage_object.wallet_file_path,
            storage_object.cid,
            storage_object.oid,
            client_shell,
            lifetime=1,
        )

        tick_epoch(client_shell)

        delete_object(
            storage_object.wallet_file_path, storage_object.cid, storage_object.oid, client_shell
        )

    @allure.title("After lock expiration with expire_at user should be able to delete object")
    @pytest.mark.parametrize(
        "object_size",
        [SIMPLE_OBJ_SIZE, COMPLEX_OBJ_SIZE],
        ids=["simple object", "complex object"],
    )
    @expect_not_raises()
    def test_after_lock_expiration_with_expire_at_user_should_be_able_to_delete_object(
        self,
        client_shell: Shell,
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

        current_epoch = ensure_fresh_epoch(client_shell)

        storage_object = user_container.generate_object(object_size, expire_at=current_epoch + 5)

        lock_object(
            storage_object.wallet_file_path,
            storage_object.cid,
            storage_object.oid,
            client_shell,
            expire_at=current_epoch + 1,
        )

        tick_epoch(client_shell)

        delete_object(
            storage_object.wallet_file_path, storage_object.cid, storage_object.oid, client_shell
        )

    @allure.title("Complex object chunks should also be protected from deletion")
    @pytest.mark.parametrize(
        # Only complex objects are required for this test
        "locked_storage_object",
        [COMPLEX_OBJ_SIZE],
        indirect=True,
    )
    def test_complex_object_chunks_should_also_be_protected_from_deletion(
        self,
        client_shell: Shell,
        locked_storage_object: StorageObjectInfo,
    ):
        """
        Complex object chunks should also be protected from deletion
        """

        chunk_object_ids = get_storage_object_chunks(locked_storage_object, client_shell)
        for chunk_object_id in chunk_object_ids:
            with allure.step(f"Try to delete chunk object {chunk_object_id}"):
                with pytest.raises(Exception, match=OBJECT_IS_LOCKED):
                    delete_object(
                        locked_storage_object.wallet_file_path,
                        locked_storage_object.cid,
                        chunk_object_id,
                        client_shell,
                    )

    @allure.title("Link object of complex object should also be protected from deletion")
    @pytest.mark.parametrize(
        # Only complex objects are required for this test
        "locked_storage_object",
        [COMPLEX_OBJ_SIZE],
        indirect=True,
    )
    def test_link_object_of_complex_object_should_also_be_protected_from_deletion(
        self,
        client_shell: Shell,
        locked_storage_object: StorageObjectInfo,
    ):
        """
        Link object of complex object should also be protected from deletion
        """

        link_object_id = get_link_object(
            locked_storage_object.wallet_file_path,
            locked_storage_object.cid,
            locked_storage_object.oid,
            client_shell,
            is_direct=False,
        )
        with allure.step(f"Try to delete link object {link_object_id}"):
            with pytest.raises(Exception, match=OBJECT_IS_LOCKED):
                delete_object(
                    locked_storage_object.wallet_file_path,
                    locked_storage_object.cid,
                    link_object_id,
                    client_shell,
                )
