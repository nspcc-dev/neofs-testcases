import logging
from typing import Optional

import allure
import pytest
from common import (
    ASSETS_DIR,
    COMPLEX_OBJ_SIZE,
    FREE_STORAGE,
    IR_WALLET_CONFIG,
    IR_WALLET_PASS,
    IR_WALLET_PATH,
    SIMPLE_OBJ_SIZE,
)
from epoch import tick_epoch
from file_helper import generate_file
from grpc_responses import OBJECT_ACCESS_DENIED, OBJECT_NOT_FOUND
from neofs_testlib.shell import Shell
from python_keywords.acl import (
    EACLAccess,
    EACLOperation,
    EACLRole,
    EACLRule,
    create_eacl,
    form_bearertoken_file,
    set_eacl,
)
from python_keywords.container import create_container
from python_keywords.neofs_verbs import put_object
from python_keywords.payment_neogo import neofs_deposit, transfer_mainnet_gas
from python_keywords.storage_group import (
    delete_storagegroup,
    get_storagegroup,
    list_storagegroup,
    put_storagegroup,
    verify_get_storage_group,
    verify_list_storage_group,
)
from wallet import init_wallet

logger = logging.getLogger("NeoLogger")
deposit = 30


@pytest.mark.parametrize(
    "object_size",
    [SIMPLE_OBJ_SIZE, COMPLEX_OBJ_SIZE],
    ids=["simple object", "complex object"],
)
@pytest.mark.storage_group
class TestStorageGroup:
    @pytest.fixture(autouse=True)
    def prepare_two_wallets(self, prepare_wallet_and_deposit):
        self.main_wallet = prepare_wallet_and_deposit
        self.other_wallet, _, _ = init_wallet(ASSETS_DIR)
        if not FREE_STORAGE:
            transfer_mainnet_gas(self.other_wallet, 31)
            neofs_deposit(self.other_wallet, 30)

    @allure.title("Test Storage Group in Private Container")
    def test_storagegroup_basic_private_container(self, client_shell, object_size):
        cid = create_container(self.main_wallet, shell=client_shell)
        file_path = generate_file(object_size)
        oid = put_object(self.main_wallet, file_path, cid, shell=client_shell)
        objects = [oid]
        storage_group = put_storagegroup(self.main_wallet, cid, objects)

        self.expect_success_for_storagegroup_operations(
            self.main_wallet, cid, objects, object_size, client_shell
        )
        self.expect_failure_for_storagegroup_operations(
            self.other_wallet, cid, objects, storage_group
        )
        self.storagegroup_operations_by_system_ro_container(
            self.main_wallet, cid, objects, object_size, client_shell
        )

    @allure.title("Test Storage Group in Public Container")
    def test_storagegroup_basic_public_container(self, client_shell, object_size):
        cid = create_container(self.main_wallet, basic_acl="public-read-write", shell=client_shell)
        file_path = generate_file(object_size)
        oid = put_object(self.main_wallet, file_path, cid, shell=client_shell)
        objects = [oid]
        self.expect_success_for_storagegroup_operations(
            self.main_wallet, cid, objects, object_size, client_shell
        )
        self.expect_success_for_storagegroup_operations(
            self.other_wallet, cid, objects, object_size, client_shell
        )
        self.storagegroup_operations_by_system_ro_container(
            self.main_wallet, cid, objects, object_size, client_shell
        )

    @allure.title("Test Storage Group in Read-Only Container")
    def test_storagegroup_basic_ro_container(self, client_shell, object_size):
        cid = create_container(self.main_wallet, basic_acl="public-read", shell=client_shell)
        file_path = generate_file(object_size)
        oid = put_object(self.main_wallet, file_path, cid, shell=client_shell)
        objects = [oid]
        self.expect_success_for_storagegroup_operations(
            self.main_wallet, cid, objects, object_size, client_shell
        )
        self.storagegroup_operations_by_other_ro_container(
            self.main_wallet, self.other_wallet, cid, objects, object_size, client_shell
        )
        self.storagegroup_operations_by_system_ro_container(
            self.main_wallet, cid, objects, object_size, client_shell
        )

    @allure.title("Test Storage Group with Bearer Allow")
    def test_storagegroup_bearer_allow(self, client_shell, object_size):
        cid = create_container(
            self.main_wallet, basic_acl="eacl-public-read-write", shell=client_shell
        )
        file_path = generate_file(object_size)
        oid = put_object(self.main_wallet, file_path, cid, shell=client_shell)
        objects = [oid]
        self.expect_success_for_storagegroup_operations(
            self.main_wallet, cid, objects, object_size, client_shell
        )
        storage_group = put_storagegroup(self.main_wallet, cid, objects)
        eacl_deny = [
            EACLRule(access=EACLAccess.DENY, role=role, operation=op)
            for op in EACLOperation
            for role in EACLRole
        ]
        set_eacl(
            self.main_wallet,
            cid,
            create_eacl(cid, eacl_deny, shell=client_shell),
            shell=client_shell,
        )
        self.expect_failure_for_storagegroup_operations(
            self.main_wallet, cid, objects, storage_group
        )
        bearer_file = form_bearertoken_file(
            self.main_wallet,
            cid,
            [
                EACLRule(operation=op, access=EACLAccess.ALLOW, role=EACLRole.USER)
                for op in EACLOperation
            ],
        )
        self.expect_success_for_storagegroup_operations(
            self.main_wallet, cid, objects, object_size, client_shell, bearer_file
        )

    @allure.title("Test to check Storage Group lifetime")
    def test_storagegroup_lifetime(self, client_shell, object_size):
        cid = create_container(self.main_wallet, shell=client_shell)
        file_path = generate_file(object_size)
        oid = put_object(self.main_wallet, file_path, cid, shell=client_shell)
        objects = [oid]
        storage_group = put_storagegroup(self.main_wallet, cid, objects, lifetime="1")
        tick_epoch()
        tick_epoch()
        with pytest.raises(Exception, match=OBJECT_NOT_FOUND):
            get_storagegroup(self.main_wallet, cid, storage_group)

    @staticmethod
    @allure.step("Run Storage Group Operations And Expect Success")
    def expect_success_for_storagegroup_operations(
        wallet: str,
        cid: str,
        obj_list: list,
        object_size: int,
        shell: Shell,
        bearer_token: Optional[str] = None,
    ):
        """
        This func verifies if the Object's owner is allowed to
        Put, List, Get and Delete the Storage Group which contains
        the Object.
        """
        storage_group = put_storagegroup(wallet, cid, obj_list, bearer_token)
        verify_list_storage_group(wallet, cid, storage_group, bearer_token)
        verify_get_storage_group(
            wallet, cid, storage_group, obj_list, object_size, shell, bearer_token
        )
        delete_storagegroup(wallet, cid, storage_group, bearer_token)

    @staticmethod
    @allure.step("Run Storage Group Operations And Expect Failure")
    def expect_failure_for_storagegroup_operations(
        wallet: str, cid: str, obj_list: list, storagegroup: str
    ):
        """
        This func verifies if the Object's owner isn't allowed to
        Put, List, Get and Delete the Storage Group which contains
        the Object.
        """
        with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
            put_storagegroup(wallet, cid, obj_list)
        with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
            list_storagegroup(wallet, cid)
        with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
            get_storagegroup(wallet, cid, storagegroup)
        with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
            delete_storagegroup(wallet, cid, storagegroup)

    @staticmethod
    @allure.step("Run Storage Group Operations On Other's Behalf In RO Container")
    def storagegroup_operations_by_other_ro_container(
        owner_wallet: str,
        other_wallet: str,
        cid: str,
        obj_list: list,
        object_size: int,
        shell: Shell,
    ):
        storage_group = put_storagegroup(owner_wallet, cid, obj_list)
        with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
            put_storagegroup(other_wallet, cid, obj_list)
        verify_list_storage_group(other_wallet, cid, storage_group)
        verify_get_storage_group(other_wallet, cid, storage_group, obj_list, object_size, shell)
        with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
            delete_storagegroup(other_wallet, cid, storage_group)

    @staticmethod
    @allure.step("Run Storage Group Operations On Systems's Behalf In RO Container")
    def storagegroup_operations_by_system_ro_container(
        wallet: str, cid: str, obj_list: list, object_size: int, shell: Shell
    ):
        """
        In this func we create a Storage Group on Inner Ring's key behalf
        and include an Object created on behalf of some user. We expect
        that System key is granted to make all operations except PUT and DELETE.
        """
        if not FREE_STORAGE:
            transfer_mainnet_gas(IR_WALLET_PATH, deposit + 1, wallet_password=IR_WALLET_PASS)
            neofs_deposit(IR_WALLET_PATH, deposit, wallet_password=IR_WALLET_PASS)
        storage_group = put_storagegroup(wallet, cid, obj_list)
        with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
            put_storagegroup(IR_WALLET_PATH, cid, obj_list, wallet_config=IR_WALLET_CONFIG)
        verify_list_storage_group(
            IR_WALLET_PATH, cid, storage_group, wallet_config=IR_WALLET_CONFIG
        )
        verify_get_storage_group(
            IR_WALLET_PATH,
            cid,
            storage_group,
            obj_list,
            object_size,
            shell,
            wallet_config=IR_WALLET_CONFIG,
        )
        with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
            delete_storagegroup(IR_WALLET_PATH, cid, storage_group, wallet_config=IR_WALLET_CONFIG)
