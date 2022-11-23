import logging
import os
import uuid
from typing import Optional

import allure
import pytest
import yaml
from common import (
    ASSETS_DIR,
    COMPLEX_OBJ_SIZE,
    FREE_STORAGE,
    IR_WALLET_PATH,
    SIMPLE_OBJ_SIZE,
    WALLET_PASS,
)
from epoch import tick_epoch
from file_helper import generate_file
from grpc_responses import OBJECT_ACCESS_DENIED, OBJECT_NOT_FOUND
from neofs_testlib.hosting import Hosting
from neofs_testlib.shell import Shell
from neofs_testlib.utils.wallet import init_wallet
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
from python_keywords.payment_neogo import deposit_gas, transfer_gas
from python_keywords.storage_group import (
    delete_storagegroup,
    get_storagegroup,
    list_storagegroup,
    put_storagegroup,
    verify_get_storage_group,
    verify_list_storage_group,
)
from utility import get_wallet_password

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
    def prepare_two_wallets(self, prepare_wallet_and_deposit, client_shell):
        self.main_wallet = prepare_wallet_and_deposit
        self.other_wallet = os.path.join(os.getcwd(), ASSETS_DIR, f"{str(uuid.uuid4())}.json")
        init_wallet(self.other_wallet, WALLET_PASS)
        if not FREE_STORAGE:
            deposit = 30
            transfer_gas(
                shell=client_shell,
                amount=deposit + 1,
                wallet_to_path=self.other_wallet,
                wallet_to_password=WALLET_PASS,
            )
            deposit_gas(
                shell=client_shell,
                amount=deposit,
                wallet_from_path=self.other_wallet,
                wallet_from_password=WALLET_PASS,
            )

    @allure.title("Test Storage Group in Private Container")
    def test_storagegroup_basic_private_container(self, client_shell, object_size, hosting):
        cid = create_container(self.main_wallet, shell=client_shell)
        file_path = generate_file(object_size)
        password = get_wallet_password(hosting=hosting, service_name="ir01")
        oid = put_object(self.main_wallet, file_path, cid, shell=client_shell)
        objects = [oid]
        storage_group = put_storagegroup(
            shell=client_shell, wallet=self.main_wallet, cid=cid, objects=objects
        )

        self.expect_success_for_storagegroup_operations(
            shell=client_shell,
            wallet=self.main_wallet,
            cid=cid,
            obj_list=objects,
            object_size=object_size,
        )
        self.expect_failure_for_storagegroup_operations(
            shell=client_shell,
            wallet=self.other_wallet,
            cid=cid,
            obj_list=objects,
            gid=storage_group,
        )
        self.storagegroup_operations_by_system_ro_container(
            shell=client_shell,
            wallet=self.main_wallet,
            cid=cid,
            obj_list=objects,
            object_size=object_size,
            hosting=hosting,
        )

    @allure.title("Test Storage Group in Public Container")
    def test_storagegroup_basic_public_container(self, client_shell, object_size, hosting):
        cid = create_container(self.main_wallet, basic_acl="public-read-write", shell=client_shell)
        file_path = generate_file(object_size)
        oid = put_object(self.main_wallet, file_path, cid, shell=client_shell)
        objects = [oid]
        self.expect_success_for_storagegroup_operations(
            shell=client_shell,
            wallet=self.main_wallet,
            cid=cid,
            obj_list=objects,
            object_size=object_size,
        )
        self.expect_success_for_storagegroup_operations(
            shell=client_shell,
            wallet=self.other_wallet,
            cid=cid,
            obj_list=objects,
            object_size=object_size,
        )
        self.storagegroup_operations_by_system_ro_container(
            shell=client_shell,
            wallet=self.main_wallet,
            cid=cid,
            obj_list=objects,
            object_size=object_size,
            hosting=hosting,
        )

    @allure.title("Test Storage Group in Read-Only Container")
    def test_storagegroup_basic_ro_container(self, client_shell, object_size, hosting):
        cid = create_container(self.main_wallet, basic_acl="public-read", shell=client_shell)
        file_path = generate_file(object_size)
        oid = put_object(self.main_wallet, file_path, cid, shell=client_shell)
        objects = [oid]
        self.expect_success_for_storagegroup_operations(
            shell=client_shell,
            wallet=self.main_wallet,
            cid=cid,
            obj_list=objects,
            object_size=object_size,
        )
        self.storagegroup_operations_by_other_ro_container(
            shell=client_shell,
            owner_wallet=self.main_wallet,
            other_wallet=self.other_wallet,
            cid=cid,
            obj_list=objects,
            object_size=object_size,
        )
        self.storagegroup_operations_by_system_ro_container(
            shell=client_shell,
            wallet=self.main_wallet,
            cid=cid,
            obj_list=objects,
            object_size=object_size,
            hosting=hosting,
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
            shell=client_shell,
            wallet=self.main_wallet,
            cid=cid,
            obj_list=objects,
            object_size=object_size,
        )
        storage_group = put_storagegroup(client_shell, self.main_wallet, cid, objects)
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
            client_shell, self.main_wallet, cid, objects, storage_group
        )
        bearer_file = form_bearertoken_file(
            self.main_wallet,
            cid,
            [
                EACLRule(operation=op, access=EACLAccess.ALLOW, role=EACLRole.USER)
                for op in EACLOperation
            ],
            shell=client_shell,
        )
        self.expect_success_for_storagegroup_operations(
            shell=client_shell,
            wallet=self.main_wallet,
            cid=cid,
            obj_list=objects,
            object_size=object_size,
            bearer=bearer_file,
        )

    @pytest.mark.skip
    @allure.title("Test to check Storage Group lifetime")
    def test_storagegroup_lifetime(self, client_shell, object_size, hosting):
        cid = create_container(self.main_wallet, shell=client_shell)
        file_path = generate_file(object_size)
        oid = put_object(self.main_wallet, file_path, cid, shell=client_shell)
        objects = [oid]
        storage_group = put_storagegroup(client_shell, self.main_wallet, cid, objects, lifetime=1)
        with allure.step("Tick two epochs"):
            for _ in range(2):
                tick_epoch(shell=client_shell, hosting=hosting)
        with pytest.raises(Exception, match=OBJECT_NOT_FOUND):
            get_storagegroup(
                shell=client_shell, wallet=self.main_wallet, cid=cid, gid=storage_group
            )

    @staticmethod
    @allure.step("Run Storage Group Operations And Expect Success")
    def expect_success_for_storagegroup_operations(
        shell: Shell,
        wallet: str,
        cid: str,
        obj_list: list,
        object_size: int,
        bearer: Optional[str] = None,
    ):
        """
        This func verifies if the Object's owner is allowed to
        Put, List, Get and Delete the Storage Group which contains
        the Object.
        """
        storage_group = put_storagegroup(shell, wallet, cid, obj_list, bearer)
        verify_list_storage_group(
            shell=shell, wallet=wallet, cid=cid, gid=storage_group, bearer=bearer
        )
        verify_get_storage_group(
            shell=shell,
            wallet=wallet,
            cid=cid,
            gid=storage_group,
            obj_list=obj_list,
            object_size=object_size,
            bearer=bearer,
        )
        delete_storagegroup(shell=shell, wallet=wallet, cid=cid, gid=storage_group, bearer=bearer)

    @staticmethod
    @allure.step("Run Storage Group Operations And Expect Failure")
    def expect_failure_for_storagegroup_operations(
        shell: Shell, wallet: str, cid: str, obj_list: list, gid: str
    ):
        """
        This func verifies if the Object's owner isn't allowed to
        Put, List, Get and Delete the Storage Group which contains
        the Object.
        """
        with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
            put_storagegroup(shell=shell, wallet=wallet, cid=cid, objects=obj_list)
        with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
            list_storagegroup(shell=shell, wallet=wallet, cid=cid)
        with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
            get_storagegroup(shell=shell, wallet=wallet, cid=cid, gid=gid)
        with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
            delete_storagegroup(shell=shell, wallet=wallet, cid=cid, gid=gid)

    @staticmethod
    @allure.step("Run Storage Group Operations On Other's Behalf In RO Container")
    def storagegroup_operations_by_other_ro_container(
        shell: Shell,
        owner_wallet: str,
        other_wallet: str,
        cid: str,
        obj_list: list,
        object_size: int,
    ):
        storage_group = put_storagegroup(shell, owner_wallet, cid, obj_list)
        with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
            put_storagegroup(shell=shell, wallet=other_wallet, cid=cid, objects=obj_list)
        verify_list_storage_group(shell=shell, wallet=other_wallet, cid=cid, gid=storage_group)
        verify_get_storage_group(
            shell=shell,
            wallet=other_wallet,
            cid=cid,
            gid=storage_group,
            obj_list=obj_list,
            object_size=object_size,
        )
        with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
            delete_storagegroup(shell=shell, wallet=other_wallet, cid=cid, gid=storage_group)

    @staticmethod
    @allure.step("Run Storage Group Operations On Systems's Behalf In RO Container")
    def storagegroup_operations_by_system_ro_container(
        shell: Shell, wallet: str, cid: str, obj_list: list, object_size: int, hosting: Hosting
    ):
        """
        In this func we create a Storage Group on Inner Ring's key behalf
        and include an Object created on behalf of some user. We expect
        that System key is granted to make all operations except PUT and DELETE.
        """
        password = get_wallet_password(hosting=hosting, service_name="ir01")
        ir_wallet_config = os.path.join(os.getcwd(), "ir_wallet_config.yml")
        with open(ir_wallet_config, "w") as file:
            yaml.dump({"password": password}, file)
        if not FREE_STORAGE:
            deposit = 30
            transfer_gas(
                shell=shell,
                amount=deposit + 1,
                wallet_to_path=IR_WALLET_PATH,
                wallet_to_password=password,
            )
            deposit_gas(
                shell=shell,
                amount=deposit,
                wallet_from_path=IR_WALLET_PATH,
                wallet_from_password=password,
            )
        storage_group = put_storagegroup(shell, wallet, cid, obj_list)
        with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
            put_storagegroup(
                shell=shell,
                wallet=IR_WALLET_PATH,
                cid=cid,
                objects=obj_list,
                wallet_config=ir_wallet_config,
            )
        verify_list_storage_group(
            shell=shell,
            wallet=IR_WALLET_PATH,
            cid=cid,
            gid=storage_group,
            wallet_config=ir_wallet_config,
        )

        verify_get_storage_group(
            shell=shell,
            wallet=IR_WALLET_PATH,
            cid=cid,
            gid=storage_group,
            obj_list=obj_list,
            object_size=object_size,
            wallet_config=ir_wallet_config,
        )
        with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
            delete_storagegroup(
                shell=shell,
                wallet=IR_WALLET_PATH,
                cid=cid,
                gid=storage_group,
                wallet_config=ir_wallet_config,
            )
