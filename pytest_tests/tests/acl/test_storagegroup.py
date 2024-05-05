import logging
from typing import Optional

import allure
import neofs_env.neofs_epoch as neofs_epoch
import pytest
from helpers.acl import (
    EACLAccess,
    EACLOperation,
    EACLRole,
    EACLRule,
    create_eacl,
    form_bearertoken_file,
    set_eacl,
)
from helpers.container import create_container
from helpers.file_helper import generate_file
from helpers.grpc_responses import OBJECT_ACCESS_DENIED, OBJECT_NOT_FOUND
from helpers.neofs_verbs import put_object_to_random_node
from helpers.storage_group import (
    delete_storagegroup,
    get_storagegroup,
    list_storagegroup,
    put_storagegroup,
    verify_get_storage_group,
    verify_list_storage_group,
)
from helpers.wallet_helpers import create_wallet
from neofs_env.neofs_env_test_base import NeofsEnvTestBase

logger = logging.getLogger("NeoLogger")
deposit = 30


@pytest.mark.parametrize(
    "object_size",
    [pytest.lazy_fixture("simple_object_size"), pytest.lazy_fixture("complex_object_size")],
    ids=["simple object", "complex object"],
)
@pytest.mark.acl
@pytest.mark.storage_group
class TestStorageGroup(NeofsEnvTestBase):
    @pytest.fixture(autouse=True)
    def prepare_two_wallets(self, default_wallet):
        self.main_wallet = default_wallet
        self.other_wallet = create_wallet()

    @allure.title("Test Storage Group in Private Container")
    def test_storagegroup_basic_private_container(self, object_size, max_object_size):
        cid = create_container(self.main_wallet.path, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
        file_path = generate_file(object_size)
        oid = put_object_to_random_node(self.main_wallet.path, file_path, cid, self.shell, neofs_env=self.neofs_env)
        objects = [oid]
        storage_group = put_storagegroup(
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            wallet=self.main_wallet.path,
            cid=cid,
            objects=objects,
        )

        self.expect_success_for_storagegroup_operations(
            wallet=self.main_wallet.path,
            cid=cid,
            obj_list=objects,
            object_size=object_size,
            max_object_size=max_object_size,
        )
        self.expect_failure_for_storagegroup_operations(
            wallet=self.other_wallet.path,
            cid=cid,
            obj_list=objects,
            gid=storage_group,
        )
        self.storagegroup_operations_by_system_ro_container(
            wallet=self.main_wallet.path,
            cid=cid,
            obj_list=objects,
            object_size=object_size,
            max_object_size=max_object_size,
        )

    @pytest.mark.sanity
    @allure.title("Test Storage Group in Public Container")
    def test_storagegroup_basic_public_container(self, object_size, max_object_size):
        cid = create_container(
            self.main_wallet.path,
            basic_acl="public-read-write",
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )
        file_path = generate_file(object_size)
        oid = put_object_to_random_node(
            self.main_wallet.path, file_path, cid, shell=self.shell, neofs_env=self.neofs_env
        )
        objects = [oid]
        self.expect_success_for_storagegroup_operations(
            wallet=self.main_wallet.path,
            cid=cid,
            obj_list=objects,
            object_size=object_size,
            max_object_size=max_object_size,
        )
        self.expect_success_for_storagegroup_operations(
            wallet=self.other_wallet.path,
            cid=cid,
            obj_list=objects,
            object_size=object_size,
            max_object_size=max_object_size,
        )
        self.storagegroup_operations_by_system_ro_container(
            wallet=self.main_wallet.path,
            cid=cid,
            obj_list=objects,
            object_size=object_size,
            max_object_size=max_object_size,
        )

    @allure.title("Test Storage Group in Read-Only Container")
    def test_storagegroup_basic_ro_container(self, object_size, max_object_size):
        cid = create_container(
            self.main_wallet.path,
            basic_acl="public-read",
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )
        file_path = generate_file(object_size)
        oid = put_object_to_random_node(
            self.main_wallet.path, file_path, cid, shell=self.shell, neofs_env=self.neofs_env
        )
        objects = [oid]
        self.expect_success_for_storagegroup_operations(
            wallet=self.main_wallet.path,
            cid=cid,
            obj_list=objects,
            object_size=object_size,
            max_object_size=max_object_size,
        )
        self.storagegroup_operations_by_other_ro_container(
            owner_wallet=self.main_wallet.path,
            other_wallet=self.other_wallet.path,
            cid=cid,
            obj_list=objects,
            object_size=object_size,
            max_object_size=max_object_size,
        )
        self.storagegroup_operations_by_system_ro_container(
            wallet=self.main_wallet.path,
            cid=cid,
            obj_list=objects,
            object_size=object_size,
            max_object_size=max_object_size,
        )

    @allure.title("Test Storage Group with Bearer Allow")
    def test_storagegroup_bearer_allow(self, object_size, max_object_size):
        cid = create_container(
            self.main_wallet.path,
            basic_acl="eacl-public-read-write",
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )
        file_path = generate_file(object_size)
        oid = put_object_to_random_node(
            self.main_wallet.path, file_path, cid, shell=self.shell, neofs_env=self.neofs_env
        )
        objects = [oid]
        self.expect_success_for_storagegroup_operations(
            wallet=self.main_wallet.path,
            cid=cid,
            obj_list=objects,
            object_size=object_size,
            max_object_size=max_object_size,
        )
        storage_group = put_storagegroup(self.shell, self.neofs_env.sn_rpc, self.main_wallet.path, cid, objects)
        eacl_deny = [
            EACLRule(access=EACLAccess.DENY, role=role, operation=op)
            for op in EACLOperation
            for role in EACLRole
            if role != EACLRole.SYSTEM
        ]
        set_eacl(
            self.main_wallet.path,
            cid,
            create_eacl(cid, eacl_deny, shell=self.shell),
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )
        self.expect_failure_for_storagegroup_operations(self.main_wallet.path, cid, objects, storage_group)
        bearer_file = form_bearertoken_file(
            self.main_wallet.path,
            cid,
            [EACLRule(operation=op, access=EACLAccess.ALLOW, role=EACLRole.USER) for op in EACLOperation],
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )
        self.expect_success_for_storagegroup_operations(
            wallet=self.main_wallet.path,
            cid=cid,
            obj_list=objects,
            object_size=object_size,
            max_object_size=max_object_size,
            bearer=bearer_file,
        )

    @allure.title("Test to check Storage Group lifetime")
    @pytest.mark.parametrize("expiration_flag", ["lifetime", "expire_at"])
    def test_storagegroup_lifetime(self, object_size, expiration_flag):
        cid = create_container(self.main_wallet.path, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
        file_path = generate_file(object_size)
        oid = put_object_to_random_node(
            self.main_wallet.path, file_path, cid, shell=self.shell, neofs_env=self.neofs_env
        )
        objects = [oid]
        current_epoch = neofs_epoch.get_epoch(self.neofs_env)
        storage_group = put_storagegroup(
            self.shell,
            self.neofs_env.sn_rpc,
            self.main_wallet.path,
            cid,
            objects,
            lifetime=1 if expiration_flag == "lifetime" else None,
            expire_at=current_epoch + 1 if expiration_flag == "expire_at" else None,
        )
        with allure.step("Tick two epochs"):
            self.tick_epochs_and_wait(2)
        with pytest.raises(Exception, match=OBJECT_NOT_FOUND):
            get_storagegroup(
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                wallet=self.main_wallet.path,
                cid=cid,
                gid=storage_group,
            )

    @allure.step("Run Storage Group Operations And Expect Success")
    def expect_success_for_storagegroup_operations(
        self,
        wallet: str,
        cid: str,
        obj_list: list,
        object_size: int,
        max_object_size: int,
        bearer: Optional[str] = None,
    ):
        """
        This func verifies if the Object's owner is allowed to
        Put, List, Get and Delete the Storage Group which contains
        the Object.
        """
        storage_group = put_storagegroup(self.shell, self.neofs_env.sn_rpc, wallet, cid, obj_list, bearer)
        verify_list_storage_group(
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            wallet=wallet,
            cid=cid,
            gid=storage_group,
            bearer=bearer,
        )
        verify_get_storage_group(
            shell=self.shell,
            neofs_env=self.neofs_env,
            wallet=wallet,
            cid=cid,
            gid=storage_group,
            obj_list=obj_list,
            object_size=object_size,
            max_object_size=max_object_size,
            bearer=bearer,
        )
        delete_storagegroup(
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            wallet=wallet,
            cid=cid,
            gid=storage_group,
            bearer=bearer,
        )

    @allure.step("Run Storage Group Operations And Expect Failure")
    def expect_failure_for_storagegroup_operations(self, wallet: str, cid: str, obj_list: list, gid: str):
        """
        This func verifies if the Object's owner isn't allowed to
        Put, List, Get and Delete the Storage Group which contains
        the Object.
        """
        with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
            put_storagegroup(
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                wallet=wallet,
                cid=cid,
                objects=obj_list,
            )
        with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
            list_storagegroup(shell=self.shell, endpoint=self.neofs_env.sn_rpc, wallet=wallet, cid=cid)
        with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
            get_storagegroup(
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                wallet=wallet,
                cid=cid,
                gid=gid,
            )
        with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
            delete_storagegroup(
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                wallet=wallet,
                cid=cid,
                gid=gid,
            )

    @allure.step("Run Storage Group Operations On Other's Behalf In RO Container")
    def storagegroup_operations_by_other_ro_container(
        self,
        owner_wallet: str,
        other_wallet: str,
        cid: str,
        obj_list: list,
        object_size: int,
        max_object_size: int,
    ):
        storage_group = put_storagegroup(self.shell, self.neofs_env.sn_rpc, owner_wallet, cid, obj_list)
        with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
            put_storagegroup(
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                wallet=other_wallet,
                cid=cid,
                objects=obj_list,
            )
        verify_list_storage_group(
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            wallet=other_wallet,
            cid=cid,
            gid=storage_group,
        )
        verify_get_storage_group(
            shell=self.shell,
            neofs_env=self.neofs_env,
            wallet=other_wallet,
            cid=cid,
            gid=storage_group,
            obj_list=obj_list,
            object_size=object_size,
            max_object_size=max_object_size,
        )
        with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
            delete_storagegroup(
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                wallet=other_wallet,
                cid=cid,
                gid=storage_group,
            )

    @allure.step("Run Storage Group Operations On Systems's Behalf In RO Container")
    def storagegroup_operations_by_system_ro_container(
        self,
        wallet: str,
        cid: str,
        obj_list: list,
        object_size: int,
        max_object_size: int,
    ):
        """
        In this func we create a Storage Group on Inner Ring's key behalf
        and include an Object created on behalf of some user. We expect
        that System key is granted to make all operations except PUT and DELETE.
        """

        ir_node = self.neofs_env.inner_ring_nodes[0]
        ir_wallet_path = ir_node.alphabet_wallet.path
        ir_wallet_config = ir_node.cli_config

        storage_group = put_storagegroup(self.shell, self.neofs_env.sn_rpc, wallet, cid, obj_list)
        with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
            put_storagegroup(
                self.shell,
                self.neofs_env.sn_rpc,
                ir_wallet_path,
                cid,
                obj_list,
                wallet_config=ir_wallet_config,
            )
        verify_list_storage_group(
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            wallet=ir_wallet_path,
            cid=cid,
            gid=storage_group,
            wallet_config=ir_wallet_config,
        )
        verify_get_storage_group(
            shell=self.shell,
            neofs_env=self.neofs_env,
            wallet=ir_wallet_path,
            cid=cid,
            gid=storage_group,
            obj_list=obj_list,
            object_size=object_size,
            max_object_size=max_object_size,
            wallet_config=ir_wallet_config,
        )
        with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
            delete_storagegroup(
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                wallet=ir_wallet_path,
                cid=cid,
                gid=storage_group,
                wallet_config=ir_wallet_config,
            )
