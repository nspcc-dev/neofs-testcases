import random

import allure
import pytest
from helpers.container import create_container
from helpers.container_access import (
    check_full_access_to_container,
    check_no_access_to_container,
    check_read_only_container,
)
from helpers.neofs_verbs import put_object_to_random_node
from helpers.object_access import (
    can_delete_object,
    can_get_head_object,
    can_get_object,
    can_get_range_hash_of_object,
    can_get_range_of_object,
    can_put_object,
    can_search_object,
)
from helpers.wellknown_acl import PRIVATE_ACL_F, PUBLIC_ACL_F, READONLY_ACL_F
from neofs_env.neofs_env_test_base import TestNeofsBase
from neofs_testlib.env.env import NodeWallet


class TestACLBasic(TestNeofsBase):
    @pytest.fixture(scope="function")
    def public_container(self, user_wallet: NodeWallet):
        with allure.step("Create public container"):
            cid_public = create_container(
                user_wallet.path,
                basic_acl=PUBLIC_ACL_F,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        yield cid_public

    @pytest.fixture(scope="function")
    def private_container(self, user_wallet: NodeWallet):
        with allure.step("Create private container"):
            cid_private = create_container(
                user_wallet.path,
                basic_acl=PRIVATE_ACL_F,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        yield cid_private

    @pytest.fixture(scope="function")
    def read_only_container(self, user_wallet: NodeWallet):
        with allure.step("Create public readonly container"):
            cid_read_only = create_container(
                user_wallet.path,
                basic_acl=READONLY_ACL_F,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        yield cid_read_only

    @pytest.mark.sanity
    @allure.title("Test basic ACL on public container")
    def test_basic_acl_public(self, not_owner_wallet: NodeWallet, user_wallet: NodeWallet, public_container, file_path):
        """
        Test basic ACL set during public container creation.
        """
        cid = public_container
        for wallet, desc in ((user_wallet, "owner"), (not_owner_wallet, "other users")):
            with allure.step("Add test objects to container"):
                # We create new objects for each wallet because check_full_access_to_container
                # deletes the object
                owner_object_oid = put_object_to_random_node(
                    wallet=user_wallet.path,
                    path=file_path,
                    cid=cid,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                    attributes={"created": "owner"},
                )
                other_object_oid = put_object_to_random_node(
                    wallet=not_owner_wallet.path,
                    path=file_path,
                    cid=cid,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                    attributes={"created": "other"},
                )
            with allure.step(f"Check {desc} has full access to public container"):
                check_full_access_to_container(
                    wallet=wallet.path,
                    cid=cid,
                    oid=owner_object_oid,
                    file_name=file_path,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                )
                check_full_access_to_container(
                    wallet=wallet.path,
                    cid=cid,
                    oid=other_object_oid,
                    file_name=file_path,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                )

    @allure.title("Test basic ACL on private container")
    def test_basic_acl_private(
        self,
        not_owner_wallet: NodeWallet,
        user_wallet: NodeWallet,
        client_shell,
        private_container,
        file_path,
    ):
        """
        Test basic ACL set during private container creation.
        """
        cid = private_container
        with allure.step("Add test objects to container"):
            owner_object_oid = put_object_to_random_node(
                wallet=user_wallet.path,
                path=file_path,
                cid=cid,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step("Check only owner has full access to private container"):
            with allure.step("Check no one except owner has access to operations with container"):
                check_no_access_to_container(
                    wallet=not_owner_wallet.path,
                    cid=cid,
                    oid=owner_object_oid,
                    file_name=file_path,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                )

            with allure.step("Check owner has full access to private container"):
                check_full_access_to_container(
                    wallet=user_wallet.path,
                    cid=cid,
                    oid=owner_object_oid,
                    file_name=file_path,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                )

    @allure.title("Test basic ACL on readonly container")
    def test_basic_acl_readonly(
        self,
        not_owner_wallet: NodeWallet,
        user_wallet: NodeWallet,
        client_shell,
        read_only_container,
        file_path,
    ):
        """
        Test basic ACL Operations for Read-Only Container.
        """
        cid = read_only_container

        with allure.step("Add test objects to container"):
            object_oid = put_object_to_random_node(
                wallet=user_wallet.path,
                path=file_path,
                cid=cid,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step("Check other has read-only access to operations with container"):
            check_read_only_container(
                wallet=not_owner_wallet.path,
                cid=cid,
                oid=object_oid,
                file_name=file_path,
                shell=client_shell,
                neofs_env=self.neofs_env,
            )

        with allure.step("Check owner has full access to public container"):
            check_full_access_to_container(
                wallet=user_wallet.path,
                cid=cid,
                oid=object_oid,
                file_name=file_path,
                shell=client_shell,
                neofs_env=self.neofs_env,
            )

    @allure.title("Test basic ACL IR and STORAGE rules compliance")
    def test_basic_acl_ir_storage_rules_compliance(
        self, user_wallet: NodeWallet, public_container: str, file_path: str
    ):
        """
        Test basic ACL IR and STORAGE rules compliance.

        IR node should be able to perform the following operations:
            GET object from container
            GET head of object from container
            SEARCH object in container
            GET range hash of object from container

        IR node should NOT be able to perform the following operations:
            PUT object to container
            GET range of object from container
            DELETE object from container

        STORAGE node should be able to perform the following operations:
            PUT object to container
            GET object from container
            GET head of object from container
            SEARCH object in container
            GET range hash of object from container

        STORAGE node should NOT be able to perform the following operations:
            GET range of object from container
            DELETE object from container
        """
        endpoint = random.choice(self.neofs_env.storage_nodes).endpoint

        ir_wallet = self.neofs_env.inner_ring_nodes[0].alphabet_wallet
        ir_wallet_config = self.neofs_env.inner_ring_nodes[0].cli_config
        storage_wallet = self.neofs_env.storage_nodes[0].wallet
        storage_wallet_config = self.neofs_env.storage_nodes[0].cli_config

        cid = public_container

        with allure.step("Add test objects to container"):
            owner_object_oid = put_object_to_random_node(
                wallet=user_wallet.path,
                path=file_path,
                cid=cid,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step("Check IR and STORAGE rules compliance"):
            with allure.step("IR node should be able to PUT object to container"):
                assert not can_put_object(
                    wallet=ir_wallet.path,
                    cid=cid,
                    file_name=file_path,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                    wallet_config=ir_wallet_config,
                )
            with allure.step("STORAGE node should be able to PUT object to container"):
                assert can_put_object(
                    wallet=storage_wallet.path,
                    cid=cid,
                    file_name=file_path,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                    wallet_config=storage_wallet_config,
                )

            with allure.step("IR node should be able to GET object from container"):
                assert can_get_object(
                    wallet=ir_wallet.path,
                    cid=cid,
                    oid=owner_object_oid,
                    file_name=file_path,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                    wallet_config=ir_wallet_config,
                )
            with allure.step("STORAGE node should be able to GET object from container"):
                assert can_get_object(
                    wallet=storage_wallet.path,
                    cid=cid,
                    oid=owner_object_oid,
                    file_name=file_path,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                    wallet_config=storage_wallet_config,
                )

            with allure.step("IR node should be able to GET head of object from container"):
                assert can_get_head_object(
                    wallet=ir_wallet.path,
                    cid=cid,
                    oid=owner_object_oid,
                    shell=self.shell,
                    endpoint=endpoint,
                    wallet_config=ir_wallet_config,
                )
            with allure.step("STORAGE node should be able to GET head of object from container"):
                assert can_get_head_object(
                    wallet=storage_wallet.path,
                    cid=cid,
                    oid=owner_object_oid,
                    shell=self.shell,
                    endpoint=endpoint,
                    wallet_config=storage_wallet_config,
                )

            with allure.step("IR node should be able to SEARCH object in container"):
                assert can_search_object(
                    wallet=ir_wallet.path,
                    cid=cid,
                    shell=self.shell,
                    endpoint=endpoint,
                    oid=owner_object_oid,
                    wallet_config=ir_wallet_config,
                )
            with allure.step("STORAGE node should be able to SEARCH object in container"):
                assert can_search_object(
                    wallet=storage_wallet.path,
                    cid=cid,
                    shell=self.shell,
                    endpoint=endpoint,
                    oid=owner_object_oid,
                    wallet_config=storage_wallet_config,
                )

            with allure.step("IR node should NOT be able to GET range of object from container"):
                assert not can_get_range_of_object(
                    wallet=ir_wallet.path,
                    cid=cid,
                    oid=owner_object_oid,
                    shell=self.shell,
                    endpoint=endpoint,
                    wallet_config=ir_wallet_config,
                )
            with allure.step("STORAGE node should NOT be able to GET range of object from container"):
                assert not can_get_range_of_object(
                    wallet=storage_wallet.path,
                    cid=cid,
                    oid=owner_object_oid,
                    shell=self.shell,
                    endpoint=endpoint,
                    wallet_config=storage_wallet_config,
                )

            with allure.step("IR node should be able to GET range hash of object from container"):
                assert can_get_range_hash_of_object(
                    wallet=ir_wallet.path,
                    cid=cid,
                    oid=owner_object_oid,
                    shell=self.shell,
                    endpoint=endpoint,
                    wallet_config=ir_wallet_config,
                )
            with allure.step("STORAGE node should be able to GET range hash of object from container"):
                assert can_get_range_hash_of_object(
                    wallet=storage_wallet.path,
                    cid=cid,
                    oid=owner_object_oid,
                    shell=self.shell,
                    endpoint=endpoint,
                    wallet_config=storage_wallet_config,
                )

            with allure.step("IR node should NOT be able to DELETE object from container"):
                assert not can_delete_object(
                    wallet=ir_wallet.path,
                    cid=cid,
                    oid=owner_object_oid,
                    shell=self.shell,
                    endpoint=endpoint,
                    wallet_config=ir_wallet_config,
                )
            with allure.step("STORAGE node should NOT be able to DELETE object from container"):
                assert not can_delete_object(
                    wallet=storage_wallet.path,
                    cid=cid,
                    oid=owner_object_oid,
                    shell=self.shell,
                    endpoint=endpoint,
                    wallet_config=storage_wallet_config,
                )
