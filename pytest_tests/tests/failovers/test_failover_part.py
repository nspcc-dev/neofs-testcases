import logging

import allure
import neofs_env.neofs_epoch as neofs_epoch
import pytest
from helpers.common import SIMPLE_OBJECT_SIZE
from helpers.container import create_container
from helpers.grpc_responses import OBJECT_NOT_FOUND
from helpers.neofs_verbs import delete_object, get_object, lock_object
from helpers.node_management import (
    check_tombstone_objects_exist,
    corrupt_fstree_structure,
    delete_node_metadata,
    find_and_extract_lock_objects,
    restore_lock_objects,
    start_storage_nodes,
    stop_storage_nodes,
    wait_all_storage_nodes_returned,
)
from helpers.storage_container import StorageContainer, StorageContainerInfo
from helpers.test_control import expect_not_raises
from neofs_testlib.env.env import NeoFSEnv, NodeWallet

logger = logging.getLogger("NeoLogger")


class TestFailoverNodePart:
    @pytest.fixture()
    def user_container(self, user_wallet: NodeWallet, neofs_env_function_scope: NeoFSEnv):
        self.neofs_env = neofs_env_function_scope
        self.shell = self.neofs_env.shell
        container_id = create_container(user_wallet.path, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
        return StorageContainer(StorageContainerInfo(container_id, user_wallet), self.shell, self.neofs_env)

    @allure.title("Enable resync metabase, delete metadata and get object")
    def test_enable_resync_metabase_delete_metadata(
        self,
        user_container: StorageContainer,
    ):
        storage_object = user_container.generate_object(int(SIMPLE_OBJECT_SIZE))

        with allure.step("Delete metabase files from storage nodes"):
            for node in self.neofs_env.storage_nodes:
                delete_node_metadata(node)

        with allure.step("Metabase resync"):
            for node in self.neofs_env.storage_nodes:
                for shard in node.shards:
                    self.neofs_env.neofs_lens().meta.resync(shard.fstree_path, shard.metabase_path)

        with allure.step("Start nodes after metabase deletion"):
            start_storage_nodes(self.neofs_env.storage_nodes)
            wait_all_storage_nodes_returned(self.neofs_env)

        with allure.step("Try to fetch object from each storage node"):
            for node in self.neofs_env.storage_nodes:
                with expect_not_raises():
                    get_object(
                        storage_object.wallet_file_path,
                        storage_object.cid,
                        storage_object.oid,
                        self.shell,
                        endpoint=node.endpoint,
                        wallet_config=user_container.get_wallet_config_path(),
                    )

    @allure.title("Delete metadata without resync metabase enabling, delete metadata try to get object")
    def test_delete_metadata(self, user_container: StorageContainer):
        storage_object = user_container.generate_object(int(SIMPLE_OBJECT_SIZE))

        with allure.step("Delete metabase files from storage nodes"):
            for node in self.neofs_env.storage_nodes:
                delete_node_metadata(node)

        with allure.step("Start nodes after metabase deletion"):
            start_storage_nodes(self.neofs_env.storage_nodes)
            wait_all_storage_nodes_returned(self.neofs_env)

        with allure.step("Try to fetch object from each storage node"):
            for node in self.neofs_env.storage_nodes:
                with pytest.raises(Exception, match=OBJECT_NOT_FOUND):
                    get_object(
                        storage_object.wallet_file_path,
                        storage_object.cid,
                        storage_object.oid,
                        self.shell,
                        endpoint=node.endpoint,
                        wallet_config=user_container.get_wallet_config_path(),
                    )

    @pytest.mark.parametrize(
        "corruption_type",
        [
            "empty_file",
            "corrupted_file",
            "renamed_file",
        ],
    )
    def test_resync_metabase_corrupt_fstree(
        self,
        user_container: StorageContainer,
        corruption_type: str,
    ):
        storage_object = user_container.generate_object(int(SIMPLE_OBJECT_SIZE))

        with allure.step(f"Corrupt fstree structure on storage nodes ({corruption_type})"):
            for node in self.neofs_env.storage_nodes:
                corrupt_fstree_structure(node, corruption_type=corruption_type)

        with allure.step("Metabase resync"):
            for node in self.neofs_env.storage_nodes:
                for shard in node.shards:
                    self.neofs_env.neofs_lens().meta.resync(shard.fstree_path, shard.metabase_path)

        with allure.step("Start nodes after fstree corruption"):
            start_storage_nodes(self.neofs_env.storage_nodes)
            wait_all_storage_nodes_returned(self.neofs_env)

        with allure.step("Try to fetch object from each storage node"):
            for node in self.neofs_env.storage_nodes:
                with pytest.raises(Exception, match=OBJECT_NOT_FOUND):
                    get_object(
                        storage_object.wallet_file_path,
                        storage_object.cid,
                        storage_object.oid,
                        self.shell,
                        endpoint=node.endpoint,
                        wallet_config=user_container.get_wallet_config_path(),
                    )

    def test_resync_metabase_inconsistent_fstree(
        self,
        user_container: StorageContainer,
    ):
        """
        Test scenario:
        1. Create a locked object
        2. For each storage node: stop it, extract LOCK objects from fstree, start it back
        3. Wait for lock expiration (tick lock_lifetime + 1 epochs)
        4. Delete the locked object after lock expiration
        5. Verify TOMBSTONE objects exist in fstree
        6. Stop all nodes and restore extracted LOCK objects back to fstree
        7. Perform metabase resync on all shards
        8. Start nodes after restoring LOCK objects
        9. Try to fetch the object from each node (result is undetermined due to inconsistent fstree)
        """
        with allure.step("Create a locked object"):
            current_epoch = neofs_epoch.ensure_fresh_epoch(self.neofs_env)
            lock_lifetime = 2
            object_lifetime = 10

            storage_object = user_container.generate_object(
                int(SIMPLE_OBJECT_SIZE), expire_at=current_epoch + object_lifetime
            )

            lock_object_id = lock_object(
                storage_object.wallet_file_path,
                storage_object.cid,
                storage_object.oid,
                self.shell,
                self.neofs_env.sn_rpc,
                lifetime=lock_lifetime,
            )
            logger.info(f"Created LOCK object with ID: {lock_object_id}")

        all_lock_objects = []

        with allure.step("Find and extract LOCK objects from fstree on all storage nodes"):
            for node in self.neofs_env.storage_nodes:
                stop_storage_nodes([node])
                lock_objects = find_and_extract_lock_objects(node, self.neofs_env)

                if lock_objects:
                    all_lock_objects.extend(lock_objects)

                start_storage_nodes([node])

        assert all_lock_objects, "No LOCK objects found in fstree"

        with allure.step(f"Wait for lock expiration (tick {lock_lifetime + 1} epochs)"):
            for _ in range(lock_lifetime + 1):
                neofs_epoch.tick_epoch_and_wait(self.neofs_env)

        with allure.step("Delete the locked object after lock expiration"):
            delete_object(
                storage_object.wallet_file_path,
                storage_object.cid,
                storage_object.oid,
                self.shell,
                self.neofs_env.sn_rpc,
            )

        with allure.step("Verify TOMBSTONE objects exist in fstree"):
            stop_storage_nodes(self.neofs_env.storage_nodes)
            tombstone_exists = check_tombstone_objects_exist(self.neofs_env.storage_nodes, self.neofs_env)
            start_storage_nodes(self.neofs_env.storage_nodes)
            wait_all_storage_nodes_returned(self.neofs_env)

            assert tombstone_exists, "No TOMBSTONE objects found after deletion"

        with allure.step("Stop nodes and restore LOCK objects to fstree"):
            stop_storage_nodes(self.neofs_env.storage_nodes)
            restore_lock_objects(all_lock_objects)

        with allure.step("Metabase resync"):
            for node in self.neofs_env.storage_nodes:
                for shard in node.shards:
                    self.neofs_env.neofs_lens().meta.resync(shard.fstree_path, shard.metabase_path)

        with allure.step("Start nodes after restoring LOCK objects"):
            start_storage_nodes(self.neofs_env.storage_nodes)
            wait_all_storage_nodes_returned(self.neofs_env)

        with allure.step(
            "Try to fetch object from each storage node (result is undetermined due to inconsistent fstree, object may or may not be fetched)"
        ):
            for node in self.neofs_env.storage_nodes:
                try:
                    get_object(
                        storage_object.wallet_file_path,
                        storage_object.cid,
                        storage_object.oid,
                        self.shell,
                        endpoint=node.endpoint,
                        wallet_config=user_container.get_wallet_config_path(),
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to fetch object from node {node} after resync with inconsistent fstree: {e}"
                    )
