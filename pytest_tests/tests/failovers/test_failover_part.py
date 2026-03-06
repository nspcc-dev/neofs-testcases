import logging

import allure
import pytest
from helpers.common import SIMPLE_OBJECT_SIZE
from helpers.container import create_container
from helpers.grpc_responses import OBJECT_NOT_FOUND
from helpers.neofs_verbs import get_object
from helpers.node_management import (
    delete_node_metadata,
    start_storage_nodes,
    wait_all_storage_nodes_returned,
)
from helpers.utility import parse_version
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
        if parse_version(self.neofs_env.get_binary_version(self.neofs_env.neofs_node_path)) <= parse_version("0.51.1"):
            for node in self.neofs_env.storage_nodes:
                node.set_metabase_resync(True)

        try:
            storage_object = user_container.generate_object(int(SIMPLE_OBJECT_SIZE))

            with allure.step("Delete metabase files from storage nodes"):
                for node in self.neofs_env.storage_nodes:
                    delete_node_metadata(node)

            if parse_version(self.neofs_env.get_binary_version(self.neofs_env.neofs_node_path)) > parse_version(
                "0.51.1"
            ):
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
        finally:
            if parse_version(self.neofs_env.get_binary_version(self.neofs_env.neofs_node_path)) <= parse_version(
                "0.51.1"
            ):
                for node in self.neofs_env.storage_nodes:
                    node.set_metabase_resync(False)

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
