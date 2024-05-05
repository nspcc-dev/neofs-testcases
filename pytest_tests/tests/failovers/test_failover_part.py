import logging

import allure
import pytest
from helpers.container import create_container
from helpers.grpc_responses import OBJECT_NOT_FOUND
from helpers.neofs_verbs import get_object
from helpers.node_management import (
    delete_node_metadata,
    start_storage_nodes,
    wait_all_storage_nodes_returned,
)
from helpers.storage_container import StorageContainer, StorageContainerInfo
from helpers.test_control import expect_not_raises
from neofs_env.neofs_env_test_base import NeofsEnvTestBase
from neofs_testlib.env.env import NeoFSEnv, NodeWallet
from neofs_testlib.shell import Shell

logger = logging.getLogger("NeoLogger")


@pytest.fixture(
    scope="function",
)
def user_container(user_wallet: NodeWallet, client_shell: Shell, neofs_env: NeoFSEnv):
    container_id = create_container(user_wallet.path, shell=client_shell, endpoint=neofs_env.sn_rpc)
    return StorageContainer(StorageContainerInfo(container_id, user_wallet), client_shell, neofs_env)


@pytest.mark.failover_part
@pytest.mark.skip(reason="Processes restarts currently affects other tests")
class TestFailoverNodePart(NeofsEnvTestBase):
    @allure.title("Enable resync metabase, delete metadata and get object")
    @pytest.mark.delete_metadata
    def test_enable_resync_metabase_delete_metadata(
        self,
        enable_metabase_resync_on_start,
        user_container: StorageContainer,
        simple_object_size: int,
    ):
        storage_object = user_container.generate_object(simple_object_size)

        with allure.step("Delete metabase files from storage nodes"):
            for node in self.neofs_env.storage_nodes:
                delete_node_metadata(node)

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
    @pytest.mark.delete_metadata
    def test_delete_metadata(self, user_container: StorageContainer, simple_object_size: int):
        storage_object = user_container.generate_object(simple_object_size)

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
