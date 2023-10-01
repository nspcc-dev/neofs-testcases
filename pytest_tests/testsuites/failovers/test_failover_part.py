import logging

import allure
import pytest
from neofs_testlib.shell import Shell

from cluster import Cluster
from cluster_test_base import ClusterTestBase
from container import create_container
from failover_utils import wait_all_storage_nodes_returned, enable_metabase_resync_on_start
from grpc_responses import OBJECT_NOT_FOUND
from helpers.container import StorageContainer, StorageContainerInfo
from neofs_verbs import get_object
from node_management import delete_node_metadata, start_storage_nodes, stop_storage_nodes
from test_control import expect_not_raises
from wallet import WalletFile, WalletFactory

logger = logging.getLogger("NeoLogger")


@pytest.fixture(
    scope="module",
)
def user_wallet(wallet_factory: WalletFactory):
    with allure.step("Create user wallet with container"):
        wallet_file = wallet_factory.create_wallet()
        return wallet_file


@pytest.fixture(
    scope="function",
)
def user_container(user_wallet: WalletFile, client_shell: Shell, cluster: Cluster):
    container_id = create_container(
        user_wallet.path, shell=client_shell, endpoint=cluster.default_rpc_endpoint
    )
    return StorageContainer(StorageContainerInfo(container_id, user_wallet), client_shell, cluster)


@pytest.mark.failover_part
class TestFailoverNodePart(ClusterTestBase):
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
            for node in self.cluster.storage_nodes:
                delete_node_metadata(node)

        with allure.step("Start nodes after metabase deletion"):
            start_storage_nodes(self.cluster.storage_nodes)
            wait_all_storage_nodes_returned(self.cluster)

        with allure.step("Try to fetch object from each storage node"):
            for node in self.cluster.storage_nodes:
                with expect_not_raises():
                    get_object(
                        storage_object.wallet_file_path,
                        storage_object.cid,
                        storage_object.oid,
                        self.shell,
                        endpoint=node.get_rpc_endpoint(),
                        wallet_config=user_container.get_wallet_config_path(),
                    )

    @allure.title(
        "Delete metadata without resync metabase enabling, delete metadata try to get object"
    )
    @pytest.mark.delete_metadata
    def test_delete_metadata(self, user_container: StorageContainer, simple_object_size: int):
        storage_object = user_container.generate_object(simple_object_size)

        with allure.step("Delete metabase files from storage nodes"):
            for node in self.cluster.storage_nodes:
                delete_node_metadata(node)

        with allure.step("Start nodes after metabase deletion"):
            start_storage_nodes(self.cluster.storage_nodes)
            wait_all_storage_nodes_returned(self.cluster)

        with allure.step("Try to fetch object from each storage node"):
            for node in self.cluster.storage_nodes:
                with pytest.raises(Exception, match=OBJECT_NOT_FOUND):
                    get_object(
                        storage_object.wallet_file_path,
                        storage_object.cid,
                        storage_object.oid,
                        self.shell,
                        endpoint=node.get_rpc_endpoint(),
                        wallet_config=user_container.get_wallet_config_path(),
                    )
