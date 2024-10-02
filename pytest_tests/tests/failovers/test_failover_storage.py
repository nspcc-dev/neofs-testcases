import logging

import allure
import pytest
from helpers.complex_object_actions import wait_object_replication
from helpers.container import create_container
from helpers.file_helper import generate_file, get_file_hash
from helpers.neofs_verbs import get_object, put_object_to_random_node
from helpers.node_management import wait_all_storage_nodes_returned
from helpers.wellknown_acl import PUBLIC_ACL
from neofs_env.neofs_env_test_base import NeofsEnvTestBase
from neofs_testlib.env.env import NeoFSEnv, StorageNode

logger = logging.getLogger("NeoLogger")
stopped_nodes: list[StorageNode] = []


@pytest.fixture
@allure.step("Return all stopped hosts")
def after_run_return_all_stopped_storage_nodes(neofs_env: NeoFSEnv):
    yield
    return_stopped_storage_nodes(neofs_env)


def return_stopped_storage_nodes(neofs_env: NeoFSEnv) -> None:
    for node in list(stopped_nodes):
        with allure.step(f"Start {node}"):
            node.start(fresh=False)
        stopped_nodes.remove(node)

    wait_all_storage_nodes_returned(neofs_env)


class TestFailoverStorage(NeofsEnvTestBase):
    @allure.title("Lose and return storage node's process")
    @pytest.mark.parametrize("hard_restart", [True, False])
    def test_storage_node_failover(
        self, default_wallet, simple_object_size, after_run_return_all_stopped_storage_nodes, hard_restart
    ):
        wallet = default_wallet
        placement_rule = "REP 2 IN X CBF 2 SELECT 2 FROM * AS X"
        source_file_path = generate_file(simple_object_size)
        cid = create_container(
            wallet.path,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            rule=placement_rule,
            basic_acl=PUBLIC_ACL,
        )
        oid = put_object_to_random_node(wallet.path, source_file_path, cid, shell=self.shell, neofs_env=self.neofs_env)
        nodes_with_object = wait_object_replication(
            cid, oid, 2, shell=self.shell, nodes=self.neofs_env.storage_nodes, neofs_env=self.neofs_env
        )

        for node_to_stop in nodes_with_object:
            if hard_restart:
                node_to_stop.kill()
            else:
                node_to_stop.stop()
            stopped_nodes.append(node_to_stop)

            object_nodes_after_stop = wait_object_replication(
                cid,
                oid,
                2,
                shell=self.shell,
                nodes=[sn for sn in self.neofs_env.storage_nodes if sn != node_to_stop],
                neofs_env=self.neofs_env,
            )
            assert node_to_stop not in object_nodes_after_stop

            with allure.step("Check object data is not corrupted"):
                got_file_path = get_object(
                    wallet.path, cid, oid, shell=self.shell, endpoint=object_nodes_after_stop[0].endpoint
                )
                assert get_file_hash(source_file_path) == get_file_hash(got_file_path)

            with allure.step("Return stopped storage nodes"):
                return_stopped_storage_nodes(self.neofs_env)

            with allure.step("Check object data is not corrupted"):
                new_nodes = wait_object_replication(
                    cid, oid, 2, shell=self.shell, nodes=self.neofs_env.storage_nodes, neofs_env=self.neofs_env
                )
                got_file_path = get_object(wallet.path, cid, oid, shell=self.shell, endpoint=new_nodes[0].endpoint)
                assert get_file_hash(source_file_path) == get_file_hash(got_file_path)
