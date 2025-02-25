import logging
import random

import allure
import pytest
from helpers.complex_object_actions import wait_object_replication
from helpers.container import create_container
from helpers.file_helper import generate_file, get_file_hash
from helpers.neofs_verbs import get_object, put_object, put_object_to_random_node
from helpers.node_management import storage_node_healthcheck, wait_all_storage_nodes_returned
from helpers.wellknown_acl import PUBLIC_ACL
from neofs_testlib.env.env import NeoFSEnv, StorageNode

logger = logging.getLogger("NeoLogger")


class TestFailoverStorage:
    @pytest.fixture
    def after_run_return_all_stopped_storage_nodes(self, neofs_env_function_scope: NeoFSEnv):
        yield
        unavailable_nodes = []
        for node in neofs_env_function_scope.storage_nodes:
            try:
                storage_node_healthcheck(node)
            except Exception:
                unavailable_nodes.append(node)
        self.return_stopped_storage_nodes(neofs_env_function_scope, unavailable_nodes)

    @allure.step("Return all stopped hosts")
    def return_stopped_storage_nodes(self, neofs_env: NeoFSEnv, stopped_nodes: list[StorageNode]) -> None:
        for node in stopped_nodes:
            with allure.step(f"Start {node}"):
                node.start(fresh=False)

        wait_all_storage_nodes_returned(neofs_env)

    @allure.title("Lose and return storage node's process")
    @pytest.mark.parametrize("hard_restart", [True, False])
    def test_storage_node_failover(
        self,
        default_wallet,
        neofs_env_function_scope: NeoFSEnv,
        simple_object_size,
        after_run_return_all_stopped_storage_nodes,
        hard_restart,
    ):
        self.neofs_env = neofs_env_function_scope
        self.shell = self.neofs_env.shell

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
                self.return_stopped_storage_nodes(self.neofs_env, [node_to_stop])

            with allure.step("Check object data is not corrupted"):
                new_nodes = wait_object_replication(
                    cid, oid, 2, shell=self.shell, nodes=self.neofs_env.storage_nodes, neofs_env=self.neofs_env
                )
                got_file_path = get_object(wallet.path, cid, oid, shell=self.shell, endpoint=new_nodes[0].endpoint)
                assert get_file_hash(source_file_path) == get_file_hash(got_file_path)

    def test_put_get_without_storage_node(
        self,
        default_wallet,
        neofs_env_function_scope: NeoFSEnv,
        simple_object_size,
        after_run_return_all_stopped_storage_nodes,
    ):
        self.neofs_env = neofs_env_function_scope
        self.shell = self.neofs_env.shell

        with allure.step("Kill one storage node"):
            dead_node = self.neofs_env.storage_nodes[0]
            alive_nodes = self.neofs_env.storage_nodes[1:]

            dead_node.kill()

        with allure.step("Create container"):
            wallet = default_wallet
            placement_rule = "REP 3"
            cid = create_container(
                wallet.path,
                shell=self.shell,
                endpoint=alive_nodes[0].endpoint,
                rule=placement_rule,
                basic_acl=PUBLIC_ACL,
            )

        with allure.step("Put objects"):
            for _ in range(10):
                source_file_path = generate_file(simple_object_size)
                oid = put_object(
                    wallet.path,
                    source_file_path,
                    cid,
                    shell=self.shell,
                    endpoint=random.choice(alive_nodes).endpoint,
                )
                wait_object_replication(cid, oid, 3, shell=self.shell, nodes=alive_nodes, neofs_env=self.neofs_env)

        with allure.step("Get last object"):
            got_file_path = get_object(wallet.path, cid, oid, shell=self.shell, endpoint=alive_nodes[0].endpoint)
            assert get_file_hash(source_file_path) == get_file_hash(got_file_path)

        with allure.step("Return stopped storage node"):
            self.return_stopped_storage_nodes(self.neofs_env, [dead_node])

        with allure.step("Get last object from previously dead node"):
            got_file_path = get_object(wallet.path, cid, oid, shell=self.shell, endpoint=dead_node.endpoint)
            assert get_file_hash(source_file_path) == get_file_hash(got_file_path)

    def test_put_get_without_storage_nodes(
        self,
        default_wallet,
        neofs_env_function_scope: NeoFSEnv,
        simple_object_size,
        after_run_return_all_stopped_storage_nodes,
    ):
        self.neofs_env = neofs_env_function_scope
        self.shell = self.neofs_env.shell

        with allure.step("Kill two storage nodes"):
            dead_nodes = self.neofs_env.storage_nodes[:2]
            alive_nodes = self.neofs_env.storage_nodes[2:]

            for dead_node in dead_nodes:
                dead_node.kill()

        with allure.step("Create container"):
            wallet = default_wallet
            placement_rule = "REP 3"
            cid = create_container(
                wallet.path,
                shell=self.shell,
                endpoint=alive_nodes[0].endpoint,
                rule=placement_rule,
                basic_acl=PUBLIC_ACL,
            )

        with allure.step("Try to put object and expect error"):
            source_file_path = generate_file(simple_object_size)
            with pytest.raises(Exception, match=r".*incomplete object PUT by placement.*"):
                put_object(
                    wallet.path,
                    source_file_path,
                    cid,
                    shell=self.shell,
                    endpoint=alive_nodes[0].endpoint,
                )

        with allure.step("Return stopped storage node"):
            self.return_stopped_storage_nodes(self.neofs_env, dead_nodes)
