import json

import allure
import pytest
from helpers.complex_object_actions import wait_object_replication
from helpers.container import (
    create_container,
    delete_container,
    get_container,
    list_containers,
    wait_for_container_creation,
    wait_for_container_deletion,
)
from helpers.file_helper import generate_file
from helpers.grpc_responses import CONTAINER_DELETION_TIMED_OUT, NOT_CONTAINER_OWNER
from helpers.neofs_verbs import put_object_to_random_node
from helpers.node_management import wait_all_storage_nodes_returned
from helpers.utility import placement_policy_from_container
from helpers.wellknown_acl import PRIVATE_ACL_F, PUBLIC_ACL
from neofs_env.neofs_env_test_base import TestNeofsBase
from neofs_testlib.env.env import NeoFSEnv, NodeWallet, StorageNode


def object_should_be_gc_marked(neofs_env: NeoFSEnv, node: StorageNode, cid: str, oid: str):
    response = neofs_env.neofs_cli(node.cli_config).control.object_status(
        address=node.wallet.address,
        endpoint=node.control_endpoint,
        object=f"{cid}/{oid}",
        wallet=node.wallet.path,
    )
    assert "GC MARKED" in response.stdout, "Unexected output from control object status command"


class TestContainer(TestNeofsBase):
    @pytest.mark.parametrize("name", ["", "test-container"], ids=["No name", "Set particular name"])
    @pytest.mark.sanity
    def test_container_creation(self, default_wallet, name):
        scenario_title = f"with name {name}" if name else "without name"
        allure.dynamic.title(f"User can create container {scenario_title}")

        wallet = default_wallet
        with open(wallet.path) as file:
            json_wallet = json.load(file)

        placement_rule = "REP 2 IN X CBF 1 SELECT 2 FROM * AS X"
        cid = create_container(
            wallet.path,
            rule=placement_rule,
            name=name,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )

        containers = list_containers(wallet.path, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
        assert cid in containers, f"Expected container {cid} in containers: {containers}"

        container_info: str = get_container(
            wallet.path,
            cid,
            json_mode=False,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )
        container_info = container_info.casefold()  # To ignore case when comparing with expected values

        info_to_check = {
            f"basic ACL: {PRIVATE_ACL_F} (private)",
            f"owner ID: {json_wallet.get('accounts')[0].get('address')}",
            f"container ID: {cid}",
        }
        if name:
            info_to_check.add(f"Name={name}")

        with allure.step("Check container has correct information"):
            expected_policy = placement_rule.casefold()
            actual_policy = placement_policy_from_container(container_info)
            assert actual_policy == expected_policy, (
                f"Expected policy\n{expected_policy} but got policy\n{actual_policy}"
            )

            for info in info_to_check:
                expected_info = info.casefold()
                assert expected_info in container_info, f"Expected {expected_info} in container info:\n{container_info}"

        with allure.step("Delete container and check it was deleted"):
            delete_container(wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
            self.tick_epochs_and_wait(1)
            wait_for_container_deletion(wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

    @allure.title("Not owner and not trusted party can NOT delete container")
    def test_only_owner_can_delete_container(self, not_owner_wallet: NodeWallet, default_wallet: str):
        cid = create_container(
            wallet=default_wallet.path,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )

        with allure.step("Try to delete container"):
            with pytest.raises(RuntimeError, match=NOT_CONTAINER_OWNER):
                delete_container(
                    wallet=not_owner_wallet.path,
                    cid=cid,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    await_mode=True,
                )

        with allure.step("Try to force delete container"):
            with pytest.raises(RuntimeError, match=CONTAINER_DELETION_TIMED_OUT):
                delete_container(
                    wallet=not_owner_wallet.path,
                    cid=cid,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    await_mode=True,
                    force=True,
                )

    @allure.title("Parallel container creation and deletion")
    def test_container_creation_deletion_parallel(self, default_wallet):
        containers_count = 3
        wallet = default_wallet
        placement_rule = "REP 2 IN X CBF 1 SELECT 2 FROM * AS X"

        cids: list[str] = []
        with allure.step(f"Create {containers_count} containers"):
            for _ in range(containers_count):
                cids.append(
                    create_container(
                        wallet.path,
                        rule=placement_rule,
                        await_mode=False,
                        shell=self.shell,
                        endpoint=self.neofs_env.sn_rpc,
                        wait_for_creation=False,
                    )
                )

        with allure.step("Wait for containers occur in container list"):
            for cid in cids:
                wait_for_container_creation(
                    wallet.path,
                    cid,
                    sleep_interval=containers_count,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                )

        with allure.step("Delete containers and check they were deleted"):
            for cid in cids:
                delete_container(wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
            self.tick_epochs_and_wait(1)
            wait_for_container_deletion(wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

    @allure.title("Container deletion while some storage nodes down")
    def test_container_deletion_while_sn_down(self, default_wallet, simple_object_size):
        with allure.step("Create container"):
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
        with allure.step("Put object"):
            oid = put_object_to_random_node(
                wallet.path, source_file_path, cid, shell=self.shell, neofs_env=self.neofs_env
            )
            nodes_with_object = wait_object_replication(
                cid, oid, 2, shell=self.shell, nodes=self.neofs_env.storage_nodes, neofs_env=self.neofs_env
            )

        stopped_nodes = []
        try:
            with allure.step("Down storage node that stores the object"):
                node_to_stop = nodes_with_object[0]
                alive_node_with_object = nodes_with_object[1]

                node_to_stop.stop()
                stopped_nodes.append(node_to_stop)

            with allure.step("Delete container"):
                delete_container(
                    wallet.path, cid, shell=self.shell, endpoint=alive_node_with_object.endpoint, await_mode=True
                )

            with allure.step("Alive node should return GC MARKED for the created object from the deleted container"):
                object_should_be_gc_marked(self.neofs_env, alive_node_with_object, cid, oid)

            with allure.step("Start storage node"):
                node_to_stop.start(fresh=False)
                wait_all_storage_nodes_returned(self.neofs_env)

            with allure.step("Previously stopped node should return GC MARKED for the created object"):
                object_should_be_gc_marked(self.neofs_env, node_to_stop, cid, oid)
        finally:
            for node in list(stopped_nodes):
                with allure.step(f"Start {node}"):
                    node.start(fresh=False)
                stopped_nodes.remove(node)

            wait_all_storage_nodes_returned(self.neofs_env)

    def test_container_global_name(self, default_wallet, simple_object_size):
        with allure.step("Create container"):
            wallet = default_wallet
            placement_rule = "REP 2 IN X CBF 2 SELECT 2 FROM * AS X"
            create_container(
                wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                rule=placement_rule,
                name="foo",
                basic_acl=PUBLIC_ACL,
                global_name=True,
            )

        with allure.step("Get NNS names"):
            raw_dumped_names = (
                self.neofs_env.neofs_adm().fschain.dump_names(f"http://{self.neofs_env.fschain_rpc}").stdout
            )
            assert "foo.container" in raw_dumped_names, "Updated name not found"

        with allure.step("Try to create container with same name"):
            wallet = default_wallet
            placement_rule = "REP 2 IN X CBF 2 SELECT 2 FROM * AS X"
            with pytest.raises(RuntimeError, match=".*name is already taken.*"):
                create_container(
                    wallet.path,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    rule=placement_rule,
                    name="foo",
                    basic_acl=PUBLIC_ACL,
                    global_name=True,
                )
