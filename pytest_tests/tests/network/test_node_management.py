import logging
import random
from time import sleep
from typing import Optional, Tuple

import allure
import pytest
from helpers.common import FSCHAIN_BLOCK_TIME
from helpers.complex_object_actions import get_nodes_with_object, wait_object_replication
from helpers.container import create_container, get_container
from helpers.file_helper import generate_file
from helpers.grpc_responses import OBJECT_NOT_FOUND, error_matches_status
from helpers.neofs_verbs import (
    delete_object,
    get_object,
    get_object_from_random_node,
    head_object,
    put_object,
    put_object_to_random_node,
)
from helpers.node_management import (
    check_node_in_map,
    delete_node_data,
    drop_object,
    exclude_node_from_network_map,
    get_locode_from_random_node,
    include_node_to_network_map,
    node_shard_list,
    node_shard_set_mode,
    storage_node_set_status,
)
from helpers.utility import (
    parse_time,
    placement_policy_from_container,
    wait_for_gc_pass_on_storage_nodes,
)
from helpers.wellknown_acl import PUBLIC_ACL
from neofs_env.neofs_env_test_base import NeofsEnvTestBase
from neofs_testlib.env.env import NodeWallet, StorageNode

logger = logging.getLogger("NeoLogger")
check_nodes: list[StorageNode] = []


@allure.title("Add one node to cluster")
class TestNodeManagement(NeofsEnvTestBase):
    @pytest.fixture
    @allure.title("Create container and pick the node with data")
    def create_container_and_pick_node(self, default_wallet: NodeWallet, simple_object_size) -> Tuple[str, StorageNode]:
        file_path = generate_file(simple_object_size)
        placement_rule = "REP 1 IN X CBF 1 SELECT 1 FROM * AS X"
        endpoint = self.neofs_env.sn_rpc

        cid = create_container(
            default_wallet.path,
            shell=self.shell,
            endpoint=endpoint,
            rule=placement_rule,
            basic_acl=PUBLIC_ACL,
        )
        oid = put_object_to_random_node(default_wallet.path, file_path, cid, self.shell, neofs_env=self.neofs_env)

        nodes = get_nodes_with_object(
            cid, oid, shell=self.shell, nodes=self.neofs_env.storage_nodes, neofs_env=self.neofs_env
        )
        assert len(nodes) == 1
        node = nodes[0]

        yield cid, node

        shards = node_shard_list(node)
        assert shards

        for shard in shards:
            node_shard_set_mode(node, shard, "read-write")

        node_shard_list(node)

    @allure.step("Tick epoch with retries")
    def tick_epoch_with_retries(self, attempts: int = 3, timeout: int = 3):
        for attempt in range(attempts):
            try:
                self.tick_epochs_and_wait(1)
            except RuntimeError:
                sleep(timeout)
                if attempt >= attempts - 1:
                    raise
                continue
            return

    @pytest.fixture
    def after_run_start_all_nodes(self):
        yield
        self.return_nodes()

    @pytest.fixture
    def return_nodes_after_test_run(self):
        yield
        self.return_nodes()

    @allure.step("Return node to cluster")
    def return_nodes(self, alive_node: Optional[StorageNode] = None) -> None:
        for node in list(check_nodes):
            with allure.step(f"Start node {node}"):
                node.start(fresh=False)

            # We need to wait for node to establish notifications from FS chain
            # Otherwise it will hang up when we will try to set status
            sleep(parse_time(FSCHAIN_BLOCK_TIME))

            with allure.step(f"Move node {node} to online state"):
                storage_node_set_status(node, status="online", retries=2)

            check_nodes.remove(node)
            sleep(parse_time(FSCHAIN_BLOCK_TIME))
            self.tick_epoch_with_retries(3)
            check_node_in_map(node, shell=self.shell, alive_node=alive_node)

    @allure.title("Add one node to cluster")
    @pytest.mark.skip(reason="Problems with nodes addition on new dev env")
    def test_add_nodes(
        self,
        default_wallet,
        return_nodes_after_test_run,
        simple_object_size,
    ):
        """
        This test remove one node from cluster then add it back. Test uses base control operations with storage nodes (healthcheck, netmap-snapshot, set-status).
        """

        wallet = default_wallet
        placement_rule_3 = "REP 3 IN X CBF 1 SELECT 3 FROM * AS X"
        placement_rule_4 = "REP 4 IN X CBF 1 SELECT 4 FROM * AS X"
        source_file_path = generate_file(simple_object_size)

        storage_nodes = self.neofs_env.storage_nodes
        random_node = random.choice(storage_nodes[1:])
        alive_node = random.choice(
            [storage_node for storage_node in storage_nodes if storage_node.sn_number != random_node.sn_number]
        )

        check_node_in_map(random_node, shell=self.shell, alive_node=alive_node)

        # Add node to recovery list before messing with it
        check_nodes.append(random_node)
        exclude_node_from_network_map(random_node, alive_node, shell=self.shell, neofs_env=self.neofs_env)
        delete_node_data(random_node)

        cid = create_container(
            wallet.path,
            rule=placement_rule_3,
            basic_acl=PUBLIC_ACL,
            shell=self.shell,
            endpoint=alive_node.endpoint,
        )
        oid = put_object(
            wallet.path,
            source_file_path,
            cid,
            shell=self.shell,
            endpoint=alive_node.endpoint,
        )
        wait_object_replication(cid, oid, 3, shell=self.shell, nodes=storage_nodes, neofs_env=self.neofs_env)

        self.return_nodes(alive_node)

        with allure.step("Check data could be replicated to new node"):
            random_node = random.choice(list(set(storage_nodes) - {random_node, alive_node}))
            # Add node to recovery list before messing with it
            check_nodes.append(random_node)
            exclude_node_from_network_map(random_node, alive_node, shell=self.shell, neofs_env=self.neofs_env)

            wait_object_replication(
                cid,
                oid,
                3,
                shell=self.shell,
                nodes=list(set(storage_nodes) - {random_node}),
                neofs_env=self.neofs_env,
            )
            include_node_to_network_map(random_node, alive_node, shell=self.shell, neofs_env=self.neofs_env)
            wait_object_replication(
                cid,
                oid,
                3,
                shell=self.shell,
                nodes=storage_nodes,
                neofs_env=self.neofs_env,
            )

        with allure.step("Check container could be created with new node"):
            cid = create_container(
                wallet.path,
                rule=placement_rule_4,
                basic_acl=PUBLIC_ACL,
                shell=self.shell,
                endpoint=alive_node.endpoint,
            )
            oid = put_object(
                wallet.path,
                source_file_path,
                cid,
                shell=self.shell,
                endpoint=alive_node.endpoint,
            )
            wait_object_replication(cid, oid, 4, shell=self.shell, nodes=storage_nodes, neofs_env=self.neofs_env)

    @pytest.mark.parametrize(
        "placement_rule,expected_copies",
        [
            ("REP 2 IN X CBF 2 SELECT 2 FROM * AS X", 2),
            ("REP 2 IN X CBF 1 SELECT 2 FROM * AS X", 2),
            ("REP 3 IN X CBF 1 SELECT 3 FROM * AS X", 3),
            ("REP 1 IN X CBF 1 SELECT 1 FROM * AS X", 1),
            ("REP 1 IN X CBF 2 SELECT 1 FROM * AS X", 1),
            ("REP 4 IN X CBF 1 SELECT 4 FROM * AS X", 4),
            ("REP 2 IN X CBF 1 SELECT 4 FROM * AS X", 2),
        ],
    )
    @allure.title("Test object copies based on placement policy")
    def test_placement_policy(self, default_wallet, placement_rule, expected_copies, simple_object_size):
        """
        This test checks object's copies based on container's placement policy.
        """
        wallet = default_wallet
        file_path = generate_file(simple_object_size)
        self.validate_object_copies(wallet.path, placement_rule, file_path, expected_copies)

    @pytest.mark.parametrize(
        "placement_rule,expected_copies,expected_nodes_id",
        [
            ("REP 4 IN X CBF 1 SELECT 4 FROM * AS X", 4, {1, 2, 3, 4}),
            (
                "REP 1 IN LOC_PLACE CBF 1 SELECT 1 FROM LOC_SW AS LOC_PLACE FILTER Country EQ Sweden AS LOC_SW",
                1,
                {3},
            ),
            (
                "REP 1 CBF 1 SELECT 1 FROM LOC_SPB FILTER 'UN-LOCODE' EQ 'RU LED' AS LOC_SPB",
                1,
                {2},
            ),
            (
                "REP 1 IN LOC_SPB_PLACE REP 1 IN LOC_MSK_PLACE CBF 1 SELECT 1 FROM LOC_SPB AS LOC_SPB_PLACE "
                "SELECT 1 FROM LOC_MSK AS LOC_MSK_PLACE "
                "FILTER 'UN-LOCODE' EQ 'RU LED' AS LOC_SPB FILTER 'UN-LOCODE' EQ 'RU MOW' AS LOC_MSK",
                2,
                {1, 2},
            ),
            (
                "REP 4 CBF 1 SELECT 4 FROM LOC_EU FILTER Continent EQ Europe AS LOC_EU",
                4,
                {1, 2, 3, 4},
            ),
            (
                "REP 1 CBF 1 SELECT 1 FROM LOC_SPB "
                "FILTER 'UN-LOCODE' NE 'RU MOW' AND 'UN-LOCODE' NE 'SE STO' AND 'UN-LOCODE' NE 'FI HEL' AS LOC_SPB",
                1,
                {2},
            ),
            (
                "REP 2 CBF 1 SELECT 2 FROM LOC_RU FILTER SubDivCode NE 'AB' AND SubDivCode NE '18' AS LOC_RU",
                2,
                {1, 2},
            ),
            (
                "REP 2 CBF 1 SELECT 2 FROM LOC_RU FILTER Country EQ 'Russia' AS LOC_RU",
                2,
                {1, 2},
            ),
            (
                "REP 2 CBF 1 SELECT 2 FROM LOC_EU FILTER Country NE 'Russia' AS LOC_EU",
                2,
                {3, 4},
            ),
        ],
    )
    @allure.title("Test object copies and storage nodes based on placement policy")
    def test_placement_policy_with_nodes(
        self,
        default_wallet,
        placement_rule,
        expected_copies,
        expected_nodes_id: set[int],
        simple_object_size,
    ):
        """
        Based on container's placement policy check that storage nodes are piked correctly and object has
        correct copies amount.
        """
        wallet = default_wallet
        file_path = generate_file(simple_object_size)
        cid, oid, found_nodes = self.validate_object_copies(wallet.path, placement_rule, file_path, expected_copies)

        assert found_nodes == expected_nodes_id, f"Expected nodes {expected_nodes_id}, got {found_nodes}"

    @pytest.mark.parametrize(
        "placement_rule,expected_copies",
        [
            ("REP 2 IN X CBF 2 SELECT 6 FROM * AS X", 2),
        ],
    )
    @allure.title("Negative cases for placement policy")
    def test_placement_policy_negative(self, default_wallet, placement_rule, expected_copies, simple_object_size):
        """
        Negative test for placement policy.
        """
        wallet = default_wallet
        file_path = generate_file(simple_object_size)
        with pytest.raises(RuntimeError, match=".*not enough nodes to SELECT from.*"):
            self.validate_object_copies(wallet.path, placement_rule, file_path, expected_copies)

    @allure.title("NeoFS object could be dropped using control command")
    @pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-testcases/issues/537")
    def test_drop_object(self, default_wallet, complex_object_size, simple_object_size):
        """
        Test checks object could be dropped using `neofs-cli control drop-objects` command.
        """
        wallet = default_wallet
        endpoint = self.neofs_env.sn_rpc
        file_path_simple, file_path_complex = generate_file(simple_object_size), generate_file(complex_object_size)

        locode = get_locode_from_random_node(self.neofs_env)
        rule = f"REP 1 CBF 1 SELECT 1 FROM * FILTER 'UN-LOCODE' EQ '{locode}' AS LOC"
        cid = create_container(wallet.path, rule=rule, shell=self.shell, endpoint=endpoint)
        oid_simple = put_object_to_random_node(
            wallet.path, file_path_simple, cid, shell=self.shell, neofs_env=self.neofs_env
        )
        oid_complex = put_object_to_random_node(
            wallet.path, file_path_complex, cid, shell=self.shell, neofs_env=self.neofs_env
        )

        for oid in (oid_simple, oid_complex):
            get_object_from_random_node(wallet.path, cid, oid, shell=self.shell, neofs_env=self.neofs_env)
            head_object(wallet.path, cid, oid, shell=self.shell, endpoint=endpoint)

        nodes_with_object = get_nodes_with_object(
            cid,
            oid_simple,
            shell=self.shell,
            nodes=self.neofs_env.storage_nodes,
            neofs_env=self.neofs_env,
        )
        random_node = random.choice(nodes_with_object)

        for oid in (oid_simple, oid_complex):
            with allure.step(f"Drop object {oid}"):
                get_object_from_random_node(wallet.path, cid, oid, shell=self.shell, neofs_env=self.neofs_env)
                head_object(wallet.path, cid, oid, shell=self.shell, endpoint=endpoint)
                drop_object(random_node, cid, oid)
                self.wait_for_obj_dropped(wallet.path, cid, oid, endpoint, get_object)
                self.wait_for_obj_dropped(wallet.path, cid, oid, endpoint, head_object)

    @pytest.mark.skip(reason="Need to clarify scenario")
    @allure.title("Control Operations with storage nodes")
    def test_shards(
        self,
        default_wallet,
        create_container_and_pick_node,
        simple_object_size,
    ):
        wallet = default_wallet
        file_path = generate_file(simple_object_size)

        cid, node = create_container_and_pick_node
        original_oid = put_object_to_random_node(wallet.path, file_path, cid, self.shell, neofs_env=self.neofs_env)

        # for mode in ('read-only', 'degraded'):
        for mode in ("degraded",):
            shards = node_shard_list(node)
            assert shards

            for shard in shards:
                node_shard_set_mode(node, shard, mode)

            shards = node_shard_list(node)
            assert shards

            with pytest.raises(RuntimeError):
                put_object_to_random_node(wallet, file_path, cid, self.shell, self.cluster)

            with pytest.raises(RuntimeError):
                delete_object(wallet, cid, original_oid, self.shell, self.neofs_env.sn_rpc)

            get_object_from_random_node(wallet, cid, original_oid, self.shell, self.cluster)

            for shard in shards:
                node_shard_set_mode(node, shard, "read-write")

            shards = node_shard_list(node)
            assert shards

            oid = put_object_to_random_node(wallet.path, file_path, cid, self.shell, neofs_env=self.neofs_env)
            delete_object(wallet.path, cid, oid, self.shell, self.neofs_env.sn_rpc)

    @allure.step("Validate object has {expected_copies} copies")
    def validate_object_copies(
        self, wallet: str, placement_rule: str, file_path: str, expected_copies: int
    ) -> set[int]:
        endpoint = self.neofs_env.sn_rpc

        for sn in self.neofs_env.storage_nodes:
            self.neofs_env.neofs_cli(sn.cli_config).netmap.nodeinfo(
                rpc_endpoint=sn.endpoint,
                wallet=wallet,
            )

        cid = create_container(wallet, rule=placement_rule, basic_acl=PUBLIC_ACL, shell=self.shell, endpoint=endpoint)
        got_policy = placement_policy_from_container(
            get_container(wallet, cid, json_mode=False, shell=self.shell, endpoint=endpoint)
        )
        assert got_policy == placement_rule.replace("'", ""), (
            f"Expected \n{placement_rule} and got policy \n{got_policy} are the same"
        )
        oid = put_object_to_random_node(wallet, file_path, cid, shell=self.shell, neofs_env=self.neofs_env)
        nodes = get_nodes_with_object(
            cid, oid, shell=self.shell, nodes=self.neofs_env.storage_nodes, neofs_env=self.neofs_env
        )
        nodes_id = {node.sn_number for node in nodes}
        assert len(nodes) == expected_copies, f"Expected {expected_copies} copies, got {len(nodes)}"
        return cid, oid, nodes_id

    @allure.step("Wait for object to be dropped")
    def wait_for_obj_dropped(self, wallet: str, cid: str, oid: str, endpoint: str, checker) -> None:
        for _ in range(3):
            try:
                checker(wallet, cid, oid, shell=self.shell, endpoint=endpoint)
                wait_for_gc_pass_on_storage_nodes()
            except Exception as err:
                if error_matches_status(err, OBJECT_NOT_FOUND):
                    return
                raise AssertionError(f'Expected "{OBJECT_NOT_FOUND}" error, got\n{err}')

        raise AssertionError(f"Object {oid} was not dropped from node")
