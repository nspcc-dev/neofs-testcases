import json
from typing import Optional

import allure
import neofs_env.neofs_epoch as neofs_epoch
import pytest
from helpers.complex_object_actions import get_nodes_with_object
from helpers.container import (
    create_container,
    get_container,
    parse_container_nodes_by_descriptor,
    parse_container_nodes_output,
)
from helpers.file_helper import generate_file, get_file_hash
from helpers.neofs_verbs import get_object, put_object_to_random_node
from helpers.node_management import netmap_endpoint_port, storage_node_by_port
from helpers.utility import parse_version, placement_policy_from_container
from helpers.wellknown_acl import PUBLIC_ACL
from neofs_env.neofs_env_test_base import TestNeofsBase
from neofs_testlib.env.env import NeoFSEnv, NodeWallet

# math.MaxUint32: NeoFS API sentinel switching CBF from multiplier to SELECT-as-max-nodes mode.
FLEXIBLE_CBF = 4294967295


def _container_nodes_output(neofs_env: NeoFSEnv, cid: str) -> str:
    sn = neofs_env.storage_nodes[0]
    return neofs_env.neofs_cli(sn.cli_config).container.nodes(rpc_endpoint=sn.endpoint, cid=cid).stdout


def _container_nodes(neofs_env: NeoFSEnv, cid: str) -> list[dict]:
    return parse_container_nodes_output(_container_nodes_output(neofs_env, cid))


def _container_nodes_by_descriptor(neofs_env: NeoFSEnv, cid: str) -> list[dict]:
    return parse_container_nodes_by_descriptor(_container_nodes_output(neofs_env, cid))


class TestRigidCBF(TestNeofsBase):
    @pytest.mark.parametrize(
        "placement_rule,expected_count,expected_country",
        [
            ("REP 1 IN X CBF 1 SELECT 1 FROM * AS X", 1, None),
            ("REP 1 IN X CBF 2 SELECT 1 FROM * AS X", 2, None),
            ("REP 1 IN X CBF 3 SELECT 1 FROM * AS X", 3, None),
            ("REP 1 IN X SELECT 1 FROM * AS X", 3, None),
            ("REP 2 IN X CBF 2 SELECT 2 FROM * AS X", 4, None),
            ("REP 1 IN X CBF 4 SELECT 2 FROM * AS X", 4, None),
            ("EC 1/1 CBF 1", 2, None),
            ("EC 1/1 CBF 2", 4, None),
            ("EC 2/1 CBF 1", 3, None),
            ("EC 2/1", 4, None),
            ("REP 1 IN X CBF 2 SELECT 1 FROM RU AS X FILTER Country EQ Russia AS RU", 2, "Russia"),
        ],
        ids=[
            "rep1_cbf1_select1",
            "rep1_cbf2_select1",
            "rep1_cbf3_select1",
            "rep1_default_cbf_select1",
            "rep2_cbf2_select2",
            "rep1_cbf4_select2_capped",
            "ec_1_1_cbf1",
            "ec_1_1_cbf2",
            "ec_2_1_cbf1",
            "ec_2_1_default_cbf",
            "rep1_cbf2_filtered_ru",
        ],
    )
    def test_rigid_cbf_node_counts(
        self,
        default_wallet: NodeWallet,
        placement_rule: str,
        expected_count: int,
        expected_country: Optional[str],
    ):
        with allure.step(f"Create container with rigid CBF policy: {placement_rule}"):
            cid = create_container(
                default_wallet.path,
                rule=placement_rule,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step(f"Verify container nodes count is {expected_count}"):
            nodes = _container_nodes(self.neofs_env, cid)
            assert len(nodes) == expected_count, (
                f"Expected {expected_count} nodes for '{placement_rule}', got {len(nodes)}: {nodes}"
            )
            if expected_country is not None:
                countries = [node.get("Country") for node in nodes]
                assert all(country == expected_country for country in countries), (
                    f"Expected all nodes in {expected_country}, got {countries}"
                )

    @pytest.mark.parametrize(
        "placement_rule",
        [
            "REP 2 IN X CBF 1 SELECT 5 FROM * AS X",
            "REP 1 IN X CBF 1 SELECT 3 FROM RU AS X FILTER Country EQ Russia AS RU",
            "REP 2 IN X CBF 1 SELECT 4 IN DISTINCT Country FROM * AS X",
        ],
        ids=[
            "select_exceeds_cluster",
            "select_exceeds_filtered_pool",
            "select_exceeds_distinct_countries",
        ],
    )
    def test_rigid_cbf_not_enough_nodes(self, default_wallet: NodeWallet, placement_rule: str):
        with allure.step(f"Verify container creation fails for unsatisfiable rigid CBF policy: {placement_rule}"):
            with pytest.raises(RuntimeError, match="not enough nodes to SELECT from"):
                create_container(
                    default_wallet.path,
                    rule=placement_rule,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                )


class TestFlexibleCBF(TestNeofsBase):
    @pytest.fixture(scope="class", autouse=True)
    def skip_if_flexible_cbf_unsupported(self, neofs_env: NeoFSEnv) -> None:
        node_version = neofs_env.get_binary_version(neofs_env.neofs_node_path)
        if parse_version(node_version) <= parse_version("0.58.2"):
            pytest.skip(f"flexible CBF is not supported by neofs-node {node_version} (<= 0.58.2)")

    @pytest.mark.parametrize(
        "placement_rule,expected_count,expected_policy",
        [
            (f"REP 2 IN X CBF {FLEXIBLE_CBF} SELECT 3 FROM * AS X", 3, None),
            (f"REP 1 IN X CBF {FLEXIBLE_CBF} SELECT 4 FROM * AS X", 4, None),
            (f"REP 1 IN X CBF {FLEXIBLE_CBF} SELECT 2 FROM * AS X", 2, None),
            (f"REP 2 IN X CBF {FLEXIBLE_CBF} SELECT 2 FROM * AS X", 2, None),
            (f"REP 2 CBF {FLEXIBLE_CBF}", 2, None),
            (f"REP 2 CBF {FLEXIBLE_CBF} SELECT 3 FROM *", 3, None),
            (f"REP 2 IN X CBF {FLEXIBLE_CBF} SELECT 6 FROM * AS X", 4, None),
            (f"REP 1 IN X CBF {FLEXIBLE_CBF} SELECT 64 FROM * AS X", 4, None),
            (
                json.dumps(
                    {
                        "replicas": [{"count": 2, "selector": "X"}],
                        "containerBackupFactor": FLEXIBLE_CBF,
                        "selectors": [{"name": "X", "count": 3, "filter": "*"}],
                    }
                ),
                3,
                f"REP 2 IN X CBF {FLEXIBLE_CBF} SELECT 3 FROM * AS X",
            ),
        ],
        ids=[
            "rep2_select3",
            "rep1_select4",
            "rep1_select2",
            "rep2_select2_exact_min",
            "rep2_no_selector",
            "rep2_unnamed_selector_3",
            "rep2_select6_capped_at_netmap",
            "rep1_select64_verify_upper_bound",
            "rep2_select3_json_policy",
        ],
    )
    def test_flexible_cbf_rep_node_counts(
        self,
        default_wallet: NodeWallet,
        placement_rule: str,
        expected_count: int,
        expected_policy: Optional[str],
    ):
        canonical_policy = expected_policy or placement_rule
        with allure.step("Create container with flexible CBF REP policy"):
            cid = create_container(
                default_wallet.path,
                rule=placement_rule,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Verify container policy round-trip"):
            container_info = get_container(
                default_wallet.path,
                cid,
                json_mode=False,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            actual_policy = placement_policy_from_container(container_info)
            assert actual_policy == canonical_policy, f"Expected policy '{canonical_policy}', got '{actual_policy}'"

        with allure.step(f"Verify container nodes count is {expected_count}"):
            nodes = _container_nodes(self.neofs_env, cid)
            assert len(nodes) == expected_count, (
                f"Expected {expected_count} nodes for '{canonical_policy}', got {len(nodes)}: {nodes}"
            )

    @pytest.mark.parametrize(
        "placement_rule,expected_count",
        [
            (f"EC 2/1 IN X CBF {FLEXIBLE_CBF} SELECT 4 FROM * AS X", 4),
            (f"EC 2/1 IN X CBF {FLEXIBLE_CBF} SELECT 3 FROM * AS X", 3),
            (f"EC 2/1 CBF {FLEXIBLE_CBF}", 3),
            (f"EC 2/1 CBF {FLEXIBLE_CBF} SELECT 4 FROM *", 4),
            (f"EC 2/1 IN X CBF {FLEXIBLE_CBF} SELECT 6 FROM * AS X", 4),
            (f"EC 1/1 IN X CBF {FLEXIBLE_CBF} SELECT 3 FROM * AS X", 3),
        ],
        ids=[
            "ec_2_1_select4",
            "ec_2_1_select3",
            "ec_2_1_no_selector",
            "ec_2_1_unnamed_selector_4",
            "ec_2_1_select6_capped_at_netmap",
            "ec_1_1_select3",
        ],
    )
    def test_flexible_cbf_ec_node_counts(
        self,
        default_wallet: NodeWallet,
        placement_rule: str,
        expected_count: int,
    ):
        with allure.step(f"Create container with flexible CBF EC policy: {placement_rule}"):
            cid = create_container(
                default_wallet.path,
                rule=placement_rule,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Verify container policy round-trip"):
            container_info = get_container(
                default_wallet.path,
                cid,
                json_mode=False,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            actual_policy = placement_policy_from_container(container_info)
            assert actual_policy == placement_rule, f"Expected policy '{placement_rule}', got '{actual_policy}'"

        with allure.step(f"Verify container nodes count is {expected_count}"):
            nodes = _container_nodes(self.neofs_env, cid)
            assert len(nodes) == expected_count, (
                f"Expected {expected_count} nodes for '{placement_rule}', got {len(nodes)}: {nodes}"
            )

    @pytest.mark.parametrize(
        "placement_rule,expected_count,expected_countries",
        [
            (
                f"REP 1 IN X CBF {FLEXIBLE_CBF} SELECT 2 FROM RU AS X FILTER Country EQ Russia AS RU",
                2,
                {"Russia"},
            ),
            (
                f"REP 1 IN X CBF {FLEXIBLE_CBF} SELECT 4 FROM RU AS X FILTER Country EQ Russia AS RU",
                2,
                {"Russia"},
            ),
            (
                f"REP 2 IN X CBF {FLEXIBLE_CBF} SELECT 3 IN DISTINCT Country FROM * AS X",
                3,
                {"Russia", "Sweden", "Finland"},
            ),
            (
                f"REP 2 IN X CBF {FLEXIBLE_CBF} SELECT 4 IN DISTINCT Country FROM * AS X",
                3,
                {"Russia", "Sweden", "Finland"},
            ),
            (
                f"REP 1 IN X CBF {FLEXIBLE_CBF} SELECT 2 IN SAME Country FROM * AS X",
                2,
                {"Russia"},
            ),
        ],
        ids=[
            "filter_ru_exact",
            "filter_ru_capped",
            "distinct_country_exact",
            "distinct_country_capped",
            "same_country_exact",
        ],
    )
    def test_flexible_cbf_filters_and_buckets(
        self,
        default_wallet: NodeWallet,
        placement_rule: str,
        expected_count: int,
        expected_countries: set[str],
    ):
        with allure.step(f"Create container with filter/bucket flexible CBF policy: {placement_rule}"):
            cid = create_container(
                default_wallet.path,
                rule=placement_rule,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step(f"Verify container lists {expected_count} nodes in {sorted(expected_countries)}"):
            nodes = _container_nodes(self.neofs_env, cid)
            countries = [node.get("Country") for node in nodes]
            assert len(nodes) == expected_count, (
                f"Expected {expected_count} nodes for '{placement_rule}', got {len(nodes)}: {nodes}"
            )
            assert set(countries) == expected_countries, (
                f"Expected countries {expected_countries} for '{placement_rule}', got {countries}"
            )

    @pytest.mark.parametrize(
        "placement_rule,expected_descriptors",
        [
            (
                f"REP 1 IN X REP 2 IN Y CBF {FLEXIBLE_CBF} SELECT 2 FROM * AS X SELECT 3 FROM * AS Y",
                [("REP", "REP 1", 2), ("REP", "REP 2", 3)],
            ),
            (
                f"EC 1/1 IN X EC 2/1 IN Y CBF {FLEXIBLE_CBF} SELECT 3 FROM * AS X SELECT 4 FROM * AS Y",
                [("EC", "EC 1/1", 3), ("EC", "EC 2/1", 4)],
            ),
        ],
        ids=[
            "two_rep_descriptors",
            "two_ec_descriptors",
        ],
    )
    def test_flexible_cbf_multi_descriptor(
        self,
        default_wallet: NodeWallet,
        placement_rule: str,
        expected_descriptors: list[tuple[str, str, int]],
    ):
        with allure.step(f"Create multi-descriptor container: {placement_rule}"):
            cid = create_container(
                default_wallet.path,
                rule=placement_rule,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Verify per-descriptor node counts"):
            descriptors = _container_nodes_by_descriptor(self.neofs_env, cid)
            actual = [(d["kind"], d["rule"], len(d["nodes"])) for d in descriptors]
            assert actual == expected_descriptors, (
                f"Expected descriptors {expected_descriptors}, got {actual} (raw: {descriptors})"
            )

    @pytest.mark.parametrize(
        "placement_rule,force,expected_error",
        [
            (f"REP 3 IN X CBF {FLEXIBLE_CBF} SELECT 2 FROM * AS X", False, "not enough nodes to SELECT from"),
            (f"EC 2/2 IN X CBF {FLEXIBLE_CBF} SELECT 3 FROM * AS X", False, "not enough nodes to SELECT from"),
            (f"REP 5 IN X CBF {FLEXIBLE_CBF} SELECT 6 FROM * AS X", False, "not enough nodes to SELECT from"),
            (f"EC 3/2 IN X CBF {FLEXIBLE_CBF} SELECT 6 FROM * AS X", False, "not enough nodes to SELECT from"),
            (
                f"REP 3 IN RU CBF {FLEXIBLE_CBF} SELECT 4 FROM RU_NODES AS RU FILTER Country EQ Russia AS RU_NODES",
                False,
                "not enough nodes to SELECT from",
            ),
            (f"REP 1 IN X CBF {FLEXIBLE_CBF} SELECT 65 FROM * AS X", True, "more than 64 nodes"),
            (f"EC 2/1 IN X CBF {FLEXIBLE_CBF} SELECT 65 FROM * AS X", True, "more than 64 nodes"),
        ],
        ids=[
            "rep_selector_below_min",
            "ec_selector_below_min",
            "rep_netmap_below_min",
            "ec_netmap_below_min",
            "rep_filter_pool_below_min",
            "rep_select_exceeds_verify_limit",
            "ec_select_exceeds_verify_limit",
        ],
    )
    def test_flexible_cbf_negative(
        self,
        default_wallet: NodeWallet,
        placement_rule: str,
        force: bool,
        expected_error: str,
    ):
        with allure.step(f"Verify invalid flexible CBF policy is rejected: {placement_rule}"):
            with pytest.raises(RuntimeError, match=expected_error):
                create_container(
                    default_wallet.path,
                    rule=placement_rule,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    force=force,
                )

    def test_flexible_cbf_object_placement(self, default_wallet: NodeWallet):
        file_size = self.neofs_env.get_object_size("simple_object_size")
        rep_rule = f"REP 2 IN X CBF {FLEXIBLE_CBF} SELECT 3 FROM * AS X"

        with allure.step("Create flexible CBF REP 2 / SELECT 3 container and put object"):
            rep_cid = create_container(
                default_wallet.path,
                rule=rep_rule,
                basic_acl=PUBLIC_ACL,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            rep_nodes = _container_nodes(self.neofs_env, rep_cid)
            assert len(rep_nodes) == 3, f"Expected 3 container nodes, got {len(rep_nodes)}: {rep_nodes}"
            container_sns = {
                storage_node_by_port(self.neofs_env, netmap_endpoint_port(node["endpoint"])) for node in rep_nodes
            }
            assert None not in container_sns, f"Failed to resolve all container nodes: {rep_nodes}"

            rep_file = generate_file(file_size)
            rep_oid = put_object_to_random_node(
                default_wallet.path,
                rep_file,
                rep_cid,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step("Verify object has 2 copies inside the 3 container nodes and is readable"):
            holders = get_nodes_with_object(
                rep_cid,
                rep_oid,
                shell=self.shell,
                nodes=self.neofs_env.storage_nodes,
                neofs_env=self.neofs_env,
            )
            assert len(holders) == 2, f"Expected 2 object copies for REP 2, got {len(holders)}: {holders}"
            assert set(holders) <= container_sns, (
                f"Object holders {holders} must be a subset of container nodes {container_sns}"
            )
            got = get_object(
                default_wallet.path,
                rep_cid,
                rep_oid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            assert get_file_hash(got) == get_file_hash(rep_file)

        ec_rule = f"EC 2/1 IN X CBF {FLEXIBLE_CBF} SELECT 4 FROM * AS X"
        with allure.step("Create flexible CBF EC 2/1 / SELECT 4 container and put object"):
            ec_cid = create_container(
                default_wallet.path,
                rule=ec_rule,
                basic_acl=PUBLIC_ACL,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            ec_nodes = _container_nodes(self.neofs_env, ec_cid)
            assert len(ec_nodes) == 4, f"Expected 4 container nodes, got {len(ec_nodes)}: {ec_nodes}"

            ec_file = generate_file(file_size)
            ec_oid = put_object_to_random_node(
                default_wallet.path,
                ec_file,
                ec_cid,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step("Verify EC object is readable from every storage node"):
            ec_file_hash = get_file_hash(ec_file)
            for sn in self.neofs_env.storage_nodes:
                got = get_object(
                    default_wallet.path,
                    ec_cid,
                    ec_oid,
                    shell=self.shell,
                    endpoint=sn.endpoint,
                )
                assert get_file_hash(got) == ec_file_hash

    def test_flexible_cbf_scales_up_to_cap(self, default_wallet: NodeWallet, neofs_env_function_scope: NeoFSEnv):
        neofs_env = neofs_env_function_scope
        rep_rule = f"REP 2 IN X CBF {FLEXIBLE_CBF} SELECT 5 FROM * AS X"
        ec_rule = f"EC 2/1 IN X CBF {FLEXIBLE_CBF} SELECT 5 FROM * AS X"

        with allure.step("Create REP and EC containers with SELECT 5 cap on 4 storage nodes"):
            rep_cid = create_container(
                default_wallet.path,
                rule=rep_rule,
                shell=neofs_env.shell,
                endpoint=neofs_env.sn_rpc,
            )
            ec_cid = create_container(
                default_wallet.path,
                rule=ec_rule,
                shell=neofs_env.shell,
                endpoint=neofs_env.sn_rpc,
            )
            assert len(_container_nodes(neofs_env, rep_cid)) == 4
            assert len(_container_nodes(neofs_env, ec_cid)) == 4

        with allure.step("Add 5th storage node and verify both containers grow to 5 nodes"):
            neofs_env.deploy_storage_nodes(
                count=1,
                node_attrs={0: ["UN-LOCODE:DE FRA", "Price:55"]},
                fschain_endpoints=neofs_env.storage_nodes[0].fschain_endpoints,
            )
            neofs_epoch.tick_epoch_and_wait(neofs_env)
            assert len(_container_nodes(neofs_env, rep_cid)) == 5
            assert len(_container_nodes(neofs_env, ec_cid)) == 5

        with allure.step("Add 6th storage node and verify both containers stay capped at 5 nodes"):
            neofs_env.deploy_storage_nodes(
                count=1,
                node_attrs={0: ["UN-LOCODE:NL AMS", "Price:66"]},
                fschain_endpoints=neofs_env.storage_nodes[0].fschain_endpoints,
            )
            neofs_epoch.tick_epoch_and_wait(neofs_env)
            assert len(_container_nodes(neofs_env, rep_cid)) == 5
            assert len(_container_nodes(neofs_env, ec_cid)) == 5
