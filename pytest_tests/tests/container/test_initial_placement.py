import json
import logging
import time

import allure
import pytest
from helpers.complex_object_actions import get_nodes_with_object, wait_object_replication
from helpers.container import create_container
from helpers.file_helper import generate_file, get_file_hash
from helpers.neofs_verbs import get_object, put_object, put_object_to_random_node
from helpers.node_management import exclude_node_from_network_map, include_node_to_network_map
from helpers.utility import parse_version
from helpers.wellknown_acl import PUBLIC_ACL
from neofs_testlib.env.env import NeoFSEnv, NodeWallet

logger = logging.getLogger("NeoLogger")


def _policy(replicas: int, cbf: int = 2, initial: dict | None = None) -> str:
    policy: dict = {
        "replicas": [{"count": replicas}],
        "containerBackupFactor": cbf,
    }
    if initial:
        policy["initial"] = initial
    return json.dumps(policy)


def _multi_vector_policy(initial: dict | None = None) -> str:
    policy: dict = {
        "replicas": [
            {"count": 1, "selector": "RU"},
            {"count": 1, "selector": "NON_RU"},
        ],
        "containerBackupFactor": 1,
        "selectors": [
            {"name": "RU", "count": 2, "filter": "RU_NODES"},
            {"name": "NON_RU", "count": 2, "filter": "NON_RU_NODES"},
        ],
        "filters": [
            {
                "name": "RU_NODES",
                "op": "OR",
                "filters": [
                    {"key": "UN-LOCODE", "op": "EQ", "value": "RU MOW"},
                    {"key": "UN-LOCODE", "op": "EQ", "value": "RU LED"},
                ],
            },
            {
                "name": "NON_RU_NODES",
                "op": "OR",
                "filters": [
                    {"key": "UN-LOCODE", "op": "EQ", "value": "SE STO"},
                    {"key": "UN-LOCODE", "op": "EQ", "value": "FI HEL"},
                ],
            },
        ],
    }
    if initial:
        policy["initial"] = initial
    return json.dumps(policy)


_slow_policer = pytest.mark.parametrize(
    "neofs_env",
    [{"disable_post_initial_queue": True, "replication_cooldown": "1h"}],
    ids=["disable_post_initial_queue=True,replication_cooldown=1h"],
    indirect=True,
)


@_slow_policer
@pytest.mark.parametrize("max_replicas", [1, 2], ids=["max_replicas=1", "max_replicas=2"])
@allure.title("Initial placement: max_replicas={max_replicas} limits initial copies")
def test_initial_placement_max_replicas(
    default_wallet: NodeWallet,
    neofs_env: NeoFSEnv,
    max_replicas: int,
):
    with allure.step("Create container"):
        cid = create_container(
            wallet=default_wallet.path,
            rule=_policy(replicas=3, cbf=1, initial={"maxReplicas": max_replicas}),
            shell=neofs_env.shell,
            endpoint=neofs_env.sn_rpc,
            basic_acl=PUBLIC_ACL,
        )

    file_path = generate_file(neofs_env.get_object_size("simple_object_size"))

    with allure.step("PUT object"):
        oid = put_object_to_random_node(
            wallet=default_wallet.path,
            path=file_path,
            cid=cid,
            shell=neofs_env.shell,
            neofs_env=neofs_env,
        )

    with allure.step("Object is immediately retrievable"):
        got = get_object(
            wallet=default_wallet.path,
            cid=cid,
            oid=oid,
            endpoint=neofs_env.sn_rpc,
            shell=neofs_env.shell,
        )
        assert get_file_hash(file_path) == get_file_hash(got)

    with allure.step(f"Exactly {max_replicas} copy/copies exist"):
        nodes_with_object = get_nodes_with_object(
            cid,
            oid,
            shell=neofs_env.shell,
            nodes=neofs_env.storage_nodes,
            neofs_env=neofs_env,
        )
        assert len(nodes_with_object) == max_replicas, (
            f"Expected {max_replicas} initial copy/copies (max_replicas={max_replicas}), "
            f"but found {len(nodes_with_object)} on {nodes_with_object}"
        )


@pytest.mark.parametrize("max_replicas", [1], ids=["max_replicas=1"])
@allure.title("Initial placement: disable_post_initial_queue=False")
def test_initial_placement_max_replicas_optimized(
    default_wallet: NodeWallet,
    neofs_env: NeoFSEnv,
    max_replicas: int,
):
    if parse_version(neofs_env.get_binary_version(neofs_env.neofs_node_path)) <= parse_version("0.52.0"):
        pytest.skip("Requires fresh neofs-node")

    replicas = 3
    with allure.step("Create container"):
        cid = create_container(
            wallet=default_wallet.path,
            rule=_policy(replicas=replicas, cbf=1, initial={"maxReplicas": max_replicas}),
            shell=neofs_env.shell,
            endpoint=neofs_env.sn_rpc,
            basic_acl=PUBLIC_ACL,
        )

    file_path = generate_file(neofs_env.get_object_size("simple_object_size"))

    with allure.step("PUT object"):
        oid = put_object_to_random_node(
            wallet=default_wallet.path,
            path=file_path,
            cid=cid,
            shell=neofs_env.shell,
            neofs_env=neofs_env,
        )

    with allure.step("Object is immediately retrievable"):
        got = get_object(
            wallet=default_wallet.path,
            cid=cid,
            oid=oid,
            endpoint=neofs_env.sn_rpc,
            shell=neofs_env.shell,
        )
        assert get_file_hash(file_path) == get_file_hash(got)

    with allure.step(f"Exactly {replicas} copy/copies exist"):
        nodes_with_object = []
        for _ in range(5):
            nodes_with_object = get_nodes_with_object(
                cid,
                oid,
                shell=neofs_env.shell,
                nodes=neofs_env.storage_nodes,
                neofs_env=neofs_env,
            )
            if len(nodes_with_object) == replicas:
                break
            time.sleep(1)
        assert len(nodes_with_object) == replicas, (
            f"Expected {replicas} copy/copies, but found {len(nodes_with_object)} on {nodes_with_object}"
        )


@_slow_policer
@allure.title("Initial placement: replica_limits=[1] limits initial copies to 1 within the vector")
def test_initial_placement_replica_limits_single_vector(
    default_wallet: NodeWallet,
    neofs_env: NeoFSEnv,
):
    ru_nodes = neofs_env.storage_nodes[:2]
    non_ru_nodes = neofs_env.storage_nodes[2:]

    ru_only_policy = json.dumps(
        {
            "replicas": [{"count": 2, "selector": "RU"}],
            "containerBackupFactor": 1,
            "selectors": [{"name": "RU", "count": 2, "filter": "RU_NODES"}],
            "filters": [
                {
                    "name": "RU_NODES",
                    "op": "OR",
                    "filters": [
                        {"key": "UN-LOCODE", "op": "EQ", "value": "RU MOW"},
                        {"key": "UN-LOCODE", "op": "EQ", "value": "RU LED"},
                    ],
                }
            ],
            "initial": {"replicaLimits": [1]},
        }
    )

    with allure.step("Create container"):
        cid = create_container(
            wallet=default_wallet.path,
            rule=ru_only_policy,
            shell=neofs_env.shell,
            endpoint=neofs_env.sn_rpc,
            basic_acl=PUBLIC_ACL,
        )

    file_path = generate_file(neofs_env.get_object_size("simple_object_size"))

    with allure.step("PUT object"):
        oid = put_object_to_random_node(
            wallet=default_wallet.path,
            path=file_path,
            cid=cid,
            shell=neofs_env.shell,
            neofs_env=neofs_env,
        )

    with allure.step("Object is immediately retrievable"):
        got = get_object(
            wallet=default_wallet.path,
            cid=cid,
            oid=oid,
            endpoint=neofs_env.sn_rpc,
            shell=neofs_env.shell,
        )
        assert get_file_hash(file_path) == get_file_hash(got)

    with allure.step("Exactly 1 copy exists on a RU node"):
        nodes_with_object = get_nodes_with_object(
            cid,
            oid,
            shell=neofs_env.shell,
            nodes=neofs_env.storage_nodes,
            neofs_env=neofs_env,
        )
        assert len(nodes_with_object) == 1, (
            f"Expected exactly 1 initial copy (replica_limits=[1]), "
            f"but found {len(nodes_with_object)} on {nodes_with_object}"
        )
        assert nodes_with_object[0] in ru_nodes, (
            f"The copy must be on a RU node ({ru_nodes}), but it is on {nodes_with_object[0]}"
        )

    with allure.step("No copy on non-RU nodes"):
        non_ru_copies = get_nodes_with_object(
            cid,
            oid,
            shell=neofs_env.shell,
            nodes=non_ru_nodes,
            neofs_env=neofs_env,
        )
        assert len(non_ru_copies) == 0, (
            f"Non-RU nodes are outside the single vector and must have no copy, but found copies on {non_ru_copies}"
        )


@_slow_policer
@allure.title("Initial placement: prefer_local prioritises the receiver's zone")
def test_initial_placement_prefer_local(
    default_wallet: NodeWallet,
    neofs_env: NeoFSEnv,
):
    ru_nodes = neofs_env.storage_nodes[:2]
    non_ru_nodes = neofs_env.storage_nodes[2:]

    with allure.step("Create container"):
        cid = create_container(
            wallet=default_wallet.path,
            rule=_multi_vector_policy(
                initial={"maxReplicas": 1, "preferLocal": True},
            ),
            shell=neofs_env.shell,
            endpoint=neofs_env.sn_rpc,
            basic_acl=PUBLIC_ACL,
        )

    file_path = generate_file(neofs_env.get_object_size("simple_object_size"))

    with allure.step("PUT object to a RU node"):
        oid = put_object(
            wallet=default_wallet.path,
            path=file_path,
            cid=cid,
            shell=neofs_env.shell,
            endpoint=ru_nodes[0].endpoint,
        )

    with allure.step("Exactly 1 copy exists in the RU zone"):
        nodes_with_object = get_nodes_with_object(
            cid,
            oid,
            shell=neofs_env.shell,
            nodes=neofs_env.storage_nodes,
            neofs_env=neofs_env,
        )
        assert len(nodes_with_object) == 1, (
            f"Expected exactly 1 initial copy (max_replicas=1), "
            f"but found {len(nodes_with_object)} on {nodes_with_object}"
        )
        assert nodes_with_object[0] in ru_nodes, (
            f"prefer_local=true: the RU vector should have been processed first "
            f"(receiver is a RU node), so the initial copy must be on a RU node "
            f"({ru_nodes}), but it is on {nodes_with_object[0]}"
        )

    with allure.step("No copy on non-RU nodes"):
        non_ru_copies = get_nodes_with_object(
            cid,
            oid,
            shell=neofs_env.shell,
            nodes=non_ru_nodes,
            neofs_env=neofs_env,
        )
        assert len(non_ru_copies) == 0, f"Non-RU nodes should have no copy yet, but found copies on {non_ru_copies}"


@_slow_policer
@allure.title("Initial placement: replica_limits=[1, 0] skips non-RU vector initially")
def test_initial_placement_multi_vector_replica_limits(
    default_wallet: NodeWallet,
    neofs_env: NeoFSEnv,
):
    ru_nodes = neofs_env.storage_nodes[:2]
    non_ru_nodes = neofs_env.storage_nodes[2:]

    with allure.step("Create container"):
        cid = create_container(
            wallet=default_wallet.path,
            rule=_multi_vector_policy(initial={"replicaLimits": [1, 0]}),
            shell=neofs_env.shell,
            endpoint=neofs_env.sn_rpc,
            basic_acl=PUBLIC_ACL,
        )

    file_path = generate_file(neofs_env.get_object_size("simple_object_size"))

    with allure.step("PUT object"):
        oid = put_object_to_random_node(
            wallet=default_wallet.path,
            path=file_path,
            cid=cid,
            shell=neofs_env.shell,
            neofs_env=neofs_env,
        )

    with allure.step("Exactly 1 copy exists on a RU node"):
        nodes_with_object = get_nodes_with_object(
            cid,
            oid,
            shell=neofs_env.shell,
            nodes=neofs_env.storage_nodes,
            neofs_env=neofs_env,
        )
        assert len(nodes_with_object) == 1, (
            f"Expected exactly 1 initial copy (RU vector only, non-RU skipped), "
            f"but found {len(nodes_with_object)} copies on {nodes_with_object}"
        )
        assert nodes_with_object[0] in ru_nodes, (
            f"The initial copy should be on a RU node ({ru_nodes}), but it is on {nodes_with_object[0]}"
        )

    with allure.step("No copy on non-RU nodes"):
        non_ru_copies = get_nodes_with_object(
            cid,
            oid,
            shell=neofs_env.shell,
            nodes=non_ru_nodes,
            neofs_env=neofs_env,
        )
        assert len(non_ru_copies) == 0, (
            f"Non-RU nodes should have no copy yet (skipped by replica_limits=[1, 0]), "
            f"but found copies on {non_ru_copies}"
        )


@_slow_policer
@allure.title("Initial placement: PUT succeeds with fewer nodes than main REP requires")
def test_initial_placement_with_excluded_nodes(
    default_wallet: NodeWallet,
    neofs_env: NeoFSEnv,
):
    alive_node = neofs_env.storage_nodes[0]
    nodes_to_exclude = neofs_env.storage_nodes[1:3]

    with allure.step("Create container"):
        cid = create_container(
            wallet=default_wallet.path,
            rule=_policy(replicas=3, cbf=1, initial={"maxReplicas": 1}),
            shell=neofs_env.shell,
            endpoint=neofs_env.sn_rpc,
            basic_acl=PUBLIC_ACL,
        )

    with allure.step("Exclude 2 nodes from the netmap"):
        for node in nodes_to_exclude:
            exclude_node_from_network_map(
                node,
                alive_node,
                shell=neofs_env.shell,
                neofs_env=neofs_env,
            )

    file_path = generate_file(neofs_env.get_object_size("simple_object_size"))

    try:
        with allure.step("PUT object to an online node"):
            oid = put_object(
                wallet=default_wallet.path,
                path=file_path,
                cid=cid,
                shell=neofs_env.shell,
                endpoint=alive_node.endpoint,
            )

        with allure.step("Object is immediately retrievable"):
            got = get_object(
                wallet=default_wallet.path,
                cid=cid,
                oid=oid,
                endpoint=alive_node.endpoint,
                shell=neofs_env.shell,
            )
            assert get_file_hash(file_path) == get_file_hash(got)

        with allure.step("Exactly 1 copy exists"):
            nodes_with_object = get_nodes_with_object(
                cid,
                oid,
                shell=neofs_env.shell,
                nodes=neofs_env.storage_nodes,
                neofs_env=neofs_env,
            )
            assert len(nodes_with_object) == 1, (
                f"Expected exactly 1 initial copy (max_replicas=1, 2 netmap nodes), "
                f"but found {len(nodes_with_object)} on {nodes_with_object}"
            )

    finally:
        with allure.step("Restore excluded nodes to the netmap"):
            for node in nodes_to_exclude:
                include_node_to_network_map(
                    node,
                    alive_node,
                    shell=neofs_env.shell,
                    neofs_env=neofs_env,
                )


@allure.title("Initial placement + policer: max_replicas=1 then full REP 2")
def test_initial_placement_max_replicas_full_replication(
    default_wallet: NodeWallet,
    neofs_env: NeoFSEnv,
):
    with allure.step("Create container"):
        cid = create_container(
            wallet=default_wallet.path,
            rule=_policy(replicas=2, cbf=2, initial={"maxReplicas": 1}),
            shell=neofs_env.shell,
            endpoint=neofs_env.sn_rpc,
            basic_acl=PUBLIC_ACL,
        )

    file_path = generate_file(neofs_env.get_object_size("simple_object_size"))

    with allure.step("PUT object"):
        oid = put_object_to_random_node(
            wallet=default_wallet.path,
            path=file_path,
            cid=cid,
            shell=neofs_env.shell,
            neofs_env=neofs_env,
        )

    with allure.step("Object is immediately retrievable"):
        got = get_object(
            wallet=default_wallet.path,
            cid=cid,
            oid=oid,
            endpoint=neofs_env.sn_rpc,
            shell=neofs_env.shell,
        )
        assert get_file_hash(file_path) == get_file_hash(got)

    with allure.step("Policer replicates object to full REP 2"):
        wait_object_replication(
            cid,
            oid,
            expected_copies=2,
            shell=neofs_env.shell,
            nodes=neofs_env.storage_nodes,
            neofs_env=neofs_env,
        )


@allure.title("Initial placement + policer: replica_limits=[1] then full REP 2")
def test_initial_placement_replica_limits_full_replication(
    default_wallet: NodeWallet,
    neofs_env: NeoFSEnv,
):
    with allure.step("Create container"):
        cid = create_container(
            wallet=default_wallet.path,
            rule=_policy(replicas=2, cbf=2, initial={"replicaLimits": [1]}),
            shell=neofs_env.shell,
            endpoint=neofs_env.sn_rpc,
            basic_acl=PUBLIC_ACL,
        )

    file_path = generate_file(neofs_env.get_object_size("simple_object_size"))

    with allure.step("PUT object"):
        oid = put_object_to_random_node(
            wallet=default_wallet.path,
            path=file_path,
            cid=cid,
            shell=neofs_env.shell,
            neofs_env=neofs_env,
        )

    with allure.step("Object is immediately retrievable"):
        got = get_object(
            wallet=default_wallet.path,
            cid=cid,
            oid=oid,
            endpoint=neofs_env.sn_rpc,
            shell=neofs_env.shell,
        )
        assert get_file_hash(file_path) == get_file_hash(got)

    with allure.step("Policer replicates object to full REP 2"):
        wait_object_replication(
            cid,
            oid,
            expected_copies=2,
            shell=neofs_env.shell,
            nodes=neofs_env.storage_nodes,
            neofs_env=neofs_env,
        )


@allure.title("Invalid initial policy: replica_limits identical to main is rejected")
def test_invalid_initial_policy_identical_to_main(
    default_wallet: NodeWallet,
    neofs_env: NeoFSEnv,
):
    bad_policy = _policy(replicas=2, cbf=2, initial={"replicaLimits": [2]})

    with allure.step("Container creation must be rejected"):
        with pytest.raises(RuntimeError):
            create_container(
                wallet=default_wallet.path,
                rule=bad_policy,
                shell=neofs_env.shell,
                endpoint=neofs_env.sn_rpc,
                await_mode=False,
                wait_for_creation=True,
            )


@allure.title("Invalid initial policy: replica_limits exceeding main REP is rejected")
def test_invalid_initial_policy_replica_limits_overflow(
    default_wallet: NodeWallet,
    neofs_env: NeoFSEnv,
):
    bad_policy = _policy(replicas=2, cbf=2, initial={"replicaLimits": [3]})

    with allure.step("Container creation must be rejected"):
        with pytest.raises(RuntimeError):
            create_container(
                wallet=default_wallet.path,
                rule=bad_policy,
                shell=neofs_env.shell,
                endpoint=neofs_env.sn_rpc,
                await_mode=False,
                wait_for_creation=True,
            )
