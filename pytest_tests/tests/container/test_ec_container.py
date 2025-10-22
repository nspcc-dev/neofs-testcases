import re

import allure
import neofs_env.neofs_epoch as neofs_epoch
import pytest
from helpers.container import (
    create_container,
    delete_container,
    list_containers,
    wait_for_container_deletion,
)
from helpers.file_helper import generate_file
from helpers.neofs_verbs import (
    get_object,
    put_object_to_random_node,
    search_objectv2,
)
from neofs_testlib.env.env import NeoFSEnv, NodeWallet


def parse_ec_nodes_count(output: str) -> int:
    """
    Parse the EC descriptor output and count the number of nodes.

    Args:
        output: The output from neofs-cli object nodes command

    Returns:
        int: Number of nodes found in the output
    """
    node_pattern = r"Node \d+:"
    nodes = re.findall(node_pattern, output)
    return len(nodes)


@pytest.mark.sanity
@pytest.mark.parametrize("neofs_env", [{"allow_ec": True}], ids=["allow_ec=True"], indirect=True)
@pytest.mark.parametrize(
    "data_shards,parity_shards",
    [
        (3, 1),
        (2, 1),
        (1, 1),
        (1, 2),
        (2, 2),
        (1, 3),
    ],
    ids=[
        "EC_3/1",
        "EC_2/1",
        "EC_1/1",
        "EC_1/2",
        "EC_2/2",
        "EC_1/3",
    ],
)
@pytest.mark.simple
def test_ec_container_sanity(default_wallet: NodeWallet, neofs_env: NeoFSEnv, data_shards: int, parity_shards: int):
    wallet = default_wallet

    placement_rule = f"EC {data_shards}/{parity_shards} CBF 1"
    cid = create_container(
        wallet.path,
        rule=placement_rule,
        name="ec-container",
        shell=neofs_env.shell,
        endpoint=neofs_env.sn_rpc,
    )

    containers = list_containers(wallet.path, shell=neofs_env.shell, endpoint=neofs_env.sn_rpc)
    assert cid in containers, f"Expected container {cid} in containers: {containers}"

    source_file_path = generate_file(neofs_env.get_object_size("simple_object_size"))

    oid = put_object_to_random_node(wallet.path, source_file_path, cid, shell=neofs_env.shell, neofs_env=neofs_env)

    get_object(
        default_wallet.path,
        cid,
        oid,
        neofs_env.shell,
        neofs_env.sn_rpc,
    )

    result = (
        neofs_env.neofs_cli(neofs_env.storage_nodes[0].cli_config)
        .object.nodes(rpc_endpoint=neofs_env.storage_nodes[0].endpoint, cid=cid, oid=oid)
        .stdout
    )
    assert f"EC {data_shards}/{parity_shards}" in result, (
        f"Expected placement rule {placement_rule} in nodes output: {result}"
    )

    expected_nodes = data_shards + parity_shards
    actual_nodes = parse_ec_nodes_count(result)
    assert actual_nodes == expected_nodes, (
        f"Expected {expected_nodes} nodes (data: {data_shards}, parity: {parity_shards}), but found {actual_nodes} nodes in output: {result}"
    )

    found_objects, _ = search_objectv2(
        rpc_endpoint=neofs_env.sn_rpc, wallet=default_wallet.path, cid=cid, shell=neofs_env.shell
    )

    assert len(found_objects) == expected_nodes + 1, (
        f"Invalid number of found objects in EC container, expected {expected_nodes + 1}, found {len(found_objects)}"
    )

    with allure.step("Delete container and check it was deleted"):
        delete_container(wallet.path, cid, shell=neofs_env.shell, endpoint=neofs_env.sn_rpc)
        current_epoch = neofs_epoch.get_epoch(neofs_env)
        neofs_epoch.tick_epoch(neofs_env)
        neofs_epoch.wait_for_epochs_align(neofs_env, current_epoch)
        wait_for_container_deletion(wallet.path, cid, shell=neofs_env.shell, endpoint=neofs_env.sn_rpc)
