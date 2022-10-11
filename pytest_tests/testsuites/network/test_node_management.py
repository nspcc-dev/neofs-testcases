import logging
from random import choice
from time import sleep

import allure
import pytest
from common import (
    COMPLEX_OBJ_SIZE,
    MAINNET_BLOCK_TIME,
    MORPH_BLOCK_TIME,
    NEOFS_CONTRACT_CACHE_TIMEOUT,
    NEOFS_NETMAP_DICT,
    STORAGE_RPC_ENDPOINT_1,
    STORAGE_WALLET_PASS,
)
from data_formatters import get_wallet_public_key
from epoch import tick_epoch
from file_helper import generate_file
from grpc_responses import OBJECT_NOT_FOUND, error_matches_status
from python_keywords.container import create_container, get_container
from python_keywords.failover_utils import wait_object_replication_on_nodes
from python_keywords.neofs_verbs import delete_object, get_object, head_object, put_object
from python_keywords.node_management import (
    check_node_in_map,
    delete_node_data,
    drop_object,
    exclude_node_from_network_map,
    get_locode,
    get_netmap_snapshot,
    include_node_to_network_map,
    node_healthcheck,
    node_set_status,
    node_shard_list,
    node_shard_set_mode,
    start_nodes,
    stop_nodes,
)
from service_helper import get_storage_service_helper
from storage_policy import get_nodes_with_object, get_simple_object_copies
from utility import parse_time, placement_policy_from_container, wait_for_gc_pass_on_storage_nodes
from wellknown_acl import PUBLIC_ACL

logger = logging.getLogger("NeoLogger")
check_nodes = []


@pytest.fixture
@allure.title("Create container and pick the node with data")
def create_container_and_pick_node(prepare_wallet_and_deposit):
    wallet = prepare_wallet_and_deposit
    file_path = generate_file()
    placement_rule = "REP 1 IN X CBF 1 SELECT 1 FROM * AS X"

    cid = create_container(wallet, rule=placement_rule, basic_acl=PUBLIC_ACL)
    oid = put_object(wallet, file_path, cid)

    nodes = get_nodes_with_object(wallet, cid, oid)
    assert len(nodes) == 1
    node = nodes[0]

    node_name = choice(
        [node_name for node_name, params in NEOFS_NETMAP_DICT.items() if params.get("rpc") == node]
    )

    yield cid, node_name

    shards = node_shard_list(node_name)
    assert shards

    for shard in shards:
        node_shard_set_mode(node_name, shard, "read-write")

    node_shard_list(node_name)


@pytest.fixture
def after_run_start_all_nodes():
    yield
    try:
        start_nodes(list(NEOFS_NETMAP_DICT.keys()))
    except Exception as err:
        logger.error(f"Node start fails with error:\n{err}")


@pytest.fixture
def return_nodes_after_test_run():
    yield
    return_nodes()


@allure.step("Return node to cluster")
def return_nodes(alive_node: str = None):
    helper = get_storage_service_helper()
    for node in list(check_nodes):
        with allure.step(f"Start node {node}"):
            helper.start_node(node)
        with allure.step(f"Waiting status ready for node {node}"):
            wait_for_node_to_be_ready(node)

        # We need to wait for node to establish notifications from morph-chain
        # Otherwise it will hang up when we will try to set status
        sleep(parse_time(MORPH_BLOCK_TIME))

        with allure.step(f"Move node {node} to online state"):
            node_set_status(node, status="online", retries=2)

        check_nodes.remove(node)
        sleep(parse_time(MORPH_BLOCK_TIME))
        for __attempt in range(3):
            try:
                tick_epoch()
                break
            except RuntimeError:
                sleep(3)

        check_node_in_map(node, alive_node)


@allure.title("Add one node to cluster")
@pytest.mark.add_nodes
@pytest.mark.node_mgmt
def test_add_nodes(prepare_tmp_dir, prepare_wallet_and_deposit, return_nodes_after_test_run):
    wallet = prepare_wallet_and_deposit
    placement_rule_3 = "REP 3 IN X CBF 1 SELECT 3 FROM * AS X"
    placement_rule_4 = "REP 4 IN X CBF 1 SELECT 4 FROM * AS X"
    source_file_path = generate_file()

    additional_node = choice(
        [
            node
            for node, node_config in NEOFS_NETMAP_DICT.items()
            if node_config.get("rpc") != STORAGE_RPC_ENDPOINT_1
        ]
    )
    alive_node = choice([node for node in NEOFS_NETMAP_DICT if node != additional_node])

    check_node_in_map(additional_node, alive_node)

    # Add node to recovery list before messing with it
    check_nodes.append(additional_node)
    exclude_node_from_network_map(additional_node, alive_node)
    delete_node_data(additional_node)

    cid = create_container(wallet, rule=placement_rule_3, basic_acl=PUBLIC_ACL)
    oid = put_object(
        wallet, source_file_path, cid, endpoint=NEOFS_NETMAP_DICT[alive_node].get("rpc")
    )
    wait_object_replication_on_nodes(wallet, cid, oid, 3)

    return_nodes(alive_node)

    with allure.step("Check data could be replicated to new node"):
        random_node = choice(
            [node for node in NEOFS_NETMAP_DICT if node not in (additional_node, alive_node)]
        )
        exclude_node_from_network_map(random_node, alive_node)

        wait_object_replication_on_nodes(wallet, cid, oid, 3, excluded_nodes=[random_node])
        include_node_to_network_map(random_node, alive_node)
        wait_object_replication_on_nodes(wallet, cid, oid, 3)

    with allure.step("Check container could be created with new node"):
        cid = create_container(wallet, rule=placement_rule_4, basic_acl=PUBLIC_ACL)
        oid = put_object(
            wallet, source_file_path, cid, endpoint=NEOFS_NETMAP_DICT[alive_node].get("rpc")
        )
        wait_object_replication_on_nodes(wallet, cid, oid, 4)


@allure.title("Control Operations with storage nodes")
@pytest.mark.node_mgmt
def test_nodes_management(prepare_tmp_dir):
    """
    This test checks base control operations with storage nodes (healthcheck, netmap-snapshot, set-status).
    """
    random_node = choice(list(NEOFS_NETMAP_DICT))
    alive_node = choice([node for node in NEOFS_NETMAP_DICT if node != random_node])

    # Calculate public key that identifies node in netmap
    random_node_wallet_path = NEOFS_NETMAP_DICT[random_node]["wallet_path"]
    random_node_netmap_key = get_wallet_public_key(random_node_wallet_path, STORAGE_WALLET_PASS)

    with allure.step("Check node {random_node} is in netmap"):
        snapshot = get_netmap_snapshot(node_name=alive_node)
        assert random_node_netmap_key in snapshot, f"Expected node {random_node} in netmap"

    with allure.step("Run health check for all storage nodes"):
        for node_name in NEOFS_NETMAP_DICT.keys():
            health_check = node_healthcheck(node_name)
            assert health_check.health_status == "READY" and health_check.network_status == "ONLINE"

    with allure.step(f"Move node {random_node} to offline state"):
        node_set_status(random_node, status="offline")

    sleep(parse_time(MORPH_BLOCK_TIME))
    tick_epoch()

    with allure.step(f"Check node {random_node} went to offline"):
        health_check = node_healthcheck(random_node)
        assert health_check.health_status == "READY" and health_check.network_status == "OFFLINE"
        snapshot = get_netmap_snapshot(node_name=alive_node)
        assert random_node_netmap_key not in snapshot, f"Expected node {random_node} not in netmap"

    with allure.step(f"Check node {random_node} went to online"):
        node_set_status(random_node, status="online")

    sleep(parse_time(MORPH_BLOCK_TIME))
    tick_epoch()

    with allure.step(f"Check node {random_node} went to online"):
        health_check = node_healthcheck(random_node)
        assert health_check.health_status == "READY" and health_check.network_status == "ONLINE"
        snapshot = get_netmap_snapshot(node_name=alive_node)
        assert random_node_netmap_key in snapshot, f"Expected node {random_node} in netmap"


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
@pytest.mark.node_mgmt
@allure.title("Test object copies based on placement policy")
def test_placement_policy(prepare_wallet_and_deposit, placement_rule, expected_copies):
    """
    This test checks object's copies based on container's placement policy.
    """
    wallet = prepare_wallet_and_deposit
    file_path = generate_file()
    validate_object_copies(wallet, placement_rule, file_path, expected_copies)


@pytest.mark.parametrize(
    "placement_rule,expected_copies,nodes",
    [
        ("REP 4 IN X CBF 1 SELECT 4 FROM * AS X", 4, ["s01", "s02", "s03", "s04"]),
        (
            "REP 1 IN LOC_PLACE CBF 1 SELECT 1 FROM LOC_SW AS LOC_PLACE FILTER Country EQ Sweden AS LOC_SW",
            1,
            ["s03"],
        ),
        ("REP 1 CBF 1 SELECT 1 FROM LOC_SPB FILTER 'UN-LOCODE' EQ 'RU LED' AS LOC_SPB", 1, ["s02"]),
        (
            "REP 1 IN LOC_SPB_PLACE REP 1 IN LOC_MSK_PLACE CBF 1 SELECT 1 FROM LOC_SPB AS LOC_SPB_PLACE "
            "SELECT 1 FROM LOC_MSK AS LOC_MSK_PLACE "
            "FILTER 'UN-LOCODE' EQ 'RU LED' AS LOC_SPB FILTER 'UN-LOCODE' EQ 'RU MOW' AS LOC_MSK",
            2,
            ["s01", "s02"],
        ),
        (
            "REP 4 CBF 1 SELECT 4 FROM LOC_EU FILTER Continent EQ Europe AS LOC_EU",
            4,
            ["s01", "s02", "s03", "s04"],
        ),
        (
            "REP 1 CBF 1 SELECT 1 FROM LOC_SPB "
            "FILTER 'UN-LOCODE' NE 'RU MOW' AND 'UN-LOCODE' NE 'SE STO' AND 'UN-LOCODE' NE 'FI HEL' AS LOC_SPB",
            1,
            ["s02"],
        ),
        (
            "REP 2 CBF 1 SELECT 2 FROM LOC_RU FILTER SubDivCode NE 'AB' AND SubDivCode NE '18' AS LOC_RU",
            2,
            ["s01", "s02"],
        ),
        (
            "REP 2 CBF 1 SELECT 2 FROM LOC_RU FILTER Country EQ 'Russia' AS LOC_RU",
            2,
            ["s01", "s02"],
        ),
        (
            "REP 2 CBF 1 SELECT 2 FROM LOC_EU FILTER Country NE 'Russia' AS LOC_EU",
            2,
            ["s03", "s04"],
        ),
    ],
)
@pytest.mark.node_mgmt
@allure.title("Test object copies and storage nodes based on placement policy")
def test_placement_policy_with_nodes(
    prepare_wallet_and_deposit, placement_rule, expected_copies, nodes
):
    """
    Based on container's placement policy check that storage nodes are piked correctly and object has
    correct copies amount.
    """
    wallet = prepare_wallet_and_deposit
    file_path = generate_file()
    cid, oid, found_nodes = validate_object_copies(
        wallet, placement_rule, file_path, expected_copies
    )
    expected_nodes = [NEOFS_NETMAP_DICT[node_name].get("rpc") for node_name in nodes]
    assert set(found_nodes) == set(
        expected_nodes
    ), f"Expected nodes {expected_nodes}, got {found_nodes}"


@pytest.mark.parametrize(
    "placement_rule,expected_copies",
    [
        ("REP 2 IN X CBF 2 SELECT 6 FROM * AS X", 2),
    ],
)
@pytest.mark.node_mgmt
@allure.title("Negative cases for placement policy")
def test_placement_policy_negative(prepare_wallet_and_deposit, placement_rule, expected_copies):
    """
    Negative test for placement policy.
    """
    wallet = prepare_wallet_and_deposit
    file_path = generate_file()
    with pytest.raises(RuntimeError, match=".*not enough nodes to SELECT from.*"):
        validate_object_copies(wallet, placement_rule, file_path, expected_copies)


@pytest.mark.skip(reason="We cover this scenario for Sbercloud in failover tests")
@pytest.mark.node_mgmt
@allure.title("NeoFS object replication on node failover")
def test_replication(prepare_wallet_and_deposit, after_run_start_all_nodes):
    """
    Test checks object replication on storage not failover and come back.
    """
    wallet = prepare_wallet_and_deposit
    file_path = generate_file()
    expected_nodes_count = 2

    cid = create_container(wallet, basic_acl=PUBLIC_ACL)
    oid = put_object(wallet, file_path, cid)

    nodes = get_nodes_with_object(wallet, cid, oid)
    assert (
        len(nodes) == expected_nodes_count
    ), f"Expected {expected_nodes_count} copies, got {len(nodes)}"

    node_names = [name for name, config in NEOFS_NETMAP_DICT.items() if config.get("rpc") in nodes]
    stopped_nodes = stop_nodes(1, node_names)

    wait_for_expected_object_copies(wallet, cid, oid)

    start_nodes(stopped_nodes)
    tick_epoch()

    for node_name in node_names:
        wait_for_node_go_online(node_name)

    wait_for_expected_object_copies(wallet, cid, oid)


@pytest.mark.node_mgmt
@allure.title("NeoFS object could be dropped using control command")
def test_drop_object(prepare_wallet_and_deposit):
    """
    Test checks object could be dropped using `neofs-cli control drop-objects` command.
    """
    wallet = prepare_wallet_and_deposit
    file_path_simple, file_path_complex = generate_file(), generate_file(COMPLEX_OBJ_SIZE)

    locode = get_locode()
    rule = f"REP 1 CBF 1 SELECT 1 FROM * FILTER 'UN-LOCODE' EQ '{locode}' AS LOC"
    cid = create_container(wallet, rule=rule)
    oid_simple = put_object(wallet, file_path_simple, cid)
    oid_complex = put_object(wallet, file_path_complex, cid)

    for oid in (oid_simple, oid_complex):
        get_object(wallet, cid, oid)
        head_object(wallet, cid, oid)

    nodes = get_nodes_with_object(wallet, cid, oid_simple)
    node_name = choice(
        [name for name, config in NEOFS_NETMAP_DICT.items() if config.get("rpc") in nodes]
    )

    for oid in (oid_simple, oid_complex):
        with allure.step(f"Drop object {oid}"):
            get_object(wallet, cid, oid)
            head_object(wallet, cid, oid)
            drop_object(node_name, cid, oid)
            wait_for_obj_dropped(wallet, cid, oid, get_object)
            wait_for_obj_dropped(wallet, cid, oid, head_object)


@pytest.mark.node_mgmt
@pytest.mark.skip(reason="Need to clarify scenario")
@allure.title("Control Operations with storage nodes")
def test_shards(prepare_wallet_and_deposit, create_container_and_pick_node):
    wallet = prepare_wallet_and_deposit
    file_path = generate_file()

    cid, node_name = create_container_and_pick_node
    original_oid = put_object(wallet, file_path, cid)

    # for mode in ('read-only', 'degraded'):
    for mode in ("degraded",):
        shards = node_shard_list(node_name)
        assert shards

        for shard in shards:
            node_shard_set_mode(node_name, shard, mode)

        shards = node_shard_list(node_name)
        assert shards

        with pytest.raises(RuntimeError):
            put_object(wallet, file_path, cid)

        with pytest.raises(RuntimeError):
            delete_object(wallet, cid, original_oid)

        # head_object(wallet, cid, original_oid)
        get_object(wallet, cid, original_oid)

        for shard in shards:
            node_shard_set_mode(node_name, shard, "read-write")

        shards = node_shard_list(node_name)
        assert shards

        oid = put_object(wallet, file_path, cid)
        delete_object(wallet, cid, oid)


@allure.step("Validate object has {expected_copies} copies")
def validate_object_copies(wallet: str, placement_rule: str, file_path: str, expected_copies: int):
    cid = create_container(wallet, rule=placement_rule, basic_acl=PUBLIC_ACL)
    got_policy = placement_policy_from_container(get_container(wallet, cid, json_mode=False))
    assert got_policy == placement_rule.replace(
        "'", ""
    ), f"Expected \n{placement_rule} and got policy \n{got_policy} are the same"
    oid = put_object(wallet, file_path, cid)
    nodes = get_nodes_with_object(wallet, cid, oid)
    assert len(nodes) == expected_copies, f"Expected {expected_copies} copies, got {len(nodes)}"
    return cid, oid, nodes


@allure.step("Wait for node {node_name} goes online")
def wait_for_node_go_online(node_name: str) -> None:
    timeout, attempts = 5, 20
    for _ in range(attempts):
        try:
            health_check = node_healthcheck(node_name)
            assert health_check.health_status == "READY" and health_check.network_status == "ONLINE"
            return
        except Exception as err:
            logger.warning(f"Node {node_name} is not online:\n{err}")
        sleep(timeout)
    raise AssertionError(
        f"Node {node_name} hasn't gone to the READY and ONLINE state after {timeout * attempts} second"
    )


@allure.step("Wait for node {node_name} is ready")
def wait_for_node_to_be_ready(node_name: str) -> None:
    timeout, attempts = 30, 6
    for _ in range(attempts):
        try:
            health_check = node_healthcheck(node_name)
            if health_check.health_status == "READY":
                return
        except Exception as err:
            logger.warning(f"Node {node_name} is not ready:\n{err}")
        sleep(timeout)
    raise AssertionError(
        f"Node {node_name} hasn't gone to the READY state after {timeout * attempts} seconds"
    )


@allure.step("Wait for {expected_copies} object copies in the wallet")
def wait_for_expected_object_copies(
    wallet: str, cid: str, oid: str, expected_copies: int = 2
) -> None:
    for i in range(2):
        copies = get_simple_object_copies(wallet, cid, oid)
        if copies == expected_copies:
            break
        tick_epoch()
        sleep(parse_time(NEOFS_CONTRACT_CACHE_TIMEOUT))
    else:
        raise AssertionError(f"There are no {expected_copies} copies during time")


@allure.step("Wait for object to be dropped")
def wait_for_obj_dropped(wallet: str, cid: str, oid: str, checker) -> None:
    for _ in range(3):
        try:
            checker(wallet, cid, oid)
            wait_for_gc_pass_on_storage_nodes()
        except Exception as err:
            if error_matches_status(err, OBJECT_NOT_FOUND):
                return
            raise AssertionError(f'Expected "{OBJECT_NOT_FOUND}" error, got\n{err}')

    raise AssertionError(f"Object {oid} was not dropped from node")
