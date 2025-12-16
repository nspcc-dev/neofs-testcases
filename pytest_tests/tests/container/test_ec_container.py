import random
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import allure
import neofs_env.neofs_epoch as neofs_epoch
import pytest
from helpers.complex_object_actions import get_nodes_with_object
from helpers.container import (
    create_container,
    delete_container,
    generate_ranges_for_ec_object,
    list_containers,
    parse_container_nodes_output,
    wait_for_container_deletion,
)
from helpers.file_helper import generate_file, get_file_content, get_file_hash
from helpers.neofs_verbs import (
    delete_object,
    get_object,
    get_range,
    get_range_hash,
    head_object,
    put_object,
    search_objectv2,
)
from helpers.node_management import drop_object, wait_all_storage_nodes_returned
from neofs_testlib.env.env import NeoFSEnv, NodeWallet
from tenacity import retry, stop_after_attempt, wait_fixed


def parse_ec_nodes_count(output: str) -> list:
    """
    Parse the EC descriptor output and extract node ports.

    Args:
        output: The output from neofs-cli object nodes command

    Returns:
        list: List of dictionaries with port information for each node
    """
    node_pattern = r"Node \d+: [a-f0-9]+ ONLINE /dns4/localhost/tcp/(\d+)"
    ports = re.findall(node_pattern, output)
    return [{"port": int(port)} for port in ports]


@retry(wait=wait_fixed(1), stop=stop_after_attempt(300), reraise=True)
def object_is_accessible(wallet_path: str, cid: str, oid: str, neofs_env: NeoFSEnv, endpoint: str):
    head_object(
        wallet_path,
        cid,
        oid,
        shell=neofs_env.shell,
        endpoint=endpoint,
    )


@retry(wait=wait_fixed(1), stop=stop_after_attempt(300), reraise=True)
def object_is_not_accessible(wallet_path: str, cid: str, oid: str, neofs_env: NeoFSEnv, endpoint: str):
    with pytest.raises(Exception):
        head_object(
            wallet_path,
            cid,
            oid,
            shell=neofs_env.shell,
            endpoint=endpoint,
        )


@pytest.mark.sanity
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
@pytest.mark.parametrize(
    "object_size",
    [
        pytest.param("simple_object_size", id="simple object", marks=pytest.mark.simple),
        pytest.param("complex_object_size", id="complex object", marks=pytest.mark.complex),
    ],
)
@pytest.mark.simple
def test_ec_container_sanity(
    default_wallet: NodeWallet, neofs_env: NeoFSEnv, data_shards: int, parity_shards: int, object_size: str
):
    wallet = default_wallet

    with allure.step("Create EC container"):
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

    with allure.step("Put object to EC container"):
        source_file_size = neofs_env.get_object_size(object_size)
        source_file_path = generate_file(source_file_size)

        main_oid = put_object(
            wallet.path,
            source_file_path,
            cid,
            neofs_env.shell,
            neofs_env.sn_rpc,
        )

    with allure.step("Verify object operations from EC container"):
        for sn in neofs_env.storage_nodes:
            get_object(
                default_wallet.path,
                cid,
                main_oid,
                neofs_env.shell,
                sn.endpoint,
            )
            head_object(
                default_wallet.path,
                cid,
                main_oid,
                neofs_env.shell,
                sn.endpoint,
            )

            for range_start, range_len in generate_ranges_for_ec_object(source_file_size):
                if range_len > 0:
                    range_cut = f"{range_start}:{range_len}"
                    _, range_content = get_range(
                        default_wallet.path,
                        cid,
                        main_oid,
                        shell=neofs_env.shell,
                        endpoint=sn.endpoint,
                        range_cut=range_cut,
                    )
                    assert (
                        get_file_content(source_file_path, content_len=range_len, mode="rb", offset=range_start)
                        == range_content
                    ), f"Expected range content to match {range_cut} slice of file payload"

                    range_hash = get_range_hash(
                        default_wallet.path,
                        cid,
                        main_oid,
                        shell=neofs_env.shell,
                        endpoint=sn.endpoint,
                        range_cut=range_cut,
                    )
                    assert get_file_hash(source_file_path, range_len, range_start) == range_hash, (
                        f"Expected range hash to match {range_cut} slice of file payload"
                    )

    with allure.step("Verify object placement in EC container"):
        result = (
            neofs_env.neofs_cli(neofs_env.storage_nodes[0].cli_config)
            .object.nodes(rpc_endpoint=neofs_env.storage_nodes[0].endpoint, cid=cid, oid=main_oid)
            .stdout
        )
        assert f"EC {data_shards}/{parity_shards}" in result, (
            f"Expected placement rule {placement_rule} in nodes output: {result}"
        )

        expected_nodes = data_shards + parity_shards
        actual_nodes = parse_ec_nodes_count(result)
        assert len(actual_nodes) == expected_nodes, (
            f"Expected {expected_nodes} nodes (data: {data_shards}, parity: {parity_shards}), but found {len(actual_nodes)} nodes in output: {result}"
        )

    with allure.step("Search for the object and its parts in EC container"):
        found_objects, _ = search_objectv2(
            rpc_endpoint=neofs_env.sn_rpc, wallet=default_wallet.path, cid=cid, shell=neofs_env.shell
        )

        if object_size == "simple_object_size":
            assert len(found_objects) == expected_nodes + 1, (
                f"Invalid number of found objects in EC container, expected {expected_nodes + 1}, found {len(found_objects)}"
            )

    with allure.step("Head all found objects to verify their existence"):
        for obj in found_objects:
            head_object(
                default_wallet.path,
                cid,
                obj["id"],
                shell=neofs_env.shell,
                endpoint=neofs_env.sn_rpc,
            )

    with allure.step("Stop storage nodes to test data recovery"):
        storage_nodes_hosting_object = []
        for sn in actual_nodes:
            sn_port = str(sn["port"])
            for storage_node in neofs_env.storage_nodes:
                if f":{sn_port}" in storage_node.endpoint and storage_node not in storage_nodes_hosting_object:
                    storage_nodes_hosting_object.append(storage_node)
                    break

        nodes_to_stop = min(parity_shards, len(storage_nodes_hosting_object))
        stopped_nodes = []

        available_nodes = storage_nodes_hosting_object.copy()
        for _ in range(nodes_to_stop):
            if not available_nodes:
                break
            node_to_kill = random.choice(available_nodes)
            available_nodes.remove(node_to_kill)
            node_to_kill.kill()
            stopped_nodes.append(node_to_kill)

        alive_nodes = [sn for sn in neofs_env.storage_nodes if sn not in stopped_nodes]
    try:
        with allure.step("Head the object after stopping nodes to verify data recovery"):
            head_object(
                default_wallet.path,
                cid,
                main_oid,
                shell=neofs_env.shell,
                endpoint=alive_nodes[0].endpoint,
            )
    finally:
        with allure.step("Restart stopped storage nodes"):
            for sn in stopped_nodes:
                sn.start(fresh=False)
            wait_all_storage_nodes_returned(neofs_env)

    with allure.step("Head all found objects to verify their existence"):
        for obj in found_objects:
            head_object(
                default_wallet.path,
                cid,
                obj["id"],
                shell=neofs_env.shell,
                endpoint=neofs_env.sn_rpc,
            )

    with allure.step("Delete object from EC container"):
        delete_object(
            wallet.path,
            cid,
            main_oid,
            neofs_env.shell,
            neofs_env.sn_rpc,
        )
        for sn in neofs_env.storage_nodes:
            with pytest.raises(Exception):
                get_object(
                    default_wallet.path,
                    cid,
                    main_oid,
                    neofs_env.shell,
                    sn.endpoint,
                )

    with allure.step("Delete container and check it was deleted"):
        delete_container(wallet.path, cid, shell=neofs_env.shell, endpoint=neofs_env.sn_rpc)
        current_epoch = neofs_epoch.get_epoch(neofs_env)
        neofs_epoch.tick_epoch(neofs_env)
        neofs_epoch.wait_for_epochs_align(neofs_env, current_epoch)
        wait_for_container_deletion(wallet.path, cid, shell=neofs_env.shell, endpoint=neofs_env.sn_rpc)


@pytest.mark.parametrize(
    "neofs_env",
    [{"replication_cooldown": "1h"}],
    ids=["replication_cooldown=1h"],
    indirect=True,
)
@pytest.mark.parametrize(
    "data_shards,parity_shards,excess_failures",
    [
        (2, 1, 2),
        (3, 1, 2),
        (2, 2, 3),
    ],
    ids=[
        "EC_2/1_fail_2_nodes",
        "EC_3/1_fail_2_nodes",
        "EC_2/2_fail_3_nodes",
    ],
)
def test_ec_beyond_tolerance_failures(
    default_wallet: NodeWallet, neofs_env: NeoFSEnv, data_shards: int, parity_shards: int, excess_failures: int
):
    wallet = default_wallet

    with allure.step("Create EC container"):
        placement_rule = f"EC {data_shards}/{parity_shards} CBF 1"
        cid = create_container(
            wallet.path,
            rule=placement_rule,
            name="ec-excess-failures-container",
            shell=neofs_env.shell,
            endpoint=neofs_env.sn_rpc,
        )

    with allure.step("Put test object"):
        source_file_path = generate_file(neofs_env.get_object_size("simple_object_size"))
        main_oid = put_object(
            wallet.path,
            source_file_path,
            cid,
            neofs_env.shell,
            neofs_env.sn_rpc,
        )

    with allure.step("Get object nodes placement"):
        result = (
            neofs_env.neofs_cli(neofs_env.storage_nodes[0].cli_config)
            .object.nodes(rpc_endpoint=neofs_env.storage_nodes[0].endpoint, cid=cid, oid=main_oid)
            .stdout
        )
        actual_nodes = parse_ec_nodes_count(result)
        storage_nodes_hosting_object = []
        for sn in actual_nodes:
            sn_port = str(sn["port"])
            for storage_node in neofs_env.storage_nodes:
                if f":{sn_port}" in storage_node.endpoint and storage_node not in storage_nodes_hosting_object:
                    storage_nodes_hosting_object.append(storage_node)
                    break

    with allure.step(f"Stop {excess_failures} nodes (exceeding failure tolerance)"):
        stopped_nodes = []
        available_nodes = storage_nodes_hosting_object.copy()

        for _ in range(excess_failures):
            if not available_nodes:
                break
            node_to_kill = random.choice(available_nodes)
            available_nodes.remove(node_to_kill)
            node_to_kill.kill(wait_until_not_ready=False)
            stopped_nodes.append(node_to_kill)

        for node in stopped_nodes:
            node._wait_until_not_ready()

        alive_nodes = [sn for sn in neofs_env.storage_nodes if sn not in stopped_nodes]

    try:
        with allure.step("Verify object is not accessible due to excessive failures"):
            with pytest.raises(Exception):
                get_object(
                    default_wallet.path,
                    cid,
                    main_oid,
                    neofs_env.shell,
                    alive_nodes[0].endpoint if alive_nodes else neofs_env.sn_rpc,
                )

    finally:
        with allure.step("Restart all stopped nodes"):
            for sn in stopped_nodes:
                sn.start(fresh=False)
            wait_all_storage_nodes_returned(neofs_env)

    with allure.step("Verify object is accessible again after node recovery"):
        get_object(
            default_wallet.path,
            cid,
            main_oid,
            neofs_env.shell,
            neofs_env.sn_rpc,
        )

    with allure.step("Clean up container"):
        delete_container(wallet.path, cid, shell=neofs_env.shell, endpoint=neofs_env.sn_rpc)


@pytest.mark.parametrize(
    "data_shards,parity_shards",
    [
        (3, 1),
    ],
    ids=[
        "EC_3/1",
    ],
)
def test_ec_multiple_objects_concurrent_operations(
    default_wallet: NodeWallet, neofs_env: NeoFSEnv, data_shards: int, parity_shards: int
):
    wallet = default_wallet
    num_objects = 5

    with allure.step("Create EC container"):
        placement_rule = f"EC {data_shards}/{parity_shards} CBF 1"
        cid = create_container(
            wallet.path,
            rule=placement_rule,
            name="ec-multi-objects-container",
            shell=neofs_env.shell,
            endpoint=neofs_env.sn_rpc,
        )

    with allure.step(f"Put {num_objects} objects concurrently"):

        def put_object_task(index):
            source_file_path = generate_file(neofs_env.get_object_size("simple_object_size"))
            return put_object(
                wallet.path,
                source_file_path,
                cid,
                neofs_env.shell,
                neofs_env.sn_rpc,
            )

        object_ids = []
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_to_index = {executor.submit(put_object_task, i): i for i in range(num_objects)}
            for future in as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    oid = future.result()
                    object_ids.append(oid)
                except Exception as exc:
                    pytest.fail(f"Object {index} put failed: {exc}")

    with allure.step("Verify all objects are accessible"):

        def get_object_task(oid):
            return get_object(
                default_wallet.path,
                cid,
                oid,
                neofs_env.shell,
                neofs_env.sn_rpc,
            )

        with ThreadPoolExecutor(max_workers=3) as executor:
            future_to_oid = {executor.submit(get_object_task, oid): oid for oid in object_ids}
            for future in as_completed(future_to_oid):
                oid = future_to_oid[future]
                try:
                    future.result()
                except Exception as exc:
                    pytest.fail(f"Object {oid} get failed: {exc}")

    with allure.step("Stop one node and verify all objects remain accessible"):
        node_to_stop = random.choice(neofs_env.storage_nodes)
        node_to_stop.kill()

        try:
            alive_nodes = [sn for sn in neofs_env.storage_nodes if sn != node_to_stop]
            for oid in object_ids:
                head_object(
                    default_wallet.path,
                    cid,
                    oid,
                    shell=neofs_env.shell,
                    endpoint=alive_nodes[0].endpoint,
                )
        finally:
            node_to_stop.start(fresh=False)
            wait_all_storage_nodes_returned(neofs_env)

    with allure.step("Delete all objects concurrently"):

        def delete_object_task(oid):
            return delete_object(
                wallet.path,
                cid,
                oid,
                neofs_env.shell,
                neofs_env.sn_rpc,
            )

        with ThreadPoolExecutor(max_workers=3) as executor:
            future_to_oid = {executor.submit(delete_object_task, oid): oid for oid in object_ids}
            for future in as_completed(future_to_oid):
                oid = future_to_oid[future]
                try:
                    future.result()
                except Exception as exc:
                    pytest.fail(f"Object {oid} delete failed: {exc}")

    with allure.step("Clean up container"):
        delete_container(wallet.path, cid, shell=neofs_env.shell, endpoint=neofs_env.sn_rpc)


@pytest.mark.parametrize(
    "invalid_rule,expected_error",
    [
        ("EC 0/1", "can't parse placement policy"),
        ("EC 1/0", "can't parse placement policy"),
        ("EC 100/1", "can't parse placement policy"),
    ],
    ids=[
        "zero_data_shards",
        "zero_parity_shards",
        "too_many_shards",
    ],
)
def test_ec_invalid_placement_policies(
    default_wallet: NodeWallet, neofs_env: NeoFSEnv, invalid_rule: str, expected_error: str
):
    wallet = default_wallet

    with allure.step(f"Attempt to create EC container with invalid rule: {invalid_rule}"):
        with pytest.raises(Exception) as exc_info:
            create_container(
                wallet.path,
                rule=invalid_rule,
                name="ec-invalid-policy-container",
                shell=neofs_env.shell,
                endpoint=neofs_env.sn_rpc,
            )

        assert expected_error.lower() in str(exc_info.value).lower(), (
            f"Expected error containing '{expected_error}' but got: {exc_info.value}"
        )


def test_ec_multiple_containers(default_wallet: NodeWallet, neofs_env: NeoFSEnv):
    wallet = default_wallet
    containers = []
    object_mappings = []

    ec_schemes = [
        (3, 1),
        (2, 1),
        (1, 1),
        (1, 2),
        (2, 2),
        (1, 3),
    ]

    try:
        with allure.step("Create multiple EC containers with different schemes"):
            for data_shards, parity_shards in ec_schemes:
                placement_rule = f"EC {data_shards}/{parity_shards} CBF 1"
                cid = create_container(
                    wallet.path,
                    rule=placement_rule,
                    name=f"ec-multi-container-{data_shards}-{parity_shards}",
                    shell=neofs_env.shell,
                    endpoint=neofs_env.sn_rpc,
                )
                containers.append(
                    {
                        "cid": cid,
                        "data_shards": data_shards,
                        "parity_shards": parity_shards,
                        "placement_rule": placement_rule,
                    }
                )

        with allure.step("Put objects in each container"):
            for container_info in containers:
                source_file_size = neofs_env.get_object_size("simple_object_size")
                source_file_path = generate_file(source_file_size)
                oid = put_object(
                    wallet.path,
                    source_file_path,
                    container_info["cid"],
                    neofs_env.shell,
                    neofs_env.sn_rpc,
                )
                object_mappings.append(
                    {
                        "cid": container_info["cid"],
                        "oid": oid,
                        "source_file_path": source_file_path,
                        "source_file_size": source_file_size,
                        "data_shards": container_info["data_shards"],
                        "parity_shards": container_info["parity_shards"],
                        "placement_rule": container_info["placement_rule"],
                    }
                )

        with allure.step("Verify comprehensive object operations for all containers"):
            for mapping in object_mappings:
                cid = mapping["cid"]
                oid = mapping["oid"]
                source_file_path = mapping["source_file_path"]
                source_file_size = mapping["source_file_size"]

                for sn in neofs_env.storage_nodes:
                    get_object(
                        default_wallet.path,
                        cid,
                        oid,
                        neofs_env.shell,
                        sn.endpoint,
                    )

                    head_object(
                        default_wallet.path,
                        cid,
                        oid,
                        neofs_env.shell,
                        sn.endpoint,
                    )

                    for range_start, range_len in generate_ranges_for_ec_object(source_file_size):
                        if range_len > 0:
                            range_cut = f"{range_start}:{range_len}"
                            _, range_content = get_range(
                                default_wallet.path,
                                cid,
                                oid,
                                shell=neofs_env.shell,
                                endpoint=sn.endpoint,
                                range_cut=range_cut,
                            )
                            assert (
                                get_file_content(source_file_path, content_len=range_len, mode="rb", offset=range_start)
                                == range_content
                            ), f"Expected range content to match {range_cut} slice of file payload for container {cid}"

        with allure.step("Verify object placement for all containers"):
            for mapping in object_mappings:
                cid = mapping["cid"]
                oid = mapping["oid"]
                data_shards = mapping["data_shards"]
                parity_shards = mapping["parity_shards"]
                placement_rule = mapping["placement_rule"]

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
                assert len(actual_nodes) == expected_nodes, (
                    f"Expected {expected_nodes} nodes (data: {data_shards}, parity: {parity_shards}), but found {len(actual_nodes)} nodes in output: {result}"
                )

        with allure.step("Test resilience with node failure across all containers"):
            node_to_stop = random.choice(neofs_env.storage_nodes)
            node_to_stop.kill()

            try:
                alive_nodes = [sn for sn in neofs_env.storage_nodes if sn != node_to_stop]
                for mapping in object_mappings:
                    head_object(
                        default_wallet.path,
                        mapping["cid"],
                        mapping["oid"],
                        shell=neofs_env.shell,
                        endpoint=alive_nodes[0].endpoint,
                    )
            finally:
                node_to_stop.start(fresh=False)
                wait_all_storage_nodes_returned(neofs_env)

        with allure.step("Delete all objects and verify deletion"):
            for mapping in object_mappings:
                cid = mapping["cid"]
                oid = mapping["oid"]

                delete_object(
                    wallet.path,
                    cid,
                    oid,
                    neofs_env.shell,
                    neofs_env.sn_rpc,
                )

                for sn in neofs_env.storage_nodes:
                    with pytest.raises(Exception):
                        get_object(
                            default_wallet.path,
                            cid,
                            oid,
                            neofs_env.shell,
                            sn.endpoint,
                        )

    finally:
        with allure.step("Clean up all containers"):
            for container_info in containers:
                delete_container(wallet.path, container_info["cid"], shell=neofs_env.shell, endpoint=neofs_env.sn_rpc)


@pytest.mark.parametrize(
    "data_shards,parity_shards",
    [
        (3, 1),
    ],
    ids=[
        "EC_3/1",
    ],
)
@pytest.mark.parametrize(
    "object_size",
    [
        pytest.param("simple_object_size", id="simple object", marks=pytest.mark.simple),
    ],
)
def test_ec_suboptimal_placement(
    default_wallet: NodeWallet, neofs_env_slow_policer: NeoFSEnv, data_shards: int, parity_shards: int, object_size: str
):
    neofs_env = neofs_env_slow_policer
    wallet = default_wallet

    with allure.step("Create EC container"):
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

    with allure.step("Put object to EC container"):
        source_file_size = neofs_env.get_object_size(object_size)
        source_file_path = generate_file(source_file_size)

        main_oid = put_object(
            wallet.path,
            source_file_path,
            cid,
            neofs_env.shell,
            neofs_env.sn_rpc,
        )

    with allure.step("Get containers node before adding new SN"):
        parse_container_nodes_output(
            neofs_env.neofs_cli(neofs_env.storage_nodes[0].cli_config)
            .container.nodes(rpc_endpoint=neofs_env.storage_nodes[0].endpoint, cid=cid)
            .stdout
        )

    with allure.step("Add new SN with a lower price"):
        existing_sn = neofs_env.storage_nodes[0]
        neofs_env.deploy_storage_nodes(
            count=1,
            node_attrs={0: ["UN-LOCODE:RU MOW", "Price:1"]},
            fschain_endpoints=existing_sn.fschain_endpoints,
        )
        neofs_env.neofs_adm().fschain.force_new_epoch(
            rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
            alphabet_wallets=neofs_env.alphabet_wallets_dir,
        )

    with allure.step("Get containers node after adding new SN"):
        containers_nodes_after = parse_container_nodes_output(
            neofs_env.neofs_cli(neofs_env.storage_nodes[0].cli_config)
            .container.nodes(rpc_endpoint=neofs_env.storage_nodes[0].endpoint, cid=cid)
            .stdout
        )
        assert any([n["Price"] == 1 for n in containers_nodes_after]), (
            "New node wasn't added to container nodes after adding new SN"
        )

    with allure.step("Verify object operations from EC container"):
        for sn in neofs_env.storage_nodes:
            get_object(
                default_wallet.path,
                cid,
                main_oid,
                neofs_env.shell,
                sn.endpoint,
            )
            head_object(
                default_wallet.path,
                cid,
                main_oid,
                neofs_env.shell,
                sn.endpoint,
            )


@pytest.mark.sanity
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
@pytest.mark.parametrize(
    "object_size",
    [
        pytest.param("simple_object_size", id="simple object", marks=pytest.mark.simple),
    ],
)
@pytest.mark.simple
def test_ec_recovery(
    default_wallet: NodeWallet, neofs_env: NeoFSEnv, data_shards: int, parity_shards: int, object_size: str
):
    wallet = default_wallet

    with allure.step("Create EC container"):
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

    with allure.step("Put object to EC container"):
        source_file_size = neofs_env.get_object_size(object_size)
        source_file_path = generate_file(source_file_size)

        main_oid = put_object(
            wallet.path,
            source_file_path,
            cid,
            neofs_env.shell,
            neofs_env.sn_rpc,
        )

    with allure.step("Verify object placement in EC container"):
        result = (
            neofs_env.neofs_cli(neofs_env.storage_nodes[0].cli_config)
            .object.nodes(rpc_endpoint=neofs_env.storage_nodes[0].endpoint, cid=cid, oid=main_oid)
            .stdout
        )
        assert f"EC {data_shards}/{parity_shards}" in result, (
            f"Expected placement rule {placement_rule} in nodes output: {result}"
        )

        expected_nodes = data_shards + parity_shards
        actual_nodes = parse_ec_nodes_count(result)
        assert len(actual_nodes) == expected_nodes, (
            f"Expected {expected_nodes} nodes (data: {data_shards}, parity: {parity_shards}), but found {len(actual_nodes)} nodes in output: {result}"
        )

    with allure.step("Search for the object and its parts in EC container"):
        found_objects, _ = search_objectv2(
            rpc_endpoint=neofs_env.sn_rpc, wallet=default_wallet.path, cid=cid, shell=neofs_env.shell
        )

        if object_size == "simple_object_size":
            assert len(found_objects) == expected_nodes + 1, (
                f"Invalid number of found objects in EC container, expected {expected_nodes + 1}, found {len(found_objects)}"
            )

    with allure.step("Drop one of the objects parts"):
        object_to_recover = random.choice([o for o in found_objects if o["id"] != main_oid])
        head_object(
            default_wallet.path,
            cid,
            object_to_recover["id"],
            shell=neofs_env.shell,
            endpoint=neofs_env.sn_rpc,
        )
        nodes_with_object = get_nodes_with_object(
            cid,
            object_to_recover["id"],
            shell=neofs_env.shell,
            nodes=neofs_env.storage_nodes,
            neofs_env=neofs_env,
        )
        node_to_drop_object_from = random.choice(nodes_with_object)
        drop_object(node_to_drop_object_from, cid, object_to_recover["id"])

    with allure.step("Verify main object is still accessible after dropping one part"):
        head_object(
            default_wallet.path,
            cid,
            main_oid,
            shell=neofs_env.shell,
            endpoint=node_to_drop_object_from.endpoint,
        )

    with allure.step("Wait until the dropped part is recovered"):
        object_is_accessible(
            default_wallet.path,
            cid,
            object_to_recover["id"],
            neofs_env,
            node_to_drop_object_from.endpoint,
        )
