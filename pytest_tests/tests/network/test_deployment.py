import os
from pathlib import Path

import allure
from helpers.common import SIMPLE_OBJECT_SIZE
from helpers.container import create_container
from helpers.file_helper import generate_file, get_file_hash
from helpers.neofs_verbs import get_object_from_random_node, put_object, put_object_to_random_node
from helpers.wallet_helpers import create_wallet, create_wallet_with_money
from neofs_testlib.env.env import NeoFSEnv, NodeWallet, StorageNode


def parse_object_info_data(raw_output: str) -> dict:
    parsed_data = {}
    lines = raw_output.strip().split("\n")
    current_key = None

    for line in lines:
        line = line.strip()
        if not line:
            continue

        if ": " in line:
            key, value = line.split(": ", 1)
            if key == "Attributes":
                parsed_data[key] = {}
                current_key = key
            else:
                parsed_data[key] = value
        elif current_key == "Attributes":
            key, value = line.split(": ", 1)
            parsed_data[current_key][key] = value

    return parsed_data


def put_get_object(neofs_env: NeoFSEnv, wallet: NodeWallet):
    cli = neofs_env.neofs_cli(neofs_env.generate_cli_config(wallet))

    result = cli.container.create(
        rpc_endpoint=neofs_env.sn_rpc,
        wallet=wallet.path,
        policy="REP 3 IN X CBF 1 SELECT 3 FROM * AS X",
        basic_acl="0FBFBFFF",
        await_mode=True,
    )

    lines = result.stdout.split("\n")
    for line in lines:
        if line.startswith("container ID:"):
            cid = line.split(": ")[1]

    result = cli.container.list(rpc_endpoint=neofs_env.sn_rpc, wallet=wallet.path)
    containers = result.stdout.split()
    assert cid in containers

    file_path = generate_file(int(SIMPLE_OBJECT_SIZE))
    original_file_hash = get_file_hash(file_path)

    oid = put_object_to_random_node(
        wallet=wallet.path,
        path=file_path,
        cid=cid,
        shell=neofs_env.shell,
        neofs_env=neofs_env,
    )

    file_path = get_object_from_random_node(
        wallet.path,
        cid,
        oid,
        neofs_env.shell,
        neofs_env=neofs_env,
    )
    assert get_file_hash(file_path) == original_file_hash
    return cid, oid, int(SIMPLE_OBJECT_SIZE)


def test_4_ir_node_deployment(neofs_env_4_ir: NeoFSEnv):
    neofs_env = neofs_env_4_ir

    with allure.step("Create wallet for test"):
        default_wallet = create_wallet()

    with allure.step("Run put get to ensure neofs setup is actually working"):
        put_get_object(neofs_env, default_wallet)


def test_7_ir_node_deployment(neofs_env_7_ir: NeoFSEnv):
    neofs_env = neofs_env_7_ir

    with allure.step("Create wallet for test"):
        default_wallet = create_wallet()

    with allure.step("Run put get to ensure neofs setup is actually working"):
        put_get_object(neofs_env, default_wallet)


def test_sn_deployment_with_writecache(neofs_env_with_writecache: NeoFSEnv):
    neofs_env = neofs_env_with_writecache

    with allure.step("Create wallet for test"):
        default_wallet = create_wallet()

    with allure.step("Disable write to blobstore"):
        for sn in neofs_env.storage_nodes:
            for shard in sn.shards:
                os.chmod(shard.fstree_path, 0o444)

    with allure.step("Run put get to ensure neofs setup is actually working"):
        cid, oid, size = put_get_object(neofs_env, default_wallet)

    with allure.step("Get object info from write cache"):
        neofs_lens = neofs_env.neofs_lens()
        storage_nodes_with_cached_object = []
        for sn in neofs_env.storage_nodes:
            for shard in sn.shards:
                shard_wc_path = shard.wc_path
                object_address = neofs_lens.write_cache.list(shard_wc_path).stdout.strip()
                if object_address:
                    object_info = parse_object_info_data(
                        neofs_lens.write_cache.get(object_address, shard_wc_path).stdout
                    )
                    assert object_info["CID"] == cid, "Invalid value in the write cache"
                    assert object_info["ID"] == oid, "Invalid value in the write cache"
                    assert int(object_info["PayloadSize"]) == size, "Invalid value in the write cache"
                    storage_nodes_with_cached_object.append(sn)
        assert len(storage_nodes_with_cached_object) == 3, (
            "Invalid number of storage nodes with a single shard containing a cached object"
        )

    with allure.step("Enable write to blobstore"):
        for sn in neofs_env.storage_nodes:
            sn.stop()
            for shard in sn.shards:
                os.chmod(shard.fstree_path, 0o777)
            sn_config_template = sn.get_config_template()
            NeoFSEnv.generate_config_file(
                config_template=sn_config_template,
                config_path=sn.storage_node_config_path,
                custom=Path(sn_config_template).is_file(),
                fschain_endpoints=[sn.neofs_env.fschain_rpc],
                shards=sn.shards,
                writecache=sn.writecache,
                wallet=sn.wallet,
                state_file=sn.state_file,
                pprof_address=sn.pprof_address,
                prometheus_address=sn.prometheus_address,
                attrs=sn.node_attrs,
            )
            sn.start(fresh=False)

    with allure.step("Flush cache"):
        for sn in neofs_env.storage_nodes:
            neofs_cli = neofs_env.neofs_cli(sn.cli_config)
            result = neofs_cli.shards.flush_cache(endpoint=sn.control_endpoint, all=True).stdout
            assert "Write-cache has been flushed" in result

    with allure.step("Stop SNs and inspect blobstore"):
        storage_nodes_with_object_in_blobstore = 0
        for sn in storage_nodes_with_cached_object:
            sn.stop()
            object_address = neofs_lens.storage.list(sn.storage_node_config_path).stdout.strip()
            if object_address:
                object_info = parse_object_info_data(
                    neofs_lens.storage.get(object_address, sn.storage_node_config_path).stdout
                )
                assert object_info["CID"] == cid, "Invalid value in the write cache"
                assert object_info["ID"] == oid, "Invalid value in the write cache"
                assert int(object_info["PayloadSize"]) == size, "Invalid value in the write cache"
                storage_nodes_with_object_in_blobstore += 1

        assert storage_nodes_with_object_in_blobstore == 3, "Invalid number of storage nodes with a persisted object"


def test_4_ir_node_deployment_with_main_chain(neofs_env_4_ir_with_mainchain: NeoFSEnv):
    neofs_env = neofs_env_4_ir_with_mainchain

    with allure.step("Create container and put object"):
        new_wallet = create_wallet_with_money(neofs_env)
        cid = create_container(
            new_wallet.path,
            rule="EC 2/2",
            shell=neofs_env.shell,
            endpoint=neofs_env.sn_rpc,
        )
        put_object(
            new_wallet.path,
            generate_file(1000),
            cid,
            neofs_env.shell,
            neofs_env.sn_rpc,
        )


def test_7_ir_node_deployment_with_main_chain(neofs_env_7_ir_with_mainchain: NeoFSEnv):
    neofs_env = neofs_env_7_ir_with_mainchain

    with allure.step("Create container and put object"):
        new_wallet = create_wallet_with_money(neofs_env)
        cid = create_container(
            new_wallet.path,
            rule="EC 2/2",
            shell=neofs_env.shell,
            endpoint=neofs_env.sn_rpc,
        )
        put_object(
            new_wallet.path,
            generate_file(1000),
            cid,
            neofs_env.shell,
            neofs_env.sn_rpc,
        )


@allure.title("SN does not die on startup when the first endpoint is unavailable")
def test_sn_deployment_with_inactive_fschain_endpoint(neofs_env_ir_only: NeoFSEnv):
    with allure.step("Add new SN with inactive fschain endpoint"):
        new_storage_node = StorageNode(
            neofs_env_ir_only,
            len(neofs_env_ir_only.storage_nodes) + 1,
            node_attrs=["UN-LOCODE:RU MOW", "Price:22"],
            fschain_endpoints=["localhost:9", neofs_env_ir_only.fschain_rpc],
        )
        neofs_env_ir_only.storage_nodes.append(new_storage_node)

    with allure.step("Start new storage node and ensure it is registered in the network"):
        new_storage_node.start(wait_until_ready=False)
        neofs_env_ir_only._wait_until_all_storage_nodes_are_ready()
        neofs_env_ir_only.neofs_adm().fschain.force_new_epoch(
            rpc_endpoint=f"http://{neofs_env_ir_only.fschain_rpc}",
            alphabet_wallets=neofs_env_ir_only.alphabet_wallets_dir,
        )
        new_storage_node._wait_until_ready()
