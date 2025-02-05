import time

import allure
import pytest
from helpers.common import SIMPLE_OBJECT_SIZE
from helpers.container import create_container
from helpers.file_helper import generate_file, get_file_hash
from helpers.neofs_verbs import get_object_from_random_node, put_object, put_object_to_random_node
from helpers.wallet_helpers import create_wallet, create_wallet_with_money
from neofs_testlib.env.env import NeoFSEnv, NodeWallet


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


@pytest.mark.parametrize("ir_nodes_count", [4, 7])
def test_multiple_ir_node_deployment(ir_nodes_count: int, clear_neofs_env: NeoFSEnv):
    neofs_env = clear_neofs_env
    with allure.step(f"Deploy neofs with {ir_nodes_count} ir nodes"):
        neofs_env.download_binaries()
        neofs_env.deploy_inner_ring_nodes(count=ir_nodes_count)
        neofs_env.deploy_storage_nodes(
            count=4,
            node_attrs={
                0: ["UN-LOCODE:RU MOW", "Price:22"],
                1: ["UN-LOCODE:RU LED", "Price:33"],
                2: ["UN-LOCODE:SE STO", "Price:11"],
                3: ["UN-LOCODE:FI HEL", "Price:44"],
            },
        )
        neofs_env.log_env_details_to_file()
        neofs_env.log_versions_to_allure()

        neofs_env.neofs_adm().fschain.set_config(
            rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
            alphabet_wallets=neofs_env.alphabet_wallets_dir,
            post_data="ContainerFee=0 ContainerAliasFee=0 MaxObjectSize=524288",
        )
        time.sleep(30)

    with allure.step("Create wallet for test"):
        default_wallet = create_wallet()

    with allure.step("Run put get to ensure neofs setup is actually working"):
        put_get_object(neofs_env, default_wallet)


def test_sn_deployment_with_writecache(clear_neofs_env: NeoFSEnv):
    neofs_env = clear_neofs_env
    with allure.step("Deploy neofs with writecache enabled"):
        neofs_env.download_binaries()
        if neofs_env.get_binary_version(neofs_env.neofs_node_path) <= "0.44.2":
            pytest.skip("Test requires fresh node version")
        neofs_env.deploy_inner_ring_nodes()
        neofs_env.deploy_storage_nodes(
            count=4,
            node_attrs={
                0: ["UN-LOCODE:RU MOW", "Price:22"],
                1: ["UN-LOCODE:RU LED", "Price:33"],
                2: ["UN-LOCODE:SE STO", "Price:11"],
                3: ["UN-LOCODE:FI HEL", "Price:44"],
            },
            writecache=True,
        )

        neofs_env.neofs_adm().fschain.set_config(
            rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
            alphabet_wallets=neofs_env.alphabet_wallets_dir,
            post_data="ContainerFee=0 ContainerAliasFee=0 MaxObjectSize=524288",
        )

    with allure.step("Create wallet for test"):
        default_wallet = create_wallet()

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

    with allure.step("Flush cache"):
        for sn in neofs_env.storage_nodes:
            neofs_cli = neofs_env.neofs_cli(sn.cli_config)
            result = neofs_cli.shards.flush_cache(endpoint=sn.control_grpc_endpoint, all=True).stdout
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


@pytest.mark.parametrize("ir_nodes_count", [4, 7])
def test_multiple_ir_node_deployment_with_main_chain(ir_nodes_count: int, clear_neofs_env: NeoFSEnv):
    neofs_env = clear_neofs_env
    with allure.step(f"Deploy neofs with {ir_nodes_count} ir nodes and main chain"):
        neofs_env.download_binaries()
        neofs_env.deploy_inner_ring_nodes(count=ir_nodes_count, with_main_chain=True)
        neofs_env.deploy_storage_nodes(
            count=1,
            node_attrs={
                0: ["UN-LOCODE:RU MOW", "Price:22"],
                1: ["UN-LOCODE:RU LED", "Price:33"],
                2: ["UN-LOCODE:SE STO", "Price:11"],
                3: ["UN-LOCODE:FI HEL", "Price:44"],
            },
        )
        neofs_env.log_env_details_to_file()
        neofs_env.log_versions_to_allure()

        neofs_adm = neofs_env.neofs_adm()
        for sn in neofs_env.storage_nodes:
            neofs_adm.fschain.refill_gas(
                rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
                alphabet_wallets=neofs_env.alphabet_wallets_dir,
                storage_wallet=sn.wallet.path,
                gas="10.0",
            )
        neofs_env.neofs_adm().fschain.set_config(
            rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
            alphabet_wallets=neofs_env.alphabet_wallets_dir,
            post_data="WithdrawFee=5",
        )

    with allure.step("Create container and put object"):
        new_wallet = create_wallet_with_money(neofs_env)
        cid = create_container(
            new_wallet.path,
            rule="REP 1",
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
