import time
from typing import Union

import allure
import neofs_env.neofs_epoch as neofs_epoch
import pytest
from helpers.neofs_verbs import CONFIG_KEYS_MAPPING, get_netmap_netinfo
from helpers.test_control import wait_for_success
from neofs_testlib.env.env import NeoFSEnv, StorageNode
from neofs_testlib.utils import wallet as wallet_utils


@allure.step("Wait until new config value arrives")
@wait_for_success(60, 5)
def wait_until_new_config_value(neofs_env: NeoFSEnv, config_key: str, config_value: int) -> int:
    net_info = get_netmap_netinfo(
        wallet=neofs_env.storage_nodes[0].wallet.path,
        wallet_config=neofs_env.storage_nodes[0].cli_config,
        endpoint=neofs_env.storage_nodes[0].endpoint,
        shell=neofs_env.shell,
    )

    assert net_info[config_key] == config_value, f"Invalid config value: {net_info[config_key]}"


@allure.step("Wait until node disappears from netmap snapshot")
@wait_for_success(60, 5)
def wait_until_node_disappears_from_netmap_snapshot(
    neofs_env: NeoFSEnv, alive_node: StorageNode, offline_node_addr: str
) -> int:
    netmap_snapshot = (
        neofs_env.neofs_cli(alive_node.cli_config)
        .netmap.snapshot(
            rpc_endpoint=alive_node.endpoint,
            wallet=alive_node.wallet.path,
        )
        .stdout
    )
    assert offline_node_addr not in netmap_snapshot, f"Node {offline_node_addr} is still in network map"


@pytest.fixture(scope="module")
def multi_ir_neofs_env():
    neofs_env = NeoFSEnv(neofs_env_config=NeoFSEnv._generate_default_neofs_env_config())
    with allure.step("Deploy neofs with 4 ir nodes"):
        neofs_env.download_binaries()
        neofs_env.deploy_inner_ring_nodes(count=4)
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
    yield neofs_env
    neofs_env.kill()


def test_control_notary_request_new_epoch(multi_ir_neofs_env: NeoFSEnv):
    if multi_ir_neofs_env.get_binary_version(multi_ir_neofs_env.neofs_node_path) <= "0.44.2":
        pytest.skip("Test requires fresh node version")
    neofs_env = multi_ir_neofs_env

    with allure.step("Create notary request to tick epoch"):
        current_epoch = neofs_epoch.ensure_fresh_epoch(neofs_env)

        ir_node = neofs_env.inner_ring_nodes[0]

        tx_hash = (
            neofs_env.neofs_cli(ir_node.cli_config)
            .control.notary_request(
                address=ir_node.alphabet_wallet.address,
                endpoint=ir_node.grpc_address,
                wallet=ir_node.alphabet_wallet.path,
                method="newEpoch",
            )
            .stdout.strip()
            .split("Transaction Hash:")[1]
            .strip()
        )

        notary_list = (
            neofs_env.neofs_cli(ir_node.cli_config)
            .control.notary_list(
                address=ir_node.alphabet_wallet.address,
                endpoint=ir_node.grpc_address,
                wallet=ir_node.alphabet_wallet.path,
            )
            .stdout.strip()
        )
        assert tx_hash in notary_list, f"Transaction hash {tx_hash} not found in notary list {notary_list}"

    with allure.step("Sign notary request by 2 more IR nodes"):
        for ir_node in neofs_env.inner_ring_nodes[1:-1]:
            neofs_env.neofs_cli(ir_node.cli_config).control.notary_sign(
                address=ir_node.alphabet_wallet.address,
                endpoint=ir_node.grpc_address,
                wallet=ir_node.alphabet_wallet.path,
                hash=tx_hash,
            )

    with allure.step("Wait until new epoch arrives"):
        neofs_epoch.wait_until_new_epoch(neofs_env, current_epoch)


@pytest.mark.parametrize(
    "key, value",
    [
        ("MaxObjectSize", 1048576),
        ("BasicIncomeRate", 50000000),
        ("AuditFee", 5000),
        ("EpochDuration", 480),
        ("ContainerFee", 2000),
        ("EigenTrustIterations", 8),
        ("EigenTrustAlpha", 0.2),
        ("InnerRingCandidateFee", 5000000000),
        ("WithdrawFee", 200000000),
        ("HomomorphicHashingDisabled", True),
        ("MaintenanceModeAllowed", True),
    ],
)
def test_control_notary_request_new_config_value(multi_ir_neofs_env: NeoFSEnv, key: str, value: Union[str, int, bool]):
    if multi_ir_neofs_env.get_binary_version(multi_ir_neofs_env.neofs_node_path) <= "0.44.2":
        pytest.skip("Test requires fresh node version")
    neofs_env = multi_ir_neofs_env

    with allure.step("Create notary request to update config value"):
        ir_node = neofs_env.inner_ring_nodes[0]

        tx_hash = (
            neofs_env.neofs_cli(ir_node.cli_config)
            .control.notary_request(
                address=ir_node.alphabet_wallet.address,
                endpoint=ir_node.grpc_address,
                wallet=ir_node.alphabet_wallet.path,
                method="setConfig",
                post_data=f"{key}={value}",
            )
            .stdout.strip()
            .split("Transaction Hash:")[1]
            .strip()
        )

        notary_list = (
            neofs_env.neofs_cli(ir_node.cli_config)
            .control.notary_list(
                address=ir_node.alphabet_wallet.address,
                endpoint=ir_node.grpc_address,
                wallet=ir_node.alphabet_wallet.path,
            )
            .stdout.strip()
        )
        assert tx_hash in notary_list, f"Transaction hash {tx_hash} not found in notary list {notary_list}"

    with allure.step("Sign notary request by 2 more IR nodes"):
        for ir_node in neofs_env.inner_ring_nodes[1:-1]:
            neofs_env.neofs_cli(ir_node.cli_config).control.notary_sign(
                address=ir_node.alphabet_wallet.address,
                endpoint=ir_node.grpc_address,
                wallet=ir_node.alphabet_wallet.path,
                hash=tx_hash,
            )

    wait_until_new_config_value(neofs_env, CONFIG_KEYS_MAPPING[key], value)


def test_control_notary_request_node_removal(multi_ir_neofs_env: NeoFSEnv):
    if multi_ir_neofs_env.get_binary_version(multi_ir_neofs_env.neofs_node_path) <= "0.44.2":
        pytest.skip("Test requires fresh node version")
    neofs_env = multi_ir_neofs_env

    with allure.step("Create notary request to remove node"):
        ir_node = neofs_env.inner_ring_nodes[0]
        sn_offline_node = neofs_env.storage_nodes[1]
        sn_offline_node_addr = str(
            wallet_utils.get_last_public_key_from_wallet(sn_offline_node.wallet.path, sn_offline_node.wallet.password)
        )

        tx_hash = (
            neofs_env.neofs_cli(ir_node.cli_config)
            .control.notary_request(
                address=ir_node.alphabet_wallet.address,
                endpoint=ir_node.grpc_address,
                wallet=ir_node.alphabet_wallet.path,
                method="removeNode",
                post_data=sn_offline_node_addr,
            )
            .stdout.strip()
            .split("Transaction Hash:")[1]
            .strip()
        )

        notary_list = (
            neofs_env.neofs_cli(ir_node.cli_config)
            .control.notary_list(
                address=ir_node.alphabet_wallet.address,
                endpoint=ir_node.grpc_address,
                wallet=ir_node.alphabet_wallet.path,
            )
            .stdout.strip()
        )
        assert tx_hash in notary_list, f"Transaction hash {tx_hash} not found in notary list {notary_list}"

    with allure.step("Sign notary request by 2 more IR nodes"):
        for ir_node in neofs_env.inner_ring_nodes[1:-1]:
            neofs_env.neofs_cli(ir_node.cli_config).control.notary_sign(
                address=ir_node.alphabet_wallet.address,
                endpoint=ir_node.grpc_address,
                wallet=ir_node.alphabet_wallet.path,
                hash=tx_hash,
            )

    with allure.step("Node should disappear in the next epoch"):
        neofs_epoch.ensure_fresh_epoch(neofs_env)
        wait_until_node_disappears_from_netmap_snapshot(neofs_env, neofs_env.storage_nodes[0], sn_offline_node_addr)
