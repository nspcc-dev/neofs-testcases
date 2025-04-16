import logging
from typing import Optional

import allure
from helpers.test_control import wait_for_success
from neofs_testlib.env.env import NeoFSEnv, StorageNode

logger = logging.getLogger("NeoLogger")


@allure.step("Ensure fresh epoch")
def ensure_fresh_epoch(neofs_env: NeoFSEnv, alive_node: Optional[StorageNode] = None) -> int:
    # ensure new fresh epoch to avoid epoch switch during test session
    alive_node = alive_node if alive_node else neofs_env.storage_nodes[0]
    current_epoch = get_epoch(neofs_env, alive_node)
    tick_epoch_and_wait(neofs_env, current_epoch, alive_node)
    epoch = get_epoch(neofs_env, alive_node)
    assert epoch > current_epoch, "Epoch wasn't ticked"
    return epoch


@allure.step("Wait for epochs align in whole cluster")
@wait_for_success(60, 5)
def wait_for_epochs_align(neofs_env: NeoFSEnv, epoch_number: Optional[int] = None) -> bool:
    epochs = []
    for node in neofs_env.storage_nodes:
        current_epoch = get_epoch(neofs_env, node)
        assert epoch_number is None or current_epoch > epoch_number, (
            f"Epoch {current_epoch} wasn't ticked yet. Expected epoch > {epoch_number}"
        )
        epochs.append(current_epoch)
    unique_epochs = list(set(epochs))
    assert len(unique_epochs) == 1, f"unaligned epochs found,  {epochs}, count of unique epochs {len(unique_epochs)}"


@allure.step("Wait until new epoch arrives")
@wait_for_success(60, 1)
def wait_until_new_epoch(neofs_env: NeoFSEnv, current_epoch: int) -> int:
    for node in neofs_env.storage_nodes:
        next_epoch = get_epoch(neofs_env, node)
        assert next_epoch == current_epoch + 1, "Next epoch didn't arrive during timeout"
    return next_epoch


@allure.step("Get Epoch")
def get_epoch(neofs_env: NeoFSEnv, alive_node: Optional[StorageNode] = None):
    alive_node = alive_node if alive_node else neofs_env.storage_nodes[0]
    cli = neofs_env.neofs_cli(alive_node.cli_config)
    epoch = cli.netmap.epoch(alive_node.endpoint, alive_node.wallet.path)
    return int(epoch.stdout)


@allure.step("Tick Epoch")
def tick_epoch(neofs_env: NeoFSEnv, alive_node: Optional[StorageNode] = None):
    """
    Tick epoch using neofs-adm or NeoGo if neofs-adm is not available (DevEnv)
    Args:
        neofs_env: neofs env instance under test
        alive_node: node to send requests to (first node in cluster by default)
    """

    alive_node = alive_node if alive_node else neofs_env.storage_nodes[0]
    neofs_env.neofs_adm().fschain.force_new_epoch(
        rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
        alphabet_wallets=neofs_env.alphabet_wallets_dir,
    )


@allure.step("Tick Epoch and wait for epochs align")
def tick_epoch_and_wait(
    neofs_env: NeoFSEnv,
    current_epoch: Optional[int] = None,
    node: Optional[StorageNode] = None,
):
    current_epoch = current_epoch if current_epoch else get_epoch(neofs_env, node)
    tick_epoch(neofs_env, node)
    wait_for_epochs_align(neofs_env, current_epoch)
