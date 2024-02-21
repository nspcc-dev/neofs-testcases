import logging
import random
import re
import threading
import time
from dataclasses import dataclass
from typing import Optional

import allure
import neofs_env.neofs_epoch as neofs_epoch
from common import MORPH_BLOCK_TIME, NEOFS_CLI_EXEC
from neofs_testlib.cli import NeofsCli
from neofs_testlib.env.env import NeoFSEnv, StorageNode
from neofs_testlib.shell import Shell
from neofs_testlib.utils import wallet as wallet_utils
from utility import parse_time

logger = logging.getLogger("NeoLogger")


@dataclass
class HealthStatus:
    network_status: Optional[str] = None
    health_status: Optional[str] = None

    @staticmethod
    def from_stdout(output: str) -> "HealthStatus":
        network, health = None, None
        for line in output.split("\n"):
            if "Network status" in line:
                network = line.split(":")[-1].strip()
            if "Health status" in line:
                health = line.split(":")[-1].strip()
        return HealthStatus(network, health)


@allure.step("Start storage node")
def start_storage_nodes(nodes: list[StorageNode]) -> None:
    """
    The function starts specified storage nodes.
    Args:
       nodes: the list of nodes to start
    """
    start_threads = []
    for node in nodes:
        start_threads.append(threading.Thread(target=node._launch_process))
    for t in start_threads:
        t.start()
    for t in start_threads:
        t.join()

    time.sleep(10)

    for node in nodes:
        with allure.step("Wait until storage node is READY"):
            node._wait_until_ready()


@allure.step("Stop storage nodes")
def stop_storage_nodes(nodes: list[StorageNode]) -> None:
    """
    The function stops specified storage nodes.
    Args:
       nodes: the list of nodes to stop
    """
    for node in nodes:
        node.stop()


@allure.step("Restart storage nodes")
def restart_storage_nodes(nodes: list[StorageNode]) -> None:
    """
    The function restarts specified storage nodes.
    Args:
       nodes: the list of nodes to restart
    """
    stop_storage_nodes(nodes)
    start_storage_nodes(nodes)


@allure.step("Get Locode from random storage node")
def get_locode_from_random_node(neofs_env: NeoFSEnv) -> str:
    node = random.choice(neofs_env.storage_nodes)
    locode = node.attrs["NEOFS_NODE_ATTRIBUTE_0"].split(":")[1].strip()
    logger.info(f"Chosen '{locode}' locode from node {node}")
    return locode


@allure.step("Healthcheck for storage node {node}")
def storage_node_healthcheck(node: StorageNode) -> HealthStatus:
    """
    The function returns storage node's health status.
    Args:
        node: storage node for which health status should be retrieved.
    Returns:
        health status as HealthStatus object.
    """
    command = "control healthcheck"
    output = _run_control_command_with_retries(node, command)
    return HealthStatus.from_stdout(output)


@allure.step("Set status for {node}")
def storage_node_set_status(node: StorageNode, status: str, retries: int = 0) -> None:
    """
    The function sets particular status for given node.
    Args:
        node: node for which status should be set.
        status: online or offline.
        retries (optional, int): number of retry attempts if it didn't work from the first time
    """
    command = f"control set-status --status {status}"
    _run_control_command_with_retries(node, command, retries)


@allure.step("Get netmap snapshot")
def get_netmap_snapshot(node: StorageNode, shell: Shell) -> str:
    """
    The function returns string representation of netmap snapshot.
    Args:
        node: node from which netmap snapshot should be requested.
    Returns:
        string representation of netmap
    """

    cli = NeofsCli(shell, NEOFS_CLI_EXEC, config_file=node.cli_config)
    return cli.netmap.snapshot(
        rpc_endpoint=node.endpoint,
        wallet=node.wallet.path,
    ).stdout


@allure.step("Get shard list for {node}")
def node_shard_list(node: StorageNode) -> list[str]:
    """
    The function returns list of shards for specified storage node.
    Args:
        node: node for which shards should be returned.
    Returns:
        list of shards.
    """
    command = "control shards list"
    output = _run_control_command_with_retries(node, command)
    return re.findall(r"Shard (.*):", output)


@allure.step("Shard set for {node}")
def node_shard_set_mode(node: StorageNode, shard: str, mode: str) -> str:
    """
    The function sets mode for specified shard.
    Args:
        node: node on which shard mode should be set.
    """
    command = f"control shards set-mode --id {shard} --mode {mode}"
    return _run_control_command_with_retries(node, command)


@allure.step("Drop object from {node}")
def drop_object(node: StorageNode, cid: str, oid: str) -> str:
    """
    The function drops object from specified node.
    Args:
        node_id str: node from which object should be dropped.
    """
    command = f"control drop-objects -o {cid}/{oid}"
    return _run_control_command_with_retries(node, command)


@allure.step("Delete data from host for node {node}")
def delete_node_data(node: StorageNode) -> None:
    node.delete_data()


@allure.step("Delete metadata from host for node {node}")
def delete_node_metadata(node: StorageNode) -> None:
    """
    The function deletes metadata from host for specified node.
    Args:
        node: node for which metadata should be deleted.
    """
    node.delete_metadata()


@allure.step("Exclude node {node_to_exclude} from network map")
def exclude_node_from_network_map(
    node_to_exclude: StorageNode,
    alive_node: StorageNode,
    shell: Shell,
    neofs_env: NeoFSEnv,
) -> None:
    node_netmap_key = wallet_utils.get_last_public_key_from_wallet(
        node_to_exclude.wallet.path, node_to_exclude.wallet.password
    )

    storage_node_set_status(node_to_exclude, status="offline")

    time.sleep(parse_time(MORPH_BLOCK_TIME))
    neofs_epoch.tick_epoch_and_wait(neofs_env)

    snapshot = get_netmap_snapshot(node=alive_node, shell=shell)
    assert (
        f"{node_netmap_key}" not in snapshot
    ), f"Expected node with key {node_netmap_key} to be absent in network map"


@allure.step("Include node {node_to_include} into network map")
def include_node_to_network_map(
    node_to_include: StorageNode,
    alive_node: StorageNode,
    shell: Shell,
    neofs_env: NeoFSEnv,
) -> None:
    storage_node_set_status(node_to_include, status="online")

    # Per suggestion of @fyrchik we need to wait for 2 blocks after we set status and after tick epoch.
    # First sleep can be omitted after https://github.com/nspcc-dev/neofs-node/issues/1790 complete.

    time.sleep(parse_time(MORPH_BLOCK_TIME) * 2)
    neofs_epoch.tick_epoch_and_wait(neofs_env)
    time.sleep(parse_time(MORPH_BLOCK_TIME) * 2)

    check_node_in_map(node_to_include, shell, alive_node)


@allure.step("Check node {node} in network map")
def check_node_in_map(
    node: StorageNode, shell: Shell, alive_node: Optional[StorageNode] = None
) -> None:
    alive_node = alive_node or node

    node_netmap_key = wallet_utils.get_last_public_key_from_wallet(
        node.wallet.path, node.wallet.password
    )
    logger.info(f"Node ({node}) netmap key: {node_netmap_key}")

    snapshot = get_netmap_snapshot(alive_node, shell)
    assert (
        f"{node_netmap_key}" in snapshot
    ), f"Expected node with key {node_netmap_key} to be in network map"


@allure.step("Wait for storage nodes returned to cluster")
def wait_all_storage_nodes_returned(neofs_env: NeoFSEnv) -> None:
    sleep_interval, attempts = 15, 20
    for _ in range(attempts):
        if is_all_storage_nodes_returned(neofs_env):
            return
        time.sleep(sleep_interval)
    raise AssertionError("Storage node(s) is broken")


def is_all_storage_nodes_returned(neofs_env: NeoFSEnv) -> bool:
    with allure.step("Run health check for all storage nodes"):
        for node in neofs_env.storage_nodes:
            try:
                health_check = storage_node_healthcheck(node)
            except Exception as err:
                logger.warning(f"Node healthcheck fails with error {err}")
                return False
            if health_check.health_status != "READY" or health_check.network_status != "ONLINE":
                return False
    return True


def _run_control_command_with_retries(node: StorageNode, command: str, retries: int = 0) -> str:
    for attempt in range(1 + retries):  # original attempt + specified retries
        try:
            return _run_control_command(node, command)
        except AssertionError as err:
            if attempt < retries:
                logger.warning(f"Command {command} failed with error {err} and will be retried")
                continue
            raise AssertionError(f"Command {command} failed with error {err}") from err


def _run_control_command(node: StorageNode, command: str) -> None:
    result = node.neofs_env.shell.exec(
        f"{node.neofs_env.neofs_cli_path} {command} --endpoint {node.control_grpc_endpoint} "
        f"--wallet {node.wallet.path} --config {node.cli_config}"
    )
    return result.stdout
