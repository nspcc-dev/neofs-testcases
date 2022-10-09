import logging
import random
import re
import time
from dataclasses import dataclass
from typing import Optional

import allure
from common import (
    MORPH_BLOCK_TIME,
    NEOFS_CLI_EXEC,
    NEOFS_NETMAP_DICT,
    STORAGE_WALLET_CONFIG,
    STORAGE_WALLET_PASS,
)
from data_formatters import get_wallet_public_key
from epoch import tick_epoch
from neofs_testlib.cli import NeofsCli
from neofs_testlib.shell import Shell
from neofs_testlib.hosting import Hosting
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


@allure.step("Stop storage nodes")
def stop_nodes(hosting: Hosting, number: int, nodes: list[str]) -> list[str]:
    """
    Shuts down the given number of randomly selected storage nodes.
    Args:
       number (int): the number of nodes to shut down
       nodes (list): the list of nodes for possible shut down
    Returns:
        (list): the list of nodes that were shut down
    """
    nodes_to_stop = random.sample(nodes, number)
    for node in nodes_to_stop:
        host = hosting.get_host_by_service(node)
        host.stop_service(node)
    return nodes_to_stop


@allure.step("Start storage nodes")
def start_nodes(hosting: Hosting, nodes: list[str]) -> None:
    """
    The function starts specified storage nodes.
    Args:
       nodes (list): the list of nodes to start
    """
    for node in nodes:
        host = hosting.get_host_by_service(node)
        host.start_service(node)


@allure.step("Get Locode")
def get_locode() -> str:
    endpoint_values = random.choice(list(NEOFS_NETMAP_DICT.values()))
    locode = endpoint_values["UN-LOCODE"]
    logger.info(f"Random locode chosen: {locode}")
    return locode


@allure.step("Healthcheck for node {node_name}")
def node_healthcheck(hosting: Hosting, node_name: str) -> HealthStatus:
    """
    The function returns node's health status.
    Args:
        node_name str: node name for which health status should be retrieved.
    Returns:
        health status as HealthStatus object.
    """
    command = "control healthcheck"
    output = _run_control_command_with_retries(hosting, node_name, command)
    return HealthStatus.from_stdout(output)


@allure.step("Set status for node {node_name}")
def node_set_status(hosting: Hosting, node_name: str, status: str, retries: int = 0) -> None:
    """
    The function sets particular status for given node.
    Args:
        node_name: node name for which status should be set.
        status: online or offline.
        retries (optional, int): number of retry attempts if it didn't work from the first time
    """
    command = f"control set-status --status {status}"
    _run_control_command_with_retries(hosting, node_name, command, retries)


@allure.step("Get netmap snapshot")
def get_netmap_snapshot(node_name: str, shell: Shell) -> str:
    """
    The function returns string representation of netmap snapshot.
    Args:
        node_name str: node name from which netmap snapshot should be requested.
    Returns:
        string representation of netmap
    """
    node_info = NEOFS_NETMAP_DICT[node_name]
    cli = NeofsCli(shell, NEOFS_CLI_EXEC, config_file=STORAGE_WALLET_CONFIG)
    return cli.netmap.snapshot(
        rpc_endpoint=node_info["rpc"],
        wallet=node_info["wallet_path"],
    ).stdout


@allure.step("Get shard list for node {node_name}")
def node_shard_list(hosting: Hosting, node_name: str) -> list[str]:
    """
    The function returns list of shards for specified node.
    Args:
        node_name str: node name for which shards should be returned.
    Returns:
        list of shards.
    """
    command = "control shards list"
    output = _run_control_command_with_retries(hosting, node_name, command)
    return re.findall(r"Shard (.*):", output)


@allure.step("Shard set for node {node_name}")
def node_shard_set_mode(hosting: Hosting, node_name: str, shard: str, mode: str) -> str:
    """
    The function sets mode for specified shard.
    Args:
        node_name str: node name on which shard mode should be set.
    """
    command = f"control shards set-mode --id {shard} --mode {mode}"
    return _run_control_command_with_retries(hosting, node_name, command)


@allure.step("Drop object from node {node_name}")
def drop_object(hosting: Hosting, node_name: str, cid: str, oid: str) -> str:
    """
    The function drops object from specified node.
    Args:
        node_name str: node name from which object should be dropped.
    """
    command = f"control drop-objects  -o {cid}/{oid}"
    return _run_control_command_with_retries(hosting, node_name, command)


@allure.step("Delete data of node {node_name}")
def delete_node_data(hosting: Hosting, node_name: str) -> None:
    host = hosting.get_host_by_service(node_name)
    host.stop_service(node_name)
    host.delete_storage_node_data(node_name)
    time.sleep(parse_time(MORPH_BLOCK_TIME))


@allure.step("Exclude node {node_to_exclude} from network map")
def exclude_node_from_network_map(hosting: Hosting, node_to_exclude: str, alive_node: str, shell: Shell) -> None:
    node_wallet_path = NEOFS_NETMAP_DICT[node_to_exclude]["wallet_path"]
    node_netmap_key = get_wallet_public_key(node_wallet_path, STORAGE_WALLET_PASS)

    node_set_status(hosting, node_to_exclude, status="offline")

    time.sleep(parse_time(MORPH_BLOCK_TIME))
    tick_epoch()

    snapshot = get_netmap_snapshot(node_name=alive_node, shell=shell)
    assert (
        node_netmap_key not in snapshot
    ), f"Expected node with key {node_netmap_key} not in network map"


@allure.step("Include node {node_to_include} into network map")
def include_node_to_network_map(hosting: Hosting, node_to_include: str, alive_node: str, shell: Shell) -> None:
    node_set_status(hosting, node_to_include, status="online")

    time.sleep(parse_time(MORPH_BLOCK_TIME))
    tick_epoch()

    check_node_in_map(node_to_include, alive_node, shell=shell)


@allure.step("Check node {node_name} in network map")
def check_node_in_map(node_name: str, shell: Shell, alive_node: Optional[str] = None) -> None:
    alive_node = alive_node or node_name
    node_wallet_path = NEOFS_NETMAP_DICT[node_name]["wallet_path"]
    node_netmap_key = get_wallet_public_key(node_wallet_path, STORAGE_WALLET_PASS)

    logger.info(f"Node {node_name} netmap key: {node_netmap_key}")

    snapshot = get_netmap_snapshot(node_name=alive_node, shell=shell)
    assert node_netmap_key in snapshot, f"Expected node with key {node_netmap_key} in network map"


def _run_control_command_with_retries(
    hosting: Hosting, node_name: str, command: str, retries: int = 0
) -> str:
    for attempt in range(1 + retries):  # original attempt + specified retries
        try:
            return _run_control_command(hosting, node_name, command)
        except AssertionError as err:
            if attempt < retries:
                logger.warning(f"Command {command} failed with error {err} and will be retried")
                continue
            raise AssertionError(f"Command {command} failed with error {err}") from err


def _run_control_command(hosting: Hosting, service_name: str, command: str) -> None:
    host = hosting.get_host_by_service(service_name)

    service_config = host.get_service_config(service_name)
    wallet_path = service_config.attributes["wallet_path"]
    wallet_password = service_config.attributes["wallet_password"]
    control_endpoint = service_config.attributes["control_endpoint"]

    shell = host.get_shell()
    wallet_config_path = f"/tmp/{service_name}-config.yaml"
    wallet_config = f'password: "{wallet_password}"'
    shell.exec(f"echo '{wallet_config}' > {wallet_config_path}")

    cli_config = host.get_cli_config("neofs-cli")

    # TODO: implement cli.control
    # cli = NeofsCli(shell, cli_config.exec_path, wallet_config_path)
    result = shell.exec(
        f"{cli_config.exec_path} {command} --endpoint {control_endpoint} "
        f"--wallet {wallet_path} --config {wallet_config_path}"
    )
    return result.stdout
