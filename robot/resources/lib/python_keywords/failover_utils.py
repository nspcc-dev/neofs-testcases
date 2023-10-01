import logging
import os
import subprocess
from time import sleep

import allure
from typing import List, Tuple, Optional
from urllib.parse import urlparse

import pytest

from utility import parse_time
from cluster import Cluster, StorageNode
from neofs_testlib.shell import Shell
from neofs_testlib.hosting import Hosting
from python_keywords.node_management import storage_node_healthcheck, stop_storage_nodes
from storage_policy import get_nodes_with_object
from common import (
    MORPH_CHAIN_SERVICE_NAME_REGEX,
    ENDPOINT_INTERNAL0,
    DOCKER_COMPOSE_ENV_FILE,
    DOCKER_COMPOSE_STORAGE_CONFIG_FILE,
    METABASE_RESYNC_TIMEOUT,
)

logger = logging.getLogger("NeoLogger")


@allure.step("Wait for object replication")
def wait_object_replication(
    cid: str,
    oid: str,
    expected_copies: int,
    shell: Shell,
    nodes: list[StorageNode],
) -> list[StorageNode]:
    sleep_interval, attempts = 15, 20
    nodes_with_object = []
    for _ in range(attempts):
        nodes_with_object = get_nodes_with_object(cid, oid, shell=shell, nodes=nodes)
        if len(nodes_with_object) >= expected_copies:
            return nodes_with_object
        sleep(sleep_interval)
    raise AssertionError(
        f"Expected {expected_copies} copies of object, but found {len(nodes_with_object)}. "
        f"Waiting time {sleep_interval * attempts}"
    )


@allure.step("Wait for storage nodes returned to cluster")
def wait_all_storage_nodes_returned(cluster: Cluster) -> None:
    sleep_interval, attempts = 15, 20
    for __attempt in range(attempts):
        if is_all_storage_nodes_returned(cluster):
            return
        sleep(sleep_interval)
    raise AssertionError("Storage node(s) is broken")


def is_all_storage_nodes_returned(cluster: Cluster) -> bool:
    with allure.step("Run health check for all storage nodes"):
        for node in cluster.storage_nodes:
            try:
                health_check = storage_node_healthcheck(node)
            except Exception as err:
                logger.warning(f"Node healthcheck fails with error {err}")
                return False
            if health_check.health_status != "READY" or health_check.network_status != "ONLINE":
                return False
    return True


@allure.step("Get morph chain endpoints")
def get_morph_chain_endpoints(hosting: Hosting) -> List[Tuple[str, str]]:
    morph_chain_config = hosting.find_service_configs(MORPH_CHAIN_SERVICE_NAME_REGEX)
    endpoints = []
    for config in morph_chain_config:
        if ENDPOINT_INTERNAL0 not in config.attributes:
            raise ValueError(
                f"{ENDPOINT_INTERNAL0} is not present in the attributes of the config: {config}"
            )
        morph_chain_addr_full = config.attributes[ENDPOINT_INTERNAL0]
        parsed_url = urlparse(morph_chain_addr_full)
        addr = parsed_url.hostname
        port = str(parsed_url.port)
        endpoints.append((addr, port))
    return endpoints


@allure.step("Docker compose restart storage nodes containers with new env file")
def docker_compose_restart_storage_nodes(cluster: Cluster):
    stop_storage_nodes(cluster.storage_nodes)
    # Not using docker-compose restart because the container needs to be started with new environment variables.
    with allure.step("Docker-compose down"):
        subprocess.run(["docker-compose", "-f", DOCKER_COMPOSE_STORAGE_CONFIG_FILE, "down"])
    with allure.step("Docker-compose up"):
        subprocess.run(["docker-compose", "-f", DOCKER_COMPOSE_STORAGE_CONFIG_FILE, "up", "-d"])
    wait_all_storage_nodes_returned(cluster)
    with allure.step("Log resync status"):
        for node in cluster.storage_nodes:
            envs = subprocess.run(
                [
                    "docker",
                    "inspect",
                    "-f",
                    "'{{range $index, $value := .Config.Env}}{{$value}} " "{{end}}'",
                    node.name,
                ],
                capture_output=True,
            )
            env_stdout = envs.stdout.decode("utf-8")
            logger.debug(f"ENV from {node.name}: {env_stdout}")


@pytest.fixture(scope="function")
@allure.title("Enable metabase resync on start")
def enable_metabase_resync_on_start(cluster: Cluster):
    """
    If there were already any environment variables in the DOCKER_COMPOSE_ENV_FILE, they should be retained and
    NEOFS_STORAGE_SHARD_0_RESYNC_METABASE and NEOFS_STORAGE_SHARD_1_RESYNC_METABASE should be added to the file.

    If NEOFS_STORAGE_SHARD_0_RESYNC_METABASE and NEOFS_STORAGE_SHARD_1_RESYNC_METABASE are explicitly specified
    as false, they must be changed to true.

    If DOCKER_COMPOSE_ENV_FILE is empty, NEOFS_STORAGE_SHARD_0_RESYNC_METABASE and
    NEOFS_STORAGE_SHARD_1_RESYNC_METABASE must be added to DOCKER_COMPOSE_ENV_FILE.

    Of course, after the test, the DOCKER_COMPOSE_ENV_FILE must return to its initial state.
    """
    file_path = DOCKER_COMPOSE_ENV_FILE
    if not os.path.exists(file_path):
        pytest.fail(f"File {file_path} does not exist!")

    with open(file_path, "r") as file:
        lines = file.readlines()
    logger.debug(f"Initial file content:\n{''.join(lines)}")

    replacements = {
        "NEOFS_STORAGE_SHARD_0_RESYNC_METABASE=false": "NEOFS_STORAGE_SHARD_0_RESYNC_METABASE=true\n",
        "NEOFS_STORAGE_SHARD_1_RESYNC_METABASE=false": "NEOFS_STORAGE_SHARD_1_RESYNC_METABASE=true\n",
    }

    unprocessed_lines = set(replacements.values())

    modified_lines = []

    for line in lines:
        for original, new in replacements.items():
            if original in line:
                line = line.replace(original, new)
                unprocessed_lines.discard(new)
        modified_lines.append(line)

    modified_lines.extend(unprocessed_lines)

    modified_content = "".join(modified_lines)

    with open(file_path, "w") as file:
        file.write(modified_content)
    logger.debug(f"Modified file content:\n{modified_content}")

    with allure.step("Restart docker compose to apply the changes"):
        docker_compose_restart_storage_nodes(cluster)

    yield

    with open(file_path, "w") as file:
        file.writelines(lines)
    logger.debug(f"Restored file content:\n{''.join(lines)}")

    with allure.step("Restart docker compose to revert the changes"):
        docker_compose_restart_storage_nodes(cluster)

    with allure.step(f"Waiting {METABASE_RESYNC_TIMEOUT} seconds for the metabase to synchronize"):
        sleep(parse_time(METABASE_RESYNC_TIMEOUT))
