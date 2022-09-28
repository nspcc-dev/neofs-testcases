import json
import logging
import os
import re
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Optional

import docker
from cli_helpers import _cmd_run
from common import (
    INFRASTRUCTURE_TYPE,
    NEOFS_CLI_EXEC,
    NEOFS_NETMAP_DICT,
    STORAGE_NODE_BIN_PATH,
    STORAGE_NODE_SSH_PASSWORD,
    STORAGE_NODE_SSH_PRIVATE_KEY_PATH,
    STORAGE_NODE_SSH_USER,
    WALLET_CONFIG,
)
from requests import HTTPError
from ssh_helper import HostClient

logger = logging.getLogger("NeoLogger")


class LocalDevEnvStorageServiceHelper:
    """
    Manages storage services running on local devenv.
    """

    # Names of all containers that are running neofs code
    ALL_CONTAINERS = [
        "s3_gate",
        "http_gate",
        "s03",
        "s01",
        "s02",
        "s04",
        "ir01",
        "morph_chain",
        "main_chain",
    ]

    def stop_node(self, node_name: str, wait: bool = True) -> None:
        container_name = _get_storage_container_name(node_name)
        client = self._get_docker_client(node_name)
        client.stop(container_name)

        if wait:
            self._wait_for_container_to_be_in_state(node_name, container_name, "exited")

    def start_node(self, node_name: str, wait: bool = True) -> None:
        container_name = _get_storage_container_name(node_name)
        client = self._get_docker_client(node_name)
        client.start(container_name)

        if wait:
            self._wait_for_container_to_be_in_state(node_name, container_name, "running")

    def run_control_command(self, node_name: str, command: str) -> str:
        control_endpoint = NEOFS_NETMAP_DICT[node_name]["control"]
        wallet_path = NEOFS_NETMAP_DICT[node_name]["wallet_path"]

        cmd = (
            f"{NEOFS_CLI_EXEC} {command} --endpoint {control_endpoint} "
            f"--wallet {wallet_path} --config {WALLET_CONFIG}"
        )
        output = _cmd_run(cmd)
        return output

    def delete_node_data(self, node_name: str) -> None:
        volume_name = _get_storage_volume_name(node_name)

        client = self._get_docker_client(node_name)
        volume_info = client.inspect_volume(volume_name)
        volume_path = volume_info["Mountpoint"]

        _cmd_run(f"rm -rf {volume_path}/*")

    def get_binaries_version(self) -> dict:
        return {}

    def dump_logs(
        self, directory_path: str, since: Optional[datetime], until: Optional[datetime]
    ) -> None:
        # All containers are running on the same host, so we can use 1st node to collect all logs
        first_node_name = next(iter(NEOFS_NETMAP_DICT))
        client = self._get_docker_client(first_node_name)

        for container_name in self.ALL_CONTAINERS:
            try:
                logs = client.logs(container_name, since=since, until=until)
            except HTTPError as exc:
                logger.info(f"Got exception while dumping container '{container_name}' logs: {exc}")
                continue
            # Dump logs to the directory
            file_path = os.path.join(directory_path, f"{container_name}-log.txt")
            with open(file_path, "wb") as file:
                file.write(logs)

    def _get_container_by_name(self, node_name: str, container_name: str) -> dict:
        client = self._get_docker_client(node_name)
        containers = client.containers(all=True)

        logger.info(f"Current containers state\n:{json.dumps(containers, indent=2)}")

        for container in containers:
            # Names in local docker environment are prefixed with /
            clean_names = set(name.strip("/") for name in container["Names"])
            if container_name in clean_names:
                return container
        return None

    def _wait_for_container_to_be_in_state(
        self, node_name: str, container_name: str, expected_state: str
    ) -> None:
        for __attempt in range(10):
            container = self._get_container_by_name(node_name, container_name)
            logger.info(f"Container info:\n{json.dumps(container, indent=2)}")
            if container and container["State"] == expected_state:
                return
            time.sleep(5)

        raise AssertionError(f"Container {container_name} is not in {expected_state} state.")

    def _get_docker_client(self, node_name: str) -> docker.APIClient:
        # For local docker we use default docker client that talks to unix socket
        client = docker.APIClient()
        return client


class CloudVmStorageServiceHelper:
    STORAGE_SERVICE = "neofs-storage.service"

    def stop_node(self, node_name: str, wait: bool = True) -> None:
        with _create_ssh_client(node_name) as ssh_client:
            cmd = f"sudo systemctl stop {self.STORAGE_SERVICE}"
            output = ssh_client.exec_with_confirmation(cmd, [""])
            logger.info(f"Stop command output: {output.stdout}")

        if wait:
            self._wait_for_service_to_be_in_state(node_name, self.STORAGE_SERVICE, "inactive")

    def start_node(self, node_name: str, wait: bool = True) -> None:
        with _create_ssh_client(node_name) as ssh_client:
            cmd = f"sudo systemctl start {self.STORAGE_SERVICE}"
            output = ssh_client.exec_with_confirmation(cmd, [""])
            logger.info(f"Start command output: {output.stdout}")

        if wait:
            self._wait_for_service_to_be_in_state(
                node_name, self.STORAGE_SERVICE, "active (running)"
            )

    def run_control_command(self, node_name: str, command: str) -> str:
        control_endpoint = NEOFS_NETMAP_DICT[node_name]["control"]
        wallet_path = NEOFS_NETMAP_DICT[node_name]["wallet_path"]

        # Private control endpoint is accessible only from the host where storage node is running
        # So, we connect to storage node host and run CLI command from there
        with _create_ssh_client(node_name) as ssh_client:
            # Copy wallet content on storage node host
            with open(wallet_path, "r") as file:
                wallet = file.read()
            remote_wallet_path = f"/tmp/{node_name}-wallet.json"
            ssh_client.exec_with_confirmation(f"echo '{wallet}' > {remote_wallet_path}", [""])

            # Put config on storage node host
            remote_config_path = f"/tmp/{node_name}-config.yaml"
            remote_config = 'password: ""'
            ssh_client.exec_with_confirmation(
                f"echo '{remote_config}' > {remote_config_path}", [""]
            )

            # Execute command
            cmd = (
                f"sudo {STORAGE_NODE_BIN_PATH}/neofs-cli {command} --endpoint {control_endpoint} "
                f"--wallet {remote_wallet_path} --config {remote_config_path}"
            )
            output = ssh_client.exec_with_confirmation(cmd, [""])
            return output.stdout

    def _wait_for_service_to_be_in_state(
        self, node_name: str, service_name: str, expected_state: str
    ) -> None:
        with _create_ssh_client(node_name) as ssh_client:
            for __attempt in range(10):
                # Run command to get service status (set --lines=0 to suppress logs output)
                # Also we don't verify return code, because for an inactive service return code will be 3
                command = f"sudo systemctl status {service_name} --lines=0"
                output = ssh_client.exec(command, verify=False)
                if expected_state in output.stdout:
                    return
                time.sleep(3)
        raise AssertionError(f"Service {service_name} is not in {expected_state} state")

    def delete_node_data(self, node_name: str) -> None:
        with _create_ssh_client(node_name) as ssh_client:
            ssh_client.exec("sudo rm -rf /srv/neofs/*")

    def get_binaries_version(self, binaries: list = None) -> dict:
        default_binaries = [
            "neo-go",
            "neofs-adm",
            "neofs-cli",
            "neofs-http-gw",
            "neofs-ir",
            "neofs-lens",
            "neofs-node",
            "neofs-s3-authmate",
            "neofs-s3-gw",
            "neogo-morph-cn",
        ]
        binaries = binaries or default_binaries

        version_map = {}
        for node_name in NEOFS_NETMAP_DICT:
            with _create_ssh_client(node_name) as ssh_client:
                for binary in binaries:
                    try:
                        out = ssh_client.exec(f"sudo {binary} --version").stdout
                    except AssertionError as err:
                        logger.error(f"Can not get version for {binary} because of\n{err}")
                        version_map[binary] = "Can not get version"
                        continue
                    version = re.search(r"version[:\s]*v?(.+)", out, re.IGNORECASE)
                    version = version.group(1).strip() if version else "Unknown"
                    if not version_map.get(binary):
                        version_map[binary] = version
                    else:
                        assert version_map[binary] == version, (
                            f"Expected binary {binary} to have identical version on all nodes "
                            f"(mismatch on node {node_name})"
                        )
        return version_map

    def dump_logs(
        self, directory_path: str, since: Optional[datetime], until: Optional[datetime]
    ) -> None:
        for node_name, node_info in NEOFS_NETMAP_DICT.items():
            with _create_ssh_client(node_name) as ssh_client:
                # We do not filter out logs of neofs services, because system logs might contain
                # information that is useful for troubleshooting
                filters = " ".join(
                    [
                        f"--since '{since:%Y-%m-%d %H:%M:%S}'" if since else "",
                        f"--until '{until:%Y-%m-%d %H:%M:%S}'" if until else "",
                    ]
                )
                result = ssh_client.exec(f"journalctl --no-pager {filters}")
                logs = result.stdout

                # Dump logs to the directory. We include node endpoint in file name, because almost
                # everywhere in Allure report we are logging endpoints rather than node names
                file_path = os.path.join(directory_path, f"{node_name}-{node_info['rpc']}-log.txt")
                with open(file_path, "w") as file:
                    file.write(logs)


class RemoteDevEnvStorageServiceHelper(LocalDevEnvStorageServiceHelper):
    """
    Manages storage services running on remote devenv.

    Most of operations are identical to local devenv, however, any interactions
    with host resources (files, etc.) require ssh into the remote host machine.
    """

    def _get_docker_client(self, node_name: str) -> docker.APIClient:
        # For remote devenv we use docker client that talks to tcp socket 2375:
        # https://docs.docker.com/engine/reference/commandline/dockerd/#daemon-socket-option
        host = _get_node_host(node_name)
        client = docker.APIClient(base_url=f"tcp://{host}:2375")
        return client

    def delete_node_data(self, node_name: str) -> None:
        volume_name = _get_storage_volume_name(node_name)

        client = self._get_docker_client(node_name)
        volume_info = client.inspect_volume(volume_name)
        volume_path = volume_info["Mountpoint"]

        # SSH into remote machine and delete files in host directory that is mounted as docker volume
        with _create_ssh_client(node_name) as ssh_client:
            # TODO: add sudo prefix after we change a user
            ssh_client.exec(f"rm -rf {volume_path}/*")


def get_storage_service_helper():
    if INFRASTRUCTURE_TYPE == "LOCAL_DEVENV":
        return LocalDevEnvStorageServiceHelper()
    if INFRASTRUCTURE_TYPE == "REMOTE_DEVENV":
        return RemoteDevEnvStorageServiceHelper()
    if INFRASTRUCTURE_TYPE == "CLOUD_VM":
        return CloudVmStorageServiceHelper()

    raise EnvironmentError(f"Infrastructure type is not supported: {INFRASTRUCTURE_TYPE}")


@contextmanager
def _create_ssh_client(node_name: str) -> HostClient:
    host = _get_node_host(node_name)
    ssh_client = HostClient(
        host,
        login=STORAGE_NODE_SSH_USER,
        password=STORAGE_NODE_SSH_PASSWORD,
        private_key_path=STORAGE_NODE_SSH_PRIVATE_KEY_PATH,
    )

    try:
        yield ssh_client
    finally:
        ssh_client.drop()


def _get_node_host(node_name: str) -> str:
    if node_name not in NEOFS_NETMAP_DICT:
        raise AssertionError(f"Node {node_name} is not found!")

    # We use rpc endpoint to determine host address, because control endpoint
    # (if it is private) will be a local address on the host machine
    node_config = NEOFS_NETMAP_DICT.get(node_name)
    host = node_config.get("rpc").split(":")[0]
    return host


def _get_storage_container_name(node_name: str) -> str:
    """
    Converts name of storage node (as it is listed in netmap) into the name of docker container
    that runs instance of this storage node.
    """
    return node_name.split(".")[0]


def _get_storage_volume_name(node_name: str) -> str:
    """
    Converts name of storage node (as it is listed in netmap) into the name of docker volume
    that contains data of this storage node.
    """
    container_name = _get_storage_container_name(node_name)
    return f"storage_storage_{container_name}"
