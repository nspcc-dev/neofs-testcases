import json
import logging
import re
import time
from contextlib import contextmanager

import docker

from cli_helpers import _cmd_run
from common import (INFRASTRUCTURE_TYPE, NEOFS_CLI_EXEC, NEOFS_NETMAP_DICT, STORAGE_NODE_BIN_PATH,
                    STORAGE_NODE_SSH_PASSWORD, STORAGE_NODE_SSH_PRIVATE_KEY_PATH,
                    STORAGE_NODE_SSH_USER, WALLET_CONFIG)
from ssh_helper import HostClient


logger = logging.getLogger('NeoLogger')


class LocalDevEnvStorageServiceHelper:
    """
    Manages storage services running on local devenv.
    """
    def stop_node(self, node_name: str) -> None:
        container_name = _get_storage_container_name(node_name)
        client = docker.APIClient()
        client.stop(container_name)

    def start_node(self, node_name: str) -> None:
        container_name = _get_storage_container_name(node_name)
        client = docker.APIClient()
        client.start(container_name)

    def wait_for_node_to_start(self, node_name: str) -> None:
        container_name = _get_storage_container_name(node_name)
        expected_state = "running"
        for __attempt in range(10):
            container = self._get_container_by_name(container_name)
            if container and container["State"] == expected_state:
                return
            time.sleep(3)
        raise AssertionError(f'Container {container_name} is not in {expected_state} state')

    def run_control_command(self, node_name: str, command: str) -> str:
        control_endpoint = NEOFS_NETMAP_DICT[node_name]["control"]
        wallet_path = NEOFS_NETMAP_DICT[node_name]["wallet_path"]

        cmd = (
            f'{NEOFS_CLI_EXEC} {command} --endpoint {control_endpoint} '
            f'--wallet {wallet_path} --config {WALLET_CONFIG}'
        )
        output = _cmd_run(cmd)
        return output

    def destroy_node(self, node_name: str) -> None:
        container_name = _get_storage_container_name(node_name)
        client = docker.APIClient()
        client.remove_container(container_name, force=True)
    
    def get_binaries_version(self) -> dict:
        return {}

    def _get_container_by_name(self, container_name: str) -> dict:
        client = docker.APIClient()
        containers = client.containers()
        for container in containers:
            if container_name in container["Names"]:
                return container
        return None


class CloudVmStorageServiceHelper:
    STORAGE_SERVICE = "neofs-storage.service"

    def stop_node(self, node_name: str) -> None:
        with _create_ssh_client(node_name) as ssh_client:
            cmd = f"systemctl stop {self.STORAGE_SERVICE}"
            output = ssh_client.exec_with_confirmation(cmd, [""])
            logger.info(f"Stop command output: {output.stdout}")

    def start_node(self, node_name: str) -> None:
        with _create_ssh_client(node_name) as ssh_client:
            cmd = f"systemctl start {self.STORAGE_SERVICE}"
            output = ssh_client.exec_with_confirmation(cmd, [""])
            logger.info(f"Start command output: {output.stdout}")

    def wait_for_node_to_start(self, node_name: str) -> None:
        expected_state = 'active (running)'
        with _create_ssh_client(node_name) as ssh_client:
            for __attempt in range(10):
                output = ssh_client.exec(f'systemctl status {self.STORAGE_SERVICE}')
                if expected_state in output.stdout:
                    return
                time.sleep(3)
            raise AssertionError(
                f'Service {self.STORAGE_SERVICE} is not in {expected_state} state'
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
            remote_wallet_path = "/tmp/{node_name}-wallet.json"
            ssh_client.exec_with_confirmation(f"echo '{wallet}' > {remote_wallet_path}", [""])

            # Put config on storage node host
            remote_config_path = "/tmp/{node_name}-config.yaml"
            remote_config = 'password: ""'
            ssh_client.exec_with_confirmation(f"echo '{remote_config}' > {remote_config_path}", [""])

            # Execute command
            cmd = (
                f'{STORAGE_NODE_BIN_PATH}/neofs-cli {command} --endpoint {control_endpoint} '
                f'--wallet {remote_wallet_path} --config {remote_config_path}'
            )
            output = ssh_client.exec_with_confirmation(cmd, [""])
            return output.stdout

    def destroy_node(self, node_name: str) -> None:
        with _create_ssh_client(node_name) as ssh_client:
            ssh_client.exec(f'systemctl stop {self.STORAGE_SERVICE}')
            ssh_client.exec('rm -rf /srv/neofs/*')

    def get_binaries_version(self, binaries: list = None) -> dict:
        default_binaries = [
            'neo-go',
            'neofs-adm',
            'neofs-cli',
            'neofs-http-gw',
            'neofs-ir',
            'neofs-lens',
            'neofs-node',
            'neofs-s3-authmate',
            'neofs-s3-gw',
            'neogo-morph-cn',
        ]
        binaries = binaries or default_binaries

        version_map = {}
        for node_name in NEOFS_NETMAP_DICT:
            with _create_ssh_client(node_name) as ssh_client:
                for binary in binaries:
                    try:
                        out = ssh_client.exec(f'{binary} --version').stdout
                    except AssertionError as err:
                        logger.error(f'Can not get version for {binary} because of\n{err}')
                        version_map[binary] = 'Can not get version'
                        continue
                    version = re.search(r'version[:\s]*v?(.+)', out, re.IGNORECASE)
                    version = version.group(1).strip() if version else 'Unknown'
                    if not version_map.get(binary):
                        version_map[binary] = version
                    else:
                        assert version_map[binary] == version, \
                            f'Expected binary {binary} to have identical version on all nodes ' \
                            f'(mismatch on node {node_name})'
        return version_map

class RemoteDevEnvStorageServiceHelper:
    """
    Manages storage services running on remote devenv.
    """
    def stop_node(self, node_name: str) -> None:
        container_name = _get_storage_container_name(node_name)
        with _create_ssh_client(node_name) as ssh_client:
            ssh_client.exec(f'docker stop {container_name}')

    def start_node(self, node_name: str) -> None:
        container_name = _get_storage_container_name(node_name)
        with _create_ssh_client(node_name) as ssh_client:
            ssh_client.exec(f'docker start {container_name}')

    def wait_for_node_to_start(self, node_name: str) -> None:
        container_name = _get_storage_container_name(node_name)
        expected_state = 'running'
        for __attempt in range(10):
            container = self._get_container_by_name(container_name)
            if container and container["State"] == expected_state:
                return
            time.sleep(3)
        raise AssertionError(f'Container {container_name} is not in {expected_state} state')

    def run_control_command(self, node_name: str, command: str) -> str:
        # On remote devenv it works same way as in cloud
        return CloudVmStorageServiceHelper().run_control_command(node_name, command)

    def destroy_node(self, node_name: str) -> None:
        container_name = _get_storage_container_name(node_name)
        with _create_ssh_client(node_name) as ssh_client:
            ssh_client.exec(f'docker rm {container_name} --force')

    def get_binaries_version(self) -> dict:
        return {}

    def _get_container_by_name(self, node_name: str, container_name: str) -> dict:
        with _create_ssh_client(node_name) as ssh_client:
            output = ssh_client.exec('docker ps -a --format "{{json .}}"')
            containers = json.loads(output)

        for container in containers:
            # unlike docker.API in docker ps output Names seems to be a string, so we check by equality
            if container["Names"] == container_name:
                return container
        return None


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
    if node_name not in NEOFS_NETMAP_DICT:
        raise AssertionError(f'Node {node_name} is not found!')

    # We use rpc endpoint to determine host address, because control endpoint
    # (if it is private) will be a local address on the host machine
    node_config = NEOFS_NETMAP_DICT.get(node_name)
    host = node_config.get('rpc').split(':')[0]
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


def _get_storage_container_name(node_name: str) -> str:
    """
    Converts name of storage name (as it is listed in netmap) into the name of docker container
    that runs instance of this storage node.
    """
    return node_name.split('.')[0]
