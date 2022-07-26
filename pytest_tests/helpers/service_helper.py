from contextlib import contextmanager
import logging

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
        container_name = node_name.split('.')[0]
        client = docker.APIClient()
        client.stop(container_name)

    def start_node(self, node_name: str) -> None:
        container_name = node_name.split('.')[0]
        client = docker.APIClient()
        client.start(container_name)

    def run_control_command(self, node_name: str, command: str) -> str:
        control_endpoint = NEOFS_NETMAP_DICT[node_name]["control"]
        wallet_path = NEOFS_NETMAP_DICT[node_name]["wallet_path"]

        cmd = (
            f'{NEOFS_CLI_EXEC} {command} --endpoint {control_endpoint} '
            f'--wallet {wallet_path} --config {WALLET_CONFIG}'
        )
        output = _cmd_run(cmd)
        return output


class CloudVmStorageServiceHelper:
    def stop_node(self, node_name: str) -> None:
        with _create_ssh_client(node_name) as ssh_client:
            cmd = "systemctl stop neofs-storage"
            output = ssh_client.exec_with_confirmation(cmd, [""])
            logger.info(f"Stop command output: {output.stdout}")

    def start_node(self, node_name: str) -> None:
        with _create_ssh_client(node_name) as ssh_client:
            cmd = "systemctl start neofs-storage"
            output = ssh_client.exec_with_confirmation(cmd, [""])
            logger.info(f"Start command output: {output.stdout}")

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


class RemoteDevEnvStorageServiceHelper:
    """
    Manages storage services running on remote devenv.
    """
    def stop_node(self, node_name: str) -> None:
        container_name = node_name.split('.')[0]
        with _create_ssh_client(node_name) as ssh_client:
            ssh_client.exec(f'docker stop {container_name}')

    def start_node(self, node_name: str) -> None:
        container_name = node_name.split('.')[0]
        with _create_ssh_client(node_name) as ssh_client:
            ssh_client.exec(f'docker start {container_name}')

    def run_control_command(self, node_name: str, command: str) -> str:
        # On remote devenv it works same way as in cloud
        return CloudVmStorageServiceHelper().run_control_command(node_name, command)


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
