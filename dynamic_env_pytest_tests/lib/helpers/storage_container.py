from dataclasses import dataclass
from typing import Optional

import allure
from file_helper import generate_file, get_file_hash
from neofs_testlib.env.env import NeoFSEnv, NodeWallet
from neofs_testlib.shell import Shell
from neofs_verbs import put_object, put_object_to_random_node
from storage_object import StorageObjectInfo


@dataclass
class StorageContainerInfo:
    cid: str
    wallet: NodeWallet
    wallet_config_path: str = None

    def get_wallet_config_path(self, neofs_env: NeoFSEnv):
        if not self.wallet_config_path:
            self.wallet_config_path = neofs_env.generate_cli_config(self.wallet)
        return self.wallet_config_path


class StorageContainer:
    def __init__(
        self,
        storage_container_info: StorageContainerInfo,
        shell: Shell,
        neofs_env: NeoFSEnv,
    ) -> None:
        self.shell = shell
        self.storage_container_info = storage_container_info
        self.neofs_env = neofs_env

    def get_id(self) -> str:
        return self.storage_container_info.cid

    def get_wallet_path(self) -> str:
        return self.storage_container_info.wallet.path

    def get_wallet_config_path(self) -> str:
        return self.storage_container_info.get_wallet_config_path(self.neofs_env)

    @allure.step("Generate new object and put in container")
    def generate_object(
        self,
        size: int,
        expire_at: Optional[int] = None,
        bearer_token: Optional[str] = None,
        endpoint: Optional[str] = None,
    ) -> StorageObjectInfo:
        with allure.step(f"Generate object with size {size}"):
            file_path = generate_file(size)
            file_hash = get_file_hash(file_path)

        container_id = self.get_id()
        wallet_path = self.get_wallet_path()
        wallet_config = self.get_wallet_config_path()
        with allure.step(f"Put object with size {size} to container {container_id}"):
            if endpoint:
                object_id = put_object(
                    wallet=wallet_path,
                    path=file_path,
                    cid=container_id,
                    expire_at=expire_at,
                    shell=self.shell,
                    endpoint=endpoint,
                    bearer=bearer_token,
                    wallet_config=wallet_config,
                )
            else:
                object_id = put_object_to_random_node(
                    wallet=wallet_path,
                    path=file_path,
                    cid=container_id,
                    expire_at=expire_at,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                    bearer=bearer_token,
                    wallet_config=wallet_config,
                )
            with allure.step(f"Store object with size {size} to container {container_id}"):
                storage_object = StorageObjectInfo(
                    container_id,
                    object_id,
                    size=size,
                    wallet_file_path=wallet_path,
                    file_path=file_path,
                    file_hash=file_hash,
                )

        return storage_object
