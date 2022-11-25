from dataclasses import dataclass
from typing import Optional

import allure
from file_helper import generate_file, get_file_hash
from neofs_testlib.shell import Shell
from neofs_verbs import put_object
from storage_object import StorageObjectInfo
from wallet import WalletFile


@dataclass
class StorageContainerInfo:
    id: str
    wallet_file: WalletFile


class StorageContainer:
    def __init__(self, storage_container_info: StorageContainerInfo, shell: Shell) -> None:
        self.shell = shell
        self.storage_container_info = storage_container_info

    def get_id(self) -> str:
        return self.storage_container_info.id

    def get_wallet_path(self) -> str:
        return self.storage_container_info.wallet_file.path

    @allure.step("Generate new object and put in container")
    def generate_object(self, size: int, expire_at: Optional[int] = None) -> StorageObjectInfo:
        with allure.step(f"Generate object with size {size}"):
            file_path = generate_file(size)
            file_hash = get_file_hash(file_path)

        container_id = self.get_id()
        wallet_path = self.get_wallet_path()

        with allure.step(f"Put object with size {size} to container {container_id}"):
            object_id = put_object(
                wallet=wallet_path,
                path=file_path,
                cid=container_id,
                expire_at=expire_at,
                shell=self.shell,
            )

            storage_object = StorageObjectInfo(
                container_id,
                object_id,
                size=size,
                wallet_file_path=wallet_path,
                file_path=file_path,
                file_hash=file_hash,
            )

        return storage_object
