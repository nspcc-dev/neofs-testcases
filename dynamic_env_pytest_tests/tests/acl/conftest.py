from dataclasses import dataclass
from typing import Optional

import allure
import pytest
from neofs_testlib.env.env import NeoFSEnv
from neofs_testlib.shell import Shell
from python_keywords.acl import EACLRole
from python_keywords.container import create_container
from python_keywords.neofs_verbs import put_object_to_random_node
from wellknown_acl import PUBLIC_ACL

from helpers.wallet_helpers import create_wallet

OBJECT_COUNT = 5


@dataclass
class Wallet:
    wallet_path: Optional[str] = None
    config_path: Optional[str] = None


@dataclass
class Wallets:
    wallets: dict[EACLRole, list[Wallet]]

    def get_wallet(self, role: EACLRole = EACLRole.USER) -> Wallet:
        return self.wallets[role][0]

    def get_wallets_list(self, role: EACLRole = EACLRole.USER) -> list[Wallet]:
        return self.wallets[role]

    def get_ir_wallet(self) -> Wallet:
        return self.wallets[EACLRole.SYSTEM][0]

    def get_storage_wallet(self) -> Wallet:
        return self.wallets[EACLRole.SYSTEM][1]


@pytest.fixture(scope="module")
def wallets(default_wallet, temp_directory, neofs_env: NeoFSEnv) -> Wallets:
    default_wallet_config_path = NeoFSEnv._generate_temp_file(extension="yml")
    NeoFSEnv.generate_config_file(
        config_template="cli_cfg.yaml",
        config_path=default_wallet_config_path,
        wallet=default_wallet,
    )

    other_wallet1 = create_wallet()
    other_wallet1_config_path = NeoFSEnv._generate_temp_file(extension="yml")
    NeoFSEnv.generate_config_file(
        config_template="cli_cfg.yaml", config_path=other_wallet1_config_path, wallet=other_wallet1
    )

    other_wallet2 = create_wallet()
    other_wallet2_config_path = NeoFSEnv._generate_temp_file(extension="yml")
    NeoFSEnv.generate_config_file(
        config_template="cli_cfg.yaml", config_path=other_wallet2_config_path, wallet=other_wallet2
    )

    ir_node = neofs_env.inner_ring_nodes[0]
    storage_node = neofs_env.storage_nodes[0]

    ir_wallet_path = ir_node.alphabet_wallet.path
    ir_wallet_config = ir_node.cli_config

    storage_wallet_path = storage_node.wallet.path
    storage_wallet_config = storage_node.cli_config

    yield Wallets(
        wallets={
            EACLRole.USER: [
                Wallet(wallet_path=default_wallet.path, config_path=default_wallet_config_path)
            ],
            EACLRole.OTHERS: [
                Wallet(wallet_path=other_wallet1.path, config_path=other_wallet1_config_path),
                Wallet(wallet_path=other_wallet2.path, config_path=other_wallet2_config_path),
            ],
            EACLRole.SYSTEM: [
                Wallet(wallet_path=ir_wallet_path, config_path=ir_wallet_config),
                Wallet(wallet_path=storage_wallet_path, config_path=storage_wallet_config),
            ],
        }
    )


@pytest.fixture(scope="function")
def eacl_container_with_objects(
    wallets: Wallets, client_shell: Shell, neofs_env: NeoFSEnv, file_path: str
):
    user_wallet = wallets.get_wallet()
    with allure.step("Create eACL public container"):
        cid = create_container(
            user_wallet.wallet_path,
            basic_acl=PUBLIC_ACL,
            shell=client_shell,
            endpoint=neofs_env.sn_rpc,
        )

    with allure.step("Add test objects to container"):
        objects_oids = [
            put_object_to_random_node(
                user_wallet.wallet_path,
                file_path,
                cid,
                attributes={"key1": "val1", "key": val, "key2": "abc"},
                shell=client_shell,
                neofs_env=neofs_env,
            )
            for val in range(OBJECT_COUNT)
        ]

    yield cid, objects_oids, file_path
