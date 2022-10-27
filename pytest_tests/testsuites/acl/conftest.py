import os
import uuid
from dataclasses import dataclass
from typing import Dict, List, Optional

import allure
import pytest
from common import (
    ASSETS_DIR,
    IR_WALLET_CONFIG,
    IR_WALLET_PATH,
    STORAGE_WALLET_CONFIG,
    STORAGE_WALLET_PATH,
    WALLET_CONFIG,
    WALLET_PASS,
)
from file_helper import generate_file
from neofs_testlib.utils.wallet import init_wallet
from python_keywords.acl import EACLRole
from python_keywords.container import create_container
from python_keywords.neofs_verbs import put_object
from wellknown_acl import PUBLIC_ACL

OBJECT_COUNT = 5


@dataclass
class Wallet:
    wallet_path: Optional[str] = None
    config_path: Optional[str] = None


@dataclass
class Wallets:
    wallets: Dict[EACLRole, List[Wallet]]

    def get_wallet(self, role: EACLRole = EACLRole.USER) -> Wallet:
        return self.wallets[role][0]

    def get_wallets_list(self, role: EACLRole = EACLRole.USER) -> List[Wallet]:
        return self.wallets[role]


@pytest.fixture(scope="module")
def wallets(prepare_wallet_and_deposit):
    other_wallets_paths = [
        os.path.join(os.getcwd(), ASSETS_DIR, f"{str(uuid.uuid4())}.json") for _ in range(2)
    ]
    for other_wallet_path in other_wallets_paths:
        init_wallet(other_wallet_path, WALLET_PASS)

    yield Wallets(
        wallets={
            EACLRole.USER: [
                Wallet(wallet_path=prepare_wallet_and_deposit, config_path=WALLET_CONFIG)
            ],
            EACLRole.OTHERS: [
                Wallet(wallet_path=other_wallet_path, config_path=WALLET_CONFIG)
                for other_wallet_path in other_wallets_paths
            ],
            EACLRole.SYSTEM: [
                Wallet(wallet_path=IR_WALLET_PATH, config_path=IR_WALLET_CONFIG),
                Wallet(wallet_path=STORAGE_WALLET_PATH, config_path=STORAGE_WALLET_CONFIG),
            ],
        }
    )


@pytest.fixture(scope="module")
def file_path():
    yield generate_file()


@pytest.fixture(scope="function")
def eacl_container_with_objects(wallets, client_shell, file_path):
    user_wallet = wallets.get_wallet()
    with allure.step("Create eACL public container"):
        cid = create_container(user_wallet.wallet_path, basic_acl=PUBLIC_ACL, shell=client_shell)

    with allure.step("Add test objects to container"):
        objects_oids = [
            put_object(
                user_wallet.wallet_path,
                file_path,
                cid,
                attributes={"key1": "val1", "key": val, "key2": "abc"},
                shell=client_shell,
            )
            for val in range(OBJECT_COUNT)
        ]

    yield cid, objects_oids, file_path

    # with allure.step('Delete eACL public container'):
    #     delete_container(user_wallet, cid)
