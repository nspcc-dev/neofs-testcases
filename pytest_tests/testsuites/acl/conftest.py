from dataclasses import dataclass
from typing import Dict, List, Optional

import allure
import pytest

from common import ASSETS_DIR, IR_WALLET_CONFIG, IR_WALLET_PATH, WALLET_CONFIG
from common import STORAGE_WALLET_PATH, STORAGE_WALLET_CONFIG
from python_keywords.acl import EACLRole
from python_keywords.container import create_container
from python_keywords.neofs_verbs import put_object
from python_keywords.utility_keywords import generate_file
from wallet import init_wallet
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
    yield Wallets(
        wallets={
            EACLRole.USER: [
                Wallet(
                    wallet_path=prepare_wallet_and_deposit, config_path=WALLET_CONFIG
                )
            ],
            EACLRole.OTHERS: [
                Wallet(
                    wallet_path=init_wallet(ASSETS_DIR)[0], config_path=WALLET_CONFIG
                ),
                Wallet(
                    wallet_path=init_wallet(ASSETS_DIR)[0], config_path=WALLET_CONFIG
                ),
            ],
            EACLRole.SYSTEM: [
                Wallet(wallet_path=IR_WALLET_PATH, config_path=IR_WALLET_CONFIG),
                Wallet(wallet_path=STORAGE_WALLET_PATH, config_path=STORAGE_WALLET_CONFIG)
            ],
        }
    )


@pytest.fixture(scope="module")
def file_path():
    yield generate_file()


@pytest.fixture(scope="function")
def eacl_container_with_objects(wallets, file_path):
    user_wallet = wallets.get_wallet()
    with allure.step("Create eACL public container"):
        cid = create_container(user_wallet.wallet_path, basic_acl=PUBLIC_ACL)

    with allure.step("Add test objects to container"):
        objects_oids = [
            put_object(
                user_wallet.wallet_path,
                file_path,
                cid,
                attributes={"key1": "val1", "key": val, "key2": "abc"},
            )
            for val in range(OBJECT_COUNT)
        ]

    yield cid, objects_oids, file_path

    # with allure.step('Delete eACL public container'):
    #     delete_container(user_wallet, cid)
