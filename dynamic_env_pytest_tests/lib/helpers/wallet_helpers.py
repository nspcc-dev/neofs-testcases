import os
import uuid
from typing import Optional

import allure
from common import ASSETS_DIR
from neofs_testlib.env.env import NodeWallet
from neofs_testlib.utils.wallet import init_wallet


@allure.title("Prepare wallet and deposit")
def create_wallet(name: Optional[str] = None) -> NodeWallet:
    if name is None:
        wallet_name = f"{str(uuid.uuid4())}.json"
    else:
        wallet_name = f"{name}.json"

    wallet_path = os.path.join(os.getcwd(), ASSETS_DIR, wallet_name)
    wallet_password = "password"
    wallet_address = init_wallet(wallet_path, wallet_password)

    allure.attach.file(wallet_path, os.path.basename(wallet_path), allure.attachment_type.JSON)

    return NodeWallet(path=wallet_path, address=wallet_address, password=wallet_password)
