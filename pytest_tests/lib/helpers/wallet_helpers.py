import os
import uuid
from typing import Optional

import allure
from helpers.common import get_assets_dir_path
from helpers.test_control import wait_for_success
from neofs_testlib.cli import NeofsCli, NeoGo
from neofs_testlib.env.env import NeoFSEnv, NodeWallet
from neofs_testlib.utils.wallet import get_last_address_from_wallet, init_wallet


@allure.title("Prepare wallet and deposit")
def create_wallet(name: Optional[str] = None) -> NodeWallet:
    if name is None:
        wallet_name = f"wallet-{str(uuid.uuid4())}.json"
    else:
        wallet_name = f"{name}.json"

    wallet_path = os.path.join(get_assets_dir_path(), wallet_name)
    wallet_password = "password"
    wallet_address = init_wallet(wallet_path, wallet_password)

    allure.attach.file(wallet_path, os.path.basename(wallet_path), allure.attachment_type.JSON)

    return NodeWallet(path=wallet_path, address=wallet_address, password=wallet_password)


def get_wallet_balance(neofs_env: NeoFSEnv, neo_go: NeoGo, wallet: NodeWallet, wallet_config: str) -> float:
    result = neo_go.nep17.balance(
        wallet.address, "GAS", f"http://{neofs_env.main_chain.rpc_address}", wallet_config=wallet_config
    )
    balance = 0.0
    for line in result.stdout.splitlines():
        if "Amount" in line:
            balance = float(line.split("Amount :")[-1].strip())
    return balance


def get_neofs_balance(neofs_env: NeoFSEnv, neofs_cli: NeofsCli, wallet: NodeWallet) -> float:
    return float(
        neofs_cli.accounting.balance(
            wallet=wallet.path,
            rpc_endpoint=neofs_env.sn_rpc,
            address=get_last_address_from_wallet(wallet.path, wallet.password),
        ).stdout.strip()
    )


@allure.step("Wait for correct neofs balance")
@wait_for_success(60, 5)
def wait_for_correct_neofs_balance(neofs_env, wallet, cli_wallet_config: str, compare_func: callable):
    neofs_cli = neofs_env.neofs_cli(cli_wallet_config)
    assert compare_func(get_neofs_balance(neofs_env, neofs_cli, wallet)), "Wallet balance in neofs is not correct"


@allure.step("Wait for correct wallet balance")
@wait_for_success(60, 5)
def wait_for_correct_wallet_balance(
    neofs_env, neo_go: NeoGo, wallet, neo_go_wallet_config: str, compare_func: callable
):
    assert compare_func(get_wallet_balance(neofs_env, neo_go, wallet, neo_go_wallet_config)), (
        "Wallet balance is not correct after"
    )
