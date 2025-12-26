import base64
import hashlib
import os
import uuid
from typing import Optional

import allure
import yaml
from helpers.common import get_assets_dir_path
from helpers.test_control import wait_for_success
from neo3.core import cryptography
from neo3.wallet.wallet import Wallet
from neofs_testlib.cli import NeofsCli, NeoGo
from neofs_testlib.env.env import NeoFSEnv, NodeWallet
from neofs_testlib.utils.converters import load_wallet
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


def get_neofs_balance_by_wallet(neofs_env: NeoFSEnv, neofs_cli: NeofsCli, wallet: NodeWallet) -> float:
    return float(
        neofs_cli.accounting.balance(
            wallet=wallet.path,
            rpc_endpoint=neofs_env.sn_rpc,
        ).stdout.strip()
    )


def get_neofs_balance_by_config(neofs_env: NeoFSEnv, wallet: NodeWallet) -> float:
    api_config = {"rpc-endpoint": neofs_env.sn_rpc, "wallet": wallet.path, "password": wallet.password}
    api_config_file = os.path.join(neofs_env._generate_temp_dir(), "neofs-cli-api-config.yaml")
    with open(api_config_file, "w") as file:
        yaml.dump(api_config, file)
    return float(neofs_env.neofs_cli(api_config_file).accounting.balance().stdout.strip())


def get_neofs_balance_by_owner(neofs_env: NeoFSEnv, neofs_cli: NeofsCli, wallet: NodeWallet) -> float:
    return float(
        neofs_cli.accounting.balance(
            rpc_endpoint=neofs_env.sn_rpc,
            owner=get_last_address_from_wallet(wallet.path, wallet.password),
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


def create_wallet_with_money(neofs_env_with_mainchain: NeoFSEnv) -> NodeWallet:
    neofs_env = neofs_env_with_mainchain

    with allure.step("Create wallet for deposit"):
        wallet = NodeWallet(
            path=neofs_env_with_mainchain._generate_temp_file(
                neofs_env._env_dir, prefix="deposit_withdrawal_test_wallet"
            ),
            address="",
            password=neofs_env.default_password,
        )
        init_wallet(wallet.path, wallet.password)
        wallet.address = get_last_address_from_wallet(wallet.path, wallet.password)
        wallet.neo_go_config = neofs_env.generate_neo_go_config(wallet)
        wallet.cli_config = neofs_env.generate_cli_config(wallet)

    with allure.step("Transfer some money to created wallet"):
        neo_go = neofs_env.neo_go()
        neo_go.nep17.transfer(
            "GAS",
            wallet.address,
            f"http://{neofs_env.main_chain.rpc_address}",
            from_address=neofs_env.main_chain.wallet.address,
            amount=1000,
            force=True,
            wallet_config=neofs_env.main_chain.neo_go_config,
            await_=True,
        )
        assert get_wallet_balance(neofs_env, neo_go, wallet, wallet.neo_go_config) == 1000.0, (
            "Money transfer from alphabet to test wallet didn't succeed"
        )

    with allure.step("Deposit money to neofs contract"):
        neo_go.nep17.transfer(
            "GAS",
            neofs_env.main_chain.neofs_contract_address,
            f"http://{neofs_env.main_chain.rpc_address}",
            from_address=wallet.address,
            amount=100,
            force=True,
            wallet_config=wallet.neo_go_config,
            await_=True,
        )
        assert get_wallet_balance(neofs_env, neo_go, wallet, wallet.neo_go_config) <= 900, (
            "Wallet balance is not correct after deposit"
        )
        wait_for_correct_neofs_balance(neofs_env, wallet, wallet.cli_config, lambda balance: balance == 100)

    return wallet


def get_private_key(wallet: NodeWallet) -> bytes:
    neo3_wallet: Wallet = load_wallet(wallet.path, passwords=[wallet.password] * 3)
    acc = neo3_wallet.accounts[0]
    return acc.private_key


def sign_string(str_to_sign: str, private_key: bytes) -> str:
    signature = cryptography.sign(str_to_sign.encode(), private_key, hash_func=hashlib.sha256)
    return base64.standard_b64encode(signature).decode("utf-8")
