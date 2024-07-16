import time

import allure
import pytest
from neofs_testlib.cli import NeofsCli, NeoGo
from neofs_testlib.env.env import NeoFSEnv, NodeWallet
from neofs_testlib.utils import wallet as wallet_utils


@pytest.fixture
def neofs_env_with_mainchain():
    neofs_env = NeoFSEnv.simple(with_main_chain=True)
    yield neofs_env
    neofs_env.kill()


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
            address=wallet_utils.get_last_address_from_wallet(wallet.path, wallet.password),
        ).stdout.strip()
    )


class TestDepositWithdrawal:
    def test_deposit_withdrawal(self, neofs_env_with_mainchain: NeoFSEnv):
        neofs_env = neofs_env_with_mainchain

        with allure.step("Create wallet for deposits/withdrawals"):
            wallet = NodeWallet(
                path=NeoFSEnv._generate_temp_file(prefix="deposit_withdrawal_test_wallet"),
                address="",
                password=neofs_env.default_password,
            )
            wallet_utils.init_wallet(wallet.path, wallet.password)
            wallet.address = wallet_utils.get_last_address_from_wallet(wallet.path, wallet.password)
            neo_go_wallet_config = neofs_env.generate_neo_go_config(wallet)
            cli_wallet_config = neofs_env.generate_cli_config(wallet)

        with allure.step("Transfer some money to created wallet"):
            neo_go = neofs_env.neo_go()
            neo_go.nep17.transfer(
                "GAS",
                wallet.address,
                f"http://{neofs_env.main_chain.rpc_address}",
                from_address=neofs_env.inner_ring_nodes[-1].alphabet_wallet.address,
                amount=1000,
                force=True,
                wallet_config=neofs_env.main_chain.neo_go_config,
            )
            time.sleep(10)
            assert (
                get_wallet_balance(neofs_env, neo_go, wallet, neo_go_wallet_config) == 1000.0
            ), "Money transfer from alphabet to test wallet didn't succeed"

        with allure.step("Deposit money to neofs contract"):
            neo_go.nep17.transfer(
                "GAS",
                neofs_env.main_chain.neofs_contract_address,
                f"http://{neofs_env.main_chain.rpc_address}",
                from_address=wallet.address,
                amount=100,
                force=True,
                wallet_config=neo_go_wallet_config,
            )
            time.sleep(10)
            assert (
                get_wallet_balance(neofs_env, neo_go, wallet, neo_go_wallet_config) <= 900
            ), "Wallet balance is not correct after deposit"
            neofs_cli = neofs_env.neofs_cli(cli_wallet_config)
            assert (
                get_neofs_balance(neofs_env, neofs_cli, wallet) == 100
            ), "Wallet balance in neofs is not correct after deposit"

        with allure.step("Withdraw some money back to the wallet"):
            neo_go.contract.invokefunction(
                neofs_env.main_chain.neofs_contract_hash,
                rpc_endpoint=f"http://{neofs_env.main_chain.rpc_address}",
                wallet_config=neo_go_wallet_config,
                method="withdraw",
                arguments=f"{wallet.address} 50",
                multisig_hash=f"{wallet.address}:Global",
                force=True,
            )
            time.sleep(10)
            assert (
                get_neofs_balance(neofs_env, neofs_cli, wallet) == 50
            ), "Wallet balance in neofs is not correct after withdrawal"
            assert (
                get_wallet_balance(neofs_env, neo_go, wallet, neo_go_wallet_config) > 940
            ), "Wallet balance is not correct after withdrawal"
