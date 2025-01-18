import allure
from helpers.test_control import wait_for_success
from helpers.wallet_helpers import get_neofs_balance, get_wallet_balance
from neofs_testlib.env.env import NeoFSEnv, NodeWallet
from neofs_testlib.utils import wallet as wallet_utils


@allure.step("Wait for correct neofs balance")
@wait_for_success(60, 5)
def wait_for_correct_neofs_balance(
    neofs_env: NeoFSEnv, wallet: NodeWallet, cli_wallet_config: str, compare_func: callable
):
    neofs_cli = neofs_env.neofs_cli(cli_wallet_config)
    assert compare_func(get_neofs_balance(neofs_env, neofs_cli, wallet)), "Wallet balance in neofs is not correct"


class TestDepositWithdrawal:
    def test_deposit_withdrawal(self, neofs_env_with_mainchain: NeoFSEnv):
        neofs_env = neofs_env_with_mainchain

        with allure.step("Create wallet for deposits/withdrawals"):
            wallet = NodeWallet(
                path=neofs_env._generate_temp_file(neofs_env._env_dir, prefix="deposit_withdrawal_test_wallet"),
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
                await_=True,
            )
            assert get_wallet_balance(neofs_env, neo_go, wallet, neo_go_wallet_config) == 1000.0, (
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
                wallet_config=neo_go_wallet_config,
                await_=True,
            )
            assert get_wallet_balance(neofs_env, neo_go, wallet, neo_go_wallet_config) <= 900, (
                "Wallet balance is not correct after deposit"
            )
            wait_for_correct_neofs_balance(neofs_env, wallet, cli_wallet_config, lambda balance: balance == 100)

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
            wait_for_correct_neofs_balance(neofs_env, wallet, cli_wallet_config, lambda balance: balance == 50)
            assert get_wallet_balance(neofs_env, neo_go, wallet, neo_go_wallet_config) > 940, (
                "Wallet balance is not correct after withdrawal"
            )
