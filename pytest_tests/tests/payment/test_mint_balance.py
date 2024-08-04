import allure
from helpers.wallet_helpers import get_neofs_balance
from neofs_testlib.env.env import NeoFSEnv, NodeWallet


def test_mint_balance(neofs_env: NeoFSEnv, user_wallet: NodeWallet):
    with allure.step("Get balance before mint"):
        cli_wallet_config = neofs_env.generate_cli_config(user_wallet)
        neofs_cli = neofs_env.neofs_cli(cli_wallet_config)
        balance_before_mint = get_neofs_balance(neofs_env, neofs_cli, user_wallet)

    with allure.step("Mint balance"):
        neofs_adm = neofs_env.neofs_adm()
        neofs_adm.morph.mint_balance(
            alphabet_wallets=neofs_env.alphabet_wallets_dir,
            amount="1",
            deposit_tx="d01a381aae45f1ed181db9d554cc5ccc69c69f4eb9d554cc5ccc69c69f4e9f4e",
            rpc_endpoint=f"http://{neofs_env.morph_rpc}",
            wallet_address=user_wallet.address,
        )

    with allure.step("Verify balance after min"):
        balance_after_mint = get_neofs_balance(neofs_env, neofs_cli, user_wallet)
        assert balance_after_mint == balance_before_mint + 1, "Invalid balance after mint-balance command"
