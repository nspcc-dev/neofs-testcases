import allure
import pytest
from helpers.wallet_helpers import get_neofs_balance, get_neofs_balance_by_owner
from neofs_testlib.env.env import NeoFSEnv, NodeWallet


@pytest.mark.parametrize(
    "get_balance_by_owner",
    [True, False],
)
def test_mint_balance(neofs_env: NeoFSEnv, user_wallet: NodeWallet, get_balance_by_owner: bool):
    allure.dynamic.title(
        f"Verify mint balance and getting balance {'by owner' if get_balance_by_owner else 'by wallet'}"
    )

    with allure.step("Get balance before mint"):
        cli_wallet_config = neofs_env.generate_cli_config(user_wallet)
        neofs_cli = neofs_env.neofs_cli(cli_wallet_config)
        if get_balance_by_owner:
            balance_before_mint = get_neofs_balance_by_owner(neofs_env, neofs_cli, user_wallet)
        else:
            balance_before_mint = get_neofs_balance(neofs_env, neofs_cli, user_wallet)

    with allure.step("Mint balance"):
        neofs_adm = neofs_env.neofs_adm()
        neofs_adm.fschain.mint_balance(
            alphabet_wallets=neofs_env.alphabet_wallets_dir,
            amount="5",
            deposit_tx="d01a381aae45f1ed181db9d554cc5ccc69c69f4eb9d554cc5ccc69c69f4e9f4e",
            rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
            wallet_address=user_wallet.address,
        )

    with allure.step("Verify balance after min"):
        if get_balance_by_owner:
            balance_after_mint = get_neofs_balance_by_owner(neofs_env, neofs_cli, user_wallet)
        else:
            balance_after_mint = get_neofs_balance(neofs_env, neofs_cli, user_wallet)
        assert balance_after_mint == balance_before_mint + 5, "Invalid balance after mint-balance command"
