import logging
import os
import uuid

import allure
import pytest
import yaml
from common import (
    ASSETS_DIR,
    FREE_STORAGE,
    NEOFS_CLI_EXEC,
    NEOFS_ENDPOINT,
    WALLET_CONFIG,
    WALLET_PASS,
)
from neofs_testlib.cli import NeofsCli
from neofs_testlib.utils.wallet import get_last_address_from_wallet, init_wallet

logger = logging.getLogger("NeoLogger")
DEPOSIT_AMOUNT = 30


@pytest.mark.sanity
@pytest.mark.payments
@pytest.mark.skipif(FREE_STORAGE, reason="Test only works on public network with paid storage")
class TestBalanceAccounting:
    @pytest.fixture(autouse=True)
    def prepare_two_wallets(self, prepare_wallet_and_deposit):
        self.user_wallet = prepare_wallet_and_deposit
        self.address = get_last_address_from_wallet(self.user_wallet, WALLET_PASS)
        another_wallet = os.path.join(os.getcwd(), ASSETS_DIR, f"{str(uuid.uuid4())}.json")
        init_wallet(another_wallet, WALLET_PASS)
        self.another_address = get_last_address_from_wallet(another_wallet, WALLET_PASS)

    @allure.title("Test balance request with wallet and address")
    def test_balance_wallet_address(self, client_shell):
        cli = NeofsCli(client_shell, NEOFS_CLI_EXEC, WALLET_CONFIG)
        result = cli.accounting.balance(
            wallet=self.user_wallet,
            rpc_endpoint=NEOFS_ENDPOINT,
            address=self.address,
        )
        assert int(result.stdout.rstrip()) == DEPOSIT_AMOUNT

    @allure.title("Test balance request with wallet only")
    def test_balance_wallet(self, client_shell):
        cli = NeofsCli(client_shell, NEOFS_CLI_EXEC, WALLET_CONFIG)
        result = cli.accounting.balance(wallet=self.user_wallet, rpc_endpoint=NEOFS_ENDPOINT)
        assert int(result.stdout.rstrip()) == DEPOSIT_AMOUNT

    @allure.title("Test balance request with wallet and wrong address")
    def test_balance_wrong_address(self, client_shell):
        with pytest.raises(Exception, match="address option must be specified and valid"):
            cli = NeofsCli(client_shell, NEOFS_CLI_EXEC, WALLET_CONFIG)
            cli.accounting.balance(
                wallet=self.user_wallet,
                rpc_endpoint=NEOFS_ENDPOINT,
                address=self.another_address,
            )

    @allure.title("Test balance request with config file")
    def test_balance_api(self, prepare_tmp_dir, client_shell):
        config_file = self.write_api_config(
            config_dir=prepare_tmp_dir, endpoint=NEOFS_ENDPOINT, wallet=self.user_wallet
        )
        logger.info(f"Config with API endpoint: {config_file}")

        cli = NeofsCli(client_shell, NEOFS_CLI_EXEC, config_file=config_file)
        result = cli.accounting.balance()

        assert int(result.stdout.rstrip()) == DEPOSIT_AMOUNT

    @staticmethod
    @allure.step("Write config with API endpoint")
    def write_api_config(config_dir: str, endpoint: str, wallet: str) -> str:
        with open(WALLET_CONFIG, "r") as file:
            wallet_config = yaml.full_load(file)
        api_config = {
            **wallet_config,
            "rpc-endpoint": endpoint,
            "wallet": wallet,
        }
        api_config_file = os.path.join(config_dir, "neofs-cli-api-config.yaml")
        with open(api_config_file, "w") as file:
            yaml.dump(api_config, file)
        return api_config_file
