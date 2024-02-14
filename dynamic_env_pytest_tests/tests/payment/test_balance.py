import logging
import os

import allure
import pytest
import yaml
from neofs_env.neofs_env_test_base import NeofsEnvTestBase
from neofs_testlib.env.env import NeoFSEnv, NodeWallet
from neofs_testlib.shell import CommandResult
from neofs_testlib.utils import wallet as wallet_utils

from helpers.wallet_helpers import create_wallet

logger = logging.getLogger("NeoLogger")
DEPOSIT_AMOUNT = 30


@pytest.mark.payments
@pytest.mark.skip("Unsupported of current version of NeoFSEnv")
class TestBalanceAccounting(NeofsEnvTestBase):
    @pytest.fixture(scope="class")
    def main_wallet(self) -> NodeWallet:
        return create_wallet()

    @pytest.fixture(scope="class")
    def other_wallet(self) -> NodeWallet:
        return create_wallet()

    @allure.step("Check deposit amount")
    def check_amount(self, result: CommandResult) -> None:
        amount_str = result.stdout.rstrip()

        try:
            amount = int(amount_str)
        except Exception as ex:
            pytest.fail(
                f"Amount parse error, should be parsable as int({DEPOSIT_AMOUNT}), but given {amount_str}: {ex}"
            )

        assert amount == DEPOSIT_AMOUNT

    @staticmethod
    @allure.step("Write config with API endpoint")
    def write_api_config(config_dir: str, endpoint: str, wallet: str, neofs_env: NeoFSEnv) -> str:
        api_config = {"rpc-endpoint": endpoint, "wallet": wallet, "password": "password"}
        api_config_file = os.path.join(config_dir, "neofs-cli-api-config.yaml")
        with open(api_config_file, "w") as file:
            yaml.dump(api_config, file)
        return api_config_file

    @pytest.mark.sanity
    @allure.title("Test balance request with wallet and address")
    def test_balance_wallet_address(self, main_wallet: NodeWallet):
        result = self.neofs_env.neofs_cli().accounting.balance(
            wallet=main_wallet.path,
            rpc_endpoint=self.neofs_env.sn_rpc,
            address=wallet_utils.get_last_address_from_wallet(
                main_wallet.path, main_wallet.password
            ),
        )

        self.check_amount(result)

    @allure.title("Test balance request with wallet only")
    def test_balance_wallet(self, main_wallet: NodeWallet):
        result = self.neofs_env.neofs_cli().accounting.balance(
            wallet=main_wallet.path, rpc_endpoint=self.neofs_env.sn_rpc
        )
        self.check_amount(result)

    @allure.title("Test balance request with wallet and wrong address")
    def test_balance_wrong_address(self, main_wallet: NodeWallet, other_wallet: NodeWallet):
        with pytest.raises(Exception, match="address option must be specified and valid"):
            self.neofs_env.neofs_cli().accounting.balance(
                wallet=main_wallet.path,
                rpc_endpoint=self.neofs_env.sn_rpc,
                address=wallet_utils.get_last_address_from_wallet(
                    other_wallet.path, other_wallet.password
                ),
            )

    @allure.title("Test balance request with config file")
    def test_balance_api(self, temp_directory: str, main_wallet: NodeWallet):
        config_file = self.write_api_config(
            config_dir=temp_directory,
            endpoint=self.neofs_env.sn_rpc,
            wallet=main_wallet.path,
        )
        logger.info(f"Config with API endpoint: {config_file}")

        result = self.neofs_env.neofs_cli(config_file).accounting.balance()

        self.check_amount(result)
