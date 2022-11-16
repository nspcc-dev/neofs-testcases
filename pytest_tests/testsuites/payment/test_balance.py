import logging
import os

import allure
import pytest
import yaml
from common import FREE_STORAGE, NEOFS_CLI_EXEC, NEOFS_ENDPOINT, WALLET_CONFIG
from neofs_testlib.cli import NeofsCli
from neofs_testlib.shell import CommandResult, Shell
from wallet import WalletFactory, WalletFile

logger = logging.getLogger("NeoLogger")
DEPOSIT_AMOUNT = 30


@pytest.mark.sanity
@pytest.mark.payments
@pytest.mark.skipif(FREE_STORAGE, reason="Test only works on public network with paid storage")
class TestBalanceAccounting:
    @pytest.fixture(scope="class")
    def main_wallet(self, wallet_factory: WalletFactory) -> WalletFile:
        return wallet_factory.create_wallet()

    @pytest.fixture(scope="class")
    def other_wallet(self, wallet_factory: WalletFactory) -> WalletFile:
        return wallet_factory.create_wallet()

    @pytest.fixture(scope="class")
    def cli(self, client_shell: Shell) -> NeofsCli:
        return NeofsCli(client_shell, NEOFS_CLI_EXEC, WALLET_CONFIG)

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

    @allure.title("Test balance request with wallet and address")
    def test_balance_wallet_address(self, main_wallet: WalletFile, cli: NeofsCli):
        result = cli.accounting.balance(
            wallet=main_wallet.path,
            rpc_endpoint=NEOFS_ENDPOINT,
            address=main_wallet.get_address(),
        )

        self.check_amount(result)

    @allure.title("Test balance request with wallet only")
    def test_balance_wallet(self, main_wallet: WalletFile, cli: NeofsCli):
        result = cli.accounting.balance(wallet=main_wallet.path, rpc_endpoint=NEOFS_ENDPOINT)
        self.check_amount(result)

    @allure.title("Test balance request with wallet and wrong address")
    def test_balance_wrong_address(
        self, main_wallet: WalletFile, other_wallet: WalletFile, cli: NeofsCli
    ):
        with pytest.raises(Exception, match="address option must be specified and valid"):
            cli.accounting.balance(
                wallet=main_wallet.path,
                rpc_endpoint=NEOFS_ENDPOINT,
                address=other_wallet.get_address(),
            )

    @allure.title("Test balance request with config file")
    def test_balance_api(self, prepare_tmp_dir: str, main_wallet: WalletFile, client_shell: Shell):
        config_file = self.write_api_config(
            config_dir=prepare_tmp_dir, endpoint=NEOFS_ENDPOINT, wallet=main_wallet.path
        )
        logger.info(f"Config with API endpoint: {config_file}")

        cli = NeofsCli(client_shell, NEOFS_CLI_EXEC, config_file=config_file)
        result = cli.accounting.balance()

        self.check_amount(result)
