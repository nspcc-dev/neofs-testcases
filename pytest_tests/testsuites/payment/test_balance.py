import logging

import pytest
import yaml
from cli_utils import NeofsCli
from common import ASSETS_DIR, NEOFS_ENDPOINT, WALLET_CONFIG
from python_keywords.payment_neogo import _address_from_wallet
from wallet import init_wallet

import allure

logger = logging.getLogger("NeoLogger")
DEPOSIT_AMOUNT = 30


@pytest.mark.payments
class TestBalanceAccounting():
    @pytest.mark.usefixtures("public_network_only")

    @pytest.fixture(autouse=True)
    def prepare_two_wallets(self, prepare_wallet_and_deposit):
        self.user_wallet = prepare_wallet_and_deposit
        self.address = _address_from_wallet(self.user_wallet, "")
        _, self.another_address, _ = init_wallet(ASSETS_DIR)

    @allure.title("Test balance request with wallet and address")
    def test_balance_wallet_address(self):
        cli = NeofsCli(config=WALLET_CONFIG)
        output = cli.accounting.balance(
            wallet=self.user_wallet, rpc_endpoint=NEOFS_ENDPOINT, address=self.address
        )
        logger.info(f"Out wallet+addres: {output}")
        assert int(output.rstrip()) == DEPOSIT_AMOUNT

    @allure.title("Test balance request with wallet only")
    def test_balance_wallet(self):
        cli = NeofsCli(config=WALLET_CONFIG)
        output = cli.accounting.balance(
            wallet=self.user_wallet,
            rpc_endpoint=NEOFS_ENDPOINT,
        )
        logger.info(f"Out wallet: {output}")
        assert int(output.rstrip()) == DEPOSIT_AMOUNT

    @allure.title("Test balance request with wallet and wrong address")
    def test_balance_wrong_address(self):
        with pytest.raises(
            Exception, match="address option must be specified and valid"
        ):
            cli = NeofsCli(config=WALLET_CONFIG)
            cli.accounting.balance(
                wallet=self.user_wallet,
                rpc_endpoint=NEOFS_ENDPOINT,
                address=self.another_address,
            )

    @allure.title("Test balance request with config file")
    def test_balance_api(self):
        config_file = self.write_api_config(
            endpoint=NEOFS_ENDPOINT, wallet=self.user_wallet
        )
        logger.info(f"YAML: {config_file}")
        cli = NeofsCli(config=config_file)
        output = cli.accounting.balance()
        logger.info(f"Out api: {output}")
        assert int(output.rstrip()) == DEPOSIT_AMOUNT

    @staticmethod
    @allure.step("Write YAML config")
    def write_api_config(endpoint: str, wallet: str) -> str:
        with open(WALLET_CONFIG) as file:
            wallet_config = yaml.load(file, Loader=yaml.FullLoader)
        api_config = {
            **wallet_config,
            "rpc-endpoint": endpoint,
            "wallet": wallet,
        }
        api_config_file = f"{ASSETS_DIR}/config.yaml"
        with open(api_config_file, "w") as file:
            yaml.dump(api_config, file)
        return api_config_file
