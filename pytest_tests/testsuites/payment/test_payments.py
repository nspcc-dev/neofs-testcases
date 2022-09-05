import logging
import pytest

from epoch import tick_epoch
from python_keywords.payment_neogo import (
    get_mainnet_balance,
    get_neofs_balance,
    get_sidechain_balance,
    neofs_deposit,
    transfer_mainnet_gas,
    withdraw_mainnet_gas,
)
from common import ASSETS_DIR
from wallet import init_wallet
from service_helper import LocalDevEnvStorageServiceHelper

import allure

logger = logging.getLogger("NeoLogger")

DEPOSIT_AMOUNT = 25
DEPOSIT = 60


@pytest.mark.payments
@allure.title("Testcase to check sidechain balance when IR emission threshold is exceeded")
def test_emission_threshold(public_network_only):
    public_network_only
    service = LocalDevEnvStorageServiceHelper()
    with allure.step("Explicitly set up NEOFS_IR_EMIT_GAS_BALANCE_THRESHOLD to 0"):
        service.start_ir(ir_names=["ir01"], config_dict={"NEOFS_IR_EMIT_GAS_BALANCE_THRESHOLD": 0})
    wallet, addr, _ = init_wallet(ASSETS_DIR)
    original_sidechain_balance = get_sidechain_balance(addr)
    transfer_mainnet_gas(wallet_to=wallet, amount=DEPOSIT)
    with allure.step("When the threshold isn't exceeded, deposit passes"):
        neofs_deposit(wallet_to=wallet, amount=DEPOSIT_AMOUNT)
        tick_epoch()
        sidechain_balance_changed = get_sidechain_balance(addr)
        assert original_sidechain_balance != sidechain_balance_changed
    with allure.step("Set up NEOFS_IR_EMIT_GAS_BALANCE_THRESHOLD to 10^16"):
        config_dict = {"NEOFS_IR_EMIT_GAS_BALANCE_THRESHOLD": 10**16}
        service.stop_ir(ir_names=["ir01"])
        service.start_ir(ir_names=["ir01"], config_dict=config_dict)
    with allure.step("Expect deposit to fail because of the exceeded IR emissoion threshold"):
        neofs_deposit(wallet_to=wallet, amount=DEPOSIT_AMOUNT)
        sidechain_balance_unchanged = get_sidechain_balance(addr)
        assert sidechain_balance_changed == sidechain_balance_unchanged
        service.stop_ir(ir_names=["ir01"])


TRANSFER_AMOUNT = 15
DEPOSIT_AMOUNT = 10
WITHDRAW_AMOUNT = 10
EXCEEDED_WITHDRAW_AMOUNT = 100


@allure.title("Testcase to check withdrawing")
def test_withdraw(public_network_only):
    public_network_only
    wallet, addr, _ = init_wallet(ASSETS_DIR)
    with allure.step("Transfer GAS from mainnet wallet to our test wallet"):
        transfer_mainnet_gas(wallet_to=wallet, amount=TRANSFER_AMOUNT)

    with allure.step("Expect Mainnet balance to reach the transferred amount"):
        mainnet_balance = get_mainnet_balance(addr)
        assert mainnet_balance == TRANSFER_AMOUNT

    with allure.step("Make deposit to NeoFS"):
        neofs_deposit(wallet_to=wallet, amount=DEPOSIT_AMOUNT)

    with allure.step("Expect Mainnet balance to decrease"):
        mainnet_balance = get_mainnet_balance(addr)
        expected_balance = TRANSFER_AMOUNT - DEPOSIT_AMOUNT
        assert mainnet_balance < expected_balance

    with allure.step("Expect NeoFS balance to reach the deposited amount"):
        neofs_balance = get_neofs_balance(wallet)
        assert neofs_balance == DEPOSIT_AMOUNT

    with allure.step("Withdraw deposit back"):
        neofs_balance_before_withdraw = get_neofs_balance(wallet)
        with allure.step(
            "Try to withdraw an amount larger than has been "
            "deposited and expect NeoFS balance to stay unchanged"
        ):
            withdraw_mainnet_gas(wlt=wallet, amount=EXCEEDED_WITHDRAW_AMOUNT)
            tick_epoch()
            neofs_balance_after_withdraw = get_neofs_balance(wallet)
            assert neofs_balance_before_withdraw == neofs_balance_after_withdraw

        with allure.step(
            "Try to withdraw an amount less than has been "
            "deposited and expect NeoFS balance reduce by this amount"
        ):
            withdraw_mainnet_gas(wlt=wallet, amount=WITHDRAW_AMOUNT)
            tick_epoch()
            neofs_balance_after_withdraw = get_neofs_balance(wallet)
            expected_balance = DEPOSIT_AMOUNT - WITHDRAW_AMOUNT
            assert neofs_balance_after_withdraw == expected_balance
        with allure.step("Check that Mainnet balance has reduced"):
            mainnet_balance_after = get_mainnet_balance(addr)
            mainnet_balance_diff = mainnet_balance_after - mainnet_balance
            assert mainnet_balance_diff < WITHDRAW_AMOUNT
