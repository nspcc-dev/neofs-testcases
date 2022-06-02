import logging
import os
import shutil
from time import sleep

import allure
import pytest
import rpc_client
import wallet
from common import (ASSETS_DIR, COMPLEX_OBJ_SIZE, MAINNET_WALLET_WIF,
                    NEO_MAINNET_ENDPOINT, SIMPLE_OBJ_SIZE)
from python_keywords.container import create_container
from python_keywords.payment_neogo import get_balance
from python_keywords.utility_keywords import generate_file_and_file_hash
from robot.api import deco
from wallet_keywords import neofs_deposit, transfer_mainnet_gas

deco.keyword = allure.step


logger = logging.getLogger('NeoLogger')
NEOFS_IR_CONTRACTS_NEOFS = 'ee3dee6d05dc79c24a5b8f6985e10d68b7cacc62'


@pytest.fixture(scope='session')
@allure.title('Init wallet with address')
def init_wallet_with_address():
    full_path = f'{os.getcwd()}/{ASSETS_DIR}'
    os.mkdir(full_path)

    yield wallet.init_wallet_w_addr(ASSETS_DIR)

    shutil.rmtree(full_path)


@pytest.fixture(scope='session')
@allure.title('Prepare wallet and deposit')
def prepare_wallet_and_deposit(init_wallet_with_address):
    deposit = 30
    wallet, addr, wif = init_wallet_with_address
    logger.info(f'Init wallet: {wallet},\naddr: {addr},\nwif: {wif}')

    txid = transfer_mainnet_gas(MAINNET_WALLET_WIF, addr, deposit + 1)
    wait_unitl_transaction_accepted_in_block(txid)
    deposit_tx = neofs_deposit(wif, deposit, contract=NEOFS_IR_CONTRACTS_NEOFS)
    wait_unitl_transaction_accepted_in_block(deposit_tx)

    sleep(5)
    return wallet, addr, wif


@pytest.fixture()
@allure.title('Prepare container')
def prepare_container(prepare_wallet_and_deposit):
    wallet, addr, wif = prepare_wallet_and_deposit
    balance = get_balance(wif)
    cid = create_container(wallet)
    new_balance = get_balance(wif)
    assert new_balance < balance
    return cid, wallet, addr


@allure.step('Wait until transaction accepted in block')
def wait_unitl_transaction_accepted_in_block(tx_id: str):
    """
    This function return True in case of accepted TX.
    Parameters:
    :param tx_id:           transaction ID
    """
    mainnet_rpc_cli = rpc_client.RPCClient(NEO_MAINNET_ENDPOINT)

    if isinstance(tx_id, bytes):
        tx_id = tx_id.decode()

    sleep_interval, attempts = 5, 10

    for __attempt in range(attempts):
        try:
            resp = mainnet_rpc_cli.get_transaction_height(tx_id)
            if resp is not None:
                logger.info(f"got block height: {resp}")
                return True
        except Exception as e:
            logger.info(f"request failed with error: {e}")
            raise e
        sleep(sleep_interval)
    raise TimeoutError(f'Timeout {sleep_interval * attempts} sec. reached on waiting for transaction accepted')


@pytest.fixture()
@allure.title('Generate files')
def generate_files():
    file_name_simple, _ = generate_file_and_file_hash(SIMPLE_OBJ_SIZE)
    large_file_name, _ = generate_file_and_file_hash(COMPLEX_OBJ_SIZE)

    return file_name_simple, large_file_name


@pytest.fixture()
@allure.title('Generate file')
def generate_file():
    file_name_simple, _ = generate_file_and_file_hash(SIMPLE_OBJ_SIZE)

    return file_name_simple
