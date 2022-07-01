import logging
import os
import shutil
from re import search
from time import sleep

import allure
import pytest
from robot.api import deco

import rpc_client
import wallet
from cli_helpers import _cmd_run
from common import (ASSETS_DIR, COMPLEX_OBJ_SIZE, COMMON_PLACEMENT_RULE,
                    MAINNET_WALLET_PATH, NEO_MAINNET_ENDPOINT, SIMPLE_OBJ_SIZE, REMOTE_HOST, CONTROL_NODE_USER,
                    CONTROL_NODE_PWD)
from payment_neogo import neofs_deposit, transfer_mainnet_gas
from python_keywords.container import create_container
from python_keywords.payment_neogo import get_balance
from python_keywords.utility_keywords import generate_file_and_file_hash
from ssh_helper import HostClient
from wellknown_acl import PUBLIC_ACL

deco.keyword = allure.step

logger = logging.getLogger('NeoLogger')


@pytest.fixture(scope='session', autouse=True)
@allure.title('Check binary versions')
def check_binary_versions(request):
    environment_dir = request.config.getoption('--alluredir')
    binaries = ['neo-go', 'neofs-cli', 'neofs-authmate', 'aws']
    env_out = {}
    for binary in binaries:
        out = _cmd_run(f'{binary} --version')
        version = search(r'(v?\d.*)\s+', out)
        version = version.group(1) if version else 'Unknown'
        env_out[binary.upper()] = version

    if environment_dir:
        with open(f'{environment_dir}/environment.properties', 'w') as out_file:
            for env, env_value in env_out.items():
                out_file.write(f'{env}={env_value}\n')


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
    local_wallet_path = None
    wallet, addr, _ = init_wallet_with_address
    logger.info(f'Init wallet: {wallet},\naddr: {addr}')

    if REMOTE_HOST:
        ssh_client = HostClient(REMOTE_HOST, CONTROL_NODE_USER, CONTROL_NODE_PWD)
        local_wallet_path = os.path.join(ASSETS_DIR, os.path.basename(MAINNET_WALLET_PATH))
        ssh_client.copy_file_from_host(MAINNET_WALLET_PATH, local_wallet_path)

    transfer_mainnet_gas(wallet, deposit + 1, wallet_path=local_wallet_path or MAINNET_WALLET_PATH)
    neofs_deposit(wallet, deposit)

    return wallet


@pytest.fixture()
@allure.title('Create Container')
def prepare_container(prepare_wallet_and_deposit):
    wallet = prepare_wallet_and_deposit
    return prepare_container_impl(wallet)


@pytest.fixture(scope='module')
@allure.title('Create Public Container')
def prepare_public_container(prepare_wallet_and_deposit):
    placement_rule = 'REP 1 IN X CBF 1 SELECT 1 FROM * AS X'
    wallet = prepare_wallet_and_deposit
    return prepare_container_impl(wallet, rule=placement_rule, basic_acl=PUBLIC_ACL)


def prepare_container_impl(wallet: str, rule=COMMON_PLACEMENT_RULE, basic_acl: str = ''):
    balance = get_balance(wallet)
    assert balance > 0, f'Expected balance is greater than 0. Got {balance}'

    cid = create_container(wallet, rule=rule, basic_acl=basic_acl)

    new_balance = get_balance(wallet)
    assert new_balance < balance, 'Expected some fee has charged'

    return cid, wallet


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


@pytest.fixture()
@allure.title('Generate large file')
def generate_large_file():
    obj_size = int(os.getenv('BIG_OBJ_SIZE', COMPLEX_OBJ_SIZE))
    file_path, file_hash = generate_file_and_file_hash(obj_size)

    return file_path, file_hash
