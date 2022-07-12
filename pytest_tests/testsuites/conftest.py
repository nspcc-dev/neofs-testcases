import logging
import os
import shutil
from re import search

import allure
import pytest
from robot.api import deco

import wallet
from cli_helpers import _cmd_run
from common import ASSETS_DIR, FREE_STORAGE, MAINNET_WALLET_PATH
from payment_neogo import neofs_deposit, transfer_mainnet_gas

def robot_keyword_adapter(name=None, tags=(), types=()):
    return allure.step(name)
deco.keyword = robot_keyword_adapter

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

    yield wallet.init_wallet(ASSETS_DIR)

    shutil.rmtree(full_path)


@pytest.fixture(scope='session')
@allure.title('Prepare wallet and deposit')
def prepare_wallet_and_deposit(init_wallet_with_address):
    wallet, addr, _ = init_wallet_with_address
    logger.info(f'Init wallet: {wallet},\naddr: {addr}')

    if not FREE_STORAGE:
        deposit = 30
        transfer_mainnet_gas(wallet, deposit + 1, wallet_path=MAINNET_WALLET_PATH)
        neofs_deposit(wallet, deposit)

    return wallet
