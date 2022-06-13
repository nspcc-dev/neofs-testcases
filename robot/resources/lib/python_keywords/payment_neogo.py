#!/usr/bin/python3

import re
import time

from common import (MAINNET_WALLET_PATH, MORPH_ENDPOINT,
        NEO_MAINNET_ENDPOINT, NEOFS_CONTRACT, MAINNET_SINGLE_ADDR)
import rpc_client
import contract
import converters
import wallet
from wrappers import run_sh_with_passwd_contract
from converters import load_wallet

from robot.api.deco import keyword
from robot.api import logger
from robot.libraries.BuiltIn import BuiltIn


ROBOT_AUTO_KEYWORDS = False

MORPH_TOKEN_POWER = 12
EMPTY_PASSWORD = ''
MAINNET_WALLET_PASS = 'one'
TX_PERSIST_TIMEOUT = 15 #seconds

NEOGO_CLI_EXEC = BuiltIn().get_variable_value("${NEOGO_CLI_EXEC}")

morph_rpc_cli = rpc_client.RPCClient(MORPH_ENDPOINT)
mainnet_rpc_cli = rpc_client.RPCClient(NEO_MAINNET_ENDPOINT)


@keyword('Withdraw Mainnet Gas')
def withdraw_mainnet_gas(wlt: str, address: str, scripthash: str, amount: int):
    cmd = (
        f"{NEOGO_CLI_EXEC} contract invokefunction -w {wlt} -a {address} "
        f"-r {NEO_MAINNET_ENDPOINT} {NEOFS_CONTRACT} withdraw {scripthash} "
        f"int:{amount}  -- {scripthash}:Global"
    )

    logger.info(f"Executing command: {cmd}")
    out = (run_sh_with_passwd_contract('', cmd, expect_confirmation=True)).decode('utf-8')
    logger.info(f"Command completed with output: {out}")
    m = re.match(r'^Sent invocation transaction (\w{64})$', out)
    if m is None:
        raise Exception("Can not get Tx.")
    tx = m.group(1)
    if not transaction_accepted(tx):
        raise RuntimeError(f"TX {tx} hasn't been processed")


def transaction_accepted(tx_id: str):
    """
    This function returns True in case of accepted TX.
    Args:
        tx_id(str): transaction ID
    Returns:
        (bool)
    """

    try:
        for _ in range(0, TX_PERSIST_TIMEOUT):
            time.sleep(1)
            resp = mainnet_rpc_cli.get_transaction_height(tx_id)
            if resp is not None:
                logger.info(f"TX is accepted in block: {resp}")
                return True
    except Exception as e:
        logger.info(f"request failed with error: {e}")
        raise e
    return False


@keyword('Get NeoFS Balance')
def get_balance(wallet_path: str):
    """
    This function returns NeoFS balance for given wallet.
    """
    wlt = load_wallet(wallet_path)
    acc = wlt.accounts[-1]
    payload = [
        {
            'type': 'Hash160',
            'value': str(acc.script_hash)
        }
    ]
    try:
        resp = morph_rpc_cli.invoke_function(
                contract.get_balance_contract_hash(MORPH_ENDPOINT),
                'balanceOf',
                payload
            )
        logger.info(f"Got response \n{resp}")
        value = int(resp['stack'][0]['value'])
        return value / (10 ** MORPH_TOKEN_POWER)
    except Exception as e:
        logger.error(f"failed to get wallet balance: {e}")
        raise e


@keyword('Transfer Mainnet Gas')
def transfer_mainnet_gas(wallet_to: str, amount: int, wallet_password: str = EMPTY_PASSWORD):
    '''
    This function transfer GAS in main chain from mainnet wallet to
    the provided wallet. If the wallet contains more than one address,
    the assets will be transferred to the last one.
    Args:
        wallet_to (str): the path to the wallet to transfer assets to
        amount (int): amount of gas to transfer
        wallet_password (optional, str): password of the wallet; it is
            required to decode the wallet and extract its addresses
    Returns:
        (void)
    '''
    wlt = load_wallet(wallet_to, wallet_password)
    address_to = wlt.accounts[-1].address
    logger.info(f"got address to: {address_to}")

    txid = wallet.nep17_transfer(MAINNET_WALLET_PATH, address_to, amount, NEO_MAINNET_ENDPOINT,
            wallet_pass=MAINNET_WALLET_PASS, addr_from=MAINNET_SINGLE_ADDR)
    if not transaction_accepted(txid):
        raise RuntimeError(f"TX {txid} hasn't been processed")


@keyword('NeoFS Deposit')
def neofs_deposit(wallet_to: str, amount: int, wallet_password: str = EMPTY_PASSWORD):
    """
    Transferring GAS from given wallet to NeoFS contract address.
    """
    # get NeoFS contract address
    deposit_addr = converters.contract_hash_to_address(NEOFS_CONTRACT)
    logger.info(f"NeoFS contract address: {deposit_addr}")

    wlt = load_wallet(wallet_to, wallet_password)
    address_to = wlt.accounts[-1].address
    logger.info(f"got address to: {address_to}")

    txid = wallet.nep17_transfer(wallet_to, deposit_addr, amount, NEO_MAINNET_ENDPOINT,
            wallet_pass=wallet_password, addr_from=address_to)
    if not transaction_accepted(txid):
        raise RuntimeError(f"TX {txid} hasn't been processed")
