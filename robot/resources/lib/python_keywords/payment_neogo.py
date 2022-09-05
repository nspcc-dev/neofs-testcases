#!/usr/bin/python3

import re
import time

from neo3 import wallet
from robot.api import logger
from robot.api.deco import keyword

import contract
import converters
import rpc_client
from common import (GAS_HASH, MAINNET_SINGLE_ADDR, MAINNET_WALLET_PATH, MAINNET_WALLET_PASS,
                    MORPH_ENDPOINT, NEO_MAINNET_ENDPOINT, NEOFS_CONTRACT, NEOGO_EXECUTABLE)
from converters import load_wallet
from wallet import nep17_transfer
from wrappers import run_sh_with_passwd_contract

ROBOT_AUTO_KEYWORDS = False

EMPTY_PASSWORD = ''
TX_PERSIST_TIMEOUT = 15     # seconds
ASSET_POWER_MAINCHAIN = 10 ** 8
ASSET_POWER_SIDECHAIN = 10 ** 12

morph_rpc_cli = rpc_client.RPCClient(MORPH_ENDPOINT)
mainnet_rpc_cli = rpc_client.RPCClient(NEO_MAINNET_ENDPOINT)


@keyword('Withdraw Mainnet Gas')
def withdraw_mainnet_gas(wlt: str, amount: int):
    address = _address_from_wallet(wlt, EMPTY_PASSWORD)
    scripthash = wallet.Account.address_to_script_hash(address)

    cmd = (
        f"{NEOGO_EXECUTABLE} contract invokefunction -w {wlt} -a {address} "
        f"-r {NEO_MAINNET_ENDPOINT} {NEOFS_CONTRACT} withdraw {scripthash} "
        f"int:{amount}  -- {scripthash}:Global"
    )

    logger.info(f"Executing command: {cmd}")
    raw_out = run_sh_with_passwd_contract('', cmd, expect_confirmation=True)
    out = raw_out.decode('utf-8')
    logger.info(f"Command completed with output: {out}")
    m = re.match(r'^Sent invocation transaction (\w{64})$', out)
    if m is None:
        raise Exception("Can not get Tx.")
    tx = m.group(1)
    if not transaction_accepted(tx):
        raise AssertionError(f"TX {tx} hasn't been processed")


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
    except Exception as out:
        logger.info(f"request failed with error: {out}")
        raise out
    return False


@keyword('Get NeoFS Balance')
def get_neofs_balance(wallet_path: str):
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
        return value / ASSET_POWER_SIDECHAIN
    except Exception as out:
        logger.error(f"failed to get wallet balance: {out}")
        raise out


@keyword('Transfer Mainnet Gas')
def transfer_mainnet_gas(wallet_to: str, amount: int, wallet_password: str = EMPTY_PASSWORD,
                         wallet_path: str = MAINNET_WALLET_PATH):
    """
    This function transfer GAS in main chain from mainnet wallet to
    the provided wallet. If the wallet contains more than one address,
    the assets will be transferred to the last one.
    Args:
        wallet_to (str): the path to the wallet to transfer assets to
        amount (int): amount of gas to transfer
        wallet_password (optional, str): password of the wallet; it is
            required to decode the wallet and extract its addresses
        wallet_path (str): path to chain node wallet
    Returns:
        (void)
    """
    address_to = _address_from_wallet(wallet_to, wallet_password)

    txid = nep17_transfer(wallet_path, address_to, amount, NEO_MAINNET_ENDPOINT,
                          wallet_pass=MAINNET_WALLET_PASS, addr_from=MAINNET_SINGLE_ADDR)
    if not transaction_accepted(txid):
        raise AssertionError(f"TX {txid} hasn't been processed")


@keyword('NeoFS Deposit')
def neofs_deposit(wallet_to: str, amount: int,
                  wallet_password: str = EMPTY_PASSWORD):
    """
    Transferring GAS from given wallet to NeoFS contract address.
    """
    # get NeoFS contract address
    deposit_addr = converters.contract_hash_to_address(NEOFS_CONTRACT)
    logger.info(f"NeoFS contract address: {deposit_addr}")

    address_to = _address_from_wallet(wallet_to, wallet_password)

    txid = nep17_transfer(wallet_to, deposit_addr, amount, NEO_MAINNET_ENDPOINT,
                          wallet_pass=wallet_password, addr_from=address_to)
    if not transaction_accepted(txid):
        raise AssertionError(f"TX {txid} hasn't been processed")


def _address_from_wallet(wlt: str, wallet_password: str):
    """
    Extracting the address from the given wallet.
    Args:
        wlt (str):  the path to the wallet to extract address from
        wallet_password (str): the password for the given wallet
    Returns:
        (str): the address for the wallet
    """
    wallet_loaded = load_wallet(wlt, wallet_password)
    address = wallet_loaded.accounts[-1].address
    logger.info(f"got address: {address}")
    return address


@keyword('Get Mainnet Balance')
def get_mainnet_balance(address: str):
    resp = mainnet_rpc_cli.get_nep17_balances(address=address)
    logger.info(f"Got getnep17balances response: {resp}")
    for balance in resp['balance']:
        if balance['assethash'] == GAS_HASH:
            return float(balance['amount'])/ASSET_POWER_MAINCHAIN
    return float(0)


@keyword('Get Sidechain Balance')
def get_sidechain_balance(address: str):
    resp = morph_rpc_cli.get_nep17_balances(address=address)
    logger.info(f"Got getnep17balances response: {resp}")
    for balance in resp['balance']:
        if balance['assethash'] == GAS_HASH:
            return float(balance['amount'])/ASSET_POWER_SIDECHAIN
    return float(0)
