#!/usr/bin/python3

import re
import time
import pexpect
import converters

from neo3 import wallet
from common import (NEOFS_NEO_API_ENDPOINT, NEO_MAINNET_ENDPOINT,
NEOFS_CONTRACT, NEOGO_CLI_EXEC)
import rpc_client
import contract
from wrappers import run_sh_with_passwd_contract
import wallet_keywords
from robot.api.deco import keyword
from robot.api import logger

ROBOT_AUTO_KEYWORDS = False

MORPH_TOKEN_POWER = 12

morph_rpc_cli = rpc_client.RPCClient(NEOFS_NEO_API_ENDPOINT)
mainnet_rpc_cli = rpc_client.RPCClient(NEO_MAINNET_ENDPOINT)


@keyword('Withdraw Mainnet Gas')
def withdraw_mainnet_gas(wallet: str, address: str, scripthash: str, amount: int):
    cmd = (
        f"{NEOGO_CLI_EXEC} contract invokefunction -w {wallet} -a {address} "
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
    return tx


@keyword('Transaction accepted in block')
def transaction_accepted_in_block(tx_id: str):
    """
    This function return True in case of accepted TX.
    Parameters:
    :param tx_id:           transaction ID
    """

    try:
        resp = mainnet_rpc_cli.get_transaction_height(tx_id)
        if resp is not None:
            logger.info(f"got block height: {resp}")
            return True
    except Exception as e:
        logger.info(f"request failed with error: {e}")
        raise e


@keyword('NeoFS Deposit')
def neofs_deposit(wif: str, amount: int):
    """
        Transferring GAS from given wallet to NeoFS contract address.
        Args:
            wif (str): the wif of the wallet to transfer GAS from
            amount (str): the amount of GAS to transfer
    """
    # get NeoFS contract address
    deposit_addr = converters.contract_hash_to_address(NEOFS_CONTRACT)
    logger.info(f"NeoFS contract address: {deposit_addr}")
    tx_id = wallet_keywords.transfer_mainnet_gas(wif, deposit_addr, amount)

    i = 0
    while i < 60: # deadline in seconds to accept transaction
        time.sleep(1)
        if transaction_accepted_in_block(tx_id):
            return
        i += 1
    raise RuntimeError(
                    f"After 60 seconds the transaction "
                    f"{tx_id} hasn't been done; exiting"
                )


@keyword('Get NeoFS Balance')
def get_balance(wif: str):
    """
    This function returns NeoFS balance for given WIF.
    """

    acc = wallet.Account.from_wif(wif, '')
    payload = [
                {
                    'type': 'Hash160',
                    'value': str(acc.script_hash)
                }
            ]
    try:
        resp = morph_rpc_cli.invoke_function(
                contract.get_balance_contract_hash(NEOFS_NEO_API_ENDPOINT),
                'balanceOf',
                payload
            )
        logger.info(resp)
        value = int(resp['stack'][0]['value'])
        return value/(10**MORPH_TOKEN_POWER)
    except Exception as e:
        logger.error(f"failed to get {wif} balance: {e}")
        raise e


def _run_sh_with_passwd(passwd, cmd):
    p = pexpect.spawn(cmd)
    p.expect(".*")
    p.sendline(passwd + '\r')
    p.wait()
    # throw a string with password prompt
    # take a string with tx hash
    tx_hash = p.read().splitlines()[-1]
    return tx_hash.decode()
