#!/usr/bin/python3

import os
import pexpect
import re

from robot.api.deco import keyword
from robot.api import logger
from neo3 import wallet

from common import *
import rpc_client
import contract

ROBOT_AUTO_KEYWORDS = False

NNS_CONTRACT = contract.get_nns_contract_hash(NEOFS_NEO_API_ENDPOINT)
BALANCE_CONTRACT_HASH = contract.get_morph_contract_hash(
            'balance.neofs', NNS_CONTRACT, NEOFS_NEO_API_ENDPOINT
        )
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
    out = _run_sh_with_passwd('', cmd)
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
                BALANCE_CONTRACT_HASH, 'balanceOf', payload
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
