#!/usr/bin/python3

import subprocess
import pexpect
import re
import uuid
import logging
import requests
import json
import os
import tarfile
import sys

sys.path.insert(0,'../neofs-keywords')
import converters
import wallet

from robot.api.deco import keyword
from robot.api import logger
import robot.errors
from robot.libraries.BuiltIn import BuiltIn

from common import *

ROBOT_AUTO_KEYWORDS = False

# path to neofs-cli executable
NEOFS_CLI_EXEC = os.getenv('NEOFS_CLI_EXEC', 'neofs-cli')
NEOGO_CLI_EXEC = os.getenv('NEOGO_CLI_EXEC', 'neo-go')


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


@keyword('NeoFS Deposit')
def neofs_deposit(wallet_file: str, address: str, scripthash: str, amount: int, wallet_pass:str=''):

    # 1) Get NeoFS contract address.
    deposit_addr = converters.contract_hash_to_address(NEOFS_CONTRACT)
    logger.info(f"deposit_addr: {deposit_addr}")

    # 2) Transfer GAS to the NeoFS contract address.
    out = wallet.new_nep17_transfer(address, deposit_addr, amount, 'GAS', wallet_file, '', NEO_MAINNET_ENDPOINT)

    if len(out) != 64:
        raise Exception("Can not get Tx.")

    return out

@keyword('Transaction accepted in block')
def transaction_accepted_in_block(tx_id):
    """
    This function return True in case of accepted TX.
    Parameters:
    :param tx_id:           transaction is
    :rtype:                 block number or Exception
    """

    logger.info("Transaction id: %s" % tx_id)

    headers = {'Content-type': 'application/json'}
    data = { "jsonrpc": "2.0", "id": 5, "method": "gettransactionheight", "params": [ tx_id ] }
    response = requests.post(NEO_MAINNET_ENDPOINT, json=data, headers=headers, verify=False)

    if not response.ok:
        raise Exception(f"""Failed:
                request: {data},
                response: {response.text},
                status code: {response.status_code} {response.reason}""")

    if (response.text == 0):
        raise Exception( "Transaction is not found in the blocks." )

    logger.info("Transaction has been found in the block %s." % response.text )
    return response.text

@keyword('Get Transaction')
def get_transaction(tx_id: str):
    """
    This function return information about TX.
    Parameters:
    :param tx_id:           transaction id
    """

    headers = {'Content-type': 'application/json'}
    data = { "jsonrpc": "2.0", "id": 5, "method": "getapplicationlog", "params": [ tx_id ] }
    response = requests.post(NEO_MAINNET_ENDPOINT, json=data, headers=headers, verify=False)

    if not response.ok:
        raise Exception(f"""Failed:
                request: {data},
                response: {response.text},
                status code: {response.status_code} {response.reason}""")
    else:
        logger.info(response.text)


@keyword('Get NeoFS Balance')
def get_balance(privkey: str):
    """
    This function returns NeoFS balance for selected public key.
    :param public_key:      neo public key
    """

    balance = _get_balance_request(privkey)

    return float(balance)


def _get_balance_request(privkey: str):
    '''
    Internal method.
    '''
    Cmd = (
        f'{NEOFS_CLI_EXEC} --key {privkey} --rpc-endpoint {NEOFS_ENDPOINT}'
        f' accounting balance'
    )
    logger.info(f"Cmd: {Cmd}")
    complProc = subprocess.run(Cmd, check=True, universal_newlines=True,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=150, shell=True)
    output = complProc.stdout
    logger.info(f"Output: {output}")

    if output is None:
        BuiltIn().fatal_error(f'Can not parse balance: "{output}"')

    logger.info(f"Balance for '{privkey}' is '{output}'" )

    return output

def _run_sh_with_passwd(passwd, cmd):
    p = pexpect.spawn(cmd)
    p.expect(".*")
    p.sendline(passwd + '\r')
    p.wait()
    # throw a string with password prompt
    # take a string with tx hash
    tx_hash = p.read().splitlines()[-1]
    return tx_hash.decode()
