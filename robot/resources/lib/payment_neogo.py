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

from robot.api.deco import keyword
from robot.api import logger
import robot.errors
from robot.libraries.BuiltIn import BuiltIn

ROBOT_AUTO_KEYWORDS = False


if os.getenv('ROBOT_PROFILE') == 'selectel_smoke':
    from selectelcdn_smoke_vars import (NEO_MAINNET_ENDPOINT,
    NEOFS_NEO_API_ENDPOINT, NEOFS_ENDPOINT, GAS_HASH, NEOFS_CONTRACT, TEMP_DIR)
else:
    from neofs_int_vars import (NEO_MAINNET_ENDPOINT,
    NEOFS_NEO_API_ENDPOINT, NEOFS_ENDPOINT, GAS_HASH, NEOFS_CONTRACT, TEMP_DIR)

# path to neofs-cli executable
NEOFS_CLI_EXEC = os.getenv('NEOFS_CLI_EXEC', 'neofs-cli')
NEOGO_CLI_EXEC = os.getenv('NEOGO_CLI_EXEC', 'neo-go')

@keyword('Init wallet')
def init_wallet():
    filename = os.getcwd() + '/' + TEMP_DIR + str(uuid.uuid4()) + ".json"
    cmd = f"{NEOGO_CLI_EXEC} wallet init -w {filename}"
    logger.info(f"Executing command: {cmd}")
    stdout  = _run_sh(cmd)
    logger.info(f"wallet init succeeded with output: {stdout}")
    return filename

@keyword('Generate wallet from WIF')
def generate_wallet_from_wif(wallet: str, wif: str):
    cmd = f"{NEOGO_CLI_EXEC} wallet import --wallet {wallet} --wif {wif}"
    out = _run_sh_wallet_gen(cmd)
    logger.info(f"Command completed with output: {out}")

@keyword('Generate wallet')
def generate_wallet(wallet: str):
    cmd = f"{NEOGO_CLI_EXEC} wallet create -w {wallet}"
    out = _run_sh_wallet_gen(cmd)
    logger.info(f"Command completed with output: {out}")

@keyword('Dump Address')
def dump_address(wallet: str):
    with open(wallet) as json_file:
        data = json.load(json_file)
        if len(data['accounts']) != 0:
            return data['accounts'][0]['address']
        else:
            raise Exception("Can not get address.")

@keyword('Dump PrivKey')
def dump_privkey(wallet: str, address: str):
    cmd = f"{NEOGO_CLI_EXEC} wallet export -w {wallet} --decrypt {address}"
    logger.info(f"Executing command: {cmd}")
    out = _run_sh_with_passwd('\r', cmd)
    logger.info(f"Command completed with output: {out}")
    return out

@keyword('Transfer Mainnet Gas')
def transfer_mainnet_gas(wallet: str, address: str, address_to: str, amount: int, wallet_pass:str=''):
    cmd = (
        f"{NEOGO_CLI_EXEC} wallet nep17 transfer -w {wallet} "
        f"-r {NEO_MAINNET_ENDPOINT} --from {address} --to {address_to} "
        f"--token GAS --amount {amount}"
    )

    logger.info(f"Executing command: {cmd}")
    out = _run_sh_with_passwd(wallet_pass, cmd)
    logger.info(f"Command completed with output: {out}")

    if not re.match(r'^(\w{64})$', out):
        raise Exception("Can not get Tx.")

    return out

@keyword('Withdraw Mainnet Gas')
def withdraw_mainnet_gas(wallet: str, address: str, scripthash: str, amount: int):
    cmd = (
        f"{NEOGO_CLI_EXEC} contract invokefunction -w {wallet} -a {address} "
        f"-r {NEO_MAINNET_ENDPOINT} {NEOFS_CONTRACT} withdraw {scripthash} "
        f"int:{amount}  -- {scripthash}"
    )

    logger.info(f"Executing command: {cmd}")
    out = _run_sh_with_passwd('', cmd)
    logger.info(f"Command completed with output: {out}")
    m = re.match(r'^Sent invocation transaction (\w{64})$', out)
    if m is None:
        raise Exception("Can not get Tx.")
    tx = m.group(1)
    return tx

@keyword('Mainnet Balance')
def mainnet_balance(address: str):
    headers = {'Content-type': 'application/json'}
    data = { "jsonrpc": "2.0", "id": 5, "method": "getnep17balances", "params": [ address ] }
    response = requests.post(NEO_MAINNET_ENDPOINT, json=data, headers=headers, verify=False)

    if not response.ok:
        raise Exception(f"""Failed:
                request: {data},
                response: {response.text},
                status code: {response.status_code} {response.reason}""")

    m = re.search(rf'"{GAS_HASH}","amount":"([\d\.]+)"', response.text)
    if not m:
        raise Exception("Can not get mainnet gas balance. Output: %s" % response.text )
    else:
        logger.info("Output: %s" % response.text)
    amount = m.group(1)
    return amount

@keyword('Expected Mainnet Balance')
def expected_mainnet_balance(address: str, expected: float):
    amount = mainnet_balance(address)
    gas_expected = int(expected * 10**8)
    if int(amount) != int(gas_expected):
        raise Exception(f"Expected amount ({gas_expected}) of GAS has not been found. Found {amount}.")
    return amount

@keyword('Expected Mainnet Balance Diff')
def expected_mainnet_balance_diff(address: str, old_value: float, expected_diff: float):
    amount = mainnet_balance(address)
    gas_expected = old_value + _convert_int_to_gas(expected_diff)
    if int(amount) != int(gas_expected):
        raise Exception(f"Balance amount ({int(amount)})) of GAS has not been changed for expected value:",
                        f"{_convert_int_to_gas(expected_diff)} from initial {old_value}.",
                        f"Expected: {old_value + _convert_int_to_gas(expected_diff)}")
    return amount

def _convert_int_to_gas(input_value: float):
    return int(input_value * 10**8)

def _convert_gas_to_int(input_value: float):
    return int(input_value / 10**8)

@keyword('NeoFS Deposit')
def neofs_deposit(wallet: str, address: str, scripthash: str, amount: int, wallet_pass:str=''):
    cmd = ( f"{NEOGO_CLI_EXEC} contract invokefunction -w {wallet} -a {address} "
            f"-r {NEO_MAINNET_ENDPOINT} {NEOFS_CONTRACT} "
            f"deposit {scripthash} int:{amount} bytes: -- {scripthash}")

    logger.info(f"Executing command: {cmd}")
    out = _run_sh_with_passwd(wallet_pass, cmd)
    logger.info(f"Command completed with output: {out}")

    m = re.match(r'^Sent invocation transaction (\w{64})$', out)
    if m is None:
        raise Exception("Can not get Tx.")

    tx = m.group(1)
    return tx

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


@keyword('Get Balance')
def get_balance(privkey: str):
    """
    This function returns NeoFS balance for selected public key.
    :param public_key:      neo public key
    """

    balance = _get_balance_request(privkey)

    return balance

@keyword('Expected Balance')
def expected_balance(privkey: str, init_amount: float, deposit_size: float):
    """
    This function returns NeoFS balance for selected public key.
    :param public_key:      neo public key
    :param init_amount:     initial number of tokens in the account
    :param deposit_size:    expected amount of the balance increasing
    """

    balance = _get_balance_request(privkey)

    deposit_change = round((float(balance) - init_amount),8)
    if deposit_change != deposit_size:
        raise Exception('Expected deposit increase: {}. This does not correspond to the actual change in account: {}'.format(deposit_size, deposit_change))

    logger.info('Expected deposit increase: {}. This correspond to the actual change in account: {}'.format(deposit_size, deposit_change))

    return deposit_change

def _get_balance_request(privkey: str):
    '''
    Internal method.
    '''
    Cmd = (
        f'{NEOFS_CLI_EXEC} --key {privkey} --rpc-endpoint {NEOFS_ENDPOINT}'
        f' accounting balance'
    )
    logger.info("Cmd: %s" % Cmd)
    complProc = subprocess.run(Cmd, check=True, universal_newlines=True,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=150, shell=True)
    output = complProc.stdout
    logger.info("Output: %s" % output)


    m = re.match(r'([\d.\.?\d*]+)', output )
    if m is None:
        BuiltIn().fatal_error('Can not parse balance: "%s"' % output)
    balance = m.group(1)

    logger.info("Balance for '%s' is '%s'" % (privkey, balance) )

    return balance

def _run_sh(args):
    complProc = subprocess.run(args, check=True, universal_newlines=True,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                timeout=150, shell=True)
    output, errors = complProc.stdout, complProc.stderr
    if errors:
        return errors
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

def _run_sh_wallet_gen(cmd):
    '''
    Internal method.
    '''
    logger.info(f"Executing command: {cmd}")
    p = pexpect.spawn(cmd)
    p.expect(".*")
    p.sendline('\r')
    p.expect(".*")
    p.sendline('\r')
    p.expect(".*")
    p.sendline('\r')
    p.wait()
    out = p.read()
    
    return out