#!/usr/bin/python3

import subprocess
import re

from robot.api.deco import keyword
from robot.api import logger

import logging
import robot.errors
import requests

from robot.libraries.BuiltIn import BuiltIn
from neocore.KeyPair import KeyPair

from Crypto import Random

ROBOT_AUTO_KEYWORDS = False

@keyword('Request NeoFS Deposit')
def request_neofs_deposit(public_key: str):
    """
    This function requests Deposit to the selected public key.
    :param public_key:      neo public key
    """

    response = requests.get('https://fs.localtest.nspcc.ru/api/deposit/'+str(public_key), verify='ca/nspcc-ca.pem')  
    
    if response.status_code != 200:
        BuiltIn().fatal_error('Can not run Deposit to {} with error: {}'.format(public_key, response.text))
    else:
        logger.info("Deposit has been completed for '%s'; tx: '%s'" % (public_key, response.text) )

    return response.text

@keyword('Get Balance')
def get_balance(public_key: str):
    """
    This function returns NeoFS balance for selected public key.
    :param public_key:      neo public key
    """

    balance = _get_balance_request(public_key)

    return balance

@keyword('Expected Balance')
def expected_balance(public_key: str, init_amount: float, deposit_size: float):
    """
    This function returns NeoFS balance for selected public key.
    :param public_key:      neo public key
    :param init_amount:     initial number of tokens in the account
    :param deposit_size:    expected amount of the balance increasing
    """

    balance = _get_balance_request(public_key)

    deposit_change = round((float(balance) - init_amount),8)
    if deposit_change != deposit_size:
        raise Exception('Expected deposit increase: {}. This does not correspond to the actual change in account: {}'.format(deposit_size, deposit_change))

    logger.info('Expected deposit increase: {}. This correspond to the actual change in account: {}'.format(deposit_size, deposit_change))

    return deposit_change


def _get_balance_request(public_key: str):
    '''
    Internal method.
    '''
    response = requests.get('https://fs.localtest.nspcc.ru/api/balance/neofs/'+str(public_key)+'/', verify='ca/nspcc-ca.pem')  
    
    if response.status_code != 200:
        raise Exception('Can not get balance for {} with error: {}'.format(public_key, response.text))
    
    m = re.match(r"\"+([\d.\.?\d*]+)", response.text )
    if m is None:
        BuiltIn().fatal_error('Can not parse balance: "%s"' % response.text)
    balance = m.group(1)

    logger.info("Balance for '%s' is '%s'" % (public_key, balance) )

    return balance