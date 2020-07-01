#!/usr/bin/python3

import subprocess
import re
import json
import binascii

from robot.api.deco import keyword
from robot.api import logger

import robot.errors
import requests

from robot.libraries.BuiltIn import BuiltIn
from neocore.KeyPair import KeyPair

from Crypto import Random

ROBOT_AUTO_KEYWORDS = False
NEOFS_NEO_API_ENDPOINT = "https://fs.localtest.nspcc.ru/neo_rpc/"

@keyword('Generate Neo private key')
def generate_neo_private_key():
    """
    This function generates new random Neo private key.
    Parameters: None
    :rtype:                 'bytes' object
    """
    private_key = Random.get_random_bytes(32)
    logger.info("Generated private key: %s" % binascii.hexlify(private_key))

    return private_key


@keyword('Get Neo public key')
def get_neo_public_key(private_key: bytes):
    """
    This function return neo public key.
    Parameters:
    :param private_key:     neo private key
    :rtype:                 string
    """
    keypair_gen = KeyPair(bytes(private_key))
    pubkey = keypair_gen.PublicKey.encode_point(True).decode("utf-8")
    logger.info("Generated public key: %s" % pubkey)
    return pubkey

@keyword('Get Neo address')
def get_neo_address(private_key: bytes):
    """
    This function return neo address.
    Parameters:
    :param private_key:     neo private key
    :rtype:                 string
    """
    keypair_gen = KeyPair(private_key)
    address = keypair_gen.GetAddress()
    logger.info("Generated Neo address: %s" % address)
    return address

@keyword('Transaction accepted in block')
def transaction_accepted_in_block(tx_id: str):
    """
    This function return True in case of accepted TX.
    Parameters:
    :param tx_id:           transaction is
    :rtype:                 block number or Exception
    """

    logger.info("Transaction id: %s" % tx_id)
    m = re.match(r"^\"0x+([\w.]+)", tx_id)
    if m is None:
        BuiltIn().fatal_error('Can not parse transaction id: "%s"' % tx_id)

    TX_request = 'curl -X POST '+NEOFS_NEO_API_ENDPOINT+' --cacert ca/nspcc-ca.pem -H \'Content-Type: application/json\' -d \'{ "jsonrpc": "2.0", "id": 5, "method": "gettransactionheight", "params": [\"'+m.group(1)+'\"] }\''
    complProc = subprocess.run(TX_request, check=True, universal_newlines=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)
    logger.info(complProc.stdout)
    response = json.loads(complProc.stdout)

    if (response['result'] == 0):
        raise Exception( "Transaction is not found in the blocks." )

    logger.info("Transaction has been found in the block %s." % response['result'] )
    return response['result']
    

@keyword('Get Transaction')
def get_transaction(tx_id: str):
    """
    This function return information about TX.
    Parameters:
    :param tx_id:           transaction id
    """

    m = re.match(r"^\"0x+([\w.]+)", tx_id)
    if m is None:
        BuiltIn().fatal_error('Can not parse transaction id: "%s"' % tx_id)

    TX_request = 'curl -X POST '+NEOFS_NEO_API_ENDPOINT+' --cacert ca/nspcc-ca.pem -H \'Content-Type: application/json\' -d \'{ "jsonrpc": "2.0", "id": 5, "method": "getapplicationlog", "params": [\"'+m.group(1)+'\"] }\''
    complProc = subprocess.run(TX_request, check=True, universal_newlines=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)
    logger.info(complProc.stdout)
    