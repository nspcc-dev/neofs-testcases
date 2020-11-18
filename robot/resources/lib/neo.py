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
NEOFS_NEO_API_ENDPOINT = "main_chain.neofs.devenv:30333"

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
    wif = keypair_gen.Export()
    logger.info("Generated Neo address: %s" % address)
    logger.info("Generated WIF: %s" % wif)
    return address

