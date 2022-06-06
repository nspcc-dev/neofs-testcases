#!/usr/bin/python3

import json
import os
import random

from neo3 import wallet
from common import NEOFS_NETMAP_DICT
import neofs_verbs
from robot.api.deco import keyword
from robot.api import logger
from robot.libraries.BuiltIn import BuiltIn

ROBOT_AUTO_KEYWORDS = False

# path to neofs-cli executable
NEOFS_CLI_EXEC = os.getenv('NEOFS_CLI_EXEC', 'neofs-cli')


# TODO: move to neofs-keywords
@keyword('Get ScriptHash')
def get_scripthash(wif: str):
    acc = wallet.Account.from_wif(wif, '')
    return str(acc.script_hash)


@keyword('Verify Head Tombstone')
def verify_head_tombstone(wallet_path: str, cid: str, oid_ts: str, oid: str):
    header = neofs_verbs.head_object(wallet_path, cid, oid_ts)
    header = header['header']

    BuiltIn().should_be_equal(header["containerID"], cid,
            msg="Tombstone Header CID is wrong")

    wlt_data = dict()
    with open(wallet_path, 'r') as fout:
        wlt_data = json.loads(fout.read())
    wlt = wallet.Wallet.from_json(wlt_data, password='')
    addr = wlt.accounts[0].address

    BuiltIn().should_be_equal(header["ownerID"], addr,
            msg="Tombstone Owner ID is wrong")

    BuiltIn().should_be_equal(header["objectType"], 'TOMBSTONE',
            msg="Header Type isn't Tombstone")

    BuiltIn().should_be_equal(header["sessionToken"]["body"]["object"]["verb"], 'DELETE',
            msg="Header Session Type isn't DELETE")

    BuiltIn().should_be_equal(header["sessionToken"]["body"]["object"]["address"]["containerID"],
            cid,
            msg="Header Session ID is wrong")

    BuiltIn().should_be_equal(header["sessionToken"]["body"]["object"]["address"]["objectID"],
            oid,
            msg="Header Session OID is wrong")


@keyword('Get control endpoint with wif')
def get_control_endpoint_with_wif(endpoint_number: str = ''):
    if endpoint_number == '':
        netmap = []
        for key in NEOFS_NETMAP_DICT.keys():
            netmap.append(key)
        endpoint_num = random.sample(netmap, 1)[0]
        logger.info(f'Random node chosen: {endpoint_num}')
    else:
        endpoint_num = endpoint_number

    endpoint_values = NEOFS_NETMAP_DICT[f'{endpoint_num}']
    endpoint_control = endpoint_values['control']
    wif = endpoint_values['wif']

    return endpoint_num, endpoint_control, wif


@keyword('Get Locode')
def get_locode():
    endpoint_values = random.choice(list(NEOFS_NETMAP_DICT.values()))
    locode = endpoint_values['UN-LOCODE']
    logger.info(f'Random locode chosen: {locode}')

    return locode
