#!/usr/bin/python3

import base64
import json
import os
import re
import random
import uuid
import base58

from neo3 import wallet
from common import (NEOFS_NETMAP, WALLET_PASS, NEOFS_ENDPOINT,
NEOFS_NETMAP_DICT, ASSETS_DIR)
from cli_helpers import _cmd_run
import json_transformers
from robot.api.deco import keyword
from robot.api import logger

ROBOT_AUTO_KEYWORDS = False

# path to neofs-cli executable
NEOFS_CLI_EXEC = os.getenv('NEOFS_CLI_EXEC', 'neofs-cli')


# TODO: move to neofs-keywords
@keyword('Get ScriptHash')
def get_scripthash(wif: str):
    acc = wallet.Account.from_wif(wif, '')
    return str(acc.script_hash)


@keyword('Verify Head Tombstone')
def verify_head_tombstone(wallet: str, cid: str, oid_ts: str, oid: str, addr: str):
    # TODO: replace with HEAD from neofs_verbs.py
    object_cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wallet {wallet} '
        f'--config {WALLET_PASS} object head --cid {cid} --oid {oid_ts} --json'
    )
    output = _cmd_run(object_cmd)
    full_headers = json.loads(output)
    logger.info(f"Output: {full_headers}")

    # Header verification
    header_cid = full_headers["header"]["containerID"]["value"]
    if json_transformers.json_reencode(header_cid) == cid:
        logger.info(f"Header CID is expected: {cid} ({header_cid} in the output)")
    else:
        raise Exception("Header CID is not expected.")

    header_owner = full_headers["header"]["ownerID"]["value"]
    if json_transformers.json_reencode(header_owner) == addr:
        logger.info(f"Header ownerID is expected: {addr} ({header_owner} in the output)")
    else:
        raise Exception("Header ownerID is not expected.")

    header_type = full_headers["header"]["objectType"]
    if header_type == "TOMBSTONE":
        logger.info(f"Header Type is expected: {header_type}")
    else:
        raise Exception("Header Type is not expected.")

    header_session_type = full_headers["header"]["sessionToken"]["body"]["object"]["verb"]
    if header_session_type == "DELETE":
        logger.info(f"Header Session Type is expected: {header_session_type}")
    else:
        raise Exception("Header Session Type is not expected.")

    header_session_cid = full_headers["header"]["sessionToken"]["body"]["object"]["address"]["containerID"]["value"]
    if json_transformers.json_reencode(header_session_cid) == cid:
        logger.info(f"Header ownerID is expected: {addr} ({header_session_cid} in the output)")
    else:
        raise Exception("Header Session CID is not expected.")

    header_session_oid = full_headers["header"]["sessionToken"]["body"]["object"]["address"]["objectID"]["value"]
    if json_transformers.json_reencode(header_session_oid) == oid:
        logger.info(f"Header Session OID (deleted object) is expected: {oid} ({header_session_oid} in the output)")
    else:
        raise Exception("Header Session OID (deleted object) is not expected.")


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


@keyword('Generate Session Token')
def generate_session_token(owner: str, pub_key: str, cid: str = "", wildcard: bool = False) -> str:

    file_path = f"{os.getcwd()}/{ASSETS_DIR}/{str(uuid.uuid4())}"

    owner_64 = base64.b64encode(base58.b58decode(owner)).decode('utf-8')
    cid_64 = base64.b64encode(cid.encode('utf-8')).decode('utf-8')
    pub_key_64 = base64.b64encode(bytes.fromhex(pub_key)).decode('utf-8')
    id_64 = base64.b64encode(uuid.uuid4().bytes).decode('utf-8')

    session_token = {
                    "body":{
                        "id":f"{id_64}",
                        "ownerID":{
                            "value":f"{owner_64}"
                        },
                        "lifetime":{
                            "exp":"100000000",
                            "nbf":"0",
                            "iat":"0"
                        },
                        "sessionKey":f"{pub_key_64}",
                        "container":{
                            "verb":"PUT",
                            "wildcard": wildcard,
                            **({ "containerID":{"value":f"{cid_64}"} } if not wildcard else {})
                        }
                    }
                }

    logger.info(f"Got this Session Token: {session_token}")

    with open(file_path, 'w', encoding='utf-8') as session_token_file:
        json.dump(session_token, session_token_file, ensure_ascii=False, indent=4)

    return file_path


@keyword ('Sign Session Token')
def sign_session_token(session_token: str, wallet: str, to_file: str=''):
    if to_file:
        to_file = f'--to {to_file}'
    cmd = (
        f'{NEOFS_CLI_EXEC} util sign session-token --from {session_token} '
        f'-w {wallet} {to_file} --config {WALLET_PASS}'
    )
    logger.info(f"cmd: {cmd}")
    _cmd_run(cmd)
