#!/usr/bin/python3.8

import base64
import json
import os
import re
import uuid
from enum import Enum, auto

import base58
from cli_helpers import _cmd_run
from common import ASSETS_DIR, NEOFS_ENDPOINT, WALLET_CONFIG
from data_formatters import pub_key_hex
from robot.api import logger
from robot.api.deco import keyword

"""
Robot Keywords and helper functions for work with NeoFS ACL.
"""

ROBOT_AUTO_KEYWORDS = False

# path to neofs-cli executable
NEOFS_CLI_EXEC = os.getenv('NEOFS_CLI_EXEC', 'neofs-cli')
EACL_LIFETIME = 100500


class AutoName(Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name


class Role(AutoName):
    USER = auto()
    SYSTEM = auto()
    OTHERS = auto()


@keyword('Get eACL')
def get_eacl(wallet_path: str, cid: str):
    cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wallet {wallet_path} '
        f'container get-eacl --cid {cid} --config {WALLET_CONFIG}'
    )
    try:
        output = _cmd_run(cmd)
        if re.search(r'extended ACL table is not set for this container', output):
            return None
        return output
    except RuntimeError as exc:
        logger.info("Extended ACL table is not set for this container")
        logger.info(f"Got exception while getting eacl: {exc}")
        return None


@keyword('Set eACL')
def set_eacl(wallet_path: str, cid: str, eacl_table_path: str):
    cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wallet {wallet_path} '
        f'container set-eacl --cid {cid} --table {eacl_table_path} --config {WALLET_CONFIG} --await'
    )
    _cmd_run(cmd)


def _encode_cid_for_eacl(cid: str) -> str:
    cid_base58 = base58.b58decode(cid)
    return base64.b64encode(cid_base58).decode("utf-8")


@keyword('Create eACL')
def create_eacl(cid: str, rules_list: list):
    table = f"{os.getcwd()}/{ASSETS_DIR}/eacl_table_{str(uuid.uuid4())}.json"
    rules = ""
    for rule in rules_list:
        # TODO: check if $Object: is still necessary for filtering in the newest releases
        rules += f"--rule '{rule}' "
    cmd = (
        f"{NEOFS_CLI_EXEC} acl extended create --cid {cid} "
        f"{rules}--out {table}"
    )
    _cmd_run(cmd)

    return table


@keyword('Form BearerToken File')
def form_bearertoken_file(wif: str, cid: str, eacl_records: list) -> str:
    """
    This function fetches eACL for given <cid> on behalf of <wif>,
    then extends it with filters taken from <eacl_records>, signs
    with bearer token and writes to file
    """
    enc_cid = _encode_cid_for_eacl(cid)
    file_path = f"{os.getcwd()}/{ASSETS_DIR}/{str(uuid.uuid4())}"

    eacl = get_eacl(wif, cid)
    json_eacl = dict()
    if eacl:
        eacl = eacl.replace('eACL: ', '')
        eacl = eacl.split('Signature')[0]
        json_eacl = json.loads(eacl)
    logger.info(json_eacl)
    eacl_result = {
        "body":
            {
                "eaclTable":
                    {
                        "containerID":
                            {
                                "value": enc_cid
                            },
                        "records": []
                    },
                "lifetime":
                    {
                        "exp": EACL_LIFETIME,
                        "nbf": "1",
                        "iat": "0"
                    }
            }
    }

    if not eacl_records:
        raise (f"Got empty eacl_records list: {eacl_records}")
    for record in eacl_records:
        op_data = {
            "operation": record['Operation'],
            "action": record['Access'],
            "filters": [],
            "targets": []
        }

        if Role(record['Role']):
            op_data['targets'] = [
                {
                    "role": record['Role']
                }
            ]
        else:
            op_data['targets'] = [
                {
                    "keys": [record['Role']]
                }
            ]

        if 'Filters' in record.keys():
            op_data["filters"].append(record['Filters'])

        eacl_result["body"]["eaclTable"]["records"].append(op_data)

    # Add records from current eACL
    if "records" in json_eacl.keys():
        for record in json_eacl["records"]:
            eacl_result["body"]["eaclTable"]["records"].append(record)

    with open(file_path, 'w', encoding='utf-8') as eacl_file:
        json.dump(eacl_result, eacl_file, ensure_ascii=False, indent=4)

    logger.info(f"Got these extended ACL records: {eacl_result}")
    sign_bearer_token(wif, file_path)
    return file_path

@keyword('EACL Rules')
def eacl_rules(access: str, verbs: list, user: str):
    """
        This function creates a list of eACL rules.
        Args:
            access (str): identifies if the following operation(s)
                        is allowed or denied
            verbs (list): a list of operations to set rules for
            user (str): a group of users (user/others) or a wallet of
                        a certain user for whom rules are set
        Returns:
            (list): a list of eACL rules
    """
    if user not in ('others', 'user'):
        pubkey = pub_key_hex(user)
        user = f"pubkey:{pubkey}"

    rules = []
    for verb in verbs:
        elements = [access, verb, user]
        rules.append(' '.join(elements))
    return rules


def sign_bearer_token(wallet_path: str, eacl_rules_file: str):
    cmd = (
        f'{NEOFS_CLI_EXEC} util sign bearer-token --from {eacl_rules_file} '
        f'--to {eacl_rules_file} --wallet {wallet_path} --config {WALLET_CONFIG} --json'
    )
    _cmd_run(cmd)
