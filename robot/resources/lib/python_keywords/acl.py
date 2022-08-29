import base64
import json
import logging
import os
import uuid
from dataclasses import dataclass
from enum import Enum
from time import sleep
from typing import Any, Dict, List, Optional, Union

import allure
import base58
from cli_helpers import _cmd_run
from cli_utils import NeofsCli
from common import ASSETS_DIR, NEOFS_CLI_EXEC, NEOFS_ENDPOINT, WALLET_CONFIG
from data_formatters import get_wallet_public_key

logger = logging.getLogger('NeoLogger')
EACL_LIFETIME = 100500
NEOFS_CONTRACT_CACHE_TIMEOUT = 30


class EACLOperation(Enum):
    PUT = 'put'
    GET = 'get'
    HEAD = 'head'
    GET_RANGE = 'getrange'
    GET_RANGE_HASH = 'getrangehash'
    SEARCH = 'search'
    DELETE = 'delete'


class EACLAccess(Enum):
    ALLOW = 'allow'
    DENY = 'deny'


class EACLRole(Enum):
    OTHERS = 'others'
    USER = 'user'
    SYSTEM = 'system'


class EACLHeaderType(Enum):
    REQUEST = 'req'  # Filter request headers
    OBJECT = 'obj'  # Filter object headers
    SERVICE = 'SERVICE'	 # Filter service headers. These are not processed by NeoFS nodes and exist for service use only


class EACLMatchType(Enum):
    STRING_EQUAL = '='  # Return true if strings are equal
    STRING_NOT_EQUAL = '!='  # Return true if strings are different


@dataclass
class EACLFilter:
    header_type: EACLHeaderType = EACLHeaderType.REQUEST
    match_type: EACLMatchType = EACLMatchType.STRING_EQUAL
    key: Optional[str] = None
    value: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {'headerType': self.header_type, 'matchType': self.match_type, 'key': self.key, 'value': self.value}


@dataclass
class EACLFilters:
    filters: Optional[List[EACLFilter]] = None

    def __str__(self):
        return ','.join(
            [f'{filter.header_type.value}:{filter.key}{filter.match_type.value}{filter.value}'
             for filter in self.filters]
        ) if self.filters else []


@dataclass
class EACLPubKey:
    keys: Optional[List[str]] = None


@dataclass
class EACLRule:
    operation: Optional[EACLOperation] = None
    access: Optional[EACLAccess] = None
    role: Optional[Union[EACLRole, str]] = None
    filters: Optional[EACLFilters] = None

    def to_dict(self) -> Dict[str, Any]:
        return {'Operation': self.operation, 'Access': self.access, 'Role': self.role,
                'Filters': self.filters or []}

    def __str__(self):
        role = self.role.value if isinstance(self.role, EACLRole) else f'pubkey:{get_wallet_public_key(self.role, "")}'
        return f'{self.access.value} {self.operation.value} {self.filters or ""} {role}'


@allure.title('Get extended ACL')
def get_eacl(wallet_path: str, cid: str) -> Optional[str]:
    cli = NeofsCli(config=WALLET_CONFIG)
    try:
        output = cli.container.get_eacl(wallet=wallet_path, rpc_endpoint=NEOFS_ENDPOINT, cid=cid)
    except RuntimeError as exc:
        logger.info("Extended ACL table is not set for this container")
        logger.info(f"Got exception while getting eacl: {exc}")
        return None
    if 'extended ACL table is not set for this container' in output:
        return None
    return output


@allure.title('Set extended ACL')
def set_eacl(wallet_path: str, cid: str, eacl_table_path: str) -> None:
    cli = NeofsCli(config=WALLET_CONFIG, timeout=60)
    cli.container.set_eacl(wallet=wallet_path, rpc_endpoint=NEOFS_ENDPOINT, cid=cid, table=eacl_table_path,
                           await_mode=True)


def _encode_cid_for_eacl(cid: str) -> str:
    cid_base58 = base58.b58decode(cid)
    return base64.b64encode(cid_base58).decode("utf-8")


def create_eacl(cid: str, rules_list: List[EACLRule]) -> str:
    table_file_path = f"{os.getcwd()}/{ASSETS_DIR}/eacl_table_{str(uuid.uuid4())}.json"
    NeofsCli().acl.extended_create(cid=cid, out=table_file_path, rule=rules_list)

    with open(table_file_path, 'r') as file:
        table_data = file.read()
        logger.info(f"Generated eACL:\n{table_data}")

    return table_file_path


def form_bearertoken_file(wif: str, cid: str, eacl_rule_list: List[Union[EACLRule, EACLPubKey]]) -> str:
    """
    This function fetches eACL for given <cid> on behalf of <wif>,
    then extends it with filters taken from <eacl_rules>, signs
    with bearer token and writes to file
    """
    enc_cid = _encode_cid_for_eacl(cid)
    file_path = f"{os.getcwd()}/{ASSETS_DIR}/{str(uuid.uuid4())}"

    eacl = get_eacl(wif, cid)
    json_eacl = dict()
    if eacl:
        eacl = eacl.replace('eACL: ', '').split('Signature')[0]
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

    assert eacl_rules, 'Got empty eacl_records list'
    for rule in eacl_rule_list:
        op_data = {
            "operation": rule.operation.value.upper(),
            "action": rule.access.value.upper(),
            "filters": rule.filters or [],
            "targets": []
        }

        if isinstance(rule.role, EACLRole):
            op_data['targets'] = [
                {
                    "role": rule.role.value.upper()
                }
            ]
        elif isinstance(rule.role, EACLPubKey):
            op_data['targets'] = [
                {
                    'keys': rule.role.keys
                }
            ]

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


def eacl_rules(access: str, verbs: list, user: str) -> list[str]:
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
        pubkey = get_wallet_public_key(user, wallet_password="")
        user = f"pubkey:{pubkey}"

    rules = []
    for verb in verbs:
        rule = f"{access} {verb} {user}"
        rules.append(rule)
    return rules


def sign_bearer_token(wallet_path: str, eacl_rules_file: str) -> None:
    cmd = (
        f'{NEOFS_CLI_EXEC} util sign bearer-token --from {eacl_rules_file} '
        f'--to {eacl_rules_file} --wallet {wallet_path} --config {WALLET_CONFIG} --json'
    )
    _cmd_run(cmd)


@allure.title('Wait for eACL cache expired')
def wait_for_cache_expired():
    sleep(NEOFS_CONTRACT_CACHE_TIMEOUT)
    return
