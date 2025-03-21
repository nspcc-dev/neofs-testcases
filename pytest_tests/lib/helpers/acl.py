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
from helpers.common import NEOFS_CLI_EXEC, TEST_FILES_DIR, WALLET_CONFIG, get_assets_dir_path
from helpers.data_formatters import get_wallet_public_key
from helpers.grpc_responses import EACL_NOT_FOUND, EACL_TABLE_IS_NOT_SET
from neofs_testlib.cli import NeofsCli
from neofs_testlib.shell import Shell
from neofs_testlib.utils.wallet import get_last_address_from_wallet

logger = logging.getLogger("NeoLogger")
EACL_LIFETIME = 100500
NEOFS_CONTRACT_CACHE_TIMEOUT = 2


class EACLOperation(Enum):
    PUT = "put"
    GET = "get"
    HEAD = "head"
    GET_RANGE = "getrange"
    GET_RANGE_HASH = "getrangehash"
    SEARCH = "search"
    DELETE = "delete"


class EACLAccess(Enum):
    ALLOW = "allow"
    DENY = "deny"


class EACLRole(Enum):
    OTHERS = "others"
    USER = "user"
    SYSTEM = "system"


class EACLRoleExtendedType(Enum):
    PUBKEY = "pubkey"
    ADDRESS = "address"

    def get_value(self, wallet_path: str, wallet_password: str) -> str:
        match self:
            case EACLRoleExtendedType.PUBKEY:
                return get_wallet_public_key(wallet_path, wallet_password)
            case EACLRoleExtendedType.ADDRESS:
                return get_last_address_from_wallet(wallet_path, wallet_password)
            case _:
                raise RuntimeError(f"Invalid EACLRoleExtendedType: {self}")


@dataclass
class EACLRoleExtended:
    role_type: EACLRoleExtendedType
    value: str


class EACLHeaderType(Enum):
    REQUEST = "req"  # Filter request headers
    OBJECT = "obj"  # Filter object headers
    SERVICE = "SERVICE"  # Filter service headers. These are not processed by NeoFS nodes and exist for service use only


class EACLMatchType(Enum):
    STRING_EQUAL = "="  # Return true if strings are equal
    STRING_NOT_EQUAL = "!="  # Return true if strings are different
    NUM_GT = ">"
    NUM_GE = ">="
    NUM_LT = "<"
    NUM_LE = "<="

    def compare(self, val1, val2):
        if self.value == ">":
            return val1 > val2
        elif self.value == ">=":
            return val1 >= val2
        elif self.value == "<":
            return val1 < val2
        elif self.value == "<=":
            return val1 <= val2
        raise AssertionError(f"Unsupported value: {self.value} for compare")


@dataclass
class EACLFilter:
    header_type: EACLHeaderType = EACLHeaderType.REQUEST
    match_type: EACLMatchType = EACLMatchType.STRING_EQUAL
    key: Optional[str] = None
    value: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "headerType": self.header_type,
            "matchType": self.match_type,
            "key": self.key,
            "value": self.value,
        }


@dataclass
class EACLFilters:
    filters: Optional[List[EACLFilter]] = None

    def __str__(self):
        return (
            ",".join(
                [
                    f"{filter.header_type.value}:{filter.key}{filter.match_type.value}{filter.value}"
                    for filter in self.filters
                ]
            )
            if self.filters
            else []
        )


@dataclass
class EACLRule:
    operation: Optional[EACLOperation] = None
    access: Optional[EACLAccess] = None
    role: Optional[Union[EACLRole, EACLRoleExtended]] = None
    filters: Optional[EACLFilters] = None
    password: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "Operation": self.operation,
            "Access": self.access,
            "Role": self.role,
            "Filters": self.filters or [],
        }

    def __str__(self):
        role = self.role.value if isinstance(self.role, EACLRole) else f"{self.role.role_type.value}:{self.role.value}"
        return f"{self.access.value} {self.operation.value} {self.filters or ''} {role}"


@allure.title("Get extended ACL")
def get_eacl(wallet_path: str, cid: str, shell: Shell, endpoint: str) -> Optional[str]:
    cli = NeofsCli(shell, NEOFS_CLI_EXEC, WALLET_CONFIG)
    try:
        result = cli.container.get_eacl(wallet=wallet_path, rpc_endpoint=endpoint, cid=cid)
    except RuntimeError as exc:
        logger.info("Extended ACL table is not set for this container")
        logger.info(f"Got exception while getting eacl: {exc}")
        return None
    if EACL_TABLE_IS_NOT_SET in result.stdout or EACL_NOT_FOUND in result.stdout:
        return None
    return result.stdout


@allure.title("Set extended ACL")
def set_eacl(
    wallet_path: str,
    cid: str,
    eacl_table_path: str,
    shell: Shell,
    endpoint: str,
    session_token: Optional[str] = None,
    force: Optional[bool] = None,
) -> None:
    cli = NeofsCli(shell, NEOFS_CLI_EXEC, WALLET_CONFIG)
    cli.container.set_eacl(
        wallet=wallet_path,
        rpc_endpoint=endpoint,
        cid=cid,
        table=eacl_table_path,
        await_mode=True,
        session=session_token,
        force=force,
    )


def _encode_cid_for_eacl(cid: str) -> str:
    cid_base58 = base58.b58decode(cid)
    return base64.b64encode(cid_base58).decode("utf-8")


def create_eacl(cid: str, rules_list: List[EACLRule], shell: Shell, wallet_config: str = None) -> str:
    table_file_path = os.path.join(get_assets_dir_path(), TEST_FILES_DIR, f"eacl_table_{str(uuid.uuid4())}.json")
    cli = NeofsCli(shell, NEOFS_CLI_EXEC, WALLET_CONFIG if not wallet_config else wallet_config)
    cli.acl.extended_create(cid=cid, out=table_file_path, rule=rules_list)

    with open(table_file_path, "r") as file:
        table_data = file.read()
        logger.info(f"Generated eACL:\n{table_data}")

    return table_file_path


def form_bearertoken_file(
    wallet_path: str,
    cid: str,
    eacl_rule_list: List[Union[EACLRule, EACLRoleExtended]],
    shell: Shell,
    endpoint: str,
    sign: Optional[bool] = True,
) -> str:
    """
    This function fetches eACL for given <cid> on behalf of <wif>,
    then extends it with filters taken from <eacl_rules>, signs
    with bearer token and writes to file
    """
    enc_cid = _encode_cid_for_eacl(cid) if cid else None
    file_path = os.path.join(get_assets_dir_path(), TEST_FILES_DIR, f"eacl-{uuid.uuid4()}")

    eacl = get_eacl(wallet_path, cid, shell, endpoint)
    json_eacl = dict()
    if eacl:
        eacl = eacl.replace("eACL: ", "").split("Signature")[0]
        json_eacl = json.loads(eacl)
    logger.info(json_eacl)
    eacl_result = {
        "body": {
            "eaclTable": {"containerID": {"value": enc_cid} if cid else enc_cid, "records": []},
            "lifetime": {"exp": EACL_LIFETIME, "nbf": "1", "iat": "0"},
        }
    }

    assert eacl_rules, "Got empty eacl_records list"
    for rule in eacl_rule_list:
        op_data = {
            "operation": rule.operation.value.upper(),
            "action": rule.access.value.upper(),
            "filters": rule.filters or [],
            "targets": [],
        }

        if isinstance(rule.role, EACLRole):
            op_data["targets"] = [{"role": rule.role.value.upper()}]
        elif isinstance(rule.role, EACLRoleExtended):
            op_data["targets"] = [{"keys": rule.role.value}]

        eacl_result["body"]["eaclTable"]["records"].append(op_data)

    # Add records from current eACL
    if "records" in json_eacl.keys():
        for record in json_eacl["records"]:
            eacl_result["body"]["eaclTable"]["records"].append(record)

    with open(file_path, "w", encoding="utf-8") as eacl_file:
        json.dump(eacl_result, eacl_file, ensure_ascii=False, indent=4)

    logger.info(f"Got these extended ACL records: {eacl_result}")
    if sign:
        sign_bearer(
            shell=shell,
            wallet_path=wallet_path,
            eacl_rules_file_from=file_path,
            eacl_rules_file_to=file_path,
            json=True,
        )
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
    if user not in ("others", "user"):
        pubkey = get_wallet_public_key(user, wallet_password="")
        user = f"pubkey:{pubkey}"

    rules = []
    for verb in verbs:
        rule = f"{access} {verb} {user}"
        rules.append(rule)
    return rules


@allure.step("Sign bearer token")
def sign_bearer(shell: Shell, wallet_path: str, eacl_rules_file_from: str, eacl_rules_file_to: str, json: bool) -> None:
    neofscli = NeofsCli(shell=shell, neofs_cli_exec_path=NEOFS_CLI_EXEC, config_file=WALLET_CONFIG)
    neofscli.util.sign_bearer_token(
        wallet=wallet_path, from_file=eacl_rules_file_from, to_file=eacl_rules_file_to, json=json
    )


@allure.title("Wait for eACL cache expired")
def wait_for_cache_expired():
    sleep(NEOFS_CONTRACT_CACHE_TIMEOUT)
    return


@allure.step("Return bearer token in base64 to caller")
def bearer_token_base64_from_file(
    bearer_path: str,
) -> str:
    with open(bearer_path, "rb") as file:
        signed = file.read()
    return base64.b64encode(signed).decode("utf-8")


@allure.step("Create bearer token")
def create_bearer_token(
    shell,
    issued_at: int,
    not_valid_before: int,
    owner: str,
    out: str,
    rpc_endpoint: str,
    json: Optional[bool] = False,
    eacl: Optional[str] = None,
    lifetime: Optional[int] = None,
    expire_at: Optional[int] = None,
) -> str:
    neofscli = NeofsCli(shell=shell, neofs_cli_exec_path=NEOFS_CLI_EXEC, config_file=WALLET_CONFIG)
    neofscli.bearer.create(
        issued_at=issued_at,
        not_valid_before=not_valid_before,
        owner=owner,
        out=out,
        rpc_endpoint=rpc_endpoint,
        json=json,
        eacl=eacl,
        lifetime=lifetime,
        expire_at=expire_at,
    )
