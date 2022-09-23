#!/usr/bin/python3

"""
    This module contains keywords for work with session token.
"""

import base64
import json
import logging
import os
import uuid

import allure
import json_transformers
from cli_helpers import _cmd_run, _run_with_passwd
from common import ASSETS_DIR, NEOFS_ENDPOINT, WALLET_CONFIG
from neo3 import wallet

logger = logging.getLogger("NeoLogger")

# path to neofs-cli executable
NEOFS_CLI_EXEC = os.getenv("NEOFS_CLI_EXEC", "neofs-cli")


@allure.step("Generate Session Token")
def generate_session_token(owner: str, session_wallet: str, cid: str = "") -> str:
    """
    This function generates session token for ContainerSessionContext
    and writes it to the file. It is able to prepare session token file
    for a specific container (<cid>) or for every container (adds
    "wildcard" field).
    Args:
        owner(str): wallet address of container owner
        session_wallet(str): the path to wallet to which we grant the
                    access via session token
        cid(optional, str): container ID of the container; if absent,
                    we assume the session token is generated for any
                    container
    Returns:
        (str): the path to the generated session token file
    """
    file_path = f"{os.getcwd()}/{ASSETS_DIR}/{uuid.uuid4()}"

    session_wlt_content = ""
    with open(session_wallet) as fout:
        session_wlt_content = json.load(fout)
    session_wlt = wallet.Wallet.from_json(session_wlt_content, password="")
    pub_key_64 = base64.b64encode(bytes.fromhex(str(session_wlt.accounts[0].public_key))).decode(
        "utf-8"
    )

    session_token = {
        "body": {
            "id": f"{base64.b64encode(uuid.uuid4().bytes).decode('utf-8')}",
            "ownerID": {"value": f"{json_transformers.encode_for_json(owner)}"},
            "lifetime": {"exp": "100000000", "nbf": "0", "iat": "0"},
            "sessionKey": f"{pub_key_64}",
            "container": {
                "verb": "PUT",
                "wildcard": cid != "",
                **(
                    {
                        "containerID": {
                            "value": f"{base64.b64encode(cid.encode('utf-8')).decode('utf-8')}"
                        }
                    }
                    if cid != ""
                    else {}
                ),
            },
        }
    }

    logger.info(f"Got this Session Token: {session_token}")
    with open(file_path, "w", encoding="utf-8") as session_token_file:
        json.dump(session_token, session_token_file, ensure_ascii=False, indent=4)

    return file_path


@allure.step("Create Session Token")
def create_session_token(owner: str, wallet_path: str, rpc: str = NEOFS_ENDPOINT):
    """
    Create session token for an object.
    Args:
        owner(str): user that writes the token
        session_wallet(str): the path to wallet to which we grant the
                    access via session token
    Returns:
        (str): the path to the generated session token file
    """
    session_token = f"{os.getcwd()}/{ASSETS_DIR}/{uuid.uuid4()}"
    cmd = (
        f"{NEOFS_CLI_EXEC} session create --address {owner} -w {wallet_path} "
        f"--out {session_token} --rpc-endpoint {rpc}"
    )
    _run_with_passwd(cmd)
    return session_token


@allure.step("Sign Session Token")
def sign_session_token(session_token: str, wlt: str):
    """
    This function signs the session token by the given wallet.
    Args:
        session_token(str): the path to the session token file
        wlt(str): the path to the signing wallet
    Returns:
        (str): the path to the signed token
    """
    signed_token = f"{os.getcwd()}/{ASSETS_DIR}/{uuid.uuid4()}"
    cmd = (
        f"{NEOFS_CLI_EXEC} util sign session-token --from {session_token} "
        f"-w {wlt} --to {signed_token} --config {WALLET_CONFIG}"
    )
    _cmd_run(cmd)
    return signed_token
