import base64
import json
import logging
import os
import uuid

import allure
import json_transformers
from common import ASSETS_DIR, NEOFS_CLI_EXEC, NEOFS_ENDPOINT, WALLET_CONFIG
from neo3 import wallet
from neofs_testlib.cli import NeofsCli
from neofs_testlib.shell import Shell

logger = logging.getLogger("NeoLogger")


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
    file_path = os.path.join(os.getcwd(), ASSETS_DIR, str(uuid.uuid4()))

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
def create_session_token(
    shell: Shell,
    owner: str,
    wallet_path: str,
    wallet_password: str,
    rpc_endpoint: str = NEOFS_ENDPOINT,
) -> str:
    """
    Create session token for an object.
    Args:
        shell: Shell instance.
        owner: User that writes the token.
        wallet_path: The path to wallet to which we grant the access via session token.
        wallet_password: Wallet password.
        rpc_endpoint: Remote node address (as 'multiaddr' or '<host>:<port>').
    Returns:
        The path to the generated session token file.
    """
    session_token = os.path.join(os.getcwd(), ASSETS_DIR, str(uuid.uuid4()))
    neofscli = NeofsCli(shell=shell, neofs_cli_exec_path=NEOFS_CLI_EXEC)
    neofscli.session.create(
        rpc_endpoint=rpc_endpoint,
        address=owner,
        wallet=wallet_path,
        wallet_password=wallet_password,
        out=session_token,
    )
    return session_token


@allure.step("Sign Session Token")
def sign_session_token(shell: Shell, session_token_file: str, wlt: str) -> str:
    """
    This function signs the session token by the given wallet.

    Args:
        shell: Shell instance.
        session_token_file: The path to the session token file.
        wlt: The path to the signing wallet.

    Returns:
        The path to the signed token.
    """
    signed_token_file = os.path.join(os.getcwd(), ASSETS_DIR, str(uuid.uuid4()))
    neofscli = NeofsCli(shell=shell, neofs_cli_exec_path=NEOFS_CLI_EXEC, config_file=WALLET_CONFIG)
    neofscli.util.sign_session_token(
        wallet=wlt, from_file=session_token_file, to_file=signed_token_file
    )
    return signed_token_file
