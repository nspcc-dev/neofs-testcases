#!/usr/bin/python3.9

"""
    This module contains keywords that utilize `neofs-cli container` commands.
"""

import json
import time
from typing import Optional

import json_transformers
from data_formatters import dict_to_attrs
from cli_helpers import _cmd_run
from common import NEOFS_ENDPOINT, NEOFS_CLI_EXEC, WALLET_CONFIG

from robot.api import logger
from robot.api.deco import keyword

ROBOT_AUTO_KEYWORDS = False
DEFAULT_PLACEMENT_RULE = "REP 2 IN X CBF 1 SELECT 4 FROM * AS X"

@keyword('Create Container')
def create_container(wallet: str, rule: str = DEFAULT_PLACEMENT_RULE, basic_acl: str = '',
                     attributes: Optional[dict] = None, session_token: str = '',
                     session_wallet: str = '', options: str = '') -> str:
    """
        A wrapper for `neofs-cli container create` call.

        Args:
            wallet (str): a wallet on whose behalf a container is created
            rule (optional, str): placement rule for container
            basic_acl (optional, str): an ACL for container, will be
                                appended to `--basic-acl` key
            attributes (optional, dict): container attributes , will be
                                appended to `--attributes` key
            session_token (optional, str): a path to session token file
            session_wallet(optional, str): a path to the wallet which signed
                                the session token; this parameter makes sense
                                when paired with `session_token`
            options (optional, str): any other options to pass to the call

        Returns:
            (str): CID of the created container
    """

    cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} container create '
        f'--wallet {session_wallet if session_wallet else wallet} '
        f'--config {WALLET_CONFIG} --policy "{rule}" '
        f'{"--basic-acl " + basic_acl if basic_acl else ""} '
        f'{"--attributes " + dict_to_attrs(attributes) if attributes else ""} '
        f'{"--session " + session_token if session_token else ""} '
        f'{options} --await'
    )
    output = _cmd_run(cmd, timeout=60)
    cid = _parse_cid(output)

    logger.info("Container created; waiting until it is persisted in sidechain")

    deadline_to_persist = 15  # seconds
    for i in range(0, deadline_to_persist):
        time.sleep(1)
        containers = list_containers(wallet)
        if cid in containers:
            break
        logger.info(f"There is no {cid} in {containers} yet; continue")
        if i + 1 == deadline_to_persist:
            raise RuntimeError(
                f"After {deadline_to_persist} seconds the container "
                f"{cid} hasn't been persisted; exiting"
            )
    return cid


@keyword('List Containers')
def list_containers(wallet: str) -> list[str]:
    """
        A wrapper for `neofs-cli container list` call. It returns all the
        available containers for the given wallet.
        Args:
            wallet (str): a wallet on whose behalf we list the containers
        Returns:
            (list): list of containers
    """
    cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wallet {wallet} '
        f'--config {WALLET_CONFIG} container list'
    )
    output = _cmd_run(cmd)
    return output.split()


@keyword('Get Container')
def get_container(wallet: str, cid: str, flag: str = '--json') -> dict:
    """
        A wrapper for `neofs-cli container get` call. It extracts container's
        attributes and rearranges them into a more compact view.
        Args:
            wallet (str): path to a wallet on whose behalf we get the container
            cid (str): ID of the container to get
            flag (str): output as json or plain text
        Returns:
            (dict, str): dict of container attributes
    """
    cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wallet {wallet} '
        f'--config {WALLET_CONFIG} --cid {cid} container get --json'
    )
    output = _cmd_run(cmd)
    if flag != '--json':
        return output
    container_info = json.loads(output)
    attributes = dict()
    for attr in container_info['attributes']:
        attributes[attr['key']] = attr['value']
    container_info['attributes'] = attributes
    container_info['ownerID'] = json_transformers.json_reencode(container_info['ownerID']['value'])
    return container_info


@keyword('Delete Container')
# TODO: make the error message about a non-found container more user-friendly
# https://github.com/nspcc-dev/neofs-contract/issues/121
def delete_container(wallet: str, cid: str) -> None:
    """
        A wrapper for `neofs-cli container delete` call.
        Args:
            wallet (str): path to a wallet on whose behalf we delete the container
            cid (str): ID of the container to delete
        This function doesn't return anything.
    """

    cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wallet {wallet} '
        f'--config {WALLET_CONFIG} container delete --cid {cid}'
    )
    _cmd_run(cmd)


def _parse_cid(output: str) -> str:
    """
    Parses container ID from a given CLI output. The input string we expect:
            container ID: 2tz86kVTDpJxWHrhw3h6PbKMwkLtBEwoqhHQCKTre1FN
            awaiting...
            container has been persisted on sidechain
    We want to take 'container ID' value from the string.

    Args:
        output (str): CLI output to parse

    Returns:
        (str): extracted CID
    """
    try:
        # taking first line from command's output
        first_line = output.split('\n')[0]
    except Exception:
        logger.error(f"Got empty output: {output}")
    splitted = first_line.split(": ")
    if len(splitted) != 2:
        raise ValueError(f"no CID was parsed from command output: \t{first_line}")
    return splitted[1]
