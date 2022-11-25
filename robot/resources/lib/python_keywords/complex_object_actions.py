#!/usr/bin/python3

"""
    This module contains functions which are used for Large Object assembling:
    getting Last Object and split and getting Link Object. It is not enough to
    simply perform a "raw" HEAD request, as noted in the issue:
    https://github.com/nspcc-dev/neofs-node/issues/1304. Therefore, the reliable
    retrieval of the aforementioned objects must be done this way: send direct
    "raw" HEAD request to the every Storage Node and return the desired OID on
    first non-null response.
"""

import logging
from typing import Optional

import allure
import neofs_verbs
from common import NEOFS_NETMAP, WALLET_CONFIG
from neofs_testlib.shell import Shell

logger = logging.getLogger("NeoLogger")


@allure.step("Get Link Object")
def get_link_object(
    wallet: str,
    cid: str,
    oid: str,
    shell: Shell,
    bearer: str = "",
    wallet_config: str = WALLET_CONFIG,
    is_direct: bool = True,
):
    """
    Args:
        wallet (str): path to the wallet on whose behalf the Storage Nodes
                        are requested
        cid (str): Container ID which stores the Large Object
        oid (str): Large Object ID
        shell: executor for cli command
        bearer (optional, str): path to Bearer token file
        wallet_config (optional, str): path to the neofs-cli config file
        is_direct: send request directly to the node or not; this flag
                   turns into `--ttl 1` key
    Returns:
        (str): Link Object ID
        When no Link Object ID is found after all Storage Nodes polling,
        the function throws an error.
    """
    for node in NEOFS_NETMAP:
        try:
            resp = neofs_verbs.head_object(
                wallet,
                cid,
                oid,
                shell=shell,
                endpoint=node,
                is_raw=True,
                is_direct=is_direct,
                bearer=bearer,
                wallet_config=wallet_config,
            )
            if resp["link"]:
                return resp["link"]
        except Exception:
            logger.info(f"No Link Object found on {node}; continue")
    logger.error(f"No Link Object for {cid}/{oid} found among all Storage Nodes")
    return None


@allure.step("Get Last Object")
def get_last_object(wallet: str, cid: str, oid: str, shell: Shell) -> Optional[str]:
    """
    Args:
        wallet (str): path to the wallet on whose behalf the Storage Nodes
                        are requested
        cid (str): Container ID which stores the Large Object
        oid (str): Large Object ID
        shell: executor for cli command
    Returns:
        (str): Last Object ID
        When no Last Object ID is found after all Storage Nodes polling,
        the function throws an error.
    """
    for node in NEOFS_NETMAP:
        try:
            resp = neofs_verbs.head_object(
                wallet, cid, oid, shell=shell, endpoint=node, is_raw=True, is_direct=True
            )
            if resp["lastPart"]:
                return resp["lastPart"]
        except Exception:
            logger.info(f"No Last Object found on {node}; continue")
    logger.error(f"No Last Object for {cid}/{oid} found among all Storage Nodes")
    return None
