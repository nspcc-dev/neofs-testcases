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

import allure
import neofs_verbs
from common import NEOFS_NETMAP, WALLET_CONFIG

logger = logging.getLogger("NeoLogger")
ROBOT_AUTO_KEYWORDS = False


@allure.step("Get Link Object")
def get_link_object(
    wallet: str, cid: str, oid: str, bearer_token: str = "", wallet_config: str = WALLET_CONFIG
):
    """
    Args:
        wallet (str): path to the wallet on whose behalf the Storage Nodes
                        are requested
        cid (str): Container ID which stores the Large Object
        oid (str): Large Object ID
        bearer_token (optional, str): path to Bearer token file
        wallet_config (optional, str): path to the neofs-cli config file
    Returns:
        (str): Link Object ID
        When no Link Object ID is found after all Storage Nodes polling,
        the function throws a native robot error.
    """
    for node in NEOFS_NETMAP:
        try:
            resp = neofs_verbs.head_object(
                wallet,
                cid,
                oid,
                endpoint=node,
                is_raw=True,
                is_direct=True,
                bearer_token=bearer_token,
                wallet_config=wallet_config,
            )
            if resp["link"]:
                return resp["link"]
        except Exception:
            logger.info(f"No Link Object found on {node}; continue")
    logger.error(f"No Link Object for {cid}/{oid} found among all Storage Nodes")
    return None


@allure.step("Get Last Object")
def get_last_object(wallet: str, cid: str, oid: str):
    """
    Args:
        wallet (str): path to the wallet on whose behalf the Storage Nodes
                        are requested
        cid (str): Container ID which stores the Large Object
        oid (str): Large Object ID
    Returns:
        (str): Last Object ID
        When no Last Object ID is found after all Storage Nodes polling,
        the function throws a native robot error.
    """
    for node in NEOFS_NETMAP:
        try:
            resp = neofs_verbs.head_object(
                wallet, cid, oid, endpoint=node, is_raw=True, is_direct=True
            )
            if resp["lastPart"]:
                return resp["lastPart"]
        except Exception:
            logger.info(f"No Last Object found on {node}; continue")
    logger.error(f"No Last Object for {cid}/{oid} found among all Storage Nodes")
    return None
