#!/usr/bin/python3

"""
    This module contains functions which are used for Large Object assemling:
    getting Last Object and split and getting Link Object. It is not enough to
    simply perform a "raw" HEAD request, as noted in the issue:
    https://github.com/nspcc-dev/neofs-node/issues/1304. Therefore, the reliable
    retrival of the aforementioned objects must be done this way: send direct
    "raw" HEAD request to the every Storage Node and return the desired OID on
    first non-null response.
"""

from common import NEOFS_NETMAP
import neofs_verbs

from robot.api.deco import keyword
from robot.api import logger
from robot.libraries.BuiltIn import BuiltIn

ROBOT_AUTO_KEYWORDS = False


@keyword('Get Link Object')
def get_link_object(wallet: str, cid: str, oid: str, bearer_token: str=""):
    """
        Args:
            wallet (str): path to the wallet on whose behalf the Storage Nodes
                            are requested
            cid (str): Container ID which stores the Large Object
            oid (str): Large Object ID
            bearer_token (optional, str): path to Bearer token file
        Returns:
            (str): Link Object ID
            When no Link Object ID is found after all Storage Nodes polling,
            the function throws a native robot error.
    """
    for node in NEOFS_NETMAP:
        try:
            resp = neofs_verbs.head_object(wallet, cid, oid,
                    endpoint=node,
                    is_raw=True,
                    is_direct=True,
                    bearer_token=bearer_token)
            if resp['link']:
                return resp['link']
        except Exception:
            logger.info(f"No Link Object found on {node}; continue")
    BuiltIn().fail(f"No Link Object for {cid}/{oid} found among all Storage Nodes")
    return None


@keyword('Get Last Object')
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
            resp = neofs_verbs.head_object(wallet, cid, oid,
                    endpoint=node,
                    is_raw=True,
                    is_direct=True)
            if resp['lastPart']:
                return resp['lastPart']
        except Exception:
            logger.info(f"No Last Object found on {node}; continue")
    BuiltIn().fail(f"No Last Object for {cid}/{oid} found among all Storage Nodes")
    return None
