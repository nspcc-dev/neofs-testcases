#!/usr/bin/python3

"""
    This module contains wrappers for NeoFS verbs executed via neofs-cli.
"""

import json
import random
import re
import uuid
from typing import Optional

import json_transformers
from cli import NeofsCli
from common import ASSETS_DIR, NEOFS_ENDPOINT, NEOFS_NETMAP, WALLET_CONFIG
from robot.api import logger
from robot.api.deco import keyword

ROBOT_AUTO_KEYWORDS = False


@keyword('Get object')
def get_object(wallet: str, cid: str, oid: str, bearer_token: Optional[str] = None, write_object: str = "",
               endpoint: str = "", xhdr: Optional[dict] = None, wallet_config: Optional[str] = None,
               no_progress: bool = True) -> str:
    """
    GET from NeoFS.

    Args:
        wallet (str): wallet on whose behalf GET is done
        cid (str): ID of Container where we get the Object from
        oid (str): Object ID
        bearer_token (optional, str): path to Bearer Token file, appends to `--bearer` key
        write_object (optional, str): path to downloaded file, appends to `--file` key
        endpoint (optional, str): NeoFS endpoint to send request to, appends to `--rpc-endpoint` key
        wallet_config(optional, str): path to the wallet config
        no_progress(optional, bool): do not show progress bar
        xhdr (optional, dict): Request X-Headers in form of Key=Value
    Returns:
        (str): path to downloaded file
    """

    wallet_config = wallet_config or WALLET_CONFIG
    if not write_object:
        write_object = str(uuid.uuid4())
    file_path = f"{ASSETS_DIR}/{write_object}"

    if not endpoint:
        endpoint = random.sample(NEOFS_NETMAP, 1)[0]

    cli = NeofsCli(config=wallet_config)
    cli.object.get(rpc_endpoint=endpoint, wallet=wallet, cid=cid, oid=oid, file=file_path,
                   bearer=bearer_token, no_progress=no_progress, xhdr=xhdr)

    return file_path


# TODO: make `bearer_token` optional
@keyword('Get Range Hash')
def get_range_hash(wallet: str, cid: str, oid: str, bearer_token: str, range_cut: str,
                   wallet_config: Optional[str] = None, xhdr: Optional[dict] = None):
    """
    GETRANGEHASH of given Object.

    Args:
        wallet (str): wallet on whose behalf GETRANGEHASH is done
        cid (str): ID of Container where we get the Object from
        oid (str): Object ID
        range_cut (str): Range to take hash from in the form offset1:length1,...,
                        value to pass to the `--range` parameter
        bearer_token (optional, str): path to Bearer Token file, appends to `--bearer` key
        wallet_config(optional, str): path to the wallet config
        xhdr (optional, dict): Request X-Headers in form of Key=Value
    Returns:
        None
    """

    wallet_config = wallet_config or WALLET_CONFIG
    cli = NeofsCli(config=wallet_config)
    output = cli.object.hash(rpc_endpoint=NEOFS_ENDPOINT, wallet=wallet, cid=cid, oid=oid, range=range_cut,
                             bearer=bearer_token, xhdr=xhdr)

    # cutting off output about range offset and length
    return output.split(':')[1].strip()


@keyword('Put object')
def put_object(wallet: str, path: str, cid: str, bearer: str = "", attributes: Optional[dict] = None,
               xhdr: Optional[dict] = None, endpoint: str = "", wallet_config: Optional[str] = None,
               expire_at: Optional[int] = None, no_progress: bool = True):
    """
    PUT of given file.

    Args:
        wallet (str): wallet on whose behalf PUT is done
        path (str): path to file to be PUT
        cid (str): ID of Container where we get the Object from
        bearer (optional, str): path to Bearer Token file, appends to `--bearer` key
        attributes (optional, str): User attributes in form of Key1=Value1,Key2=Value2
        endpoint(optional, str): NeoFS endpoint to send request to
        wallet_config(optional, str): path to the wallet config
        no_progress(optional, bool): do not show progress bar
        expire_at (optional, int): Last epoch in the life of the object
        xhdr (optional, dict): Request X-Headers in form of Key=Value
    Returns:
        (str): ID of uploaded Object
    """
    wallet_config = wallet_config or WALLET_CONFIG
    if not endpoint:
        endpoint = random.sample(NEOFS_NETMAP, 1)[0]
        if not endpoint:
            logger.info(f'---DEB:\n{NEOFS_NETMAP}')

    cli = NeofsCli(config=wallet_config)
    output = cli.object.put(rpc_endpoint=endpoint, wallet=wallet, file=path, cid=cid, attributes=attributes,
                            bearer=bearer, expire_at=expire_at, no_progress=no_progress, xhdr=xhdr)

    # splitting CLI output to lines and taking the penultimate line
    id_str = output.strip().split('\n')[-2]
    oid = id_str.split(':')[1]
    return oid.strip()


@keyword('Delete object')
def delete_object(wallet: str, cid: str, oid: str, bearer: str = "", wallet_config: Optional[str] = None,
                  xhdr: Optional[dict] = None):
    """
    DELETE an Object.

    Args:
        wallet (str): wallet on whose behalf DELETE is done
        cid (str): ID of Container where we get the Object from
        oid (str): ID of Object we are going to delete
        bearer (optional, str): path to Bearer Token file, appends to `--bearer` key
        wallet_config(optional, str): path to the wallet config
        xhdr (optional, dict): Request X-Headers in form of Key=Value
    Returns:
        (str): Tombstone ID
    """

    wallet_config = wallet_config or WALLET_CONFIG
    cli = NeofsCli(config=wallet_config)
    output = cli.object.delete(rpc_endpoint=NEOFS_ENDPOINT, wallet=wallet, cid=cid, oid=oid, bearer=bearer,
                               xhdr=xhdr)

    id_str = output.split('\n')[1]
    tombstone = id_str.split(':')[1]
    return tombstone.strip()


@keyword('Get Range')
def get_range(wallet: str, cid: str, oid: str, range_cut: str, wallet_config: Optional[str] = None,
              bearer: str = "", xhdr: Optional[dict] = None):
    """
    GETRANGE an Object.

    Args:
        wallet (str): wallet on whose behalf GETRANGE is done
        cid (str): ID of Container where we get the Object from
        oid (str): ID of Object we are going to request
        range_cut (str): range to take data from in the form offset:length
        bearer (optional, str): path to Bearer Token file, appends to `--bearer` key
        wallet_config(optional, str): path to the wallet config
        xhdr (optional, dict): Request X-Headers in form of Key=Value
    Returns:
        (str, bytes) - path to the file with range content and content of this file as bytes
    """
    wallet_config = wallet_config or WALLET_CONFIG
    range_file = f"{ASSETS_DIR}/{uuid.uuid4()}"

    cli = NeofsCli(config=wallet_config)
    cli.object.range(rpc_endpoint=NEOFS_ENDPOINT, wallet=wallet, cid=cid, oid=oid, range=range_cut, file=range_file,
                     bearer=bearer, xhdr=xhdr)

    with open(range_file, 'rb') as fout:
        content = fout.read()
    return range_file, content


@keyword('Search object')
def search_object(wallet: str, cid: str, bearer: str = "", filters: Optional[dict] = None,
                  expected_objects_list: Optional[list] = None, wallet_config: Optional[str] = None,
                  xhdr: Optional[dict] = None) -> list:
    """
    SEARCH an Object.

    Args:
        wallet (str): wallet on whose behalf SEARCH is done
        cid (str): ID of Container where we get the Object from
        bearer (optional, str): path to Bearer Token file, appends to `--bearer` key
        filters (optional, dict): key=value pairs to filter Objects
        expected_objects_list (optional, list): a list of ObjectIDs to compare found Objects with
        wallet_config(optional, str): path to the wallet config
        xhdr (optional, dict): Request X-Headers in form of Key=Value
    Returns:
        (list): list of found ObjectIDs
    """

    wallet_config = wallet_config or WALLET_CONFIG
    cli = NeofsCli(config=wallet_config)
    output = cli.object.search(
        rpc_endpoint=NEOFS_ENDPOINT, wallet=wallet, cid=cid, bearer=bearer, xhdr=xhdr,
        filters=[f'{filter_key} EQ {filter_val}' for filter_key, filter_val in filters.items()] if filters else None)

    found_objects = re.findall(r'(\w{43,44})', output)

    if expected_objects_list:
        if sorted(found_objects) == sorted(expected_objects_list):
            logger.info(f"Found objects list '{found_objects}' ",
                        f"is equal for expected list '{expected_objects_list}'")
        else:
            logger.warn(f"Found object list {found_objects} ",
                        f"is not equal to expected list '{expected_objects_list}'")

    return found_objects


@keyword('Head object')
def head_object(wallet: str, cid: str, oid: str, bearer_token: str = "",
                xhdr: Optional[dict] = None, endpoint: str = None, json_output: bool = True,
                is_raw: bool = False, is_direct: bool = False, wallet_config: Optional[str] = None):
    """
    HEAD an Object.

    Args:
        wallet (str): wallet on whose behalf HEAD is done
        cid (str): ID of Container where we get the Object from
        oid (str): ObjectID to HEAD
        bearer_token (optional, str): path to Bearer Token file, appends to `--bearer` key
        endpoint(optional, str): NeoFS endpoint to send request to
        json_output(optional, bool): return reponse in JSON format or not; this flag
                                    turns into `--json` key
        is_raw(optional, bool): send "raw" request or not; this flag
                                    turns into `--raw` key
        is_direct(optional, bool): send request directly to the node or not; this flag
                                    turns into `--ttl 1` key
        wallet_config(optional, str): path to the wallet config
        xhdr (optional, dict): Request X-Headers in form of Key=Value
    Returns:
        depending on the `json_output` parameter value, the function returns
        (dict): HEAD response in JSON format
        or
        (str): HEAD response as a plain text
    """

    wallet_config = wallet_config or WALLET_CONFIG
    cli = NeofsCli(config=wallet_config)
    output = cli.object.head(rpc_endpoint=endpoint or NEOFS_ENDPOINT, wallet=wallet, cid=cid, oid=oid,
                             bearer=bearer_token, json_mode=json_output, raw=is_raw,
                             ttl=1 if is_direct else None, xhdr=xhdr)

    if not json_output:
        return output

    try:
        decoded = json.loads(output)
    except Exception as exc:
        # If we failed to parse output as JSON, the cause might be
        # the plain text string in the beginning of the output.
        # Here we cut off first string and try to parse again.
        logger.info(f"failed to parse output: {exc}")
        logger.info("parsing output in another way")
        fst_line_idx = output.find('\n')
        decoded = json.loads(output[fst_line_idx:])

    # If response is Complex Object header, it has `splitId` key
    if 'splitId' in decoded.keys():
        logger.info("decoding split header")
        return json_transformers.decode_split_header(decoded)

    # If response is Last or Linking Object header,
    # it has `header` dictionary and non-null `split` dictionary
    if 'split' in decoded['header'].keys():
        if decoded['header']['split']:
            logger.info("decoding linking object")
            return json_transformers.decode_linking_object(decoded)

    if decoded['header']['objectType'] == 'STORAGE_GROUP':
        logger.info("decoding storage group")
        return json_transformers.decode_storage_group(decoded)

    if decoded['header']['objectType'] == 'TOMBSTONE':
        logger.info("decoding tombstone")
        return json_transformers.decode_tombstone(decoded)

    logger.info("decoding simple header")
    return json_transformers.decode_simple_header(decoded)
