#!/usr/bin/python3

'''
    This module contains wrappers for NeoFS verbs executed via neofs-cli.
'''

import json
import os
import random
import re
import uuid

import json_transformers
from cli_helpers import _cmd_run
from common import ASSETS_DIR, NEOFS_ENDPOINT, NEOFS_NETMAP, WALLET_CONFIG
from data_formatters import dict_to_attrs
from robot.api import logger
from robot.api.deco import keyword

ROBOT_AUTO_KEYWORDS = False

# path to neofs-cli executable
NEOFS_CLI_EXEC = os.getenv('NEOFS_CLI_EXEC', 'neofs-cli')


@keyword('Get object')
def get_object(wallet: str, cid: str, oid: str, bearer_token: str = "",
               write_object: str = "", endpoint: str = "", options: str = "",
               wallet_config: str = WALLET_CONFIG):
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
        options (optional, str): any options which `neofs-cli object get` accepts
    Returns:
        (str): path to downloaded file
    """

    if not write_object:
        write_object = str(uuid.uuid4())
    file_path = f"{ASSETS_DIR}/{write_object}"

    if not endpoint:
        endpoint = random.sample(NEOFS_NETMAP, 1)[0]

    cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {endpoint} --wallet {wallet} '
        f'object get --cid {cid} --oid {oid} --file {file_path} --config {wallet_config} '
        f'{"--bearer " + bearer_token if bearer_token else ""} '
        f'{options}'
    )
    _cmd_run(cmd)
    return file_path


# TODO: make `bearer_token` optional
@keyword('Get Range Hash')
def get_range_hash(wallet: str, cid: str, oid: str, bearer_token: str, range_cut: str,
                   wallet_config: str = WALLET_CONFIG, options: str = ""):
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
        options (optional, str): any options which `neofs-cli object hash` accepts
    Returns:
        None
    """
    cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wallet {wallet} '
        f'object hash --cid {cid} --oid {oid} --range {range_cut} --config {wallet_config} '
        f'{"--bearer " + bearer_token if bearer_token else ""} '
        f'{options}'
    )
    output = _cmd_run(cmd)
    # cutting off output about range offset and length
    return output.split(':')[1].strip()


@keyword('Put object')
def put_object(wallet: str, path: str, cid: str, bearer: str = "", user_headers: dict = {},
               endpoint: str = "", wallet_config: str = WALLET_CONFIG, options: str = ""):
    """
    PUT of given file.

    Args:
        wallet (str): wallet on whose behalf PUT is done
        path (str): path to file to be PUT
        cid (str): ID of Container where we get the Object from
        bearer (optional, str): path to Bearer Token file, appends to `--bearer` key
        user_headers (optional, dict): Object attributes, append to `--attributes` key
        endpoint(optional, str): NeoFS endpoint to send request to
        wallet_config(optional, str): path to the wallet config
        options (optional, str): any options which `neofs-cli object put` accepts
    Returns:
        (str): ID of uploaded Object
    """
    if not endpoint:
        endpoint = random.sample(NEOFS_NETMAP, 1)[0]
        if not endpoint:
            logger.info(f'---DEB:\n{NEOFS_NETMAP}')
    cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {endpoint} --wallet {wallet} '
        f'object put --file {path} --cid {cid} {options} --config {wallet_config} '
        f'{"--bearer " + bearer if bearer else ""} '
        f'{"--attributes " + dict_to_attrs(user_headers) if user_headers else ""}'
    )
    output = _cmd_run(cmd)
    # splitting CLI output to lines and taking the penultimate line
    id_str = output.strip().split('\n')[-2]
    oid = id_str.split(':')[1]
    return oid.strip()


@keyword('Delete object')
def delete_object(wallet: str, cid: str, oid: str, bearer: str = "", wallet_config: str = WALLET_CONFIG,
                  options: str = ""):
    """
    DELETE an Object.

    Args:
        wallet (str): wallet on whose behalf DELETE is done
        cid (str): ID of Container where we get the Object from
        oid (str): ID of Object we are going to delete
        bearer (optional, str): path to Bearer Token file, appends to `--bearer` key
        wallet_config(optional, str): path to the wallet config
        options (optional, str): any options which `neofs-cli object delete` accepts
    Returns:
        (str): Tombstone ID
    """
    cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wallet {wallet} '
        f'object delete --cid {cid} --oid {oid} {options} --config {wallet_config} '
        f'{"--bearer " + bearer if bearer else ""}'
    )
    output = _cmd_run(cmd)
    id_str = output.split('\n')[1]
    tombstone = id_str.split(':')[1]
    return tombstone.strip()


# TODO: remove `file_path` parameter as it is a boilerplate
# TODO: make `bearer` an optional parameter
@keyword('Get Range')
def get_range(wallet: str, cid: str, oid: str, file_path: str, bearer: str, range_cut: str,
              wallet_config: str = WALLET_CONFIG, options: str = ""):
    """
    GETRANGE an Object.

    Args:
        wallet (str): wallet on whose behalf GETRANGE is done
        cid (str): ID of Container where we get the Object from
        oid (str): ID of Object we are going to request
        file_path (str): file path
        range_cut (str): range to take data from in the form offset:length
        bearer (optional, str): path to Bearer Token file, appends to `--bearer` key
        wallet_config(optional, str): path to the wallet config
        options (optional, str): any options which `neofs-cli object range` accepts
    Returns:
        (void)
    """
    range_file = f"{ASSETS_DIR}/{uuid.uuid4()}"
    cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wallet {wallet} '
        f'object range --cid {cid} --oid {oid} --range {range_cut} --config {wallet_config} '
        f'{options} --file {range_file} '
        f'{"--bearer " + bearer if bearer else ""} '
    )
    _cmd_run(cmd)
    content = ''
    with open(range_file, 'rb') as fout:
        content = fout.read()
    return range_file, content


@keyword('Search object')
def search_object(wallet: str, cid: str, keys: str = "", bearer: str = "", filters: dict = {},
                  expected_objects_list=[], wallet_config: str = WALLET_CONFIG, options: str = ""):
    """
    SEARCH an Object.

    Args:
        wallet (str): wallet on whose behalf SEARCH is done
        cid (str): ID of Container where we get the Object from
        keys(optional, str): any keys for Object SEARCH which `neofs-cli object search`
                            accepts, e.g. `--oid`
        bearer (optional, str): path to Bearer Token file, appends to `--bearer` key
        filters (optional, dict): key=value pairs to filter Objects
        expected_objects_list (optional, list): a list of ObjectIDs to compare found Objects with
        wallet_config(optional, str): path to the wallet config
        options(optional, str): any other options which `neofs-cli object search` might accept
    Returns:
        (list): list of found ObjectIDs
    """
    filters_result = ""
    if filters:
        filters_result += "--filters "
        logger.info(filters)
        filters_result += ','.join(
            map(lambda i: f"'{i} EQ {filters[i]}'", filters))

    cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wallet {wallet} '
        f'object search {keys} --cid {cid} {filters_result} --config {wallet_config} '
        f'{"--bearer " + bearer if bearer else ""} {options}'
    )
    output = _cmd_run(cmd)

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
                options: str = "", endpoint: str = "", json_output: bool = True,
                is_raw: bool = False, is_direct: bool = False, wallet_config: str = WALLET_CONFIG):
    """
    HEAD an Object.

    Args:
        wallet (str): wallet on whose behalf HEAD is done
        cid (str): ID of Container where we get the Object from
        oid (str): ObjectID to HEAD
        bearer_token (optional, str): path to Bearer Token file, appends to `--bearer` key
        options (optional, str): any options which `neofs-cli object head` accepts
        endpoint(optional, str): NeoFS endpoint to send request to
        json_output(optional, bool): return reponse in JSON format or not; this flag
                                    turns into `--json` key
        is_raw(optional, bool): send "raw" request or not; this flag
                                    turns into `--raw` key
        is_direct(optional, bool): send request directly to the node or not; this flag
                                    turns into `--ttl 1` key
        wallet_config(optional, str): path to the wallet config
    Returns:
        depending on the `json_output` parameter value, the function returns
        (dict): HEAD response in JSON format
        or
        (str): HEAD response as a plain text
    """
    cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {endpoint if endpoint else NEOFS_ENDPOINT} '
        f'--wallet {wallet} --config {wallet_config} '
        f'object head --cid {cid} --oid {oid} {options} '
        f'{"--bearer " + bearer_token if bearer_token else ""} '
        f'{"--json" if json_output else ""} '
        f'{"--raw" if is_raw else ""} '
        f'{"--ttl 1" if is_direct else ""}'
    )
    output = _cmd_run(cmd)
    if not json_output:
        return output

    decoded = ""
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
