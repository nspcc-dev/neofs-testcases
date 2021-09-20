#!/usr/bin/python3.8

import base58
import base64
import binascii
from datetime import datetime
import docker
import hashlib
import json
import os
import re
import random
import subprocess

from neo3 import wallet
from robot.api.deco import keyword
from robot.api import logger

from common import *
from cli_helpers import _cmd_run

ROBOT_AUTO_KEYWORDS = False

# path to neofs-cli executable
NEOFS_CLI_EXEC = os.getenv('NEOFS_CLI_EXEC', 'neofs-cli')


@keyword('Get ScriptHash')
def get_scripthash(wif: str):
    acc = wallet.Account.from_wif(wif, '')
    return str(acc.script_hash)


@keyword('Stop nodes')
def stop_nodes(down_num: int, *nodes_list):

    # select nodes to stop from list
    stop_nodes = random.sample(nodes_list, down_num)

    for node in stop_nodes:
        m = re.search(r'(s\d+).', node)
        node = m.group(1)

        client = docker.APIClient()
        client.stop(node)

    return stop_nodes


@keyword('Start nodes')
def start_nodes(*nodes_list):

    for node in nodes_list:
        m = re.search(r'(s\d+).', node)
        node = m.group(1)
        client = docker.APIClient()
        client.start(node)

@keyword('Get nodes with object')
def get_nodes_with_object(private_key: str, cid: str, oid: str):
    storage_nodes = _get_storage_nodes()
    copies = 0

    nodes_list = []

    for node in storage_nodes:
        search_res = _search_object(node, private_key, cid, oid)
        if search_res:
            if re.search(fr'({oid})', search_res):
                nodes_list.append(node)

    logger.info(f"Nodes with object: {nodes_list}")
    return nodes_list


@keyword('Get nodes without object')
def get_nodes_without_object(private_key: str, cid: str, oid: str):
    storage_nodes = _get_storage_nodes()
    copies = 0

    nodes_list = []

    for node in storage_nodes:
        search_res = _search_object(node, private_key, cid, oid)
        if search_res:
            if not re.search(fr'({oid})', search_res):
                nodes_list.append(node)
        else:
            nodes_list.append(node)

    logger.info(f"Nodes without object: {nodes_list}")
    return nodes_list


@keyword('Validate storage policy for object')
def validate_storage_policy_for_object(private_key: str, expected_copies: int, cid, oid,
                expected_node_list=[], storage_nodes=[]):
    storage_nodes = storage_nodes if len(storage_nodes) != 0 else _get_storage_nodes()
    copies = 0
    found_nodes = []

    for node in storage_nodes:
        search_res = _search_object(node, private_key, cid, oid)
        if search_res:
            if re.search(fr'({oid})', search_res):
                copies += 1
                found_nodes.append(node)

    if copies != expected_copies:
        raise Exception(f"Object copies is not match storage policy.",
                        f"Found: {copies}, expected: {expected_copies}.")
    else:
        logger.info(f"Found copies: {copies}, expected: {expected_copies}")

    logger.info(f"Found nodes: {found_nodes}")

    if expected_node_list:
        if sorted(found_nodes) == sorted(expected_node_list):
            logger.info(f"Found node list '{found_nodes}' is equal for expected list '{expected_node_list}'")
        else:
            raise Exception(f"Found node list '{found_nodes}' is not equal to expected list '{expected_node_list}'")


@keyword('Get Range')
def get_range(private_key: str, cid: str, oid: str, range_file: str, bearer: str,
        range_cut: str, options:str=""):
    bearer_token = ""
    if bearer:
        bearer_token = f"--bearer {bearer}"

    Cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wif {private_key} '
        f'object range --cid {cid} --oid {oid} {bearer_token} --range {range_cut} '
        f'--file {ASSETS_DIR}/{range_file} {options}'
    )
    logger.info(f"Cmd: {Cmd}")
    _cmd_run(Cmd)


@keyword('Create container')
def create_container(private_key: str, basic_acl:str, rule:str, user_headers: str=''):
    if rule == "":
        logger.error("Cannot create container with empty placement rule")

    if basic_acl:
        basic_acl = f"--basic-acl {basic_acl}"
    if user_headers:
        user_headers = f"--attributes {user_headers}"

    createContainerCmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wif {private_key} '
        f'container create --policy "{rule}" {basic_acl} {user_headers} --await'
    )
    logger.info(f"Cmd: {createContainerCmd}")
    output = _cmd_run(createContainerCmd)
    cid = _parse_cid(output)
    logger.info(f"Created container {cid} with rule {rule}")

    return cid

@keyword('Container List')
def container_list(private_key: str):
    Cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wif {private_key} '
        f'container list'
    )
    logger.info(f"Cmd: {Cmd}")
    output = _cmd_run(Cmd)

    container_list = re.findall(r'(\w{43,44})', output)
    logger.info(f"Containers list: {container_list}")
    return container_list


@keyword('Container Existing')
def container_existing(private_key: str, cid: str):
    Cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wif {private_key} '
        f'container list'
    )
    logger.info(f"Cmd: {Cmd}")
    output = _cmd_run(Cmd)

    _find_cid(output, cid)
    return


@keyword('Search object')
def search_object(private_key: str, cid: str, keys: str, bearer: str, filters: str,
        expected_objects_list=[], options:str=""):
    bearer_token = ""
    filters_result = ""

    if bearer:
        bearer_token = f"--bearer {bearer}"
    if filters:
        for filter_item in filters.split(','):
            filter_item = re.sub(r'=', ' EQ ', filter_item)
            filters_result += f"--filters '{filter_item}' "

    object_cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wif {private_key} '
        f'object search {keys} --cid {cid} {bearer_token} {filters_result} {options}'
    )
    logger.info(f"Cmd: {object_cmd}")
    output = _cmd_run(object_cmd)

    found_objects = re.findall(r'(\w{43,44})', output)

    if expected_objects_list:
        if sorted(found_objects) == sorted(expected_objects_list):
            logger.info(f"Found objects list '{found_objects}' ",
                        f"is equal for expected list '{expected_objects_list}'")
        else:
            raise Exception(f"Found object list {found_objects} ",
                                f"is not equal to expected list '{expected_objects_list}'")

    return found_objects


@keyword('Get Split objects')
def get_component_objects(private_key: str, cid: str, oid: str):
    logger.info("Collect Split objects list from Linked object.")
    split_id = ""
    nodes = _get_storage_nodes()
    for node in nodes:
        try:
            header_virtual = head_object(private_key, cid, oid, '', '', '--raw --ttl 1', node, True)
            if header_virtual:
                parsed_header_virtual = parse_object_virtual_raw_header(header_virtual)

                if 'Linking object' in parsed_header_virtual.keys():
                    return _collect_split_objects_from_header(private_key, cid, parsed_header_virtual)

                elif 'Split ID' in parsed_header_virtual.keys():
                    logger.info(f"parsed_header_virtual: !@ {parsed_header_virtual}" )
                    split_id = parsed_header_virtual['Split ID']

        except:
            logger.warn("Linking object has not been found.")

    # Get all existing objects
    full_obj_list = search_object(private_key, cid, None, None, None, None, '--phy')

    # Search expected Linking object
    for targer_oid in full_obj_list:
        header = head_object(private_key, cid, targer_oid, '', '', '--raw')
        header_parsed = _get_raw_split_information(header)
        if header_parsed['Split ID'] == split_id and 'Split ChildID' in header_parsed.keys():
            logger.info("Linking object has been found in additional check (head of all objects).")
            return _collect_split_objects_from_header(private_key, cid, parsed_header_virtual)

    raise Exception("Linking object is not found at all - all existed objects have been headed.")

def _collect_split_objects_from_header(private_key, cid, parsed_header):
    header_link = head_object(private_key, cid, parsed_header['Linking object'], '', '', '--raw')
    header_link_parsed = _get_raw_split_information(header_link)
    return header_link_parsed['Split ChildID']


@keyword('Verify Split Chain')
def verify_split_chain(private_key: str, cid: str, oid: str):

    header_virtual_parsed = dict()
    header_last_parsed = dict()

    marker_last_obj = 0
    marker_link_obj = 0

    final_verif_data = dict()

    # Get Latest object
    logger.info("Collect Split objects information and verify chain of the objects.")
    nodes = _get_storage_nodes()
    for node in nodes:
        try:
            header_virtual = head_object(private_key, cid, oid, '', '', '--raw --ttl 1', node, True)
            parsed_header_virtual = parse_object_virtual_raw_header(header_virtual)

            if 'Last object' in parsed_header_virtual.keys():
                header_last = head_object(private_key, cid,
                                    parsed_header_virtual['Last object'],
                                    '', '', '--raw')
                header_last_parsed = _get_raw_split_information(header_last)
                marker_last_obj = 1

                # Recursive chain validation up to the first object
                final_verif_data = _verify_child_link(private_key, cid, oid, header_last_parsed, final_verif_data)
                break
            logger.info(f"Found Split Object with header:\n\t{parsed_header_virtual}")
            logger.info("Continue to search Last Split Object")

        except RuntimeError as e:
            logger.info(f"Failed while collectiong Split Objects: {e}")
            continue

    if marker_last_obj == 0:
        raise Exception("Last object has not been found")

    # Get Linking object
    logger.info("Compare Split objects result information with Linking object.")
    for node in nodes:
        try:
            header_virtual = head_object(private_key, cid, oid, '', '', '--raw --ttl 1', node, True)
            parsed_header_virtual = parse_object_virtual_raw_header(header_virtual)
            if 'Linking object' in parsed_header_virtual.keys():

                header_link = head_object(private_key, cid,
                                parsed_header_virtual['Linking object'],
                                '', '', '--raw')
                header_link_parsed = _get_raw_split_information(header_link)
                marker_link_obj = 1

                reversed_list = final_verif_data['ID List'][::-1]

                if header_link_parsed['Split ChildID'] == reversed_list:
                    logger.info(f"Split objects list from Linked Object is equal to expected "
                                f"{', '.join(header_link_parsed['Split ChildID'])}")
                else:
                    raise Exception(f"Split objects list from Linking Object "
                                    f"({', '.join(header_link_parsed['Split ChildID'])}) "
                                    f"is not equal to expected ({', '.join(reversed_list)})")

                if int(header_link_parsed['PayloadLength']) == 0:
                    logger.info("Linking object Payload is equal to expected - zero size.")
                else:
                    raise Exception("Linking object Payload is not equal to expected. Should be zero.")

                if header_link_parsed['Type'] == 'regular':
                    logger.info("Linking Object Type is 'regular' as expected.")
                else:
                    raise Exception("Object Type is not 'regular'.")

                if header_link_parsed['Split ID'] == final_verif_data['Split ID']:
                    logger.info(f"Linking Object Split ID is equal to expected {final_verif_data['Split ID']}.")
                else:
                    raise Exception(f"Split ID from Linking Object ({header_link_parsed['Split ID']}) "
                                    f"is not equal to expected ({final_verif_data['Split ID']})")

                break
            logger.info(f"Found Linking Object with header:\n\t{parsed_header_virtual}")
            logger.info("Continue to search Linking Object")
        except RuntimeError as e:
            logger.info(f"Failed while collecting Split Object: {e}")
            continue

    if marker_link_obj == 0:
        raise Exception("Linked object has not been found")


    logger.info("Compare Split objects result information with Virtual object.")

    header_virtual = head_object(private_key, cid, oid, '', '', '')
    header_virtual_parsed = _get_raw_split_information(header_virtual)

    if int(header_virtual_parsed['PayloadLength']) == int(final_verif_data['PayloadLength']):
        logger.info(f"Split objects PayloadLength are equal to Virtual Object Payload "
                    f"{header_virtual_parsed['PayloadLength']}")
    else:
        raise Exception(f"Split objects PayloadLength from Virtual Object "
                        f"({header_virtual_parsed['PayloadLength']}) is not equal "
                        f"to expected ({final_verif_data['PayloadLength']})")

    if header_link_parsed['Type'] == 'regular':
        logger.info("Virtual Object Type is 'regular' as expected.")
    else:
        raise Exception("Object Type is not 'regular'.")

    return 1


def _verify_child_link(private_key: str, cid: str, oid: str, header_last_parsed: dict, final_verif_data: dict):

    if 'PayloadLength' in final_verif_data.keys():
        final_verif_data['PayloadLength'] = int(final_verif_data['PayloadLength']) + int(header_last_parsed['PayloadLength'])
    else:
        final_verif_data['PayloadLength'] = int(header_last_parsed['PayloadLength'])

    if header_last_parsed['Type'] != 'regular':
        raise Exception("Object Type is not 'regular'.")

    if 'Split ID' in final_verif_data.keys():
        if final_verif_data['Split ID'] != header_last_parsed['Split ID']:
             raise Exception(f"Object Split ID ({header_last_parsed['Split ID']}) is not expected ({final_verif_data['Split ID']}).")
    else:
        final_verif_data['Split ID'] = header_last_parsed['Split ID']

    if 'ID List' in final_verif_data.keys():
        final_verif_data['ID List'].append(header_last_parsed['ID'])
    else:
        final_verif_data['ID List'] = []
        final_verif_data['ID List'].append(header_last_parsed['ID'])

    if 'Split PreviousID' in header_last_parsed.keys():
        header_virtual = head_object(private_key, cid, header_last_parsed['Split PreviousID'], '', '', '--raw')
        parsed_header_virtual = _get_raw_split_information(header_virtual)

        final_verif_data = _verify_child_link(private_key, cid, oid, parsed_header_virtual, final_verif_data)
    else:
        logger.info("Chain of the objects has been parsed from the last object ot the first.")

    return final_verif_data

def _get_raw_split_information(header):
    result_header = dict()

    # Header - Constant attributes

    # ID
    m = re.search(r'^ID: (\w+)', header)
    if m is not None:
        result_header['ID'] = m.group(1)
    else:
        raise Exception(f"no ID was parsed from object header: \t{header}")

    # Type
    m = re.search(r'Type:\s+(\w+)', header)
    if m is not None:
        result_header['Type'] = m.group(1)
    else:
        raise Exception(f"no Type was parsed from object header: \t{header}")

    # PayloadLength
    m = re.search(r'Size: (\d+)', header)
    if m is not None:
        result_header['PayloadLength'] = m.group(1)
    else:
        raise Exception(f"no PayloadLength was parsed from object header: \t{header}")

    # Header - Optional attributes

    # SplitID
    m = re.search(r'Split ID:\s+([\w-]+)', header)
    if m is not None:
        result_header['Split ID'] = m.group(1)

    # Split PreviousID
    m = re.search(r'Split PreviousID:\s+(\w+)', header)
    if m is not None:
        result_header['Split PreviousID'] = m.group(1)

    # Split ParentID
    m = re.search(r'Split ParentID:\s+(\w+)', header)
    if m is not None:
        result_header['Split ParentID'] = m.group(1)

    # Split ChildID list
    found_objects = re.findall(r'Split ChildID:\s+(\w+)', header)
    if found_objects:
        result_header['Split ChildID'] = found_objects
    logger.info(f"Result: {result_header}")

    return result_header

@keyword('Verify Head Tombstone')
def verify_head_tombstone(private_key: str, cid: str, oid_ts: str, oid: str, addr: str):
    object_cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wif {private_key} '
        f'object head --cid {cid} --oid {oid_ts} --json'
    )
    logger.info(f"Cmd: {object_cmd}")
    output = _cmd_run(object_cmd)
    full_headers = json.loads(output)
    logger.info(f"Output: {full_headers}")

    # Header verification
    header_cid = full_headers["header"]["containerID"]["value"]
    if (_json_cli_decode(header_cid) == cid):
        logger.info(f"Header CID is expected: {cid} ({header_cid} in the output)")
    else:
        raise Exception("Header CID is not expected.")

    header_owner = full_headers["header"]["ownerID"]["value"]
    if (_json_cli_decode(header_owner) == addr):
        logger.info(f"Header ownerID is expected: {addr} ({header_owner} in the output)")
    else:
        raise Exception("Header ownerID is not expected.")

    header_type = full_headers["header"]["objectType"]
    if (header_type == "TOMBSTONE"):
        logger.info(f"Header Type is expected: {header_type}")
    else:
        raise Exception("Header Type is not expected.")

    header_session_type = full_headers["header"]["sessionToken"]["body"]["object"]["verb"]
    if (header_session_type == "DELETE"):
        logger.info(f"Header Session Type is expected: {header_session_type}")
    else:
        raise Exception("Header Session Type is not expected.")

    header_session_cid = full_headers["header"]["sessionToken"]["body"]["object"]["address"]["containerID"]["value"]
    if (_json_cli_decode(header_session_cid) == cid):
        logger.info(f"Header ownerID is expected: {addr} ({header_session_cid} in the output)")
    else:
        raise Exception("Header Session CID is not expected.")

    header_session_oid = full_headers["header"]["sessionToken"]["body"]["object"]["address"]["objectID"]["value"]
    if (_json_cli_decode(header_session_oid) == oid):
        logger.info(f"Header Session OID (deleted object) is expected: {oid} ({header_session_oid} in the output)")
    else:
        raise Exception("Header Session OID (deleted object) is not expected.")


def _json_cli_decode(data: str):
    return base58.b58encode(base64.b64decode(data)).decode("utf-8")

@keyword('Head object')
def head_object(private_key: str, cid: str, oid: str, bearer_token: str="",
    user_headers:str="", options:str="", endpoint: str="", json_output: bool = False):

    if bearer_token:
        bearer_token = f"--bearer {bearer_token}"
    if endpoint == "":
        endpoint = NEOFS_ENDPOINT

    object_cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {endpoint} --wif {private_key} object '
        f'head --cid {cid} --oid {oid} {bearer_token} {options} {"--json" if json_output else ""}'
    )
    logger.info(f"Cmd: {object_cmd}")
    output = _cmd_run(object_cmd)

    if user_headers:
        for key in user_headers.split(","):
            if re.search(fr'({key})', output):
                logger.info(f"User header {key} was parsed from command output")
            else:
                raise Exception(f"User header {key} was not found in the command output: \t{output}")
    return output


@keyword('Get container attributes')
def get_container_attributes(private_key: str, cid: str, endpoint: str="", json_output: bool = False):

    if endpoint == "":
        endpoint = NEOFS_ENDPOINT

    container_cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {endpoint} --wif {private_key} '
        f'--cid {cid} container get {"--json" if json_output else ""}'
    )
    logger.info(f"Cmd: {container_cmd}")
    output = _cmd_run(container_cmd)
    return output

@keyword('Parse Object Virtual Raw Header')
def parse_object_virtual_raw_header(header: str):
    result_header = dict()
    m = re.search(r'Split ID:\s+([\w-]+)', header)
    if m != None:
        if m.start() != m.end(): # e.g., if match found something
            result_header['Split ID'] = m.group(1)

    m = re.search(r'Linking object:\s+(\w+)', header)
    if m != None:
        if m.start() != m.end(): # e.g., if match found something
            result_header['Linking object'] = m.group(1)

    m = re.search(r'Last object:\s+(\w+)', header)
    if m != None:
        if m.start() != m.end(): # e.g., if match found something
            result_header['Last object'] = m.group(1)

    logger.info(f"Result: {result_header}")
    return result_header

@keyword('Decode Object System Header Json')
def decode_object_system_header_json(header):
    result_header = dict()
    json_header = json.loads(header)

    # Header - Constant attributes

    # ID
    ID = json_header["objectID"]["value"]
    if ID is not None:
        result_header["ID"] = _json_cli_decode(ID)
    else:
        raise Exception(f"no ID was parsed from header: \t{header}" )

    # CID
    CID = json_header["header"]["containerID"]["value"]
    if CID is not None:
        result_header["CID"] = _json_cli_decode(CID)
    else:
        raise Exception(f"no CID was parsed from header: \t{header}")

    # OwnerID
    OwnerID = json_header["header"]["ownerID"]["value"]
    if OwnerID is not None:
        result_header["OwnerID"] = _json_cli_decode(OwnerID)
    else:
        raise Exception(f"no OwnerID was parsed from header: \t{header}")

    # CreatedAtEpoch
    CreatedAtEpoch = json_header["header"]["creationEpoch"]
    if CreatedAtEpoch is not None:
        result_header["CreatedAtEpoch"] = CreatedAtEpoch
    else:
        raise Exception(f"no CreatedAtEpoch was parsed from header: \t{header}")

    # PayloadLength
    PayloadLength = json_header["header"]["payloadLength"]
    if PayloadLength is not None:
        result_header["PayloadLength"] = PayloadLength
    else:
        raise Exception(f"no PayloadLength was parsed from header: \t{header}")


    # HomoHash
    HomoHash = json_header["header"]["homomorphicHash"]["sum"]
    if HomoHash is not None:
        result_header["HomoHash"] = _json_cli_decode(HomoHash)
    else:
        raise Exception(f"no HomoHash was parsed from header: \t{header}")

    # Checksum
    Checksum = json_header["header"]["payloadHash"]["sum"]
    if Checksum is not None:
        Checksum_64_d = base64.b64decode(Checksum)
        result_header["Checksum"] = binascii.hexlify(Checksum_64_d)
    else:
        raise Exception(f"no Checksum was parsed from header: \t{header}")

    # Type
    Type = json_header["header"]["objectType"]
    if Type is not None:
        result_header["Type"] = Type
    else:
        raise Exception(f"no Type was parsed from header: \t{header}")

    # Header - Optional attributes

    # Attributes
    attributes = []
    attribute_list = json_header["header"]["attributes"]
    if attribute_list is not None:
        for e in attribute_list:
            values_list = list(e.values())
            attribute = values_list[0] + '=' + values_list[1]
            attributes.append(attribute)
        result_header["Attributes"] = attributes
    else:
        raise Exception(f"no Attributes were parsed from header: \t{header}")

    return     result_header


@keyword('Decode Container Attributes Json')
def decode_container_attributes_json(header):
    result_header = dict()
    json_header = json.loads(header)

    attributes = []
    attribute_list = json_header["attributes"]
    if attribute_list is not None:
        for e in attribute_list:
            values_list = list(e.values())
            attribute = values_list[0] + '=' + values_list[1]
            attributes.append(attribute)
        result_header["Attributes"] = attributes
    else:
        raise Exception(f"no Attributes were parsed from header: \t{header}")

    return     result_header


@keyword('Verify Head Attribute')
def verify_head_attribute(header, attribute):
    attribute_list = header["Attributes"]
    if (attribute in attribute_list):
        logger.info(f"Attribute {attribute} is found")
    else:
        raise Exception(f"Attribute {attribute} was not found")


@keyword('Delete object')
def delete_object(private_key: str, cid: str, oid: str, bearer: str, options: str=""):
    bearer_token = ""
    if bearer:
        bearer_token = f"--bearer {bearer}"

    object_cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wif {private_key} '
        f'object delete --cid {cid} --oid {oid} {bearer_token} {options}'
    )
    logger.info(f"Cmd: {object_cmd}")
    output = _cmd_run(object_cmd)
    tombstone = _parse_oid(output)

    return tombstone


@keyword('Delete Container')
# TODO: make the error message about a non-found container more user-friendly https://github.com/nspcc-dev/neofs-contract/issues/121
def delete_container(cid: str, private_key: str):

    deleteContainerCmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wif {private_key} '
        f'container delete --cid {cid} --await'
    )
    logger.info(f"Cmd: {deleteContainerCmd}")
    _cmd_run(deleteContainerCmd)


@keyword('Get file name')
def get_file_name(filepath):
    filename = os.path.basename(filepath)
    return filename


@keyword('Get file hash')
def get_file_hash(filename : str):
    file_hash = _get_file_hash(filename)
    return file_hash


@keyword('Verify file hash')
def verify_file_hash(filename, expected_hash):
    file_hash = _get_file_hash(filename)
    if file_hash == expected_hash:
        logger.info(f"Hash is equal to expected: {file_hash}")
    else:
        raise Exception(f"File hash '{file_hash}' is not equal to {expected_hash}")


@keyword('Put object')
def put_object(private_key: str, path: str, cid: str, bearer: str, user_headers: str,
    endpoint: str="", options: str="" ):
    logger.info("Going to put the object")

    if not endpoint:
      endpoint = random.sample(_get_storage_nodes(), 1)[0]

    if user_headers:
        user_headers = f"--attributes {user_headers}"

    if bearer:
        bearer = f"--bearer {bearer}"

    putobject_cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {endpoint} --wif {private_key} object '
        f'put --file {path} --cid {cid} {bearer} {user_headers} {options}'
    )
    logger.info(f"Cmd: {putobject_cmd}")
    output = _cmd_run(putobject_cmd)
    oid = _parse_oid(output)
    return oid


@keyword('Get Nodes Log Latest Timestamp')
def get_logs_latest_timestamp():
    """
    Keyword return:
    nodes_logs_time -- structure (dict) of nodes container name (key) and latest logs timestamp (value)
    """
    nodes = _get_storage_nodes()
    client_api = docker.APIClient()

    nodes_logs_time = dict()

    for node in nodes:
        container = node.split('.')[0]
        log_line = client_api.logs(container, tail=1)

        m = re.search(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z)', str(log_line))
        if m != None:
            timestamp = m.group(1)

        timestamp_date = datetime.fromisoformat(timestamp[:-1])

        nodes_logs_time[container] = timestamp_date

    logger.info(f"Latest logs timestamp list: {nodes_logs_time}")

    return nodes_logs_time


@keyword('Find in Nodes Log')
def find_in_nodes_Log(line: str, nodes_logs_time: dict):

    client_api = docker.APIClient()
    container_names = list()

    for docker_container in client_api.containers():
        container_names.append(docker_container['Names'][0][1:])

    global_count = 0

    for container in nodes_logs_time.keys():
        # check if container exists
        if container in container_names:
            # Get log since timestamp
            timestamp_date = nodes_logs_time[container]
            log_lines = client_api.logs(container, since=timestamp_date)
            logger.info(f"Timestamp since: {timestamp_date}")
            found_count = len(re.findall(line, log_lines.decode("utf-8") ))
            logger.info(f"Node {container} log - found counter: {found_count}")
            global_count += found_count

        else:
            logger.info(f"Container {container} has not been found.")

    if global_count > 0:
        logger.info(f"Expected line '{line}' has been found in the logs.")
    else:
        raise Exception(f"Expected line '{line}' has not been found in the logs.")

    return 1


@keyword('Get Range Hash')
def get_range_hash(private_key: str, cid: str, oid: str, bearer_token: str,
        range_cut: str, options: str=""):
    if bearer_token:
        bearer_token = f"--bearer {bearer_token}"

    object_cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wif {private_key} '
        f'object hash --cid {cid} --oid {oid} --range {range_cut} '
        f'{bearer_token} {options}'
    )
    logger.info(f"Cmd: {object_cmd}")
    _cmd_run(object_cmd)


@keyword('Get object')
def get_object(private_key: str, cid: str, oid: str, bearer_token: str,
    write_object: str, endpoint: str="", options: str="" ):

    file_path = f"{ASSETS_DIR}/{write_object}"

    logger.info("Going to get the object")
    if not endpoint:
      endpoint = random.sample(_get_storage_nodes(), 1)[0]


    if bearer_token:
        bearer_token = f"--bearer {bearer_token}"

    object_cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {endpoint} --wif {private_key} '
        f'object get --cid {cid} --oid {oid} --file {file_path} {bearer_token} '
        f'{options}'
    )
    logger.info(f"Cmd: {object_cmd}")
    _cmd_run(object_cmd)
    return file_path



@keyword('Put Storagegroup')
def put_storagegroup(private_key: str, cid: str, bearer_token: str="", *oid_list):

    cmd_oid_line = ",".join(oid_list)

    if bearer_token:
        bearer_token = f"--bearer {bearer_token}"

    object_cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wif {private_key} storagegroup '
        f'put --cid {cid} --members {cmd_oid_line} {bearer_token}'
    )
    logger.info(f"Cmd: {object_cmd}")
    output = _cmd_run(object_cmd)
    oid = _parse_oid(output)

    return oid


@keyword('List Storagegroup')
def list_storagegroup(private_key: str, cid: str, bearer_token: str="", *expected_list):

    if bearer_token:
        bearer_token = f"--bearer {bearer_token}"

    object_cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wif {private_key} '
        f'storagegroup list --cid {cid} {bearer_token}'
    )

    logger.info(f"Cmd: {object_cmd}")
    output = _cmd_run(object_cmd)
    found_objects = re.findall(r'(\w{43,44})', output)

    if expected_list:
        if sorted(found_objects) == sorted(expected_list):
            logger.info(f"Found storage group list '{found_objects}' is equal for expected list '{expected_list}'")
        else:
            raise Exception(f"Found storage group '{found_objects}' is not equal to expected list '{expected_list}'")

    return found_objects


@keyword('Get Storagegroup')
def get_storagegroup(private_key: str, cid: str, oid: str, bearer_token: str, expected_size,  *expected_objects_list):

    if bearer_token:
        bearer_token = f"--bearer {bearer_token}"

    object_cmd = f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wif {private_key} storagegroup get --cid {cid} --id {oid} {bearer_token}'
    logger.info(f"Cmd: {object_cmd}")
    output = _cmd_run(object_cmd)

    if expected_size:
        if re.search(fr'Group size: {expected_size}', output):
            logger.info(f"Group size {expected_size} has been found in the output")
        else:
            raise Exception(f"Group size {expected_size} has not been found in the output")

    found_objects = re.findall(r'\s(\w{43,44})\s', output)

    if expected_objects_list:
        if sorted(found_objects) == sorted(expected_objects_list):
            logger.info(f"Found objects list '{found_objects}' is equal for expected list '{expected_objects_list}'")
        else:
            raise Exception(f"Found object list '{found_objects}' is not equal to expected list '{expected_objects_list}'")


@keyword('Delete Storagegroup')
def delete_storagegroup(private_key: str, cid: str, oid: str, bearer_token: str=""):

    if bearer_token:
        bearer_token = f"--bearer {bearer_token}"

    object_cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wif {private_key} storagegroup '
        f'delete --cid {cid} --id {oid} {bearer_token}'
    )
    logger.info(f"Cmd: {object_cmd}")
    output = _cmd_run(object_cmd)

    m = re.search(r'Tombstone: ([a-zA-Z0-9-]+)', output)
    if m.start() != m.end(): # e.g., if match found something
        oid = m.group(1)
    else:
        raise Exception(f"no Tombstone ID was parsed from command output: \t{output}")
    return oid


def _get_file_hash(filename):
    blocksize = 65536
    hash = hashlib.md5()
    with open(filename, "rb") as f:
        for block in iter(lambda: f.read(blocksize), b""):
            hash.update(block)
    logger.info(f"Hash: {hash.hexdigest()}")
    return hash.hexdigest()

def _find_cid(output: str, cid: str):
    """
    This function parses CID from given CLI output.
    Parameters:
    - output: a string with command run output
    """
    if re.search(fr'({cid})', output):
        logger.info(f"CID {cid} was parsed from command output: \t{output}")
    else:
        raise Exception(f"no CID {cid} was parsed from command output: \t{output}")
    return cid

def _parse_oid(input_str: str):
    """
    This function parses OID from given CLI output. The input string we
    expect:
        Object successfully stored
          ID: 4MhrLA7RXTBXCsaNnbahYVAPuoQdiUPuyNEWnywvoSEs
          CID: HeZu2DXBuPve6HXbuHZx64knS7KcGtfSj2L59Li72kkg
    We want to take 'ID' value from the string.

    Parameters:
    - input_str: a string with command run output
    """
    try:
        # taking second string from command output
        snd_str = input_str.split('\n')[1]
    except:
        logger.error(f"Got empty input: {input_str}")
    splitted = snd_str.split(": ")
    if len(splitted) != 2:
        raise Exception(f"no OID was parsed from command output: \t{snd_str}")
    return splitted[1]

def _parse_cid(input_str: str):
    """
    This function parses CID from given CLI output. The input string we
    expect:
            container ID: 2tz86kVTDpJxWHrhw3h6PbKMwkLtBEwoqhHQCKTre1FN
            awaiting...
            container has been persisted on sidechain
    We want to take 'container ID' value from the string.

    Parameters:
    - input_str: a string with command run output
    """
    try:
        # taking first string from command output
        fst_str = input_str.split('\n')[0]
    except:
        logger.error(f"Got empty output: {input_str}")
    splitted = fst_str.split(": ")
    if len(splitted) != 2:
        raise Exception(f"no CID was parsed from command output: \t{fst_str}")
    return splitted[1]

def _get_storage_nodes():
    # TODO: fix to get netmap from neofs-cli
    logger.info(f"Storage nodes: {NEOFS_NETMAP}")
    return NEOFS_NETMAP

def _search_object(node:str, private_key: str, cid:str, oid: str):
    if oid:
        oid_cmd = "--oid %s" % oid
    Cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {node} --wif {private_key} --ttl 1 '
        f'object search --root --cid {cid} {oid_cmd}'
    )

    output = _cmd_run(Cmd)
    if re.search(fr'{oid}', output):
        return oid
    else:
        logger.info("Object is not found.")

    if re.search(r'local node is outside of object placement', output):
        logger.info("Server is not presented in container.")
    elif ( re.search(r'timed out after 30 seconds', output) or re.search(r'no route to host', output) or re.search(r'i/o timeout', output)):
        logger.warn("Node is unavailable")
