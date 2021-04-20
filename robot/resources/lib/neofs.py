#!/usr/bin/python3

import subprocess
import os
import re
import binascii
import uuid
import hashlib
from robot.api.deco import keyword
from robot.api import logger
import random
import base64
import base58
import docker
import json
import tarfile
import shutil

import time
from datetime import datetime

from common import *

ROBOT_AUTO_KEYWORDS = False

CLI_PREFIX = ""
# path to neofs-cli executable
NEOFS_CLI_EXEC = os.getenv('NEOFS_CLI_EXEC', 'neofs-cli')

@keyword('Form WIF from String')
def form_wif_from_string(private_key: str):
    wif = ""
    Cmd = f'{NEOFS_CLI_EXEC} util keyer {private_key}'
    logger.info("Cmd: %s" % Cmd)
    complProc = subprocess.run(Cmd, check=True, universal_newlines=True,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=150, shell=True)
    output = complProc.stdout
    logger.info("Output: %s" % output)

    m = re.search(r'WIF\s+(\w+)', output)
    if m.start() != m.end():
        wif = m.group(1)
    else:
        raise Exception("Can not get WIF.")

    return wif


@keyword('Get ScriptHash')
def get_scripthash(privkey: str):
    scripthash = ""
    Cmd = f'{NEOFS_CLI_EXEC} util keyer -u {privkey}'
    logger.info("Cmd: %s" % Cmd)
    complProc = subprocess.run(Cmd, check=True, universal_newlines=True,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=150, shell=True)
    output = complProc.stdout
    logger.info("Output: %s" % output)

    m = re.search(r'ScriptHash3.0   (\w+)', output)
    if m.start() != m.end():
        scripthash = m.group(1)
    else:
        raise Exception("Can not get ScriptHash.")

    return scripthash


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
            if re.search(r'(%s)' % (oid), search_res):
                nodes_list.append(node)

    logger.info("Nodes with object: %s" % nodes_list)
    return nodes_list


@keyword('Get nodes without object')
def get_nodes_without_object(private_key: str, cid: str, oid: str):
    storage_nodes = _get_storage_nodes()
    copies = 0

    nodes_list = []

    for node in storage_nodes:
        search_res = _search_object(node, private_key, cid, oid)
        if search_res:
            if not re.search(r'(%s)' % (oid), search_res):
                nodes_list.append(node)
        else:
            nodes_list.append(node)

    logger.info("Nodes without object: %s" % nodes_list)
    return nodes_list


@keyword('Validate storage policy for object')
def validate_storage_policy_for_object(private_key: str, expected_copies: int, cid, oid, *expected_node_list):
    storage_nodes = _get_storage_nodes()
    copies = 0
    found_nodes = []

    for node in storage_nodes:
        search_res = _search_object(node, private_key, cid, oid)
        if search_res:
            if re.search(r'(%s)' % (oid), search_res):
                copies += 1
                found_nodes.append(node)

    if copies != expected_copies:
        raise Exception("Object copies is not match storage policy. Found: %s, expexted: %s." % (copies, expected_copies))
    else:
        logger.info("Found copies: %s, expected: %s" % (copies, expected_copies))

    logger.info("Found nodes: %s" % found_nodes)

    if expected_node_list:
        if sorted(found_nodes) == sorted(expected_node_list):
            logger.info("Found node list '{}' is equal for expected list '{}'".format(found_nodes, expected_node_list))
        else:
            raise Exception("Found node list '{}' is not equal to expected list '{}'".format(found_nodes, expected_node_list))


@keyword('Get eACL')
def get_eacl(private_key: str, cid: str):

    Cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --key {private_key} '
        f'container get-eacl --cid {cid}'
    )
    logger.info("Cmd: %s" % Cmd)
    try:
        complProc = subprocess.run(Cmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=150, shell=True)
        output = complProc.stdout
        logger.info("Output: %s" % output)

        return output

    except subprocess.CalledProcessError as e:
        if re.search(r'extended ACL table is not set for this container', e.output):
            logger.info("Extended ACL table is not set for this container.")
        else:
            raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))


@keyword('Get Epoch')
def get_epoch(private_key: str):
    cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --key {private_key} '
        f'netmap epoch'
    )
    logger.info(f"Cmd: {cmd}")
    try:
        complProc = subprocess.run(cmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=150, shell=True)
        output = complProc.stdout
        logger.info(f"Output: {output}")
        return int(output)
    except subprocess.CalledProcessError as e:
        raise Exception(f"command '{e.cmd}' return with error (code {e.returncode}): {e.output}")

@keyword('Set eACL')
def set_eacl(private_key: str, cid: str, eacl: str, add_keys: str = ""):
    file_path = TEMP_DIR + eacl
    cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --key {private_key} '
        f'container set-eacl --cid {cid} --table {file_path} {add_keys}'
    )
    logger.info(f"Cmd: {cmd}")
    try:
        complProc = subprocess.run(cmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=150, shell=True)
        output = complProc.stdout
        logger.info(f"Output: {output}")
    except subprocess.CalledProcessError as e:
        raise Exception(f"command '{e.cmd}' return with error (code {e.returncode}): {e.output}")

@keyword('Form BearerToken file')
def form_bearertoken_file(private_key: str, cid: str, file_name: str, eacl_oper_list,
        lifetime_exp: str ):
    cid_base58_b = base58.b58decode(cid)
    cid_base64 = base64.b64encode(cid_base58_b).decode("utf-8")
    eacl = get_eacl(private_key, cid)
    json_eacl = {}
    file_path = TEMP_DIR + file_name

    if eacl:
        res_json = re.split(r'[\s\n]+Signature:', eacl)
        input_eacl = res_json[0].replace('eACL: ', '')
        json_eacl = json.loads(input_eacl)

    eacl_result = {"body":{ "eaclTable": { "containerID": { "value": cid_base64 }, "records": [] }, "lifetime": {"exp": lifetime_exp, "nbf": "1", "iat": "0"} } }

    if eacl_oper_list:
        for record in eacl_oper_list:
            op_data = dict()

            if record['Role'] == "USER" or record['Role'] == "SYSTEM" or record['Role'] == "OTHERS":
                op_data = {"operation":record['Operation'],"action":record['Access'],"filters": [],"targets":[{"role":record['Role']}]}
            else:
                op_data = {"operation":record['Operation'],"action":record['Access'],"filters": [],"targets":[{"keys": [ record['Role'] ]}]}

            if 'Filters' in record.keys():
                op_data["filters"].append(record['Filters'])

            eacl_result["body"]["eaclTable"]["records"].append(op_data)

        # Add records from current eACL
        if "records" in json_eacl.keys():
            for record in json_eacl["records"]:
                eacl_result["body"]["eaclTable"]["records"].append(record)

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(eacl_result, f, ensure_ascii=False, indent=4)

        logger.info(eacl_result)

    # Sign bearer token
    Cmd = (
        f'{NEOFS_CLI_EXEC} util sign bearer-token --from {file_path} '
        f'--to {file_path} --key {private_key} --json'
    )
    logger.info("Cmd: %s" % Cmd)

    try:
        complProc = subprocess.run(Cmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)
        output = complProc.stdout
        logger.info("Output: %s" % str(output))
    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))

    return file_path

@keyword('Form eACL json common file')
def form_eacl_json_common_file(file_name, eacl_oper_list ):
    # Input role can be Role (USER, SYSTEM, OTHERS) or public key.
    file_path = TEMP_DIR + file_name
    eacl = {"records":[]}

    logger.info(eacl_oper_list)

    if eacl_oper_list:
        for record in eacl_oper_list:
            op_data = dict()

            if record['Role'] == "USER" or record['Role'] == "SYSTEM" or record['Role'] == "OTHERS":
                op_data = {"operation":record['Operation'],"action":record['Access'],"filters": [],"targets":[{"role":record['Role']}]}
            else:
                op_data = {"operation":record['Operation'],"action":record['Access'],"filters": [],"targets":[{"keys": [ record['Role'] ]}]}

            if 'Filters' in record.keys():
                op_data["filters"].append(record['Filters'])

            eacl["records"].append(op_data)

        logger.info(eacl)

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(eacl, f, ensure_ascii=False, indent=4)

    return file_name


@keyword('Get Range')
def get_range(private_key: str, cid: str, oid: str, range_file: str, bearer: str,
        range_cut: str, options:str=""):
    bearer_token = ""
    if bearer:
        bearer_token = f"--bearer {TEMP_DIR}{bearer}"

    Cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --key {private_key} '
        f'object range --cid {cid} --oid {oid} {bearer_token} --range {range_cut} '
        f'--file {TEMP_DIR}{range_file} {options}'
    )
    logger.info("Cmd: %s" % Cmd)

    try:
        complProc = subprocess.run(Cmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=150, shell=True)
        output = complProc.stdout
        logger.info("Output: %s" % str(output))
    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))

@keyword('Create container')
def create_container(private_key: str, basic_acl:str="",
        rule:str="REP 2 IN X CBF 1 SELECT 2 FROM * AS X"):
    if basic_acl != "":
        basic_acl = "--basic-acl " + basic_acl

    createContainerCmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --key {private_key} '
        f'container create --policy "{rule}" {basic_acl} --await'
    )
    logger.info("Cmd: %s" % createContainerCmd)
    complProc = subprocess.run(createContainerCmd, check=True, universal_newlines=True,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=300, shell=True)
    output = complProc.stdout
    logger.info("Output: %s" % output)
    cid = _parse_cid(output)
    logger.info("Created container %s with rule '%s'" % (cid, rule))

    return cid


@keyword('Container List')
def container_list(private_key: str):
    Cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --key {private_key} '
        f'container list'
    )
    logger.info("Cmd: %s" % Cmd)
    complProc = subprocess.run(Cmd, check=True, universal_newlines=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)
    logger.info("Output: %s" % complProc.stdout)

    container_list = re.findall(r'(\w{43,44})', complProc.stdout)
    logger.info("Containers list: %s" % container_list)
    return container_list

@keyword('Container Existing')
def container_existing(private_key: str, cid: str):
    Cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --key {private_key} '
        f'container list'
    )
    logger.info("Cmd: %s" % Cmd)
    complProc = subprocess.run(Cmd, check=True, universal_newlines=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)
    logger.info("Output: %s" % complProc.stdout)

    _find_cid(complProc.stdout, cid)
    return

@keyword('Generate file of bytes')
def generate_file_of_bytes(size):
    """
    generate big binary file with the specified size in bytes
    :param size:        the size in bytes, can be declared as 6e+6 for example
    :return:string      filename
    """

    size = int(float(size))

    filename = TEMP_DIR + str(uuid.uuid4())
    with open('%s'%filename, 'wb') as fout:
        fout.write(os.urandom(size))

    logger.info("Random binary file with size %s bytes has been generated." % str(size))
    return os.path.abspath(os.getcwd()) + '/' + filename


@keyword('Search object')
def search_object(private_key: str, cid: str, keys: str, bearer: str, filters: str,
        expected_objects_list=[], options:str=""):
    bearer_token = ""
    filters_result = ""

    if bearer:
        bearer_token = f"--bearer {TEMP_DIR}{bearer}"
    if filters:
        for filter_item in filters.split(','):
            filter_item = re.sub(r'=', ' EQ ', filter_item)
            filters_result += f"--filters '{filter_item}' "

    object_cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --key {private_key} '
        f'object search {keys} --cid {cid} {bearer_token} {filters_result} {options}'
    )
    logger.info("Cmd: %s" % object_cmd)
    try:
        complProc = subprocess.run(object_cmd, check=True, universal_newlines=True,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)

        logger.info("Output: %s" % complProc.stdout)

        found_objects = re.findall(r'(\w{43,44})', complProc.stdout)

        if expected_objects_list:
            if sorted(found_objects) == sorted(expected_objects_list):
                logger.info("Found objects list '{}' is equal for expected list '{}'".format(found_objects, expected_objects_list))
            else:
                raise Exception("Found object list '{}' is not equal to expected list '{}'".format(found_objects, expected_objects_list))

        return found_objects

    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))
        

@keyword('Get Split objects')
def get_component_objects(private_key: str, cid: str, oid: str):

    logger.info("Collect Split objects list from Linked object.")
    split_id = ""
    nodes = _get_storage_nodes()
    for node in nodes:
        header_virtual = head_object(private_key, cid, oid, '', '', '--raw --ttl 1', node, True)
        if header_virtual:
            parsed_header_virtual = parse_object_virtual_raw_header(header_virtual)

            if 'Linking object' in parsed_header_virtual.keys():
                return _collect_split_objects_from_header(private_key, cid, parsed_header_virtual)

            elif 'Split ID' in parsed_header_virtual.keys():
                logger.info(f"parsed_header_virtual: !@ {parsed_header_virtual}" )
                split_id = parsed_header_virtual['Split ID']

    logger.warn("Linking object has not been found.")

    # Get all existing objects
    full_obj_list = search_object(private_key, cid, None, None, None, None, '--phy')
  
    # Search expected Linking object
    for targer_oid in full_obj_list:
        header = head_object(private_key, cid, targer_oid, '', '', '--raw')
        header_parsed = parse_object_system_header(header)
        if header_parsed['Split ID'] == split_id and 'Split ChildID' in header_parsed.keys():
            logger.info("Linking object has been found in additional check (head of all objects).")
            return _collect_split_objects_from_header(private_key, cid, parsed_header_virtual)

    raise Exception("Linking object is not found at all - all existed objects have been headed.")

def _collect_split_objects_from_header(private_key, cid, parsed_header):
    header_link = head_object(private_key, cid, parsed_header['Linking object'], '', '', '--raw')
    header_link_parsed = parse_object_system_header(header_link)
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
        header_virtual = head_object(private_key, cid, oid, '', '', '--raw --ttl 1', node, True)
        parsed_header_virtual = parse_object_virtual_raw_header(header_virtual)

        if 'Last object' in parsed_header_virtual.keys():
            header_last = head_object(private_key, cid, parsed_header_virtual['Last object'], '', '', '--raw')
            header_last_parsed = parse_object_system_header(header_last)
            marker_last_obj = 1

            # Recursive chain validation up to the first object
            final_verif_data = _verify_child_link(private_key, cid, oid, header_last_parsed, final_verif_data)
            break

    if marker_last_obj == 0:
        raise Exception("Latest object has not been found.")

    # Get Linking object
    logger.info("Compare Split objects result information with Linking object.")
    for node in nodes:

        header_virtual = head_object(private_key, cid, oid, '', '', '--raw --ttl 1', node, True)
        parsed_header_virtual = parse_object_virtual_raw_header(header_virtual)
        if 'Linking object' in parsed_header_virtual.keys():

            header_link = head_object(private_key, cid, parsed_header_virtual['Linking object'], '', '', '--raw')
            header_link_parsed = parse_object_system_header(header_link)
            marker_link_obj = 1

            reversed_list = final_verif_data['ID List'][::-1]

            if header_link_parsed['Split ChildID'] == reversed_list:
                logger.info("Split objects list from Linked Object is equal to expected %s" % ', '.join(header_link_parsed['Split ChildID']))
            else:
                raise Exception("Split objects list from Linking Object (%s) is not equal to expected (%s)" % ', '.join(header_link_parsed['Split ChildID']), ', '.join(reversed_list) )

            if int(header_link_parsed['PayloadLength']) == 0:
                logger.info("Linking object Payload is equal to expected - zero size.")
            else:
                raise Exception("Linking object Payload is not equal to expected. Should be zero.")

            if header_link_parsed['Type'] == 'regular':
                logger.info("Linking Object Type is 'regular' as expected.")
            else:
                raise Exception("Object Type is not 'regular'.")

            if header_link_parsed['Split ID'] == final_verif_data['Split ID']:
                logger.info("Linking Object Split ID is equal to expected %s." % final_verif_data['Split ID'] )
            else:
                raise Exception("Split ID from Linking Object (%s) is not equal to expected (%s)" % header_link_parsed['Split ID'], ffinal_verif_data['Split ID'] )

            break

    if marker_link_obj == 0:
        raise Exception("Linked object has not been found.")


    logger.info("Compare Split objects result information with Virtual object.")

    header_virtual = head_object(private_key, cid, oid, '', '', '')
    header_virtual_parsed = parse_object_system_header(header_virtual)

    if int(header_virtual_parsed['PayloadLength']) == int(final_verif_data['PayloadLength']):
        logger.info("Split objects PayloadLength are equal to Virtual Object Payload %s" % header_virtual_parsed['PayloadLength'])
    else:
        raise Exception("Split objects PayloadLength from Virtual Object (%s) is not equal to expected (%s)" % header_virtual_parsed['PayloadLength'], final_verif_data['PayloadLength'] )

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
             raise Exception("Object Split ID (%s) is not expected (%s)." % header_last_parsed['Split ID'], final_verif_data['Split ID'])
    else:
        final_verif_data['Split ID'] = header_last_parsed['Split ID']

    if 'ID List' in final_verif_data.keys():
        final_verif_data['ID List'].append(header_last_parsed['ID'])
    else:
        final_verif_data['ID List'] = []
        final_verif_data['ID List'].append(header_last_parsed['ID'])

    if 'Split PreviousID' in header_last_parsed.keys():
        header_virtual = head_object(private_key, cid, header_last_parsed['Split PreviousID'], '', '', '--raw')
        parsed_header_virtual = parse_object_system_header(header_virtual)

        final_verif_data = _verify_child_link(private_key, cid, oid, parsed_header_virtual, final_verif_data)
    else:
        logger.info("Chain of the objects has been parsed from the last object ot the first.")

    return final_verif_data

@keyword('Get Docker Logs')
def get_container_logs(testcase_name: str):
    #client = docker.APIClient()
    
    client = docker.from_env()

    tar_name = "artifacts/dockerlogs("+testcase_name+").tar.gz"
    tar = tarfile.open(tar_name, "w:gz")

    for container in client.containers.list():
        file_name = "artifacts/docker_log_" + container.name
        with open(file_name,'wb') as out:
            out.write(container.logs())
        logger.info(container.name)

        tar.add(file_name)
        os.remove(file_name)
    
    tar.close()
    
    return 1

@keyword('Verify Head Tombstone')
def verify_head_tombstone(private_key: str, cid: str, oid_ts: str, oid: str, addr: str):
    object_cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --key {private_key} '
        f'object head --cid {cid} --oid {oid_ts} --json'
    )
    logger.info("Cmd: %s" % object_cmd)

    try:
        complProc = subprocess.run(object_cmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)
        full_headers = json.loads(complProc.stdout)
        logger.info("Output: %s" % full_headers)

        # Header verification
        header_cid = full_headers["header"]["containerID"]["value"]
        if (_json_cli_decode(header_cid) == cid):
            logger.info("Header CID is expected: %s (%s in the output)" % (cid, header_cid))
        else:
            raise Exception("Header CID is not expected.")

        header_owner = full_headers["header"]["ownerID"]["value"]
        if (_json_cli_decode(header_owner) == addr):
            logger.info("Header ownerID is expected: %s (%s in the output)" % (addr, header_owner))
        else:
            raise Exception("Header ownerID is not expected.")

        header_type = full_headers["header"]["objectType"]
        if (header_type == "TOMBSTONE"):
            logger.info("Header Type is expected: %s" % header_type)
        else:
            raise Exception("Header Type is not expected.")

        header_session_type = full_headers["header"]["sessionToken"]["body"]["object"]["verb"]
        if (header_session_type == "DELETE"):
            logger.info("Header Session Type is expected: %s" % header_session_type)
        else:
            raise Exception("Header Session Type is not expected.")

        header_session_cid = full_headers["header"]["sessionToken"]["body"]["object"]["address"]["containerID"]["value"]
        if (_json_cli_decode(header_session_cid) == cid):
            logger.info("Header ownerID is expected: %s (%s in the output)" % (addr, header_session_cid))
        else:
            raise Exception("Header Session CID is not expected.")

        header_session_oid = full_headers["header"]["sessionToken"]["body"]["object"]["address"]["objectID"]["value"]
        if (_json_cli_decode(header_session_oid) == oid):
            logger.info("Header Session OID (deleted object) is expected: %s (%s in the output)" % (oid, header_session_oid))
        else:
            raise Exception("Header Session OID (deleted object) is not expected.")

    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))

def _json_cli_decode(data: str):
    return base58.b58encode(base64.b64decode(data)).decode("utf-8")

@keyword('Head object')
def head_object(private_key: str, cid: str, oid: str, bearer_token: str="",
    user_headers:str="", options:str="", endpoint: str="", ignore_failure: bool = False):

    if bearer_token:
        bearer_token = f"--bearer {TEMP_DIR}{bearer_token}"
    if endpoint == "":
        endpoint = NEOFS_ENDPOINT

    object_cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {endpoint} --key {private_key} object '
        f'head --cid {cid} --oid {oid} {bearer_token} {options}'
    )
    logger.info("Cmd: %s" % object_cmd)
    try:
        complProc = subprocess.run(object_cmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)
        logger.info("Output: %s" % complProc.stdout)

        if user_headers:
            for key in user_headers.split(","):
                if re.search(r'(%s)' % key, complProc.stdout):
                    logger.info("User header %s was parsed from command output" % key)
                else:
                    raise Exception("User header %s was not found in the command output: \t%s" % (key, complProc.stdout))
        return complProc.stdout

    except subprocess.CalledProcessError as e:
        if ignore_failure:
            logger.info("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))
            return e.output
        else:
            raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))

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

    logger.info("Result: %s" % result_header)
    return result_header

@keyword('Parse Object System Header')
def parse_object_system_header(header: str):
    result_header = dict()

    # Header - Constant attributes

    # ID
    m = re.search(r'^ID: (\w+)', header)
    if m is not None:
        result_header['ID'] = m.group(1)
    else:
        raise Exception("no ID was parsed from object header: \t%s" % header)

    # CID
    m = re.search(r'CID: (\w+)', header)
    if m is not None:
        result_header['CID'] = m.group(1)
    else:
        raise Exception("no CID was parsed from object header: \t%s" % header)

    # Owner
    m = re.search(r'Owner: ([a-zA-Z0-9]+)', header)
    if m is not None:
        result_header['OwnerID'] = m.group(1)
    else:
        raise Exception("no OwnerID was parsed from object header: \t%s" % header)

    # CreatedAtEpoch
    m = re.search(r'CreatedAt: (\d+)', header)
    if m is not None:
        result_header['CreatedAtEpoch'] = m.group(1)
    else:
        raise Exception("no CreatedAtEpoch was parsed from object header: \t%s" % header)

    # PayloadLength
    m = re.search(r'Size: (\d+)', header)
    if m is not None:
        result_header['PayloadLength'] = m.group(1)
    else:
        raise Exception("no PayloadLength was parsed from object header: \t%s" % header)

    # HomoHash
    m = re.search(r'HomoHash:\s+(\w+)', header)
    if m is not None:
        result_header['HomoHash'] = m.group(1)
    else:
        raise Exception("no HomoHash was parsed from object header: \t%s" % header)

    # Checksum
    m = re.search(r'Checksum:\s+(\w+)', header)
    if m is not None:
        result_header['Checksum'] = m.group(1)
    else:
        raise Exception("no Checksum was parsed from object header: \t%s" % header)

    # Type
    m = re.search(r'Type:\s+(\w+)', header)
    if m is not None:
        result_header['Type'] = m.group(1)
    else:
        raise Exception("no Type was parsed from object header: \t%s" % header)


    # Header - Optional attributes
    m = re.search(r'Split ID:\s+([\w-]+)', header)
    if m is not None:
        result_header['Split ID'] = m.group(1)

    m = re.search(r'Split PreviousID:\s+(\w+)', header)
    if m is not None:
        result_header['Split PreviousID'] = m.group(1)

    m = re.search(r'Split ParentID:\s+(\w+)', header)
    if m is not None:
        result_header['Split ParentID'] = m.group(1)

    # Split ChildID list
    found_objects = re.findall(r'Split ChildID:\s+(\w+)', header)
    if found_objects:
        result_header['Split ChildID'] = found_objects


    logger.info("Result: %s" % result_header)
    return result_header


@keyword('Delete object')
def delete_object(private_key: str, cid: str, oid: str, bearer: str, options: str=""):
    bearer_token = ""
    if bearer:
        bearer_token = f"--bearer {TEMP_DIR}{bearer}"

    object_cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --key {private_key} '
        f'object delete --cid {cid} --oid {oid} {bearer_token} {options}'
    )
    logger.info("Cmd: %s" % object_cmd)
    try:
        complProc = subprocess.run(object_cmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30, shell=True)
        logger.info("Output: %s" % complProc.stdout)
        tombstone = _parse_oid(complProc.stdout)
        return tombstone
    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))

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
        logger.info("Hash is equal to expected: %s" % file_hash)
    else:
        raise Exception("File hash '{}' is not equal to {}".format(file_hash, expected_hash))

@keyword('Cleanup Files')
def cleanup_file():
    if os.path.isdir(TEMP_DIR):
        try:                                
            shutil.rmtree(TEMP_DIR)
        except OSError as e:
            raise Exception(f"Error: '{e.TEMP_DIR}' - {e.strerror}.")
    else:
        logger.warn(f"Error: '{TEMP_DIR}' file not found")
    logger.info(f"File '{TEMP_DIR}' has been deleted.")

@keyword('Put object')
def put_object(private_key: str, path: str, cid: str, bearer: str, user_headers: str,
    endpoint: str="", options: str="" ):
    logger.info("Going to put the object")

    if not endpoint:
      endpoint = random.sample(_get_storage_nodes(), 1)[0]

    if user_headers:
        user_headers = f"--attributes {user_headers}"

    if bearer:
        bearer = f"--bearer {TEMP_DIR}{bearer}"

    putobject_cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {endpoint} --key {private_key} object '
        f'put --file {path} --cid {cid} {bearer} {user_headers} {options}'
    )
    logger.info("Cmd: %s" % putobject_cmd)
    try:
        complProc = subprocess.run(putobject_cmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120, shell=True)
        logger.info("Output: %s" % complProc.stdout)
        oid = _parse_oid(complProc.stdout)
        return oid
    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))


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
    
    logger.info("Latest logs timestamp list: %s" % nodes_logs_time)

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
            logger.info("Timestamp since: %s " % timestamp_date)
            found_count = len(re.findall(line, log_lines.decode("utf-8") ))
            logger.info("Node %s log - found counter: %s" % (container, found_count))
            global_count += found_count
            
        else:
            logger.info("Container %s has not been found." % container)

    if global_count > 0:
        logger.info("Expected line '%s' has been found in the logs." % line)
    else:
        raise Exception("Expected line '%s' has not been found in the logs." % line)

    return 1



@keyword('Get Range Hash')
def get_range_hash(private_key: str, cid: str, oid: str, bearer_token: str,
        range_cut: str, options: str=""):
    if bearer_token:
        bearer_token = f"--bearer {TEMP_DIR}{bearer_token}"

    object_cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --key {private_key} '
        f'object hash --cid {cid} --oid {oid} --range {range_cut} '
        f'{bearer_token} {options}'
    )
    logger.info("Cmd: %s" % object_cmd)
    try:
        complProc = subprocess.run(object_cmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60, shell=True)
        logger.info("Output: %s" % complProc.stdout)
    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))

@keyword('Get object')
def get_object(private_key: str, cid: str, oid: str, bearer_token: str,
    write_object: str, endpoint: str="", options: str="" ):

    file_path = TEMP_DIR + write_object

    logger.info("Going to put the object")
    if not endpoint:
      endpoint = random.sample(_get_storage_nodes(), 1)[0]

    
    if bearer_token:
        bearer_token = f"--bearer {TEMP_DIR}{bearer_token}"

    object_cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {endpoint} --key {private_key} '
        f'object get --cid {cid} --oid {oid} --file {file_path} {bearer_token} '
        f'{options}'
    )
    logger.info("Cmd: %s" % object_cmd)
    try:
        complProc = subprocess.run(object_cmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120, shell=True)
        logger.info("Output: %s" % complProc.stdout)
    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))
    return file_path



@keyword('Put Storagegroup')
def put_storagegroup(private_key: str, cid: str, bearer_token: str="", *oid_list):

    cmd_oid_line = ",".join(oid_list) 

    if bearer_token:
        bearer_token = f"--bearer {TEMP_DIR}{bearer_token}"

    object_cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --key {private_key} storagegroup '
        f'put --cid {cid} --members {cmd_oid_line} {bearer_token}'
    )
    logger.info(f"Cmd: {object_cmd}")
    try:
        complProc = subprocess.run(object_cmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60, shell=True)
        logger.info(f"Output: {complProc.stdout}" )

        oid = _parse_oid(complProc.stdout)
        return oid
    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))


@keyword('List Storagegroup')
def list_storagegroup(private_key: str, cid: str, bearer_token: str="", *expected_list):

    if bearer_token:
        bearer_token = f"--bearer {TEMP_DIR}{bearer_token}"

    object_cmd = ( 
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --key {private_key} '
        f'storagegroup list --cid {cid} {bearer_token}'
    )

    logger.info(f"Cmd: {object_cmd}")
    try:
        complProc = subprocess.run(object_cmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)
        logger.info(f"Output: {complProc.stdout}")

        found_objects = re.findall(r'(\w{43,44})', complProc.stdout)

        if expected_list:
            if sorted(found_objects) == sorted(expected_list):
                logger.info("Found storage group list '{}' is equal for expected list '{}'".format(found_objects, expected_list))
            else:
                raise Exception("Found storage group '{}' is not equal to expected list '{}'".format(found_objects, expected_list))

        return found_objects
        
    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))


@keyword('Get Storagegroup')
def get_storagegroup(private_key: str, cid: str, oid: str, bearer_token: str, expected_size,  *expected_objects_list):

    if bearer_token:
        bearer_token = f"--bearer {TEMP_DIR}{bearer_token}"

    object_cmd = f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --key {private_key} storagegroup get --cid {cid} --id {oid} {bearer_token}'
    logger.info(f"Cmd: {object_cmd}")
    try:
        complProc = subprocess.run(object_cmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60, shell=True)
        logger.info(f"Output: {complProc.stdout}")
      
        if expected_size:
            if re.search(r'Group size: %s' % expected_size, complProc.stdout):
                logger.info("Group size %s has been found in the output" % (expected_size))
            else:
                raise Exception("Group size %s has not been found in the output" % (expected_size))

        found_objects = re.findall(r'\s(\w{43,44})\s', complProc.stdout)

        if expected_objects_list:
            if sorted(found_objects) == sorted(expected_objects_list):
                logger.info("Found objects list '{}' is equal for expected list '{}'".format(found_objects, expected_objects_list))
            else:
                raise Exception("Found object list '{}' is not equal to expected list '{}'".format(found_objects, expected_objects_list))


    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))


@keyword('Delete Storagegroup')
def delete_storagegroup(private_key: str, cid: str, oid: str, bearer_token: str=""):

    if bearer_token:
        bearer_token = f"--bearer {TEMP_DIR}{bearer_token}"

    object_cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --key {private_key} storagegroup '
        f'delete --cid {cid} --id {oid} {bearer_token}'
    )
    logger.info(f"Cmd: {object_cmd}")
    try:
        complProc = subprocess.run(object_cmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60, shell=True)
        logger.info(f"Output: {complProc.stdout}")

        m = re.search(r'Tombstone: ([a-zA-Z0-9-]+)', complProc.stdout)
        if m.start() != m.end(): # e.g., if match found something
            oid = m.group(1)
        else:
            raise Exception("no Tombstone ID was parsed from command output: \t%s" % complProc.stdout)
        return oid

    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))



def _exec_cli_cmd(private_key: bytes, postfix: str):
    # Get linked objects from first
    object_cmd = (
        f'{NEOFS_CLI_EXEC} --raw --host {NEOFS_ENDPOINT} '
        f'--key {binascii.hexlify(private_key).decode()} {postfix}'
    )
    logger.info("Cmd: %s" % object_cmd)
    try:
        complProc = subprocess.run(object_cmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)
        logger.info("Output: %s" % complProc.stdout)
    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))
    return complProc.stdout

def _get_file_hash(filename):
    blocksize = 65536
    hash = hashlib.md5()
    with open(filename, "rb") as f:
        for block in iter(lambda: f.read(blocksize), b""):
            hash.update(block)
    logger.info("Hash: %s" % hash.hexdigest())
    return hash.hexdigest()

def _find_cid(output: str, cid: str):
    """
    This function parses CID from given CLI output.
    Parameters:
    - output: a string with command run output
    """
    if re.search(r'(%s)' % cid, output):
        logger.info("CID %s was parsed from command output: \t%s" % (cid, output))
    else:
        raise Exception("no CID %s was parsed from command output: \t%s" % (cid, output))
    return cid

def _parse_oid(output: str):
    """
    This function parses OID from given CLI output.
    Parameters:
    - output: a string with command run output
    """
    m = re.search(r'ID: ([a-zA-Z0-9-]+)', output)
    if m.start() != m.end(): # e.g., if match found something
        oid = m.group(1)
    else:
        raise Exception("no OID was parsed from command output: \t%s" % output)
    return oid

def _parse_cid(output: str):
    """
    This function parses CID from given CLI output.
    Parameters:
    - output: a string with command run output
    """
    m = re.search(r'container ID: (\w+)', output)
    if not m.start() != m.end(): # e.g., if match found something
        raise Exception("no CID was parsed from command output: \t%s" % (output))
    cid = m.group(1)
    return cid

def _get_storage_nodes():
    # TODO: fix to get netmap from neofs-cli
    logger.info("Storage nodes: %s" % NEOFS_NETMAP)
    return NEOFS_NETMAP

def _search_object(node:str, private_key: str, cid:str, oid: str):
    if oid:
        oid_cmd = "--oid %s" % oid
    Cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {node} --key {private_key} --ttl 1 '
        f'object search --root --cid {cid} {oid_cmd}'
    )
    try:
        logger.info(Cmd)
        complProc = subprocess.run(Cmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30, shell=True)
        logger.info("Output: %s" % complProc.stdout)
        if re.search(r'%s' % oid, complProc.stdout):
            return oid
        else:
            logger.info("Object is not found.")

    except subprocess.CalledProcessError as e:
        if re.search(r'local node is outside of object placement', e.output):
            logger.info("Server is not presented in container.")
        elif ( re.search(r'timed out after 30 seconds', e.output) or re.search(r'no route to host', e.output) or re.search(r'i/o timeout', e.output)):
            logger.warn("Node is unavailable")
        else:
            raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))
