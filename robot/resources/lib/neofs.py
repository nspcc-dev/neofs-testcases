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

if os.getenv('ROBOT_PROFILE') == 'selectel_smoke':
    from selectelcdn_smoke_vars import (NEOGO_CLI_PREFIX, NEO_MAINNET_ENDPOINT,
    NEOFS_NEO_API_ENDPOINT, NEOFS_ENDPOINT)
else:
    from neofs_int_vars import (NEOGO_CLI_PREFIX, NEO_MAINNET_ENDPOINT,
    NEOFS_NEO_API_ENDPOINT, NEOFS_ENDPOINT)

ROBOT_AUTO_KEYWORDS = False

CLI_PREFIX = ""

@keyword('Form WIF from String')
def form_wif_from_string(private_key: str):
    wif = ""
    Cmd = f'neofs-cli util keyer {private_key}'
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


@keyword('Get ScripHash')
def get_scripthash(privkey: str):
    scripthash = ""
    Cmd = f'neofs-cli util keyer -u {privkey}'
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

        Cmd = f'docker stop {node}'
        logger.info("Cmd: %s" % Cmd)

        try:
            complProc = subprocess.run(Cmd, check=True, universal_newlines=True,
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=150, shell=True)
            output = complProc.stdout
            logger.info("Output: %s" % output)

        except subprocess.CalledProcessError as e:
            raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))

    return stop_nodes


@keyword('Start nodes')
def start_nodes(*nodes_list):

    for node in nodes_list:
        m = re.search(r'(s\d+).', node)
        node = m.group(1)

        Cmd = f'docker start {node}'
        logger.info("Cmd: %s" % Cmd)

        try:
            complProc = subprocess.run(Cmd, check=True, universal_newlines=True,
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=150, shell=True)
            output = complProc.stdout
            logger.info("Output: %s" % output)

        except subprocess.CalledProcessError as e:
            raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))


@keyword('Get nodes with object')
def get_nodes_with_object(private_key: str, cid: str, oid: str):
    storage_nodes = _get_storage_nodes(private_key)
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
    storage_nodes = _get_storage_nodes(private_key)
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
    storage_nodes = _get_storage_nodes(private_key)
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

    Cmd = f'neofs-cli --rpc-endpoint {NEOFS_ENDPOINT} --key {private_key} container get-eacl --cid {cid}'
    logger.info("Cmd: %s" % Cmd)
    try:
        complProc = subprocess.run(Cmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=150, shell=True)
        output = complProc.stdout
        logger.info("Output: %s" % output)

        return output

    except subprocess.CalledProcessError as e:
        if re.search(r'extended ACL table is not set for this container', e.output):
            logger.info("Server is not presented in container.")
        else:
            raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))



@keyword('Set eACL')
def set_eacl(private_key: str, cid: str, eacl: str, add_keys: str = ""):

    Cmd = f'neofs-cli --rpc-endpoint {NEOFS_ENDPOINT} --key {private_key}  container set-eacl --cid {cid} --table {eacl} {add_keys}'
    logger.info("Cmd: %s" % Cmd)
    complProc = subprocess.run(Cmd, check=True, universal_newlines=True,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=150, shell=True)
    output = complProc.stdout
    logger.info("Output: %s" % output)



@keyword('Form BearerToken file for all ops')
def form_bearertoken_file_for_all_ops(file_name: str, private_key: str, cid: str, action: str, target_role: str, lifetime_exp: str ):

    eacl = get_eacl(private_key, cid)
    input_records = ""
    if eacl:
        res_json = re.split(r'[\s\n]+\][\s\n]+\}[\s\n]+Signature:', eacl)
        records = re.split(r'"records": \[', res_json[0])
        input_records = ",\n" + records[1]

    myjson = """
{
  "body": {
    "eaclTable": {
      "containerID": {
        "value": \"""" +  cid + """"
      },
      "records": [
        {
          "operation": "GET",
          "action": \"""" +  action + """",
          "targets": [
            {
              "role": \"""" +  target_role + """"
            }
          ]
        },
        {
          "operation": "PUT",
          "action": \"""" +  action + """",
          "targets": [
            {
              "role": \"""" +  target_role + """"
            }
          ]
        },
        {
          "operation": "HEAD",
          "action": \"""" +  action + """",
          "targets": [
            {
              "role": \"""" +  target_role + """"
            }
          ]
        },
        {
          "operation": "DELETE",
          "action": \"""" +  action + """",
          "targets": [
            {
              "role": \"""" +  target_role + """"
            }
          ]
        },
        {
          "operation": "SEARCH",
          "action": \"""" +  action + """",
          "targets": [
            {
              "role": \"""" +  target_role + """"
            }
          ]
        },
        {
          "operation": "GETRANGE",
          "action": \"""" +  action + """",
          "targets": [
            {
              "role": \"""" +  target_role + """"
            }
          ]
        },
        {
          "operation": "GETRANGEHASH",
          "action": \"""" +  action + """",
          "targets": [
            {
              "role": \"""" +  target_role + """"
            }
          ]
        }""" + input_records + """
      ]
    },
    "lifetime": {
      "exp": \"""" + lifetime_exp + """",
      "nbf": "1",
      "iat": "0"
    }
  }
}
"""
    with open(file_name,'w') as out:
        out.write(myjson)
    logger.info("Output: %s" % myjson)

    # Sign bearer token
    Cmd = f'neofs-cli util sign bearer-token --from {file_name} --to {file_name} --key {private_key} --json'
    logger.info("Cmd: %s" % Cmd)

    try:
        complProc = subprocess.run(Cmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)
        output = complProc.stdout
        logger.info("Output: %s" % str(output))
    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))

    return file_name



@keyword('Form BearerToken file filter for all ops')
def form_bearertoken_file_filter_for_all_ops(file_name: str, private_key: str, cid: str, action: str, target_role: str, lifetime_exp: str, matchType: str, key: str, value: str):

    # SEARCH should be allowed without filters to use GET, HEAD, DELETE, and SEARCH? Need to clarify.

    eacl = get_eacl(private_key, cid)
    input_records = ""
    if eacl:
        res_json = re.split(r'[\s\n]+\][\s\n]+\}[\s\n]+Signature:', eacl)
        records = re.split(r'"records": \[', res_json[0])
        input_records = ",\n" + records[1]

    myjson = """
{
  "body": {
    "eaclTable": {
      "containerID": {
        "value": \"""" +  cid + """"
      },
      "records": [
        {
          "operation": "GET",
          "action": \"""" +  action + """",
          "filters": [
            {
              "headerType": "OBJECT",
              "matchType": \"""" +  matchType + """",
              "key": \"""" +  key + """",
              "value": \"""" +  value + """"
            }
          ],
          "targets": [
            {
              "role": \"""" +  target_role + """"
            }
          ]
        },
        {
          "operation": "PUT",
          "action": \"""" +  action + """",
          "filters": [
            {
              "headerType": "OBJECT",
              "matchType": \"""" +  matchType + """",
              "key": \"""" +  key + """",
              "value": \"""" +  value + """"
            }
          ],
          "targets": [
            {
              "role": \"""" +  target_role + """"
            }
          ]
        },
        {
          "operation": "HEAD",
          "action": \"""" +  action + """",
          "filters": [
            {
              "headerType": "OBJECT",
              "matchType": \"""" +  matchType + """",
              "key": \"""" +  key + """",
              "value": \"""" +  value + """"
            }
          ],
          "targets": [
            {
              "role": \"""" +  target_role + """"
            }
          ]
        },
        {
          "operation": "DELETE",
          "action": \"""" +  action + """",
          "filters": [
            {
              "headerType": "OBJECT",
              "matchType": \"""" +  matchType + """",
              "key": \"""" +  key + """",
              "value": \"""" +  value + """"
            }
          ],
          "targets": [
            {
              "role": \"""" +  target_role + """"
            }
          ]
        },
        {
          "operation": "SEARCH",
          "action": \"""" +  action + """",
          "targets": [
            {
              "role": \"""" +  target_role + """"
            }
          ]
        },
        {
          "operation": "GETRANGE",
          "action": \"""" +  action + """",
          "filters": [
            {
              "headerType": "OBJECT",
              "matchType": \"""" +  matchType + """",
              "key": \"""" +  key + """",
              "value": \"""" +  value + """"
            }
          ],
          "targets": [
            {
              "role": \"""" +  target_role + """"
            }
          ]
        },
        {
          "operation": "GETRANGEHASH",
          "action": \"""" +  action + """",
          "filters": [
            {
              "headerType": "OBJECT",
              "matchType": \"""" +  matchType + """",
              "key": \"""" +  key + """",
              "value": \"""" +  value + """"
            }
          ],
          "targets": [
            {
              "role": \"""" +  target_role + """"
            }
          ]
        }""" + input_records + """
      ]
    },
    "lifetime": {
      "exp": \"""" +  lifetime_exp + """",
      "nbf": "1",
      "iat": "0"
    }
  }
}
"""
    with open(file_name,'w') as out:
        out.write(myjson)
    logger.info("Output: %s" % myjson)

    # Sign bearer token
    Cmd = f'neofs-cli util sign bearer-token --from {file_name} --to {file_name} --key {private_key} --json'
    logger.info("Cmd: %s" % Cmd)

    try:
        complProc = subprocess.run(Cmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)
        output = complProc.stdout
        logger.info("Output: %s" % str(output))
    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))

    return file_name



@keyword('Form eACL json file')
def form_eacl_json_file(file_name: str, operation: str, action: str, matchType: str, key: str, value: str, target_role: str):

    myjson = """
{
  "records": [
    {
      "operation": \"""" +  operation + """",
      "action": \"""" +  action + """",
      "filters": [
         {
           "headerType": "OBJECT",
           "matchType": \"""" +  matchType + """",
           "key": \"""" +  key + """",
           "value": \"""" +  value + """"
         }
       ],
      "targets": [
        {
          "role": \"""" +  target_role + """"
        }
      ]
    }
  ]
}
"""
    with open(file_name,'w') as out:
        out.write(myjson)
    logger.info("Output: %s" % myjson)

    return file_name




@keyword('Get Range')
def get_range(private_key: str, cid: str, oid: str, range_file: str, bearer: str, range_cut: str):

    bearer_token = ""
    if bearer:
        bearer_token = f"--bearer {bearer}"

    Cmd = f'neofs-cli --rpc-endpoint {NEOFS_ENDPOINT} --key {private_key} object range --cid {cid} --oid {oid} {bearer_token} --range {range_cut} --file {range_file} '
    logger.info("Cmd: %s" % Cmd)

    try:
        complProc = subprocess.run(Cmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=150, shell=True)
        output = complProc.stdout
        logger.info("Output: %s" % str(output))
    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))


@keyword('Create container')
def create_container(private_key: str, basic_acl:str="", rule:str="REP 2 IN X CBF 1 SELECT 2 FROM * AS X"):

    if basic_acl != "":
        basic_acl = "--basic-acl " + basic_acl

    createContainerCmd = f'neofs-cli --rpc-endpoint {NEOFS_ENDPOINT} --key {private_key} container create --policy "{rule}" {basic_acl} --await'
    logger.info("Cmd: %s" % createContainerCmd)
    complProc = subprocess.run(createContainerCmd, check=True, universal_newlines=True,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=150, shell=True)
    output = complProc.stdout
    logger.info("Output: %s" % output)
    cid = _parse_cid(output)
    logger.info("Created container %s with rule '%s'" % (cid, rule))

    return cid


@keyword('Container Existing')
def container_existing(private_key: str, cid: str):
    Cmd = f'neofs-cli --rpc-endpoint {NEOFS_ENDPOINT} --key {private_key} container list'
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

    filename = str(uuid.uuid4())
    with open('%s'%filename, 'wb') as fout:
        fout.write(os.urandom(size))

    logger.info("Random binary file with size %s bytes has been generated." % str(size))
    return os.path.abspath(os.getcwd()) + '/' + filename


@keyword('Search object')
def search_object(private_key: str, cid: str, keys: str, bearer: str, filters: str, *expected_objects_list ):

    bearer_token = ""
    if bearer:
        bearer_token = f"--bearer {bearer}"


    if filters:
        filters = f"--filters {filters}"

    ObjectCmd = f'neofs-cli --rpc-endpoint {NEOFS_ENDPOINT} --key {private_key} object search {keys} --cid {cid} {bearer_token} {filters}'
    logger.info("Cmd: %s" % ObjectCmd)
    try:
        complProc = subprocess.run(ObjectCmd, check=True, universal_newlines=True,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)

        logger.info("Output: %s" % complProc.stdout)

        if expected_objects_list:
            found_objects = re.findall(r'(\w{43,44})', complProc.stdout)


            if sorted(found_objects) == sorted(expected_objects_list):
                logger.info("Found objects list '{}' is equal for expected list '{}'".format(found_objects, expected_objects_list))
            else:
                raise Exception("Found object list '{}' is not equal to expected list '{}'".format(found_objects, expected_objects_list))



    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))

'''
@keyword('Verify Head Tombstone')
def verify_head_tombstone(private_key: str, cid: str, oid: str):

    ObjectCmd = f'neofs-cli --rpc-endpoint {NEOFS_ENDPOINT} --key {private_key} object head --cid {cid} --oid {oid} --full-headers'
    logger.info("Cmd: %s" % ObjectCmd)
    try:
        complProc = subprocess.run(ObjectCmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)
        logger.info("Output: %s" % complProc.stdout)

        if re.search(r'Type=Tombstone\s+Value=MARKED', complProc.stdout):
            logger.info("Tombstone header 'Type=Tombstone Value=MARKED' was parsed from command output")
        else:
            raise Exception("Tombstone header 'Type=Tombstone Value=MARKED' was not found in the command output: \t%s" % (complProc.stdout))

    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))

@keyword('Verify linked objects')
def verify_linked_objects(private_key: bytes, cid: str, oid: str, payload_size: float):

    payload_size = int(float(payload_size))

    # Get linked objects from first
    postfix = f'object head --cid {cid} --oid {oid} --full-headers'
    output = _exec_cli_cmd(private_key, postfix)
    child_obj_list = []

    for m in re.finditer(r'Type=Child ID=([\w-]+)', output):
        child_obj_list.append(m.group(1))

    if not re.search(r'PayloadLength=0', output):
        raise Exception("Payload is not equal to zero in the parent object %s." % obj)

    if not child_obj_list:
        raise Exception("Child objects was not found.")
    else:
        logger.info("Child objects: %s" % child_obj_list)

    # HEAD and validate each child object:
    payload = 0
    parent_id = "00000000-0000-0000-0000-000000000000"
    first_obj = None
    child_obj_list_headers = {}

    for obj in child_obj_list:
        postfix = f'object head --cid {cid} --oid {obj} --full-headers'
        output = _exec_cli_cmd(private_key, postfix)
        child_obj_list_headers[obj] = output
        if re.search(r'Type=Previous ID=00000000-0000-0000-0000-000000000000', output):
            first_obj = obj
            logger.info("First child object %s has been found" % first_obj)

    if not first_obj:
        raise Exception("Can not find first object with zero Parent ID.")
    else:

        _check_linked_object(first_obj, child_obj_list_headers, payload_size, payload, parent_id)

    return child_obj_list_headers.keys()


def _check_linked_object(obj:str, child_obj_list_headers:dict, payload_size:int, payload:int, parent_id:str):

    output = child_obj_list_headers[obj]
    logger.info("Verify headers of the child object %s" % obj)

    if not re.search(r'Type=Previous ID=%s' % parent_id, output):
        raise Exception("Incorrect previos ID %s in the child object %s." % parent_id, obj)
    else:
        logger.info("Previous ID is equal for expected: %s" % parent_id)

    m = re.search(r'PayloadLength=(\d+)', output)
    if m.start() != m.end():
        payload += int(m.group(1))
    else:
        raise Exception("Can not get payload for the object %s." % obj)

    if payload > payload_size:
        raise Exception("Payload exceeds expected total payload %s." % payload_size)

    elif payload == payload_size:
        if not re.search(r'Type=Next ID=00000000-0000-0000-0000-000000000000', output):
            raise Exception("Incorrect previos ID in the last child object %s." % obj)
        else:
            logger.info("Next ID is correct for the final child object: %s" % obj)

    else:
        m = re.search(r'Type=Next ID=([\w-]+)', output)
        if m:
            # next object should be in the expected list
            logger.info(m.group(1))
            if m.group(1) not in child_obj_list_headers.keys():
                raise Exception(f'Next object {m.group(1)} is not in the expected list: {child_obj_list_headers.keys()}.')
            else:
                logger.info(f'Next object {m.group(1)} is in the expected list: {child_obj_list_headers.keys()}.')

            _check_linked_object(m.group(1), child_obj_list_headers, payload_size, payload, obj)

        else:
            raise Exception("Can not get Next object ID for the object %s." % obj)

'''


@keyword('Head object')
def head_object(private_key: str, cid: str, oid: str, bearer: str, user_headers:str=""):
    options = ""

    bearer_token = ""
    if bearer:
        bearer_token = f"--bearer {bearer}"

    ObjectCmd = f'neofs-cli --rpc-endpoint {NEOFS_ENDPOINT} --key {private_key} object head --cid {cid} --oid {oid} {bearer_token} {options}'
    logger.info("Cmd: %s" % ObjectCmd)
    try:
        complProc = subprocess.run(ObjectCmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)
        logger.info("Output: %s" % complProc.stdout)

        for key in user_headers.split(","):
        #    user_header = f'Key={key} Val={user_headers_dict[key]}'
            if re.search(r'(%s)' % key, complProc.stdout):
                logger.info("User header %s was parsed from command output" % key)
            else:
                raise Exception("User header %s was not found in the command output: \t%s" % (key, complProc.stdout))

        return complProc.stdout

    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))




@keyword('Parse Object System Header')
def parse_object_system_header(header: str):
    result_header = dict()

    #SystemHeader
    logger.info("Input: %s" % header)
    # ID
    m = re.search(r'ID: (\w+)', header)
    if m.start() != m.end(): # e.g., if match found something
        result_header['ID'] = m.group(1)
    else:
        raise Exception("no ID was parsed from object header: \t%s" % output)

    # CID
    m = re.search(r'CID: (\w+)', header)
    if m.start() != m.end(): # e.g., if match found something
        result_header['CID'] = m.group(1)
    else:
        raise Exception("no CID was parsed from object header: \t%s" % output)

    # Owner
    m = re.search(r'Owner: ([a-zA-Z0-9]+)', header)
    if m.start() != m.end(): # e.g., if match found something
        result_header['OwnerID'] = m.group(1)
    else:
        raise Exception("no OwnerID was parsed from object header: \t%s" % output)
    # PayloadLength
    m = re.search(r'Size: (\d+)', header)
    if m.start() != m.end(): # e.g., if match found something
        result_header['PayloadLength'] = m.group(1)
    else:
        raise Exception("no PayloadLength was parsed from object header: \t%s" % output)

    # CreatedAtUnixTime
    m = re.search(r'Timestamp=(\d+)', header)
    if m.start() != m.end(): # e.g., if match found something
        result_header['CreatedAtUnixTime'] = m.group(1)
    else:
        raise Exception("no CreatedAtUnixTime was parsed from object header: \t%s" % output)

    # CreatedAtEpoch
    m = re.search(r'CreatedAt: (\d+)', header)
    if m.start() != m.end(): # e.g., if match found something
        result_header['CreatedAtEpoch'] = m.group(1)
    else:
        raise Exception("no CreatedAtEpoch was parsed from object header: \t%s" % output)

    logger.info("Result: %s" % result_header)
    return result_header

@keyword('Delete object')
def delete_object(private_key: str, cid: str, oid: str, bearer: str):

    bearer_token = ""
    if bearer:
        bearer_token = f"--bearer {bearer}"

    ObjectCmd = f'neofs-cli --rpc-endpoint {NEOFS_ENDPOINT} --key {private_key} object delete --cid {cid} --oid {oid} {bearer_token}'
    try:
        complProc = subprocess.run(ObjectCmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)
        logger.info("Output: %s" % complProc.stdout)
    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))


@keyword('Get file hash')
def get_file_hash(filename):
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
def cleanup_file(*filename_list):

    for filename in filename_list:
        if os.path.isfile(filename):
            try:
                os.remove(filename)
            except OSError as e:
                raise Exception("Error: '%s' - %s." % (e.filename, e.strerror))
        else:
            logger.warn("Error: '%s' file not found" % filename)

        logger.info("File '%s' has been deleted." % filename)


@keyword('Put object to NeoFS')
def put_object(private_key: str, path: str, cid: str, bearer: str, user_headers: str, endpoint: str="" ):
    logger.info("Going to put the object")

    if not endpoint:
      endpoint = random.sample(_get_storage_nodes(private_key), 1)[0]

    if user_headers:
        user_headers = f"--attributes {user_headers}"

    if bearer:
        bearer = f"--bearer {bearer}"

    putObjectCmd = f'neofs-cli --rpc-endpoint {endpoint} --key {private_key} object put --file {path} --cid {cid} {bearer} {user_headers}'
    logger.info("Cmd: %s" % putObjectCmd)

    try:
        complProc = subprocess.run(putObjectCmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60, shell=True)
        logger.info("Output: %s" % complProc.stdout)
        oid = _parse_oid(complProc.stdout)
        return oid
    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))



@keyword('Get Range Hash')
def get_range_hash(private_key: str, cid: str, oid: str, bearer_token: str, range_cut: str):

    if bearer_token:
        bearer_token = f"--bearer {bearer}"

    ObjectCmd = f'neofs-cli --rpc-endpoint {NEOFS_ENDPOINT} --key {private_key} object hash --cid {cid} --oid {oid} --range {range_cut} {bearer_token}'

    logger.info("Cmd: %s" % ObjectCmd)
    try:
        complProc = subprocess.run(ObjectCmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60, shell=True)
        logger.info("Output: %s" % complProc.stdout)
    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))


@keyword('Get object from NeoFS') 
def get_object(private_key: str, cid: str, oid: str, bearer_token: str, read_object: str, endpoint: str="" ):
    # TODO: add object return instead of read_object (uuid)

    logger.info("Going to put the object")

    if not endpoint:
      endpoint = random.sample(_get_storage_nodes(private_key), 1)[0]

    
    if bearer_token:
        bearer_token = f"--bearer {bearer_token}"

    ObjectCmd = f'neofs-cli --rpc-endpoint {endpoint} --key {private_key} object get --cid {cid} --oid {oid} --file {read_object} {bearer_token}'

    logger.info("Cmd: %s" % ObjectCmd)
    try:
        complProc = subprocess.run(ObjectCmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60, shell=True)
        logger.info("Output: %s" % complProc.stdout)
    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))



def _exec_cli_cmd(private_key: bytes, postfix: str):

    # Get linked objects from first
    ObjectCmd = f'{CLI_PREFIX}neofs-cli --raw --host {NEOFS_ENDPOINT} --key {binascii.hexlify(private_key).decode()} {postfix}'
    logger.info("Cmd: %s" % ObjectCmd)
    try:
        complProc = subprocess.run(ObjectCmd, check=True, universal_newlines=True,
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

def _get_storage_nodes(private_key: bytes):
    storage_nodes = ['s01.neofs.devenv:8080', 's02.neofs.devenv:8080','s03.neofs.devenv:8080','s04.neofs.devenv:8080']
    #NetmapCmd = f'{CLI_PREFIX}neofs-cli --host {NEOFS_ENDPOINT} --key {binascii.hexlify(private_key).decode()} status netmap'
    #complProc = subprocess.run(NetmapCmd, check=True, universal_newlines=True,
    #        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)
    #output = complProc.stdout
    #logger.info("Netmap: %s" % output)
    #for m in re.finditer(r'"address":"/ip4/(\d+\.\d+\.\d+\.\d+)/tcp/(\d+)"', output):
    #    storage_nodes.append(m.group(1)+":"+m.group(2))

    #if not storage_nodes:
    #    raise Exception("Storage nodes was not found.")


    # Will be fixed when netmap will be added to cli

    #storage_nodes.append()
    logger.info("Storage nodes: %s" % storage_nodes)
    return storage_nodes


def _search_object(node:str, private_key: str, cid:str, oid: str):
    # --filters objectID={oid}
    if oid:
        oid_cmd = "--oid %s" % oid
    Cmd = f'{CLI_PREFIX}neofs-cli --rpc-endpoint {node} --key {private_key}  --ttl 1 object search --root --cid {cid} {oid_cmd}'

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

        elif ( re.search(r'timed out after 30 seconds', e.output) or re.search(r'no route to host', e.output) ):
            logger.warn("Node is unavailable")

        else:
            raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))



