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

if os.getenv('ROBOT_PROFILE') == 'selectel_smoke':
    from selectelcdn_smoke_vars import (NEOGO_CLI_PREFIX, NEO_MAINNET_ENDPOINT,
    NEOFS_NEO_API_ENDPOINT, NEOFS_ENDPOINT, NEOFS_NETMAP)
else:
    from neofs_int_vars import (NEOGO_CLI_PREFIX, NEO_MAINNET_ENDPOINT,
    NEOFS_NEO_API_ENDPOINT, NEOFS_ENDPOINT, NEOFS_NETMAP)

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
    
    cid_base58_b = base58.b58decode(cid)
    cid_base64 = base64.b64encode(cid_base58_b).decode("utf-8") 

    if eacl:
        res_json = re.split(r'[\s\n]+\][\s\n]+\}[\s\n]+Signature:', eacl)
        records = re.split(r'"records": \[', res_json[0])
        input_records = ",\n" + records[1]

    myjson = """
{
  "body": {
    "eaclTable": {
      "containerID": {
        "value": \"""" +  str(cid_base64) + """"
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

    cid_base58_b = base58.b58decode(cid)
    cid_base64 = base64.b64encode(cid_base58_b).decode("utf-8") 

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
        "value": \"""" +  str(cid_base64) + """"
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
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=300, shell=True)
    output = complProc.stdout
    logger.info("Output: %s" % output)
    cid = _parse_cid(output)
    logger.info("Created container %s with rule '%s'" % (cid, rule))

    return cid


@keyword('Container List')
def container_list(private_key: str):
    Cmd = f'neofs-cli --rpc-endpoint {NEOFS_ENDPOINT} --key {private_key} container list'
    logger.info("Cmd: %s" % Cmd)
    complProc = subprocess.run(Cmd, check=True, universal_newlines=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)
    logger.info("Output: %s" % complProc.stdout)

    container_list = re.findall(r'(\w{43,44})', complProc.stdout)
    
    logger.info("Containers list: %s" % container_list)

    return container_list


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

        found_objects = re.findall(r'(\w{43,44})', complProc.stdout)

        if expected_objects_list:    
            if sorted(found_objects) == sorted(expected_objects_list):
                logger.info("Found objects list '{}' is equal for expected list '{}'".format(found_objects, expected_objects_list))
            else:
                raise Exception("Found object list '{}' is not equal to expected list '{}'".format(found_objects, expected_objects_list))
            
        return found_objects

    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))

    
@keyword('Verify Split Chain')
def verify_split_chain(private_key: str, cid: str, oid: str):

    header_virtual_parsed = dict()
    header_last_parsed = dict()

    marker_last_obj = 0
    marker_link_obj = 0

    final_verif_data = dict()

    # Get Latest object
    logger.info("Collect Split objects information and verify chain of the objects.")
    nodes = _get_storage_nodes(private_key)
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


@keyword('Verify Head Tombstone')
def verify_head_tombstone(private_key: str, cid: str, oid_ts: str, oid: str, addr: str):

    ObjectCmd = f'neofs-cli --rpc-endpoint {NEOFS_ENDPOINT} --key {private_key} object head --cid {cid} --oid {oid_ts} --json'
    logger.info("Cmd: %s" % ObjectCmd)
   
    try:
        complProc = subprocess.run(ObjectCmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)
        

        full_headers = json.loads(complProc.stdout)
        logger.info("Output: %s" % full_headers)

        # Header verification
        # TODO: add try or exist pre-check
        header_cid = full_headers["header"]["containerID"]["value"]
        if (base58.b58encode(base64.b64decode(header_cid)).decode("utf-8") == cid):
            logger.info("Header CID is expected: %s (%s in the output)" % (cid, header_cid))
        else:
            raise Exception("Header CID is not expected.")

        header_owner = full_headers["header"]["ownerID"]["value"]
        if (base58.b58encode(base64.b64decode(header_owner)).decode("utf-8") == addr):
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
        if (base58.b58encode(base64.b64decode(header_session_cid)).decode("utf-8") == cid):
            logger.info("Header ownerID is expected: %s (%s in the output)" % (addr, header_session_cid))
        else:
            raise Exception("Header Session CID is not expected.")

        header_session_oid = full_headers["header"]["sessionToken"]["body"]["object"]["address"]["objectID"]["value"]
        if (base58.b58encode(base64.b64decode(header_session_oid)).decode("utf-8") == oid):
            logger.info("Header Session OID (deleted object) is expected: %s (%s in the output)" % (oid, header_session_oid))
        else:
            raise Exception("Header Session OID (deleted object) is not expected.")

    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))


@keyword('Head object')
def head_object(private_key: str, cid: str, oid: str, bearer_token: str="", user_headers:str="", keys:str="", endpoint: str="", ignore_failure: bool = False):
    options = ""

    if bearer_token:
        bearer_token = f"--bearer {bearer_token}"

    if endpoint == "":
        endpoint = NEOFS_ENDPOINT

    ObjectCmd = f'neofs-cli --rpc-endpoint {endpoint} --key {private_key} object head --cid {cid} --oid {oid} {bearer_token} {keys}'
    logger.info("Cmd: %s" % ObjectCmd)
    try:
        complProc = subprocess.run(ObjectCmd, check=True, universal_newlines=True,
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
    # Header - Optional attributes
    
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
    
    # CreatedAtEpoch
    m = re.search(r'CreatedAt: (\d+)', header)
    if m.start() != m.end(): # e.g., if match found something
        result_header['CreatedAtEpoch'] = m.group(1)
    else:
        raise Exception("no CreatedAtEpoch was parsed from object header: \t%s" % output)

    # PayloadLength
    m = re.search(r'Size: (\d+)', header)
    if m.start() != m.end(): # e.g., if match found something
        result_header['PayloadLength'] = m.group(1)
    else:
        raise Exception("no PayloadLength was parsed from object header: \t%s" % output)

    # HomoHash
    m = re.search(r'HomoHash:\s+(\w+)', header)
    if m.start() != m.end(): # e.g., if match found something
        result_header['HomoHash'] = m.group(1)
    else:
        raise Exception("no HomoHash was parsed from object header: \t%s" % output)

    # Checksum
    m = re.search(r'Checksum:\s+(\w+)', header)
    if m.start() != m.end(): # e.g., if match found something
        result_header['Checksum'] = m.group(1)
    else:
        raise Exception("no Checksum was parsed from object header: \t%s" % output)

    # Type
    m = re.search(r'Type:\s+(\w+)', header)
    if m.start() != m.end(): # e.g., if match found something
        result_header['Type'] = m.group(1)
    else:
        raise Exception("no Type was parsed from object header: \t%s" % output)


    # Header - Optional attributes
    m = re.search(r'Split ID:\s+([\w-]+)', header)
    if m != None:
        if m.start() != m.end(): # e.g., if match found something
            result_header['Split ID'] = m.group(1)

    m = re.search(r'Split PreviousID:\s+(\w+)', header)
    if m != None:
        if m.start() != m.end(): # e.g., if match found something
            result_header['Split PreviousID'] = m.group(1)

    m = re.search(r'Split ParentID:\s+(\w+)', header)
    if m != None:
        if m.start() != m.end(): # e.g., if match found something
            result_header['Split ParentID'] = m.group(1)

    # Split ChildID list
    found_objects = re.findall(r'Split ChildID:\s+(\w+)', header)
    if found_objects:
        result_header['Split ChildID'] = found_objects


    logger.info("Result: %s" % result_header)
    return result_header


@keyword('Delete object')
def delete_object(private_key: str, cid: str, oid: str, bearer: str):

    bearer_token = ""
    if bearer:
        bearer_token = f"--bearer {bearer}"

    ObjectCmd = f'neofs-cli --rpc-endpoint {NEOFS_ENDPOINT} --key {private_key} object delete --cid {cid} --oid {oid} {bearer_token}'
    logger.info("Cmd: %s" % ObjectCmd)
    try:
        complProc = subprocess.run(ObjectCmd, check=True, universal_newlines=True,
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
    #storage_nodes = ['s01.neofs.devenv:8080', 's02.neofs.devenv:8080','s03.neofs.devenv:8080','s04.neofs.devenv:8080']
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
    logger.info("Storage nodes: %s" % NEOFS_NETMAP)
    return NEOFS_NETMAP


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

        elif ( re.search(r'timed out after 30 seconds', e.output) or re.search(r'no route to host', e.output) or re.search(r'i/o timeout', e.output)):
            logger.warn("Node is unavailable")

        else:
            raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))



