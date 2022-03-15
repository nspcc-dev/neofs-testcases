#!/usr/bin/python3

import base64
from datetime import datetime
import hashlib
import json
import os
import re
import random
import uuid
import docker
import base58

from neo3 import wallet
from common import *
from robot.api.deco import keyword
from robot.api import logger

from cli_helpers import _run_with_passwd, _cmd_run
import neofs_verbs
import json_transformers

ROBOT_AUTO_KEYWORDS = False

# path to neofs-cli executable
NEOFS_CLI_EXEC = os.getenv('NEOFS_CLI_EXEC', 'neofs-cli')


# TODO: move to neofs-keywords
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
    copies = 0

    nodes_list = []

    for node in NEOFS_NETMAP:
        search_res = _search_object(node, private_key, cid, oid)
        if search_res:
            if oid in search_res:
                nodes_list.append(node)

    logger.info(f"Nodes with object: {nodes_list}")
    return nodes_list


@keyword('Get nodes without object')
def get_nodes_without_object(private_key: str, cid: str, oid: str):
    copies = 0

    nodes_list = []

    for node in NEOFS_NETMAP:
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
    storage_nodes = storage_nodes if len(storage_nodes) != 0 else NEOFS_NETMAP
    copies = 0
    found_nodes = []
    oid = oid.strip()

    for node in storage_nodes:
        search_res = _search_object(node, private_key, cid, oid)
        if search_res:
            if oid in search_res:
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



@keyword('Create container')
def create_container(private_key: str, basic_acl:str, rule:str, user_headers: str='', session: str=''):
    if rule == "":
        logger.error("Cannot create container with empty placement rule")

    if basic_acl:
        basic_acl = f"--basic-acl {basic_acl}"
    if user_headers:
        user_headers = f"--attributes {user_headers}"
    if session:
        session = f"--session {session}"

    createContainerCmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wallet {private_key} '
        f'container create --policy "{rule}" {basic_acl} {user_headers} {session} --await'
    )
    logger.info(f"Cmd: {createContainerCmd}")
    output = _cmd_run(createContainerCmd)
    cid = _parse_cid(output)
    logger.info(f"Created container {cid} with rule {rule}")

    return cid

@keyword('Container List')
def container_list(private_key: str):
    Cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wallet {private_key} '
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
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wallet {private_key} '
        f'container list'
    )
    logger.info(f"Cmd: {Cmd}")
    output = _cmd_run(Cmd)

    _find_cid(output, cid)
    return



@keyword('Get Split objects')
def get_component_objects(private_key: str, cid: str, oid: str):
    logger.info("Collect Split objects list from Linked object.")
    split_id = ""
    for node in NEOFS_NETMAP:
        try:
            parsed_header_virtual = neofs_verbs.head_object(private_key, cid, oid, options=' --ttl 1',
                                endpoint=node, is_raw=True)

            if 'link' in parsed_header_virtual.keys():
                return _collect_split_objects_from_header(private_key, cid, parsed_header_virtual)

            elif 'split' in parsed_header_virtual['header'].keys():
                logger.info(f"parsed_header_virtual: !@ {parsed_header_virtual}" )
                split_id = parsed_header_virtual['header']['splitID']

        except:
            logger.warn("Linking object has not been found.")

    # Get all existing objects
    full_obj_list = neofs_verbs.search_object(private_key, cid, None, None, None, None, '--phy')

    # Search expected Linking object
    for targer_oid in full_obj_list:
        header_parsed = neofs_verbs.head_object(private_key, cid, targer_oid, is_raw=True)
        if header_parsed['header']['split']:
            if header_parsed['header']['split']['splitID'] == split_id and 'children' in header_parsed['header']['split'].keys():
                logger.info("Linking object has been found in additional check (head of all objects).")
                return _collect_split_objects_from_header(private_key, cid, parsed_header_virtual)


    raise Exception("Linking object is not found at all - all existed objects have been headed.")

def _collect_split_objects_from_header(private_key, cid, parsed_header):
    header_link_parsed = neofs_verbs.head_object(private_key, cid, parsed_header['link'])
    return header_link_parsed['header']['split']['children']


@keyword('Verify Split Chain')
def verify_split_chain(wif: str, cid: str, oid: str):

    header_virtual_parsed = dict()
    header_last_parsed = dict()

    marker_last_obj = 0
    marker_link_obj = 0

    final_verif_data = dict()

    # Get Latest object
    logger.info("Collect Split objects information and verify chain of the objects.")
    for node in NEOFS_NETMAP:
        try:
            parsed_header_virtual = neofs_verbs.head_object(wif, cid, oid, options=' --ttl 1',
                            endpoint=node, is_raw=True)
            logger.info(f"header {parsed_header_virtual}")

            if 'lastPart' in parsed_header_virtual.keys():
                header_last_parsed = neofs_verbs.head_object(wif, cid,
                                    parsed_header_virtual['lastPart'],
                                    is_raw=True)
                marker_last_obj = 1

                # Recursive chain validation up to the first object
                final_verif_data = _verify_child_link(wif, cid, oid, header_last_parsed['header'], final_verif_data)
                break
            logger.info(f"Found Split Object with header:\n\t{parsed_header_virtual}")
            logger.info("Continue to search Last Split Object")

        except Exception as exc:
            logger.info(f"Failed while collectiong Split Objects: {exc}")
            continue

    if marker_last_obj == 0:
        raise Exception("Last object has not been found")

    # Get Linking object
    logger.info("Compare Split objects result information with Linking object.")
    for node in NEOFS_NETMAP:
        try:
            parsed_header_virtual = neofs_verbs.head_object(wif, cid, oid, options=' --ttl 1',
                            endpoint=node, is_raw=True)
            if 'link' in parsed_header_virtual.keys():
                header_link_parsed = neofs_verbs.head_object(wif, cid,
                                parsed_header_virtual['link'],
                                is_raw=True)
                marker_link_obj = 1

                reversed_list = final_verif_data['header']['split']['children'][::-1]

                if header_link_parsed['header']['split']['children'] == reversed_list:
                    logger.info(f"Split objects list from Linked Object is equal to expected "
                                f"{', '.join(header_link_parsed['header']['split']['children'])}")
                else:
                    raise Exception(f"Split objects list from Linking Object "
                                    f"({', '.join(header_link_parsed['header']['split']['children'])}) "
                                    f"is not equal to expected ({', '.join(reversed_list)})")

                if int(header_link_parsed['header']['payloadLength']) == 0:
                    logger.info("Linking object Payload is equal to expected - zero size.")
                else:
                    raise Exception("Linking object Payload is not equal to expected. Should be zero.")

                if header_link_parsed['header']['objectType'] == 'REGULAR':
                    logger.info("Linking Object Type is 'regular' as expected.")
                else:
                    raise Exception("Object Type is not 'regular'.")

                if header_link_parsed['header']['split']['children'] == final_verif_data['split']['children']:
                    logger.info(f"Linking Object Split ID is equal to expected {final_verif_data['split']['children']}.")
                else:
                    raise Exception(f"Split ID from Linking Object ({header_link_parsed['header']['split']['children']}) "
                                    f"is not equal to expected ({final_verif_data['split']['children']})")

                break
            logger.info(f"Found Linking Object with header:\n\t{parsed_header_virtual}")
            logger.info("Continue to search Linking Object")
        except RuntimeError as e:
            logger.info(f"Failed while collecting Split Object: {e}")
            continue

    if marker_link_obj == 0:
        raise Exception("Linked object has not been found")


    logger.info("Compare Split objects result information with Virtual object.")

    header_virtual_parsed = neofs_verbs.head_object(wif, cid, oid)

    if int(header_virtual_parsed['header']['payloadLength']) == int(final_verif_data['payloadLength']):
        logger.info(f"Split objects PayloadLength are equal to Virtual Object Payload "
                    f"{header_virtual_parsed['header']['payloadLength']}")
    else:
        raise Exception(f"Split objects PayloadLength from Virtual Object "
                        f"({header_virtual_parsed['header']['payloadLength']}) is not equal "
                        f"to expected ({final_verif_data['payloadLength']})")

    if header_link_parsed['header']['objectType'] == 'REGULAR':
        logger.info("Virtual Object Type is 'regular' as expected.")
    else:
        raise Exception("Object Type is not 'regular'.")


def _verify_child_link(wif: str, cid: str, oid: str, header_last_parsed: dict, final_verif_data: dict):

    if 'payloadLength' in final_verif_data.keys():
            final_verif_data['payloadLength'] = int(final_verif_data['payloadLength']) + int(header_last_parsed['payloadLength'])
    else:
        final_verif_data['payloadLength'] = int(header_last_parsed['payloadLength'])

    if header_last_parsed['objectType'] != 'REGULAR':
        raise Exception("Object Type is not 'regular'.")

    if 'split' in final_verif_data.keys():
        if final_verif_data['split']['splitID'] != header_last_parsed['split']['splitID']:
             raise Exception(f"Object Split ID ({header_last_parsed['split']['splitID']}) "
                            f"is not expected ({final_verif_data['split']['splitID']}).")
    else:
        final_verif_data['split']['splitID'] = header_last_parsed['split']['splitID']

    if 'children' in final_verif_data['split'].keys():
        final_verif_data['split']['children'].append(header_last_parsed['split']['children'])
    else:
        final_verif_data['split']['children'] = []
        final_verif_data['split']['children'].append(header_last_parsed['split']['children'])

    if 'previous' in header_last_parsed['split'].keys():
        parsed_header_virtual = neofs_verbs.head_object(wif, cid, header_last_parsed['split']['previous'], is_raw=True)

        final_verif_data = _verify_child_link(wif, cid, oid, parsed_header_virtual, final_verif_data)
    else:
        logger.info("Chain of the objects has been parsed from the last object ot the first.")

    return final_verif_data


@keyword('Verify Head Tombstone')
def verify_head_tombstone(private_key: str, cid: str, oid_ts: str, oid: str, addr: str):
    # TODO: replace with HEAD from neofs_verbs.py
    object_cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wallet {private_key} '
        f'object head --cid {cid} --oid {oid_ts} --json'
    )
    logger.info(f"Cmd: {object_cmd}")
    output = _cmd_run(object_cmd)
    full_headers = json.loads(output)
    logger.info(f"Output: {full_headers}")

    # Header verification
    header_cid = full_headers["header"]["containerID"]["value"]
    if (json_transformers.json_reencode(header_cid) == cid):
        logger.info(f"Header CID is expected: {cid} ({header_cid} in the output)")
    else:
        raise Exception("Header CID is not expected.")

    header_owner = full_headers["header"]["ownerID"]["value"]
    if (json_transformers.json_reencode(header_owner) == addr):
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
    if (json_transformers.json_reencode(header_session_cid) == cid):
        logger.info(f"Header ownerID is expected: {addr} ({header_session_cid} in the output)")
    else:
        raise Exception("Header Session CID is not expected.")

    header_session_oid = full_headers["header"]["sessionToken"]["body"]["object"]["address"]["objectID"]["value"]
    if (json_transformers.json_reencode(header_session_oid) == oid):
        logger.info(f"Header Session OID (deleted object) is expected: {oid} ({header_session_oid} in the output)")
    else:
        raise Exception("Header Session OID (deleted object) is not expected.")


@keyword('Get container attributes')
def get_container_attributes(private_key: str, cid: str, endpoint: str="", json_output: bool = False):

    if endpoint == "":
        endpoint = NEOFS_ENDPOINT

    container_cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {endpoint} --wallet {private_key} '
        f'--cid {cid} container get {"--json" if json_output else ""}'
    )
    logger.info(f"Cmd: {container_cmd}")
    output = _cmd_run(container_cmd)
    return output


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



@keyword('Delete Container')
# TODO: make the error message about a non-found container more user-friendly https://github.com/nspcc-dev/neofs-contract/issues/121
def delete_container(cid: str, private_key: str):

    deleteContainerCmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wallet {private_key} '
        f'container delete --cid {cid}'
    )
    logger.info(f"Cmd: {deleteContainerCmd}")
    _cmd_run(deleteContainerCmd)


@keyword('Get file hash')
def get_file_hash(filename : str):
    file_hash = _get_file_hash(filename)
    return file_hash


@keyword('Get control endpoint with wif')
def get_control_endpoint_with_wif(endpoint_number: str = ''):
    if endpoint_number == '':
        netmap = []
        for key in NEOFS_NETMAP_DICT.keys():
            netmap.append(key)
        endpoint_num = random.sample(netmap, 1)[0]
        logger.info(f'Random node chosen: {endpoint_num}')
    else:
        endpoint_num = endpoint_number

    endpoint_values = NEOFS_NETMAP_DICT[f'{endpoint_num}']
    endpoint_control = endpoint_values['control']
    wif = endpoint_values['wif']

    return endpoint_num, endpoint_control, wif

@keyword('Get Locode')
def get_locode():
    endpoint_values = random.choice(list(NEOFS_NETMAP_DICT.values()))
    locode = endpoint_values['UN-LOCODE']
    logger.info(f'Random locode chosen: {locode}')

    return locode


@keyword('Get Nodes Log Latest Timestamp')
def get_logs_latest_timestamp():
    """
    Keyword return:
    nodes_logs_time -- structure (dict) of nodes container name (key) and latest logs timestamp (value)
    """
    client_api = docker.APIClient()

    nodes_logs_time = dict()

    for node in NEOFS_NETMAP:
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





@keyword('Put Storagegroup')
def put_storagegroup(private_key: str, cid: str, bearer_token: str="", *oid_list):

    cmd_oid_line = ",".join(oid_list)

    if bearer_token:
        bearer_token = f"--bearer {bearer_token}"

    object_cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wallet {private_key} storagegroup '
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
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wallet {private_key} '
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

    object_cmd = f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wallet {private_key} storagegroup get --cid {cid} --id {oid} {bearer_token}'
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
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} --wallet {private_key} storagegroup '
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

@keyword('Generate Session Token')
def generate_session_token(owner: str, pub_key: str, cid: str = "", wildcard: bool = False) -> str:

    file_path = f"{os.getcwd()}/{ASSETS_DIR}/{str(uuid.uuid4())}"

    owner_64 = base64.b64encode(base58.b58decode(owner)).decode('utf-8')
    cid_64 = base64.b64encode(cid.encode('utf-8')).decode('utf-8')
    pub_key_64 = base64.b64encode(bytes.fromhex(pub_key)).decode('utf-8')
    id_64 = base64.b64encode(uuid.uuid4().bytes).decode('utf-8')

    session_token = {
                    "body":{
                        "id":f"{id_64}",
                        "ownerID":{
                            "value":f"{owner_64}"
                        },
                        "lifetime":{
                            "exp":"100000000",
                            "nbf":"0",
                            "iat":"0"
                        },
                        "sessionKey":f"{pub_key_64}",
                        "container":{
                            "verb":"PUT",
                            "wildcard": wildcard,
                            **({ "containerID":{"value":f"{cid_64}"} } if not wildcard else {})
                        }
                    }
                }

    logger.info(f"Got this Session Token: {session_token}")

    with open(file_path, 'w', encoding='utf-8') as session_token_file:
        json.dump(session_token, session_token_file, ensure_ascii=False, indent=4)

    return file_path


@keyword ('Sign Session Token')
def sign_session_token(session_token: str, wallet: str, to_file: str=''):
    if to_file:
        to_file = f'--to {to_file}'
    cmd = (
        f'{NEOFS_CLI_EXEC} util sign session-token --from {session_token} '
        f'-w {wallet} {to_file}'
    )
    logger.info(f"cmd: {cmd}")
    _run_with_passwd(cmd)


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


def _search_object(node:str, private_key: str, cid:str, oid: str):
    Cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {node} --wallet {private_key} --ttl 1 '
        f'object search --root --cid {cid} --oid {oid}'
    )

    output = _cmd_run(Cmd)
    return output
