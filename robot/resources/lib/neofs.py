#!/usr/bin/python3

import subprocess
import os
import re
import binascii
import uuid
import hashlib
from robot.api.deco import keyword
from robot.api import logger


ROBOT_AUTO_KEYWORDS = False

NEOFS_ENDPOINT = "192.168.123.71:8080"
CLI_PREFIX = "docker exec neofs-cli "

@keyword('Form Privkey from String')
def form_privkey_from_string(private_key: str):
    return bytes.fromhex(private_key) 


@keyword('Get nodes with object')
def get_nodes_with_object(private_key: bytes, cid, oid):
    storage_nodes = _get_storage_nodes(private_key)
    copies = 0

    nodes_list = []

    for node in storage_nodes:
        if re.search(r'(%s: %s)' % (cid, oid), _search_object(node, private_key, cid, oid)):
            nodes_list.append(node)

    logger.info("Nodes with object: %s" % nodes_list)


@keyword('Get nodes without object')
def get_nodes_without_object(private_key: bytes, cid, oid):
    storage_nodes = _get_storage_nodes(private_key)
    copies = 0

    nodes_list = []

    for node in storage_nodes:
        if not re.search(r'(%s: %s)' % (cid, oid), _search_object(node, private_key, cid, oid)):
            nodes_list.append(node)

    logger.info("Nodes with object: %s" % nodes_list)


@keyword('Validate storage policy for object')
def validate_storage_policy_for_object(private_key: bytes, expected_copies: int, cid, oid):
    storage_nodes = _get_storage_nodes(private_key)
    copies = 0
    for node in storage_nodes:
        if re.search(r'(%s: %s)' % (cid, oid), _search_object(node, private_key, cid, oid)):
            copies += 1

    logger.info("Copies: %s" % copies)
    
    if copies < expected_copies:
        raise Exception("Not enough object copies to match storage policyю Found: %s, expexted: %s." % (copies, expected_copies))



#docker exec neofs-cli neofs-cli --host 192.168.123.71:8080 --key 22b2f3faea9383e27262364c96d8e5ef7e893abf7a6ad7bf31ee1f2c2b3cfc42 
# object get-range --cid 4H9iChvzYdBg6qntfYUWGWCzsJFBDdo99KegefsD721Q --oid a101d078-b3d4-4325-8fe8-41dce6917097 0:10
#fead193c1f6f488255f7

@keyword('Get Range')
def get_range(private_key: bytes, cid: str, oid: str, range_cut: str):

    Cmd = f'{CLI_PREFIX}neofs-cli --host {NEOFS_ENDPOINT} --key {binascii.hexlify(private_key).decode()} object get-range --cid {cid} --oid {oid} {range_cut}'
    logger.info("Cmd: %s" % Cmd)
    complProc = subprocess.run(Cmd, check=True, universal_newlines=True,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=150, shell=True)
    output = complProc.stdout
    logger.info("Output: %s" % output)


@keyword('Create container')
def create_container(private_key: bytes, basic_acl:str=""):
    rule = "RF 2 SELECT 2 Node"
    if basic_acl != "":
        basic_acl = "--acl " + basic_acl

    createContainerCmd = f'{CLI_PREFIX}neofs-cli --host {NEOFS_ENDPOINT} --key {binascii.hexlify(private_key).decode()} container put --rule "{rule}" {basic_acl}'
    logger.info("Cmd: %s" % createContainerCmd)
    complProc = subprocess.run(createContainerCmd, check=True, universal_newlines=True,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=150, shell=True)
    output = complProc.stdout
    logger.info("Output: %s" % output)
    cid = _parse_cid(output)
    logger.info("Created container %s with rule '%s'" % (cid, rule))

    return cid


@keyword('Container Existing')
def container_existing(private_key: bytes, cid: str):
    Cmd = f'{CLI_PREFIX}neofs-cli --host {NEOFS_ENDPOINT} --key {binascii.hexlify(private_key).decode()} container list'
    logger.info("CMD: %s" % Cmd)
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
    return filename


@keyword('Search object')
def search_object(private_key: bytes, cid: str, keys: str, *expected_objects_list, **kwargs ):

    logger.info(expected_objects_list)
    logger.info(kwargs)
    option = ""

    if kwargs:
        for key, value in dict(kwargs).items():
            option = f'{option} {key} {value}'

    ObjectCmd = f'{CLI_PREFIX}neofs-cli --host {NEOFS_ENDPOINT} --key {binascii.hexlify(private_key).decode()} object search {keys} --cid {cid} {option}'
    logger.info("Cmd: %s" % ObjectCmd)
    try:
        complProc = subprocess.run(ObjectCmd, check=True, universal_newlines=True,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)

        logger.info("Output: %s" % complProc.stdout)

        if expected_objects_list is not None:
            found_objects = re.findall(r'%s: ([\-\w]+)' % cid, complProc.stdout)

            if sorted(found_objects) == sorted(expected_objects_list):
                logger.info("Found objects list '{}' is equal for expected list '{}'".format(found_objects, expected_objects_list))
            else:
                raise Exception("Found object list '{}' is not equal to expected list '{}'".format(found_objects, expected_objects_list))



    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))    


@keyword('Verify Head Tombstone')
def verify_head_tombstone(private_key: bytes, cid: str, oid: str):

    ObjectCmd = f'{CLI_PREFIX}neofs-cli --host {NEOFS_ENDPOINT} --key {binascii.hexlify(private_key).decode()} object head --cid {cid} --oid {oid} --full-headers'
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


@keyword('Head object')
def head_object(private_key: bytes, cid: str, oid: str, full_headers:bool=False, **user_headers_dict):
    options = ""
    if full_headers:
        options = "--full-headers"

    ObjectCmd = f'{CLI_PREFIX}neofs-cli --host {NEOFS_ENDPOINT} --key {binascii.hexlify(private_key).decode()} object head --cid {cid} --oid {oid} {options}'
    logger.info("Cmd: %s" % ObjectCmd)
    try:
        complProc = subprocess.run(ObjectCmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)
        logger.info("Output: %s" % complProc.stdout)

        for key in user_headers_dict:
            user_header = f'Key={key} Val={user_headers_dict[key]}'
            if re.search(r'(%s)' % user_header, complProc.stdout):
                logger.info("User header %s was parsed from command output" % user_header)
            else:
                raise Exception("User header %s was not found in the command output: \t%s" % (user_header, complProc.stdout))
   
    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))


@keyword('Delete object')
def delete_object(private_key: bytes, cid: str, oid: str):
    ObjectCmd = f'{CLI_PREFIX}neofs-cli --host {NEOFS_ENDPOINT} --key {binascii.hexlify(private_key).decode()} object delete --cid {cid} --oid {oid}'
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


@keyword('Create storage group')
def create_storage_group(private_key: bytes, cid: str, *objects_list):
    objects = ""

    for oid in objects_list:
        objects = f'{objects} --oid {oid}'

    ObjectCmd = f'{CLI_PREFIX}neofs-cli --host {NEOFS_ENDPOINT} --key {binascii.hexlify(private_key).decode()} sg put --cid {cid} {objects}'
    complProc = subprocess.run(ObjectCmd, check=True, universal_newlines=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)
    logger.info("Output: %s" % complProc.stdout)
    sgid = _parse_oid(complProc.stdout)
    return sgid


@keyword('Get storage group')
def get_storage_group(private_key: bytes, cid: str, sgid: str):
    ObjectCmd = f'{CLI_PREFIX}neofs-cli --host {NEOFS_ENDPOINT} --key {binascii.hexlify(private_key).decode()} sg get --cid {cid} --sgid {sgid}'
    logger.info("Cmd: %s" % ObjectCmd)
    try:
        complProc = subprocess.run(ObjectCmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)
        logger.info("Output: %s" % complProc.stdout)
    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))
        

@keyword('Cleanup File')
# remove temp files
def cleanup_file(filename: str):
    if os.path.isfile(filename):
        try:
            os.remove(filename)
        except OSError as e:  
            raise Exception("Error: '%s' - %s." % (e.filename, e.strerror))
    else:    
        raise Exception("Error: '%s' file not found" % filename)
   
    logger.info("File '%s' has been deleted." % filename)


@keyword('Put object to NeoFS')
def put_object(private_key: bytes, path: str, cid: str, **kwargs):
    logger.info("Going to put the object")
    user_headers = ""
    user_headers_dict = kwargs
    if kwargs:
        logger.info(kwargs)
        for key, value in dict(kwargs).items():
            user_headers = f'{user_headers} --user "{key}"="{value}"'


    # Put object to cli container
    putObjectCont = f'docker cp {path} neofs-cli:/ '
    logger.info("Cmd: %s" % putObjectCont)
    complProc = subprocess.run(putObjectCont, check=True, universal_newlines=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60, shell=True)


    putObjectCmd = f'{CLI_PREFIX}neofs-cli --host {NEOFS_ENDPOINT} --key {binascii.hexlify(private_key).decode()} object put --verify --file {path}  --cid {cid} {user_headers}'
    logger.info("Cmd: %s" % putObjectCmd)
    complProc = subprocess.run(putObjectCmd, check=True, universal_newlines=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60, shell=True)
    logger.info("Output: %s" % complProc.stdout)
    oid = _parse_oid(complProc.stdout)
    return oid


@keyword('Get object from NeoFS')
def get_object(private_key: bytes, cid: str, oid: str, read_object: str):
    ObjectCmd = f'{CLI_PREFIX}neofs-cli --host {NEOFS_ENDPOINT} --key {binascii.hexlify(private_key).decode()} object get --cid {cid} --oid {oid} --file {read_object}'

    logger.info("Cmd: %s" % ObjectCmd)
    try:
        complProc = subprocess.run(ObjectCmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60, shell=True)
        logger.info("Output: %s" % complProc.stdout)
    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))

    # Get object from cli container
    getObjectCont = f'docker cp neofs-cli:/{read_object} . '
    logger.info("Cmd: %s" % getObjectCont)
    complProc = subprocess.run(getObjectCont, check=True, universal_newlines=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60, shell=True)


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
    m = re.search(r'Success! Container <(([a-zA-Z0-9])+)> created', output)
    if m.start() != m.end(): # e.g., if match found something
        cid = m.group(1)
    else:
        raise Exception("no CID was parsed from command output: \t%s" % (output))

    return cid

def _get_storage_nodes(private_key: bytes):
    storage_nodes = []
    NetmapCmd = f'{CLI_PREFIX}neofs-cli --host {NEOFS_ENDPOINT} --key {binascii.hexlify(private_key).decode()} status netmap'
    complProc = subprocess.run(NetmapCmd, check=True, universal_newlines=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)
    output = complProc.stdout
    logger.info("Netmap: %s" % output)
    for m in re.finditer(r'"address":"/ip4/(\d+\.\d+\.\d+\.\d+)/tcp/(\d+)"', output):
        storage_nodes.append(m.group(1)+":"+m.group(2))
    
    if not storage_nodes:
        raise Exception("Storage nodes was not found.")

    logger.info("Storage nodes: %s" % storage_nodes)
    return storage_nodes


def _search_object(node:str, private_key: bytes, cid:str, oid: str):
    Cmd = f'{CLI_PREFIX}neofs-cli --host {node}  --ttl 1 --key {binascii.hexlify(private_key).decode()} object search --root --cid {cid} ID {oid}'
    complProc = subprocess.run(Cmd, check=True, universal_newlines=True,
    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)
    logger.info(Cmd)
    logger.info("Output:")
    logger.info(complProc.stdout)

    return complProc.stdout