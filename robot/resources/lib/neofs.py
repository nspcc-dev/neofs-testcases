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


# docker exec neofs-cli neofs-cli --host 192.168.123.71:8080 --key 1ed43848107fd2d513c38ebfba3bb8c33d5abd2b6a99fafb09d07a30191989af container set-eacl --cid DNG1DCV3PTfxuYCLdbdMpRmrumfvacyWmyqLzNrV1koi --eacl 0a4b080210021a1e080310011a0a686561646572206b6579220c6865616465722076616c7565222508031221031a6c6fbbdf02ca351745fa86b9ba5a9452d785ac4f7fc2b7548ca2a46c4fcf4a
# Updating ACL rules of container...

@keyword('Get eACL')
def get_eacl(private_key: bytes, cid: str):

    Cmd = f'{CLI_PREFIX}neofs-cli --host {NEOFS_ENDPOINT} --key {binascii.hexlify(private_key).decode()} container get-eacl --cid {cid}'
    logger.info("Cmd: %s" % Cmd)
    complProc = subprocess.run(Cmd, check=True, universal_newlines=True,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=150, shell=True)
    output = complProc.stdout
    logger.info("Output: %s" % output)



@keyword('Convert Str to Hex Str with Len')
def conver_str_to_hex(string_convert: str):
    converted = binascii.hexlify(bytes(string_convert, encoding= 'utf-8')).decode("utf-8")
    prev_len_2 = '{:04x}'.format(int(len(converted)/2))

    return str(prev_len_2)+str(converted)


@keyword('Set custom eACL')
def set_custom_eacl(private_key: bytes, cid: str, eacl_prefix: str, eacl_slice: str, eacl_postfix: str):
   
   logger.info(str(eacl_prefix))
   logger.info(str(eacl_slice))
   logger.info(str(eacl_postfix))

   eacl = str(eacl_prefix) + str(eacl_slice) + str(eacl_postfix)
   logger.info("Custom eACL: %s" % eacl)

   set_eacl(private_key, cid, eacl)
   return



@keyword('Set eACL')
def set_eacl(private_key: bytes, cid: str, eacl: str):

    Cmd = f'{CLI_PREFIX}neofs-cli --host {NEOFS_ENDPOINT} --key {binascii.hexlify(private_key).decode()} container set-eacl --cid {cid} --eacl {eacl}'
    logger.info("Cmd: %s" % Cmd)
    complProc = subprocess.run(Cmd, check=True, universal_newlines=True,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=150, shell=True)
    output = complProc.stdout
    logger.info("Output: %s" % output)



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
   
        return complProc.stdout

    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))

    


@keyword('Parse Object System Header')
def parse_object_system_header(header: str):
    result_header = dict()

    #SystemHeader
    logger.info("Input: %s" % header)
    # ID
    m = re.search(r'- ID=([a-zA-Z0-9-]+)', header)
    if m.start() != m.end(): # e.g., if match found something
        result_header['ID'] = m.group(1)
    else:
        raise Exception("no ID was parsed from object header: \t%s" % output)

    # CID
    m = re.search(r'- CID=([a-zA-Z0-9]+)', header)
    if m.start() != m.end(): # e.g., if match found something
        result_header['CID'] = m.group(1)
    else:
        raise Exception("no CID was parsed from object header: \t%s" % output)

    # Owner
    m = re.search(r'- OwnerID=([a-zA-Z0-9]+)', header)
    if m.start() != m.end(): # e.g., if match found something
        result_header['OwnerID'] = m.group(1)
    else:
        raise Exception("no OwnerID was parsed from object header: \t%s" % output)
    
    # Version
    m = re.search(r'- Version=(\d+)', header)
    if m.start() != m.end(): # e.g., if match found something
        result_header['Version'] = m.group(1)
    else:
        raise Exception("no Version was parsed from object header: \t%s" % output)


    # PayloadLength
    m = re.search(r'- PayloadLength=(\d+)', header)
    if m.start() != m.end(): # e.g., if match found something
        result_header['PayloadLength'] = m.group(1)
    else:
        raise Exception("no PayloadLength was parsed from object header: \t%s" % output)


 
    # CreatedAtUnixTime
    m = re.search(r'- CreatedAt={UnixTime=(\d+)', header)
    if m.start() != m.end(): # e.g., if match found something
        result_header['CreatedAtUnixTime'] = m.group(1)
    else:
        raise Exception("no CreatedAtUnixTime was parsed from object header: \t%s" % output)

    # CreatedAtEpoch
    m = re.search(r'- CreatedAt={UnixTime=\d+ Epoch=(\d+)', header)
    if m.start() != m.end(): # e.g., if match found something
        result_header['CreatedAtEpoch'] = m.group(1)
    else:
        raise Exception("no CreatedAtEpoch was parsed from object header: \t%s" % output)

    logger.info("Result: %s" % result_header)
    return result_header

#	SystemHeader:
#		- ID=c9fdc3e8-6576-4822-9bc4-2a0addcbf105
#		- CID=42n81QNr7o513t2pTGuzM2PPFiHLhJ1MeSCJzizQW1wP
#		- OwnerID=ANwbVH8nyWfTg7G6L9uzZxfXhKUhdjTYDa
#		- Version=1
#		- PayloadLength=1024
#		- CreatedAt={UnixTime=1597330026 Epoch=2427}


@keyword('Parse Object Extended Header')
def parse_object_extended_header(header: str):
    result_header = dict()

 
    pattern = re.compile(r'- Type=(\w+)\n.+Value=(.+)\n')
    # key in dict.keys()

    for (f_type, f_val) in re.findall(pattern, header):
        logger.info("found: %s - %s" % (f_type, f_val))
        if f_type not in result_header.keys():
            result_header[f_type] = []
        
        # if {} -> dict -> if re.search(r'(%s)' % cid, output):
        result_header[f_type].append(f_val)

    logger.info("Result: %s" % result_header)
    return result_header
#	ExtendedHeaders:
#		- Type=UserHeader
#		  Value={Key=key1 Val=1}
#		- Type=UserHeader
#		  Value={Key=key2 Val='abc1'}
#		- Type=Token
#		  Value={ID=6143e50f-5dbf-4964-ba16-266517e4fe9a Verb=Put}
#		- Type=HomoHash
#		  Value=4c3304688e23b884f29a3e50cb65e067357d074f52e1e634a940a7488f40a3f53ffb0cb94d4b9c619432307fa615eb076d0c3d153acdd77835acac0553992238
#		- Type=PayloadChecksum
#		  Value=776bc1c03d2c72885c4976b000e2483df57275964308cc67eb36a829cad9a2c3
#		- Type=Integrity
#		  Value={Checksum=45859b067c6525b6f9fa78b9764ceca0a0eeb506cefd71c374aabd4cfd773430 Signature=04e80f81919fa14879b04fcad0fab411ebb0b7c38f00f030c98a4813ae402300b79b666c705317b358a17963d50ee5dceab4f6f3599e54da210b860df2f8b2a63c}


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