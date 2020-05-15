#!/usr/bin/python3

import subprocess
import re

from robot.api.deco import keyword
from robot.api import logger

ROBOT_AUTO_KEYWORDS = False

NEOFS_ENDPOINT = '10.78.30.13:8080'
#NEOFS_ENDPOINT = '85.143.219.93:8080'
NEOFS_KEY = 'L3UcodxBNukNuXnMKzH7rUn3pvgLGrNkGqeUnvnPySxBFHVR8xmL'
#NEOFS_KEY = 'KxDgvEKzgSBPPfuVfw67oPQBSjidEiqTHURKSDL1R7yGaGYAeYnr'

@keyword('Create container')
def create_container():
    logger.info("Creating container")

    createContainerCmd = f'neofs-cli --host {NEOFS_ENDPOINT} --key {NEOFS_KEY} container put --rule "RF 1 SELECT 2 Node"'
    complProc = subprocess.run(createContainerCmd, check=True, universal_newlines=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)
    cid = parse_cid(complProc.stdout)
    return cid

def parse_cid(output: str):
    """
    This function parses CID from given CLI output.
    Parameters:
    - output: a string with command run output
    """
    m = re.search(r'Success! Container <(([a-zA-Z0-9])+)> created', output)
    if m.start() != m.end(): # e.g., if match found something
        cid = m.group(1)
    else:
        logger.warn("no CID was parsed from command output: \t%s" % output)
        return
    return cid

@keyword('Write object to NeoFS')
def write_object(path: str, cid: str):
    logger.info("Going to put an object")

    putObjectCmd = f'neofs-cli --host {NEOFS_ENDPOINT} --key {NEOFS_KEY} object put --file {path}  --cid {cid}'
    complProc = subprocess.run(putObjectCmd, check=True, universal_newlines=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)
    oid = parse_oid(complProc.stdout)
    return oid

def parse_oid(output: str):
    """
    This function parses OID from given CLI output.
    Parameters:
    - output: a string with command run output
    """
    m = re.search(r'ID: ([a-zA-Z0-9-]+)', output)
    if m.start() != m.end(): # e.g., if match found something
        oid = m.group(1)
    else:
        logger.warn("no OID was parsed from command output: \t%s" % output)
        return
    return oid


@keyword('Read object from NeoFS')
def read_object(cid: str, oid: str, read_object: str):
    logger.info("Going to get an object")

    getObjectCmd = f'neofs-cli --host {NEOFS_ENDPOINT} --key {NEOFS_KEY} object get --cid {cid} --oid {oid} --file {read_object}'
    complProc = subprocess.run(getObjectCmd, check=True, universal_newlines=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)
    logger.info(complProc.stdout)
