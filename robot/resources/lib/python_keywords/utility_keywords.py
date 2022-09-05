#!/usr/bin/python3.8

import hashlib
import os
import tarfile
from typing import Tuple
import uuid

import docker
import wallet
from common import ASSETS_DIR, SIMPLE_OBJ_SIZE
from cli_helpers import _cmd_run
from robot.api import logger
from robot.api.deco import keyword
from robot.libraries.BuiltIn import BuiltIn

ROBOT_AUTO_KEYWORDS = False


def generate_file(size: int = SIMPLE_OBJ_SIZE) -> str:
    """
    Function generates a binary file with the specified size in bytes.
    Args:
        size (int): the size in bytes, can be declared as 6e+6 for example
    Returns:
        (str): the path to the generated file
    """
    file_path = f"{os.getcwd()}/{ASSETS_DIR}/{str(uuid.uuid4())}"
    with open(file_path, 'wb') as fout:
        fout.write(os.urandom(size))
    logger.info(f"file with size {size} bytes has been generated: {file_path}")

    return file_path


@keyword('Generate file')
def generate_file_and_file_hash(size: int) -> Tuple[str, str]:
    """
    Function generates a binary file with the specified size in bytes
    and its hash.
    Args:
        size (int): the size in bytes, can be declared as 6e+6 for example
    Returns:
        (str): the path to the generated file
        (str): the hash of the generated file
    """
    file_path = generate_file(size)
    file_hash = get_file_hash(file_path)

    return file_path, file_hash


@keyword('Get File Hash')
def get_file_hash(filename: str, len: int = None):
    """
    This function generates hash for the specified file.
    Args:
        filename (str): the path to the file to generate hash for
        len (int): how many bytes to read
    Returns:
        (str): the hash of the file
    """
    file_hash = hashlib.sha256()
    with open(filename, "rb") as out:
        if len:
            file_hash.update(out.read(len))
        else:
            file_hash.update(out.read())
    return file_hash.hexdigest()


@keyword('Generate Wallet')
def generate_wallet():
    return wallet.init_wallet(ASSETS_DIR)


@keyword('Get Docker Logs')
def get_container_logs(testcase_name: str) -> None:
    client = docker.APIClient(base_url='unix://var/run/docker.sock')
    logs_dir = BuiltIn().get_variable_value("${OUTPUT_DIR}")
    tar_name = f"{logs_dir}/dockerlogs({testcase_name}).tar.gz"
    tar = tarfile.open(tar_name, "w:gz")
    for container in client.containers():
        container_name = container['Names'][0][1:]
        if (client.inspect_container(container_name)['Config']['Domainname']
                == "neofs.devenv"):
            file_name = f"{logs_dir}/docker_log_{container_name}"
            with open(file_name, 'wb') as out:
                out.write(client.logs(container_name))
            logger.info(f"Collected logs from container {container_name}")
            tar.add(file_name)
            os.remove(file_name)
    tar.close()
