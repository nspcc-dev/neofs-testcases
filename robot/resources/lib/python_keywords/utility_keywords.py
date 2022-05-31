#!/usr/bin/python3.8

import hashlib
import os
import tarfile
import uuid

import docker
from cli_helpers import _cmd_run
from common import SIMPLE_OBJ_SIZE, ASSETS_DIR
from robot.api import logger
from robot.api.deco import keyword
from robot.libraries.BuiltIn import BuiltIn

ROBOT_AUTO_KEYWORDS = False


@keyword('Generate file')
def generate_file_and_file_hash(size: int) -> str:
    """
    Function generates a big binary file with the specified size in bytes and its hash.
    Args:
        size (int): the size in bytes, can be declared as 6e+6 for example
    Returns:
        (str): the path to the generated file
        (str): the hash of the generated file
    """
    filename = f"{os.getcwd()}/{ASSETS_DIR}/{str(uuid.uuid4())}"
    with open(filename, 'wb') as fout:
        fout.write(os.urandom(size))
    logger.info(f"file with size {size} bytes has been generated: {filename}")

    file_hash = get_file_hash(filename)

    return filename, file_hash


@keyword('Get File Hash')
def get_file_hash(filename: str):
    """
    This function generates hash for the specified file.
    Args:
        filename (str): the path to the file to generate hash for
    Returns:
        (str): the hash of the file
    """
    blocksize = 65536
    file_hash = hashlib.md5()
    with open(filename, "rb") as out:
        for block in iter(lambda: out.read(blocksize), b""):
            file_hash.update(block)
    return file_hash.hexdigest()


@keyword('Get Docker Logs')
def get_container_logs(testcase_name: str) -> None:
    client = docker.APIClient(base_url='unix://var/run/docker.sock')
    logs_dir = BuiltIn().get_variable_value("${OUTPUT_DIR}")
    tar_name = f"{logs_dir}/dockerlogs({testcase_name}).tar.gz"
    tar = tarfile.open(tar_name, "w:gz")
    for container in client.containers():
        container_name = container['Names'][0][1:]
        if client.inspect_container(container_name)['Config']['Domainname'] == "neofs.devenv":
            file_name = f"{logs_dir}/docker_log_{container_name}"
            with open(file_name, 'wb') as out:
                out.write(client.logs(container_name))
            logger.info(f"Collected logs from container {container_name}")
            tar.add(file_name)
            os.remove(file_name)
    tar.close()


@keyword('Make Up')
def make_up(services: list = [], config_dict: dict = {}):
    test_path = os.getcwd()
    dev_path = os.getenv('DEVENV_PATH', '../neofs-dev-env')
    os.chdir(dev_path)

    if len(services) > 0:
        for service in services:
            if config_dict != {}:
                with open(f"{dev_path}/.int_test.env", "a") as out:
                    for key, value in config_dict.items():
                        out.write(f'{key}={value}')
            cmd = f'make up/{service}'
            _cmd_run(cmd)
    else:
        cmd = f'make up/basic; make update.max_object_size val={SIMPLE_OBJ_SIZE}'
        _cmd_run(cmd, timeout=120)

    os.chdir(test_path)


@keyword('Make Down')
def make_down(services: list = []):
    test_path = os.getcwd()
    dev_path = os.getenv('DEVENV_PATH', '../neofs-dev-env')
    os.chdir(dev_path)

    if len(services) > 0:
        for service in services:
            cmd = f'make down/{service}'
            _cmd_run(cmd)
            with open(f"{dev_path}/.int_test.env", "w"):
                pass
    else:
        cmd = 'make down; make clean'
        _cmd_run(cmd, timeout=60)

    os.chdir(test_path)
