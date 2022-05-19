#!/usr/bin/python3.8

import os
import tarfile
import uuid
import hashlib
import docker

from common import SIMPLE_OBJ_SIZE, ASSETS_DIR
from cli_helpers import _cmd_run
from robot.api.deco import keyword
from robot.api import logger
from robot.libraries.BuiltIn import BuiltIn


ROBOT_AUTO_KEYWORDS = False

@keyword('Generate file of bytes')
def generate_file_of_bytes(size: str) -> str:
    """
    Function generates big binary file with the specified size in bytes.
    :param size:        the size in bytes, can be declared as 6e+6 for example
    """
    size = int(float(size))
    filename = f"{os.getcwd()}/{ASSETS_DIR}/{uuid.uuid4()}"
    with open(filename, 'wb') as fout:
        fout.write(os.urandom(size))
    logger.info(f"file with size {size} bytes has been generated: {filename}")
    return filename

@keyword('Generate file')
def generate_file_and_file_hash(size: str) -> str:
    """
    Function generates a big binary file with the specified size in bytes and its hash.
    Args:
        size (str): the size in bytes, can be declared as 6e+6 for example
    Returns:
        (str): the path to the generated file
        (str): the hash of the generated file
    """
    size = int(float(size))
    filename = f"{os.getcwd()}/{ASSETS_DIR}/{str(uuid.uuid4())}"
    with open(filename, 'wb') as fout:
        fout.write(os.urandom(size))
    logger.info(f"file with size {size} bytes has been generated: {filename}")

    file_hash = _get_file_hash(filename)

    return filename, file_hash

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
            with open(file_name,'wb') as out:
                out.write(client.logs(container_name))
            logger.info(f"Collected logs from container {container_name}")
            tar.add(file_name)
            os.remove(file_name)
    tar.close()

@keyword('Make Up')
def make_up(services: list=[], config_dict: dict={}):
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
def make_down(services: list=[]):
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

def _get_file_hash(filename: str):
    blocksize = 65536
    file_hash = hashlib.md5()
    with open(filename, "rb") as out:
        for block in iter(lambda: out.read(blocksize), b""):
            file_hash.update(block)
    logger.info(f"Hash: {file_hash.hexdigest()}")
    return file_hash.hexdigest()
