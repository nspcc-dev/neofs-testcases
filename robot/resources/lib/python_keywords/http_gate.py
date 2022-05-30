#!/usr/bin/python3

import shutil
import sys
import uuid
import zipfile
from urllib.parse import quote_plus

import allure
import requests
from robot.api import logger
from robot.api.deco import keyword
from robot.libraries.BuiltIn import BuiltIn

from common import HTTP_GATE

ROBOT_AUTO_KEYWORDS = False

if "pytest" in sys.modules:
    import os
    ASSETS_DIR = os.getenv("ASSETS_DIR", "TemporaryDir/")
else:
    ASSETS_DIR = BuiltIn().get_variable_value("${ASSETS_DIR}")


@keyword('Get via HTTP Gate')
def get_via_http_gate(cid: str, oid: str):
    """
    This function gets given object from HTTP gate
    :param cid:      CID to get object from
    :param oid:      object OID
    """
    request = f'{HTTP_GATE}/get/{cid}/{oid}'
    resp = requests.get(request, stream=True)

    if not resp.ok:
        raise Exception(f"""Failed to get object via HTTP gate:
                request: {resp.request.path_url},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}""")

    logger.info(f'Request: {request}')
    _attach_allure_step(request, resp.status_code)

    filename = f"{ASSETS_DIR}/{cid}_{oid}"
    with open(filename, "wb") as get_file:
        shutil.copyfileobj(resp.raw, get_file)
    return filename


@keyword('Get via Zip HTTP Gate')
def get_via_zip_http_gate(cid: str, prefix: str):
    """
    This function gets given object from HTTP gate
    :param cid:      CID to get object from
    :param prefix:   common prefix
    """
    request = f'{HTTP_GATE}/zip/{cid}/{prefix}'
    resp = requests.get(request, stream=True)

    if not resp.ok:
        raise Exception(f"""Failed to get object via HTTP gate:
                request: {resp.request.path_url},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}""")

    logger.info(f'Request: {request}')
    _attach_allure_step(request, resp.status_code)

    filename = f'{ASSETS_DIR}/{cid}_archive.zip'
    with open(filename, 'wb') as get_file:
        shutil.copyfileobj(resp.raw, get_file)

    with zipfile.ZipFile(filename, 'r') as zip_ref:
        zip_ref.extractall(ASSETS_DIR)

    return f'{ASSETS_DIR}/{prefix}'


@keyword('Get via HTTP Gate by attribute')
def get_via_http_gate_by_attribute(cid: str, attribute: dict):
    """
    This function gets given object from HTTP gate
    :param cid:         CID to get object from
    :param attribute:   attribute name: attribute value pair
    """
    attr_name = list(attribute.keys())[0]
    attr_value = quote_plus(str(attribute.get(attr_name)))
    request = f'{HTTP_GATE}/get_by_attribute/{cid}/{quote_plus(str(attr_name))}/{attr_value}'
    resp = requests.get(request, stream=True)

    if not resp.ok:
        raise Exception(f"""Failed to get object via HTTP gate:
                request: {resp.request.path_url},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}""")

    logger.info(f'Request: {request}')
    _attach_allure_step(request, resp.status_code)

    filename = f"{ASSETS_DIR}/{cid}_{str(uuid.uuid4())}"
    with open(filename, "wb") as get_file:
        shutil.copyfileobj(resp.raw, get_file)
    return filename


@keyword('Upload via HTTP Gate')
def upload_via_http_gate(cid: str, path: str, headers: dict = None) -> str:
    """
    This function gets given object from HTTP gate
    :param cid:    CID to get object from
    :param path:   File path to upload
    :param headers: Object header
    """
    request = f'{HTTP_GATE}/upload/{cid}'
    files = {'upload_file': open(path, 'rb')}
    body = {
        'filename': path
    }
    resp = requests.post(request, files=files, data=body, headers=headers)

    if not resp.ok:
        raise Exception(f"""Failed to get object via HTTP gate:
                request: {resp.request.path_url},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}""")

    logger.info(f'Request: {request}')
    _attach_allure_step(request, resp.json(), req_type='POST')

    assert resp.json().get('object_id'), f'OID found in response {resp}'

    return resp.json().get('object_id')


def _attach_allure_step(request: str, status_code: int, req_type='GET'):
    if 'allure' in sys.modules:
        command_attachment = (
            f"REQUEST: '{request}'\n"
            f'RESPONSE:\n {status_code}\n'
        )
        with allure.step(f'{req_type} Request'):
            allure.attach(command_attachment, f'{req_type} Request', allure.attachment_type.TEXT)
