import logging
import os
import re
import shutil
import uuid
import zipfile
from random import choice
from typing import Optional
from urllib.parse import quote_plus

import allure
import requests
from cli_helpers import _cmd_run
from common import HTTP_GATE
from file_helper import get_file_hash
from neofs_testlib.shell import Shell
from python_keywords.neofs_verbs import get_object
from python_keywords.storage_policy import get_nodes_without_object

logger = logging.getLogger("NeoLogger")

ASSETS_DIR = os.getenv("ASSETS_DIR", "TemporaryDir/")


@allure.step("Get via HTTP Gate")
def get_via_http_gate(cid: str, oid: str, request: Optional[str] = None):
    """
    This function gets given object from HTTP gate
    :param cid:      CID to get object from
    :param oid:      object OID
    :param request:  (optional) http request, if ommited - use default [/get/{cid}/{oid}]
    """
    # if `request` parameter ommited, use default
    if request is None:
        request = f"{HTTP_GATE}/get/{cid}/{oid}"
    resp = requests.get(request, stream=True)

    if not resp.ok:
        raise Exception(
            f"""Failed to get object via HTTP gate:
                request: {resp.request.path_url},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}"""
        )

    logger.info(f"Request: {request}")
    _attach_allure_step(request, resp.status_code)

    file_path = os.path.join(os.getcwd(), ASSETS_DIR, f"{cid}_{oid}")
    with open(file_path, "wb") as file:
        shutil.copyfileobj(resp.raw, file)
    return file_path


@allure.step("Get via Zip HTTP Gate")
def get_via_zip_http_gate(cid: str, prefix: str):
    """
    This function gets given object from HTTP gate
    :param cid:      CID to get object from
    :param prefix:   common prefix
    """
    request = f"{HTTP_GATE}/zip/{cid}/{prefix}"
    resp = requests.get(request, stream=True)

    if not resp.ok:
        raise Exception(
            f"""Failed to get object via HTTP gate:
                request: {resp.request.path_url},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}"""
        )

    logger.info(f"Request: {request}")
    _attach_allure_step(request, resp.status_code)

    file_path = os.path.join(os.getcwd(), ASSETS_DIR, f"{cid}_archive.zip")
    with open(file_path, "wb") as file:
        shutil.copyfileobj(resp.raw, file)

    with zipfile.ZipFile(file_path, "r") as zip_ref:
        zip_ref.extractall(ASSETS_DIR)

    return os.path.join(os.getcwd(), ASSETS_DIR, prefix)


@allure.step("Get via HTTP Gate by attribute")
def get_via_http_gate_by_attribute(cid: str, attribute: dict, request: Optional[str] = None):
    """
    This function gets given object from HTTP gate
    :param cid:         CID to get object from
    :param attribute:   attribute name: attribute value pair
    :param request:     (optional) http request, if ommited - use default [/get_by_attribute/{Key}/{Value}]
    """
    attr_name = list(attribute.keys())[0]
    attr_value = quote_plus(str(attribute.get(attr_name)))
    # if `request` parameter ommited, use default
    if request is None:
        request = f"{HTTP_GATE}/get_by_attribute/{cid}/{quote_plus(str(attr_name))}/{attr_value}"
    resp = requests.get(request, stream=True)

    if not resp.ok:
        raise Exception(
            f"""Failed to get object via HTTP gate:
                request: {resp.request.path_url},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}"""
        )

    logger.info(f"Request: {request}")
    _attach_allure_step(request, resp.status_code)

    file_path = os.path.join(os.getcwd(), ASSETS_DIR, f"{cid}_{str(uuid.uuid4())}")
    with open(file_path, "wb") as file:
        shutil.copyfileobj(resp.raw, file)
    return file_path


@allure.step("Upload via HTTP Gate")
def upload_via_http_gate(cid: str, path: str, headers: dict = None) -> str:
    """
    This function upload given object through HTTP gate
    :param cid:    CID to get object from
    :param path:   File path to upload
    :param headers: Object header
    """
    request = f"{HTTP_GATE}/upload/{cid}"
    files = {"upload_file": open(path, "rb")}
    body = {"filename": path}
    resp = requests.post(request, files=files, data=body, headers=headers)

    if not resp.ok:
        raise Exception(
            f"""Failed to get object via HTTP gate:
                request: {resp.request.path_url},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}"""
        )

    logger.info(f"Request: {request}")
    _attach_allure_step(request, resp.json(), req_type="POST")

    assert resp.json().get("object_id"), f"OID found in response {resp}"

    return resp.json().get("object_id")


@allure.step("Upload via HTTP Gate using Curl")
def upload_via_http_gate_curl(
    cid: str, filepath: str, large_object=False, headers: dict = None
) -> str:
    """
    This function upload given object through HTTP gate using curl utility.
    :param cid:    CID to get object from
    :param filepath:   File path to upload
    :param headers: Object header
    """
    request = f"{HTTP_GATE}/upload/{cid}"
    files = f"file=@{filepath};filename={os.path.basename(filepath)}"
    cmd = f"curl -F '{files}' {request}"
    if large_object:
        files = f"file=@pipe;filename={os.path.basename(filepath)}"
        cmd = f"mkfifo pipe;cat {filepath} > pipe & curl --no-buffer -F '{files}' {request}"
    output = _cmd_run(cmd)
    oid_re = re.search(r'"object_id": "(.*)"', output)
    if not oid_re:
        raise AssertionError(f'Could not find "object_id" in {output}')
    return oid_re.group(1)


@allure.step("Get via HTTP Gate using Curl")
def get_via_http_curl(cid: str, oid: str) -> str:
    """
    This function gets given object from HTTP gate using curl utility.
    :param cid:      CID to get object from
    :param oid:      object OID
    """
    request = f"{HTTP_GATE}/get/{cid}/{oid}"
    file_path = os.path.join(os.getcwd(), ASSETS_DIR, f"{cid}_{oid}_{str(uuid.uuid4())}")

    cmd = f"curl {request} > {file_path}"
    _cmd_run(cmd)

    return file_path


def _attach_allure_step(request: str, status_code: int, req_type="GET"):
    command_attachment = f"REQUEST: '{request}'\n" f"RESPONSE:\n {status_code}\n"
    with allure.step(f"{req_type} Request"):
        allure.attach(command_attachment, f"{req_type} Request", allure.attachment_type.TEXT)


@allure.step("Try to get object and expect error")
def try_to_get_object_and_expect_error(cid: str, oid: str, error_pattern: str) -> None:
    try:
        get_via_http_gate(cid=cid, oid=oid)
        raise AssertionError(f"Expected error on getting object with cid: {cid}")
    except Exception as err:
        match = error_pattern.casefold() in str(err).casefold()
        assert match, f"Expected {err} to match {error_pattern}"


@allure.step("Verify object can be get using HTTP header attribute")
def get_object_by_attr_and_verify_hashes(oid: str, file_name: str, cid: str, attrs: dict) -> None:
    got_file_path_http = get_via_http_gate(cid=cid, oid=oid)
    got_file_path_http_attr = get_via_http_gate_by_attribute(cid=cid, attribute=attrs)

    assert_hashes_are_equal(file_name, got_file_path_http, got_file_path_http_attr)


@allure.step("Verify object can be get using HTTP")
def get_object_and_verify_hashes(
    oid: str, file_name: str, wallet: str, cid: str, shell: Shell, object_getter=None
) -> None:
    nodes = get_nodes_without_object(wallet=wallet, cid=cid, oid=oid, shell=shell)
    random_node = choice(nodes)
    object_getter = object_getter or get_via_http_gate

    got_file_path = get_object(wallet=wallet, cid=cid, oid=oid, shell=shell, endpoint=random_node)
    got_file_path_http = object_getter(cid=cid, oid=oid)

    assert_hashes_are_equal(file_name, got_file_path, got_file_path_http)


def assert_hashes_are_equal(orig_file_name: str, got_file_1: str, got_file_2: str) -> None:
    msg = "Expected hashes are equal for files {f1} and {f2}"
    got_file_hash_http = get_file_hash(got_file_1)
    assert get_file_hash(got_file_2) == got_file_hash_http, msg.format(f1=got_file_2, f2=got_file_1)
    assert get_file_hash(orig_file_name) == got_file_hash_http, msg.format(
        f1=orig_file_name, f2=got_file_1
    )


def return_header_from_attr(attrs: dict) -> dict:
    return {f"X-Attribute-{_key}": _value for _key, _value in attrs.items()}


@allure.step(
    "Try to get object via http (pass http_request and optional attributes) and expect error"
)
def try_to_get_object_via_passed_request_and_expect_error(
    cid: str, oid: str, error_pattern: str, http_request: str, attrs: dict = None
) -> None:
    try:
        if attrs == None:
            get_via_http_gate(cid=cid, oid=oid, request=http_request)
        else:
            get_via_http_gate_by_attribute(cid=cid, attribute=attrs, request=http_request)
        raise AssertionError(f"Expected error on getting object with cid: {cid}")
    except Exception as err:
        match = error_pattern.casefold() in str(err).casefold()
        assert match, f"Expected {err} to match {error_pattern}"
