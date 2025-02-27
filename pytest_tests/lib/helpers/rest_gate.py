import json
import logging
import os
import random
import shutil
import uuid
import zipfile
from typing import Optional, Union
from urllib.parse import quote

import allure
import requests
from helpers.cli_helpers import _cmd_run
from helpers.common import (
    DEFAULT_OBJECT_OPERATION_TIMEOUT,
    DEFAULT_REST_OPERATION_TIMEOUT,
    SIMPLE_OBJECT_SIZE,
    get_assets_dir_path,
)
from helpers.complex_object_actions import get_nodes_without_object
from helpers.file_helper import get_file_hash
from helpers.neofs_verbs import get_object
from neofs_testlib.shell import Shell

logger = logging.getLogger("NeoLogger")


@allure.step("Get via REST Gate")
def get_via_rest_gate(
    cid: str,
    oid: str,
    endpoint: str,
    request_path: Optional[str] = None,
    return_response=False,
    download=False,
    skip_options_verify=False,
) -> Union[str, requests.Response]:
    """
    This function gets given object from REST gate
    cid:          container id to get object from
    oid:          object ID
    endpoint:     REST gate endpoint
    request_path: (optional) REST request, if ommited - use default [{endpoint}/objects/{cid}/by_id/{oid}]
    return_response: (optional) either return internal requests.Response object or not
    """

    # if `request_path` parameter ommited, use default
    download_attribute = ""
    if download:
        download_attribute = "?download=true"
    if request_path is None:
        request = f"{endpoint}/objects/{cid}/by_id/{oid}{download_attribute}"
    else:
        request = f"{endpoint}{request_path}{download_attribute}"

    if not skip_options_verify:
        verify_options_request(request)
    resp = requests.get(request, stream=True, timeout=DEFAULT_OBJECT_OPERATION_TIMEOUT)

    if not resp.ok:
        raise Exception(
            f"""Failed to get object via REST gate:
                request: {resp.request.path_url},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}"""
        )

    logger.info(f"Request: {request}")
    _attach_allure_step(request, resp.status_code)

    file_path = os.path.join(get_assets_dir_path(), f"{cid}_{oid}")
    with open(file_path, "wb") as file:
        shutil.copyfileobj(resp.raw, file)
    if not return_response:
        return file_path
    return resp


@allure.step("Get via Zip HTTP Gate")
def get_via_zip_http_gate(cid: str, prefix: str, endpoint: str):
    """
    This function gets given object from HTTP gate
    cid:      container id to get object from
    prefix:   common prefix
    endpoint: http gate endpoint
    """
    request = f"{endpoint}/zip/{cid}/{prefix}"
    resp = requests.get(request, stream=True, timeout=DEFAULT_OBJECT_OPERATION_TIMEOUT)

    if not resp.ok:
        raise Exception(
            f"""Failed to get object via HTTP gate:
                request: {resp.request.path_url},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}"""
        )

    logger.info(f"Request: {request}")
    _attach_allure_step(request, resp.status_code)

    file_path = os.path.join(get_assets_dir_path(), f"{cid}_archive.zip")
    with open(file_path, "wb") as file:
        shutil.copyfileobj(resp.raw, file)

    with zipfile.ZipFile(file_path, "r") as zip_ref:
        zip_ref.extractall(get_assets_dir_path())

    return os.path.join(get_assets_dir_path(), prefix)


@allure.step("Get via REST Gate by attribute")
def get_via_rest_gate_by_attribute(
    cid: str, attribute: dict, endpoint: str, request_path: Optional[str] = None, skip_options_verify=False
):
    """
    This function gets given object from REST gate
    cid:          CID to get object from
    attribute:    attribute {name: attribute} value pair
    endpoint:     REST gate endpoint
    request_path: (optional) REST request path, if ommited - use default [{endpoint}/objects/{Key}/by_attribute/{Value}]
    """
    attr_name = list(attribute.keys())[0]
    attr_value = quote(str(attribute.get(attr_name)))
    # if `request_path` parameter ommited, use default
    if request_path is None:
        request = f"{endpoint}/objects/{cid}/by_attribute/{quote(str(attr_name))}/{attr_value}"
    else:
        request = f"{endpoint}{request_path}"

    if not skip_options_verify:
        verify_options_request(request)
    resp = requests.get(request, stream=True, timeout=DEFAULT_OBJECT_OPERATION_TIMEOUT)

    if not resp.ok:
        raise Exception(
            f"""Failed to get object via REST gate:
                request: {resp.request.path_url},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}"""
        )

    logger.info(f"Request: {request}")
    _attach_allure_step(request, resp.status_code)

    file_path = os.path.join(get_assets_dir_path(), f"{cid}_{str(uuid.uuid4())}")
    with open(file_path, "wb") as file:
        shutil.copyfileobj(resp.raw, file)
    return file_path


@allure.step("Upload via REST Gate")
def upload_via_rest_gate(
    cid: str,
    path: str,
    endpoint: str,
    headers: dict = None,
    cookies: dict = None,
    file_content_type: str = None,
    error_pattern: Optional[str] = None,
) -> str:
    """
    This function upload given object through REST gate
    cid:      CID to get object from
    path:     File path to upload
    endpoint: REST gate endpoint
    headers:  Object header
    file_content_type: Special Multipart Content-Type header
    """
    request = f"{endpoint}/upload/{cid}"
    if not file_content_type:
        files = {"upload_file": open(path, "rb")}
    else:
        files = {"upload_file": (path, open(path, "rb"), file_content_type)}
    body = {"filename": path}
    verify_options_request(request)
    resp = requests.post(
        request, files=files, data=body, headers=headers, cookies=cookies, timeout=DEFAULT_OBJECT_OPERATION_TIMEOUT
    )

    if not resp.ok:
        if error_pattern:
            match = error_pattern.casefold() in str(resp.text).casefold()
            assert match, f"Expected {resp.text} to match {error_pattern}"
            return ""
        raise Exception(
            f"""Failed to get object via REST gate:
                request: {resp.request.path_url},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}"""
        )

    logger.info(f"Request: {request}")
    _attach_allure_step(request, resp.json(), req_type="POST")

    assert resp.json().get("object_id"), f"OID found in response {resp}"

    return resp.json().get("object_id")


@allure.step("Check is the passed object large")
def is_object_large(filepath: str) -> bool:
    """
    This function check passed file size and return True if file_size > SIMPLE_OBJECT_SIZE
    filepath: File path to check
    """
    file_size = os.path.getsize(filepath)
    logger.info(f"Size= {file_size}")
    if file_size > int(SIMPLE_OBJECT_SIZE):
        return True
    else:
        return False


@allure.step("Upload via REST Gate using Curl")
def upload_via_rest_gate_curl(
    cid: str,
    filepath: str,
    endpoint: str,
    headers: list = None,
    error_pattern: Optional[str] = None,
) -> str:
    """
    This function upload given object through REST gate using curl utility.
    cid: CID to get object from
    filepath: File path to upload
    headers: Object header
    endpoint: REST gate endpoint
    error_pattern: [optional] expected error message from the command
    """
    request = f"{endpoint}/upload/{cid}"
    attributes = ""
    if headers:
        # parse attributes
        attributes = " ".join(headers)

    files = f"file=@{filepath};filename={os.path.basename(filepath)}"
    cmd = f"curl --silent -F '{files}' {attributes} {request}"
    output = _cmd_run(cmd)

    if error_pattern:
        match = error_pattern.casefold() in str(output).casefold()
        assert match, f"Expected {output} to match {error_pattern}"
        return ""

    try:
        response_json = json.loads(output)
    except json.JSONDecodeError:
        raise AssertionError(f"Invalid JSON response: {output}")
    if "object_id" not in response_json:
        raise AssertionError(f'Could not find "object_id" in JSON response: {output}')
    return response_json["object_id"]


def _attach_allure_step(request: str, status_code: int, req_type="GET"):
    command_attachment = f"REQUEST: '{request}'\nRESPONSE:\n {status_code}\n"
    with allure.step(f"{req_type} Request"):
        allure.attach(command_attachment, f"{req_type} Request", allure.attachment_type.TEXT)


@allure.step("Try to get object and expect error")
def try_to_get_object_and_expect_error(cid: str, oid: str, error_pattern: str, endpoint: str) -> None:
    try:
        get_via_rest_gate(cid=cid, oid=oid, endpoint=endpoint)
        raise AssertionError(f"Expected error on getting object with cid: {cid}")
    except Exception as err:
        match = error_pattern.casefold() in str(err).casefold()
        assert match, f"Expected {err} to match {error_pattern}"


@allure.step("Verify object can be get using REST header attribute")
def get_object_by_attr_and_verify_hashes(
    oid: str,
    file_name: str,
    cid: str,
    attrs: dict,
    endpoint: str,
    request_path: Optional[str] = None,
    request_path_attr: Optional[str] = None,
) -> None:
    got_file_path_rest = get_via_rest_gate(cid=cid, oid=oid, endpoint=endpoint, request_path=request_path)
    got_file_path_rest_attr = get_via_rest_gate_by_attribute(
        cid=cid, attribute=attrs, endpoint=endpoint, request_path=request_path_attr
    )
    assert_hashes_are_equal(file_name, got_file_path_rest, got_file_path_rest_attr)


def get_object_and_verify_hashes(
    oid: str,
    file_name: str,
    wallet: str,
    cid: str,
    shell: Shell,
    nodes: list,
    endpoint: str,
    object_getter=None,
) -> None:
    nodes_list = get_nodes_without_object(
        wallet=wallet,
        cid=cid,
        oid=oid,
        shell=shell,
        nodes=nodes,
    )
    # for some reason we can face with case when nodes_list is empty due to object resides in all nodes
    if nodes_list:
        random_node = random.choice(nodes_list)
    else:
        random_node = random.choice(nodes)

    object_getter = object_getter or get_via_rest_gate

    got_file_path = get_object(
        wallet=wallet,
        cid=cid,
        oid=oid,
        shell=shell,
        endpoint=random_node.get_rpc_endpoint(),
    )
    got_file_path_rest = object_getter(cid=cid, oid=oid, endpoint=endpoint)

    assert_hashes_are_equal(file_name, got_file_path, got_file_path_rest)


def assert_hashes_are_equal(orig_file_name: str, got_file_1: str, got_file_2: str) -> None:
    msg = "Expected hashes are equal for files {f1} and {f2}"
    got_file_hash_rest = get_file_hash(got_file_1)
    assert get_file_hash(got_file_2) == got_file_hash_rest, msg.format(f1=got_file_2, f2=got_file_1)
    assert get_file_hash(orig_file_name) == got_file_hash_rest, msg.format(f1=orig_file_name, f2=got_file_1)


def attr_into_header(attrs: dict) -> dict:
    return {f"X-Attribute-{_key}": _value for _key, _value in attrs.items()}


@allure.step("Convert each attribute (Key=Value) to the following format: -H 'X-Attribute-Key: Value'")
def attr_into_str_header_curl(attrs: dict) -> list:
    headers = []
    for k, v in attrs.items():
        headers.append(f"-H 'X-Attribute-{k}: {v}'")
    logger.info(f"[List of Attrs for curl:] {headers}")
    return headers


@allure.step("Convert each attribute (Key=Value) to the following format: 'X-Attribute-Key: Value'")
def attr_into_str_header(attrs: dict) -> list:
    return {f"X-Attribute-{k}": f"{v}" for k, v in attrs.items()}


@allure.step("Try to get object via REST gate (pass http_request and optional attributes) and expect error")
def try_to_get_object_via_passed_request_and_expect_error(
    cid: str,
    oid: str,
    error_pattern: str,
    endpoint: str,
    http_request_path: str,
    attrs: dict = None,
) -> None:
    try:
        if attrs is None:
            get_via_rest_gate(
                cid=cid, oid=oid, endpoint=endpoint, request_path=http_request_path, skip_options_verify=True
            )
        else:
            get_via_rest_gate_by_attribute(
                cid=cid, attribute=attrs, endpoint=endpoint, request_path=http_request_path, skip_options_verify=True
            )
        raise AssertionError(f"Expected error on getting object with cid: {cid}")
    except Exception as err:
        match = error_pattern.casefold() in str(err).casefold()
        assert match, f"Expected {err} to match {error_pattern}"


@allure.step("New Upload via REST Gate")
def new_upload_via_rest_gate(
    cid: str,
    path: str,
    endpoint: str,
    headers: dict = None,
    cookies: dict = None,
    file_content_type: str = None,
    error_pattern: Optional[str] = None,
) -> str:
    """
    This function upload given object through REST gate
    cid:      CID to get object from
    path:     File path to upload
    endpoint: REST gate endpoint
    headers:  Object header
    file_content_type: Content-Type header
    """
    request = f"{endpoint}/objects/{cid}"

    with open(path, "rb") as file:
        file_content = file.read()

    if headers is None:
        headers = {}

    if file_content_type:
        headers["Content-Type"] = file_content_type

    verify_options_request(request)
    resp = requests.post(
        request, data=file_content, headers=headers, cookies=cookies, timeout=DEFAULT_OBJECT_OPERATION_TIMEOUT
    )

    if not resp.ok:
        if error_pattern:
            match = error_pattern.casefold() in str(resp.text).casefold()
            assert match, f"Expected {resp.text} to match {error_pattern}"
            return ""
        raise Exception(
            f"""Failed to get object via REST gate:
                request: {resp.request.path_url},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}"""
        )

    logger.info(f"Request: {request}")
    _attach_allure_step(request, resp.json(), req_type="POST")

    assert resp.json().get("object_id"), f"OID found in response {resp}"

    return resp.json().get("object_id")


def new_attr_into_header(attrs: dict) -> dict:
    json_string = json.dumps(attrs)
    return {"X-Attributes": json_string}


@allure.step("Get epoch duration via REST Gate")
def get_epoch_duration_via_rest_gate(endpoint: str) -> int:
    """
    This function gets network info from REST gate and extracts "epochDuration" from the response
    endpoint:     REST gate endpoint
    """

    request = f"{endpoint}/network-info"

    verify_options_request(request)
    resp = requests.get(request, stream=True, timeout=DEFAULT_OBJECT_OPERATION_TIMEOUT)

    if not resp.ok:
        raise Exception(
            f"""Failed to get network info via REST gate:
                request: {resp.request.path_url},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}"""
        )

    logger.info(f"Request: {request}")
    _attach_allure_step(request, resp.json())

    epoch_duration = resp.json().get("epochDuration")
    return epoch_duration


def verify_options_request(request):
    options_resp = requests.options(request, timeout=DEFAULT_REST_OPERATION_TIMEOUT)
    assert options_resp.status_code == 200, "Invalid status code for OPTIONS request"
    for cors_header in ("Access-Control-Allow-Headers", "Access-Control-Allow-Methods", "Access-Control-Allow-Origin"):
        assert cors_header in options_resp.headers, f"Not CORS header {cors_header} in OPTIONS response"


@allure.step("Create container via REST GW")
def create_container(
    endpoint: str,
    container_name: str,
    placement_policy: str,
    basic_acl: str,
    bearer_token: str,
    bearer_signature: str,
    bearer_signature_key: str,
    wallet_connect=False,
) -> str:
    request = f"{endpoint}/containers"
    body = {
        "containerName": container_name,
        "placementPolicy": placement_policy,
        "basicAcl": basic_acl,
    }
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "X-Bearer-Signature": bearer_signature,
        "X-Bearer-Signature-Key": bearer_signature_key,
    }
    params = {}

    if wallet_connect:
        params["walletConnect"] = "true"

    resp = requests.put(request, json=body, headers=headers, params=params, timeout=60)

    if not resp.ok:
        raise Exception(
            f"""Failed to create container via REST gate:
                request: {resp.request.path_url},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}"""
        )

    logger.info(f"Request: {request}")
    _attach_allure_step(request, resp.json(), req_type="PUT")

    assert resp.json().get("containerId"), f"CID not found in response {resp.json()}"

    return resp.json().get("containerId")


@allure.step("Get token for container operations via REST GW")
def get_container_token(
    endpoint: str, bearer_owner_id: str, bearer_lifetime: int = 100, bearer_for_all_users: bool = True, verb="PUT"
) -> str:
    request = f"{endpoint}/auth"
    body = [
        {"container": {"verb": verb}, "name": str(uuid.uuid4())},
    ]
    resp = requests.post(
        request,
        json=body,
        headers={
            "X-Bearer-Owner-Id": bearer_owner_id,
            "X-Bearer-Lifetime": str(bearer_lifetime),
            "X-Bearer-For-All-Users": str(bearer_for_all_users),
        },
        timeout=60,
    )

    if not resp.ok:
        raise Exception(
            f"""Failed to get auth token via REST gate:
                request: {resp.request.path_url},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}"""
        )

    logger.info(f"Request: {request}")
    logger.info(f"Response: {resp.json()}")
    _attach_allure_step(request, resp.json(), req_type="POST")

    return resp.json()[0]["token"]


@allure.step("Get containers list via REST GW")
def get_containers_list(endpoint: str) -> dict:
    request = f"{endpoint}/containers"
    resp = requests.get(request, timeout=60)

    if not resp.ok:
        raise Exception(
            f"""Failed to get containers list via REST gate:
                request: {resp.request.path_url},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}"""
        )

    logger.info(f"Request: {request}")
    logger.info(f"Response: {resp.json()}")
    _attach_allure_step(request, resp.json(), req_type="GET")

    return resp.json()


@allure.step("Get container info via REST GW")
def get_container_info(endpoint: str, container_id: str) -> dict:
    request = f"{endpoint}/containers/{container_id}"
    resp = requests.get(request, timeout=60)

    if not resp.ok:
        raise Exception(
            f"""Failed to get container info via REST gate:
                request: {resp.request.path_url},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}"""
        )

    logger.info(f"Request: {request}")
    logger.info(f"Response: {resp.json()}")
    _attach_allure_step(request, resp.json(), req_type="GET")

    return resp.json()


@allure.step("Get container eacl via REST GW")
def get_container_eacl(endpoint: str, container_id: str) -> dict:
    request = f"{endpoint}/containers/{container_id}/eacl"
    resp = requests.get(request, timeout=60)

    if not resp.ok:
        raise Exception(
            f"""Failed to get container eacl via REST gate:
                request: {resp.request.path_url},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}"""
        )

    logger.info(f"Request: {request}")
    logger.info(f"Response: {resp.json()}")
    _attach_allure_step(request, resp.json(), req_type="GET")

    return resp.json()


@allure.step("Delete container via REST GW")
def delete_container(
    endpoint: str,
    container_id: str,
    bearer_token: str,
    bearer_signature: str,
    bearer_signature_key: str,
    wallet_connect=False,
) -> dict:
    request = f"{endpoint}/containers/{container_id}"

    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "X-Bearer-Signature": bearer_signature,
        "X-Bearer-Signature-Key": bearer_signature_key,
    }
    params = {}

    if wallet_connect:
        params["walletConnect"] = "true"

    resp = requests.delete(request, headers=headers, params=params, timeout=60)

    if not resp.ok:
        raise Exception(
            f"""Failed to delete container via REST gate:
                request: {resp.request.path_url},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}"""
        )

    logger.info(f"Request: {request}")
    logger.info(f"Response: {resp.json()}")
    _attach_allure_step(request, resp.json(), req_type="DELETE")

    return resp.json()
