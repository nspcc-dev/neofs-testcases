import json
import logging
import os
import random
import shutil
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Union
from urllib.parse import quote

import allure
import requests
from helpers.common import (
    DEFAULT_OBJECT_OPERATION_TIMEOUT,
    DEFAULT_REST_OPERATION_TIMEOUT,
    get_assets_dir_path,
)
from helpers.complex_object_actions import get_nodes_without_object
from helpers.file_helper import get_file_hash
from helpers.neofs_verbs import get_object
from neofs_testlib.shell import Shell

logger = logging.getLogger("NeoLogger")


class SearchV2FilterMatch(Enum):
    MatchStringEqual = "EQ"
    MatchStringNotEqual = "NE"
    MatchNotPresent = "NOPRESENT"
    MatchCommonPrefix = "COMMON_PREFIX"
    MatchNumGT = "GT"
    MatchNumGE = "GE"
    MatchNumLT = "LT"
    MatchNumLE = "LE"


@dataclass
class SearchV2Filter:
    key: str
    match_: SearchV2FilterMatch
    value: Optional[str] = None

    @classmethod
    def convert_from_cli(cls, cli_filter: str) -> "SearchV2Filter":
        key, op, *right_side = cli_filter.split(" ")
        return SearchV2Filter(
            key=key,
            match_=SearchV2FilterMatch(op),
            value=right_side[0] if right_side else None,
        )

    def to_json(self):
        res = {"key": self.key, "match": self.match_.name}
        if self.value:
            res["value"] = self.value
        return res


@allure.step("Get via REST Gate")
def get_via_rest_gate(
    cid: str,
    oid: str,
    endpoint: str,
    return_response=False,
    download=False,
    skip_options_verify=False,
) -> Union[str, requests.Response]:
    """
    This function gets given object from REST gate
    cid:          container id to get object from
    oid:          object ID
    endpoint:     REST gate endpoint
    return_response: (optional) either return internal requests.Response object or not
    """

    # if `request_path` parameter ommited, use default
    download_attribute = ""
    if download:
        download_attribute = "?download=true"
    request = f"{endpoint}/objects/{cid}/by_id/{oid}{download_attribute}"

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


@allure.step("Head via REST Gate")
def head_via_rest_gate(
    cid: str,
    oid: str,
    endpoint: str,
) -> Union[str, requests.Response]:
    """
    This function heads given object from REST gate
    cid:          container id to get object from
    oid:          object ID
    endpoint:     REST gate endpoint
    """
    request = f"{endpoint}/objects/{cid}/by_id/{oid}"

    verify_options_request(request)
    resp = requests.head(request, stream=True, timeout=DEFAULT_OBJECT_OPERATION_TIMEOUT)

    if not resp.ok:
        raise Exception(
            f"""Failed to head object via REST gate:
                request: {resp.request.path_url},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}"""
        )

    logger.info(f"Request: {request}")
    _attach_allure_step(request, resp.status_code)

    return resp.headers


@allure.step("Get via REST Gate by attribute")
def get_via_rest_gate_by_attribute(cid: str, attribute: dict, endpoint: str, skip_options_verify=False):
    """
    This function gets given object from REST gate
    cid:          CID to get object from
    attribute:    attribute {name: attribute} value pair
    endpoint:     REST gate endpoint
    request_path: (optional) REST request path, if ommited - use default [{endpoint}/objects/{Key}/by_attribute/{Value}]
    """
    attr_name = list(attribute.keys())[0]
    attr_value = quote(str(attribute.get(attr_name)))
    request = f"{endpoint}/objects/{cid}/by_attribute/{quote(str(attr_name))}/{attr_value}"

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


@allure.step("Head via REST Gate by attribute")
def head_via_rest_gate_by_attribute(cid: str, attribute: dict, endpoint: str):
    """
    This function heads given object from REST gate
    cid:          CID to get object from
    attribute:    attribute {name: attribute} value pair
    endpoint:     REST gate endpoint
    request_path: (optional) REST request path, if ommited - use default [{endpoint}/objects/{Key}/by_attribute/{Value}]
    """
    attr_name = list(attribute.keys())[0]
    attr_value = quote(str(attribute.get(attr_name)))
    request = f"{endpoint}/objects/{cid}/by_attribute/{quote(str(attr_name))}/{attr_value}"

    verify_options_request(request)
    resp = requests.head(request, stream=True, timeout=DEFAULT_OBJECT_OPERATION_TIMEOUT)

    if not resp.ok:
        raise Exception(
            f"""Failed to head object via REST gate:
                request: {resp.request.path_url},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}"""
        )

    logger.info(f"Request: {request}")
    _attach_allure_step(request, resp.status_code)

    return resp.headers


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
) -> None:
    got_file_path_rest = get_via_rest_gate(cid=cid, oid=oid, endpoint=endpoint)
    got_file_path_rest_attr = get_via_rest_gate_by_attribute(cid=cid, attribute=attrs, endpoint=endpoint)
    assert_hashes_are_equal(file_name, got_file_path_rest, got_file_path_rest_attr)


@allure.step("Verify object can be head using REST header attribute")
def head_object_by_attr_and_verify(
    oid: str,
    cid: str,
    attrs: dict,
    endpoint: str,
) -> None:
    headers_with_oid = head_via_rest_gate(cid=cid, oid=oid, endpoint=endpoint)
    headers_with_attr = head_via_rest_gate_by_attribute(cid=cid, attribute=attrs, endpoint=endpoint)
    assert headers_with_oid["X-Object-Id"] == headers_with_attr["X-Object-Id"], (
        f"headers not equal, expected: {headers_with_oid['X-Object-Id']}, got: {headers_with_attr['X-Object-Id']}"
    )


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


@allure.step("New Upload via REST Gate")
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


def attr_into_header(attrs: dict) -> dict:
    str_attrs = {k: str(v) if isinstance(v, int) else v for k, v in attrs.items()}
    json_string = json.dumps(str_attrs)
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
    session_token: str,
    wallet_connect=False,
) -> str:
    """
    Create container via REST gateway using session token v2.

    Args:
        endpoint: REST gateway endpoint
        container_name: Name of the container
        placement_policy: Placement policy string
        basic_acl: Basic ACL string
        session_token: Complete signed session token (base64 encoded)
        wallet_connect: Use WalletConnect signature scheme

    Returns:
        str: Container ID
    """
    request = f"{endpoint}/containers"
    body = {
        "containerName": container_name,
        "placementPolicy": placement_policy,
        "basicAcl": basic_acl,
    }
    headers = {
        "Authorization": f"Bearer {session_token}",
    }
    params = {}

    if wallet_connect:
        params["walletConnect"] = "true"

    resp = requests.post(request, json=body, headers=headers, params=params, timeout=60)

    if not resp.ok:
        raise Exception(
            f"""Failed to create container via REST gate:
                request: {resp.request.path_url},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}"""
        )

    logger.info(f"Request: {request}")
    _attach_allure_step(request, resp.json(), req_type="POST")

    assert resp.json().get("containerId"), f"CID not found in response {resp.json()}"

    return resp.json().get("containerId")


@allure.step("Get token for container operations via REST GW")
def get_container_token(
    endpoint: str,
    bearer_owner_id: str,
    bearer_lifetime: int = 100,
    verb="CONTAINER_PUT",
) -> tuple[str, str]:
    """
    Get unsigned session token for container operations via /v2/auth/session.

    Args:
        endpoint: REST gateway endpoint
        bearer_owner_id: Token issuer ID (account address)
        bearer_lifetime: Token lifetime in seconds
        verb: Container operation verb (CONTAINER_PUT, CONTAINER_DELETE, CONTAINER_SET_EACL, etc.)

    Returns:
        tuple: (unsigned_token, lock) - both as base64 strings
    """
    request = f"{endpoint.replace('v1', 'v2')}/auth/session"

    body = {
        "owner": bearer_owner_id,
        "targets": [bearer_owner_id],
        "contexts": [{"verbs": [verb]}],
        "expiration-duration": f"{bearer_lifetime}s",
    }

    resp = requests.post(
        request,
        json=body,
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

    return resp.json()["token"], resp.json()["lock"]


@allure.step("Complete session token via REST GW")
def complete_session_token(
    endpoint: str,
    unsigned_token: str,
    lock: str,
    signature: str,
    public_key: str,
    scheme: str = "WALLETCONNECT",
) -> str:
    """
    Complete session token by attaching signature.

    Args:
        endpoint: REST gateway endpoint
        unsigned_token: Base64 encoded unsigned token from /v2/auth/session
        lock: Base64 encoded lock from /v2/auth/session
        signature: Base64 encoded signature
        public_key: Hex encoded public key (or base64 for N3 scheme)
        scheme: Signature scheme (WALLETCONNECT, SHA512, DETERMINISTIC_SHA256, N3)

    Returns:
        str: Base64 encoded complete signed token (lock + signed_token)
    """
    request = f"{endpoint.replace('v1', 'v2')}/auth/session/complete"

    body = {
        "token": unsigned_token,
        "lock": lock,
        "value": signature,
        "key": public_key,
        "scheme": scheme,
    }

    resp = requests.post(
        request,
        json=body,
        timeout=60,
    )

    if not resp.ok:
        raise Exception(
            f"""Failed to complete session token via REST gate:
                request: {resp.request.path_url},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}"""
        )

    logger.info(f"Request: {request}")
    logger.info(f"Response: {resp.json()}")
    _attach_allure_step(request, resp.json(), req_type="POST")

    return resp.json()["token"]


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
    session_token: str,
    wallet_connect=False,
) -> dict:
    """
    Delete container via REST gateway using session token v2.

    Args:
        endpoint: REST gateway endpoint
        container_id: Container ID to delete
        session_token: Complete signed session token (base64 encoded)
        wallet_connect: Use WalletConnect signature scheme

    Returns:
        dict: Response from REST gateway
    """
    request = f"{endpoint}/containers/{container_id}"

    headers = {
        "Authorization": f"Bearer {session_token}",
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


@allure.step("Searchv2 via REST Gate")
def searchv2(
    endpoint: str,
    cid: str,
    cursor: Optional[str] = None,
    limit: Optional[int] = None,
    filters: Optional[list[SearchV2Filter]] = None,
    attributes: Optional[list[str]] = None,
) -> str:
    request = f"{endpoint}/v2/objects/{cid}/search"

    params = {}
    if cursor:
        params["cursor"] = cursor

    if limit:
        params["limit"] = limit

    search_request = {}
    if filters:
        search_request["filters"] = [f.to_json() for f in filters]

    if attributes:
        search_request["attributes"] = attributes

    headers = {
        "Content-Type": "application/json",
    }
    resp = requests.post(request, params=params, json=search_request, headers=headers)

    if not resp.ok:
        raise AssertionError(
            f"""Failed to searchv2 object via REST gate:
                request: {resp.request.path_url},
                params: {params},
                json: {search_request},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}"""
        )

    _attach_allure_step(f"{request=}; {params=}; {search_request=}", resp.json(), req_type="POST")
    return resp.json()
