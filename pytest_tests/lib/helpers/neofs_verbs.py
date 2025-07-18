import json
import logging
import os
import random
import re
import uuid
from typing import Any, Optional, Union

import allure
from helpers import json_transformers
from helpers.common import (
    NEOFS_CLI_EXEC,
    TEST_FILES_DIR,
    TEST_OBJECTS_DIR,
    WALLET_CONFIG,
    get_assets_dir_path,
)
from neofs_testlib.cli import NeofsCli
from neofs_testlib.shell import Shell

logger = logging.getLogger("NeoLogger")

# https://github.com/nspcc-dev/neofs-api/blob/5c3535423c564fe63991812cb84d8334db1c553a/object/service.proto#L319
NEOFS_API_HEADER_LIMIT = 16384

CONFIG_KEYS_MAPPING = {
    "MaxObjectSize": "maximum_object_size",
    "BasicIncomeRate": "storage_price",
    "EpochDuration": "epoch_duration",
    "ContainerFee": "container_fee",
    "EigenTrustIterations": "number_of_eigentrust_iterations",
    "EigenTrustAlpha": "eigentrust_alpha",
    "WithdrawFee": "withdrawal_fee",
    "HomomorphicHashingDisabled": "homomorphic_hashing_disabled",
}


@allure.step("Get object from random node")
def get_object_from_random_node(
    wallet: str,
    cid: str,
    oid: str,
    shell: Shell,
    neofs_env=None,
    bearer: Optional[str] = None,
    write_object: Optional[str] = None,
    xhdr: Optional[dict] = None,
    wallet_config: Optional[str] = None,
    no_progress: bool = True,
    session: Optional[str] = None,
) -> str:
    """
    GET from NeoFS random storage node

    Args:
        wallet: wallet on whose behalf GET is done
        cid: ID of Container where we get the Object from
        oid: Object ID
        shell: executor for cli command
        bearer (optional, str): path to Bearer Token file, appends to `--bearer` key
        write_object (optional, str): path to downloaded file, appends to `--file` key
        endpoint: NeoFS endpoint to send request to, appends to `--rpc-endpoint` key
        wallet_config(optional, str): path to the wallet config
        no_progress(optional, bool): do not show progress bar
        xhdr (optional, dict): Request X-Headers in form of Key=Value
        session (optional, dict): path to a JSON-encoded container session token
    Returns:
        (str): path to downloaded file
    """
    if neofs_env:
        endpoint = random.choice(neofs_env.storage_nodes).endpoint
    return get_object(
        wallet,
        cid,
        oid,
        shell,
        endpoint,
        bearer,
        write_object,
        xhdr,
        wallet_config,
        no_progress,
        session,
    )


@allure.step("Get object from {endpoint}")
def get_object(
    wallet: str,
    cid: str,
    oid: str,
    shell: Shell,
    endpoint: str = None,
    bearer: Optional[str] = None,
    write_object: Optional[str] = None,
    xhdr: Optional[dict] = None,
    wallet_config: Optional[str] = None,
    no_progress: bool = True,
    session: Optional[str] = None,
) -> str:
    """
    GET from NeoFS.

    Args:
        wallet (str): wallet on whose behalf GET is done
        cid (str): ID of Container where we get the Object from
        oid (str): Object ID
        shell: executor for cli command
        bearer: path to Bearer Token file, appends to `--bearer` key
        write_object: path to downloaded file, appends to `--file` key
        endpoint: NeoFS endpoint to send request to, appends to `--rpc-endpoint` key
        wallet_config(optional, str): path to the wallet config
        no_progress(optional, bool): do not show progress bar
        xhdr (optional, dict): Request X-Headers in form of Key=Value
        session (optional, dict): path to a JSON-encoded container session token
    Returns:
        (str): path to downloaded file
    """

    if not write_object:
        write_object = str(uuid.uuid4())
    file_path = os.path.join(get_assets_dir_path(), TEST_OBJECTS_DIR, write_object)

    cli = NeofsCli(shell, NEOFS_CLI_EXEC, wallet_config or WALLET_CONFIG)
    cli.object.get(
        rpc_endpoint=endpoint,
        wallet=wallet,
        cid=cid,
        oid=oid,
        file=file_path,
        bearer=bearer,
        no_progress=no_progress,
        xhdr=xhdr,
        session=session,
    )

    return file_path


@allure.step("Get Range Hash from {endpoint}")
def get_range_hash(
    wallet: str,
    cid: str,
    oid: str,
    range_cut: str,
    shell: Shell,
    endpoint: str,
    bearer: Optional[str] = None,
    wallet_config: Optional[str] = None,
    xhdr: Optional[dict] = None,
    session: Optional[str] = None,
):
    """
    GETRANGEHASH of given Object.

    Args:
        wallet: wallet on whose behalf GETRANGEHASH is done
        cid: ID of Container where we get the Object from
        oid: Object ID
        shell: executor for cli command
        bearer: path to Bearer Token file, appends to `--bearer` key
        range_cut: Range to take hash from in the form offset1:length1,...,
                        value to pass to the `--range` parameter
        endpoint: NeoFS endpoint to send request to, appends to `--rpc-endpoint` key
        wallet_config: path to the wallet config
        xhdr: Request X-Headers in form of Key=Values
        session: Filepath to a JSON- or binary-encoded token of the object RANGEHASH session.
    Returns:
        None
    """
    cli = NeofsCli(shell, NEOFS_CLI_EXEC, wallet_config or WALLET_CONFIG)
    result = cli.object.hash(
        rpc_endpoint=endpoint,
        wallet=wallet,
        cid=cid,
        oid=oid,
        range=range_cut,
        bearer=bearer,
        xhdr=xhdr,
        session=session,
    )

    # cutting off output about range offset and length
    return result.stdout.split(":")[1].strip()


@allure.step("Put object to random node")
def put_object_to_random_node(
    wallet: str,
    path: str,
    cid: str,
    shell: Shell,
    neofs_env=None,
    bearer: Optional[str] = None,
    attributes: Optional[dict] = None,
    xhdr: Optional[dict] = None,
    wallet_config: Optional[str] = None,
    lifetime: Optional[int] = None,
    expire_at: Optional[int] = None,
    no_progress: bool = True,
    session: Optional[str] = None,
):
    """
    PUT of given file to a random storage node.

    Args:
        wallet: wallet on whose behalf PUT is done
        path: path to file to be PUT
        cid: ID of Container where we get the Object from
        shell: executor for cli command
        neofs_env: neofs env under test
        bearer: path to Bearer Token file, appends to `--bearer` key
        attributes: User attributes in form of Key1=Value1,Key2=Value2
        wallet_config: path to the wallet config
        no_progress: do not show progress bar
        lifetime: Lock lifetime - relative to the current epoch.
        expire_at: Last epoch in the life of the object - absolute value.
        xhdr: Request X-Headers in form of Key=Value
        session: path to a JSON-encoded container session token
    Returns:
        ID of uploaded Object
    """

    if neofs_env:
        endpoint = random.choice(neofs_env.storage_nodes).endpoint
    return put_object(
        wallet,
        path,
        cid,
        shell,
        endpoint,
        bearer,
        attributes,
        xhdr,
        wallet_config,
        expire_at,
        no_progress,
        session,
        lifetime,
    )


@allure.step("Put object at {endpoint} in container {cid}")
def put_object(
    wallet: str,
    path: str,
    cid: str,
    shell: Shell,
    endpoint: str,
    bearer: Optional[str] = None,
    attributes: Optional[dict] = None,
    xhdr: Optional[dict] = None,
    wallet_config: Optional[str] = None,
    expire_at: Optional[int] = None,
    no_progress: bool = True,
    session: Optional[str] = None,
    lifetime: Optional[int] = None,
    timeout: Optional[str] = "180s",
):
    """
    PUT of given file.

    Args:
        wallet: wallet on whose behalf PUT is done
        path: path to file to be PUT
        cid: ID of Container where we get the Object from
        shell: executor for cli command
        bearer: path to Bearer Token file, appends to `--bearer` key
        attributes: User attributes in form of Key1=Value1,Key2=Value2
        endpoint: NeoFS endpoint to send request to, appends to `--rpc-endpoint` key
        wallet_config: path to the wallet config
        no_progress: do not show progress bar
        expire_at: Last epoch in the life of the object
        xhdr: Request X-Headers in form of Key=Value
        session: path to a JSON-encoded container session token
        lifetime: Lock lifetime - relative to the current epoch.
    Returns:
        (str): ID of uploaded Object
    """

    cli = NeofsCli(shell, NEOFS_CLI_EXEC, wallet_config or WALLET_CONFIG)
    result = cli.object.put(
        rpc_endpoint=endpoint,
        wallet=wallet,
        file=path,
        cid=cid,
        attributes=attributes,
        bearer=bearer,
        lifetime=lifetime,
        expire_at=expire_at,
        no_progress=no_progress,
        xhdr=xhdr,
        session=session,
        timeout=timeout,
    )

    # splitting CLI output to lines and taking the penultimate line
    id_str = result.stdout.strip().split("\n")[-2]
    oid = id_str.split(":")[1]
    return oid.strip()


@allure.step("Delete object {cid}/{oid} from {endpoint}")
def delete_object(
    wallet: str,
    cid: str,
    oid: str,
    shell: Shell,
    endpoint: str = None,
    bearer: str = "",
    wallet_config: Optional[str] = None,
    xhdr: Optional[dict] = None,
    session: Optional[str] = None,
):
    """
    DELETE an Object.

    Args:
        wallet: wallet on whose behalf DELETE is done
        cid: ID of Container where we get the Object from
        oid: ID of Object we are going to delete
        shell: executor for cli command
        bearer: path to Bearer Token file, appends to `--bearer` key
        endpoint: NeoFS endpoint to send request to, appends to `--rpc-endpoint` key
        wallet_config: path to the wallet config
        xhdr: Request X-Headers in form of Key=Value
        session: path to a JSON-encoded container session token
    Returns:
        (str): Tombstone ID
    """

    cli = NeofsCli(shell, NEOFS_CLI_EXEC, wallet_config or WALLET_CONFIG)
    result = cli.object.delete(
        rpc_endpoint=endpoint,
        wallet=wallet,
        cid=cid,
        oid=oid,
        bearer=bearer,
        xhdr=xhdr,
        session=session,
    )

    id_str = result.stdout.split("\n")[1]
    tombstone = id_str.split(":")[1]
    return tombstone.strip()


@allure.step("Get Range")
def get_range(
    wallet: str,
    cid: str,
    oid: str,
    range_cut: str,
    shell: Shell,
    endpoint: str = None,
    wallet_config: Optional[str] = None,
    bearer: str = "",
    xhdr: Optional[dict] = None,
    session: Optional[str] = None,
):
    """
    GETRANGE an Object.

    Args:
        wallet: wallet on whose behalf GETRANGE is done
        cid: ID of Container where we get the Object from
        oid: ID of Object we are going to request
        range_cut: range to take data from in the form offset:length
        shell: executor for cli command
        endpoint: NeoFS endpoint to send request to, appends to `--rpc-endpoint` key
        bearer: path to Bearer Token file, appends to `--bearer` key
        wallet_config: path to the wallet config
        xhdr: Request X-Headers in form of Key=Value
        session: path to a JSON-encoded container session token
    Returns:
        (str, bytes) - path to the file with range content and content of this file as bytes
    """
    range_file_path = os.path.join(get_assets_dir_path(), TEST_OBJECTS_DIR, str(uuid.uuid4()))

    cli = NeofsCli(shell, NEOFS_CLI_EXEC, wallet_config or WALLET_CONFIG)
    cli.object.range(
        rpc_endpoint=endpoint,
        wallet=wallet,
        cid=cid,
        oid=oid,
        range=range_cut,
        file=range_file_path,
        bearer=bearer,
        xhdr=xhdr,
        session=session,
    )

    with open(range_file_path, "rb") as file:
        content = file.read()
    return range_file_path, content


@allure.step("Lock Object")
def lock_object(
    wallet: str,
    cid: str,
    oid: str,
    shell: Shell,
    endpoint: str,
    lifetime: Optional[int] = None,
    expire_at: Optional[int] = None,
    address: Optional[str] = None,
    bearer: Optional[str] = None,
    session: Optional[str] = None,
    wallet_config: Optional[str] = None,
    ttl: Optional[int] = None,
    xhdr: Optional[dict] = None,
) -> str:
    """
    Lock object in container.

    Args:
        address: Address of wallet account.
        bearer: File with signed JSON or binary encoded bearer token.
        cid: Container ID.
        oid: Object ID.
        lifetime: Lock lifetime.
        expire_at: Lock expiration epoch.
        shell: executor for cli command
        endpoint: NeoFS endpoint to send request to, appends to `--rpc-endpoint` key
        session: Path to a JSON-encoded container session token.
        ttl: TTL value in request meta header (default 2).
        wallet: WIF (NEP-2) string or path to the wallet or binary key.
        xhdr: Dict with request X-Headers.

    Returns:
        Lock object ID
    """

    cli = NeofsCli(shell, NEOFS_CLI_EXEC, wallet_config or WALLET_CONFIG)
    result = cli.object.lock(
        rpc_endpoint=endpoint,
        lifetime=lifetime,
        expire_at=expire_at,
        address=address,
        wallet=wallet,
        cid=cid,
        oid=oid,
        bearer=bearer,
        xhdr=xhdr,
        session=session,
        ttl=ttl,
    )

    # splitting CLI output to lines and taking the penultimate line
    id_str = result.stdout.strip().split("\n")[0]
    oid = id_str.split(":")[1]
    return oid.strip()


@allure.step("Search object")
def search_object(
    wallet: str,
    cid: str,
    shell: Shell,
    endpoint: str,
    bearer: str = "",
    filters: Optional[list] = None,
    expected_objects_list: Optional[list] = None,
    wallet_config: Optional[str] = None,
    xhdr: Optional[dict] = None,
    session: Optional[str] = None,
    phy: bool = False,
    root: bool = False,
    fail_on_assert=False,
) -> list:
    """
    SEARCH an Object.

    Args:
        wallet: wallet on whose behalf SEARCH is done
        cid: ID of Container where we get the Object from
        shell: executor for cli command
        bearer: path to Bearer Token file, appends to `--bearer` key
        endpoint: NeoFS endpoint to send request to, appends to `--rpc-endpoint` key
        filters: list of filter objects
        expected_objects_list: a list of ObjectIDs to compare found Objects with
        wallet_config: path to the wallet config
        xhdr: Request X-Headers in form of Key=Value
        session: path to a JSON-encoded container session token
        phy: Search physically stored objects.
        root: Search for user objects.
        fail_on_assert: fail if expected_objects_list is not matched to the found objects list.

    Returns:
        list of found ObjectIDs
    """

    cli = NeofsCli(shell, NEOFS_CLI_EXEC, wallet_config or WALLET_CONFIG)
    result = cli.object.search(
        rpc_endpoint=endpoint,
        wallet=wallet,
        cid=cid,
        bearer=bearer,
        xhdr=xhdr,
        filters=filters,
        session=session,
        phy=phy,
        root=root,
    )

    found_objects = re.findall(r"(\w{43,44})", result.stdout)

    if expected_objects_list:
        if sorted(found_objects) == sorted(expected_objects_list):
            logger.info(f"Found objects list '{found_objects}' is equal for expected list '{expected_objects_list}'")
        else:
            warning = f"Found object list {found_objects} is not equal to expected list '{expected_objects_list}'"
            logger.warning(warning)
            if fail_on_assert:
                raise AssertionError(warning)

    return found_objects


def parse_searchv2_output(raw_output: str) -> tuple[list[dict], Union[str, None]]:
    lines = raw_output.strip().split("\n")[1:]

    objects = []
    cursor = None
    current_object = None

    for line in lines:
        line = line.strip()

        if "Cursor" in line:
            cursor = line.split(":")[1].strip()
        elif re.match(r"^[a-zA-Z0-9]{40,}$", line):
            if current_object:
                objects.append(current_object)
            current_object = {"id": line, "attrs": []}
        elif ":" in line and current_object:
            splitted_line = line.split(":")
            if "Timestamp" in line:
                key = splitted_line[0].strip()
                value = ":".join(splitted_line[1:]).strip()
            else:
                key = ":".join(splitted_line[:-1]).strip()
                value = splitted_line[-1].strip()
            current_object["attrs"].append({key.strip(): value.strip()})

    if current_object:
        objects.append(current_object)

    return objects, cursor


def generate_filter_json_file(filters: list[str]) -> str:
    trasformed_filters = []
    for f in filters:
        parts = f.split(" ", 2)  # Split into three parts: key, matchType, value
        if len(parts) == 3:
            trasformed_filters.append(
                {"key": parts[0], "matchType": parts[1], "value": "" if parts[2] == '""' else parts[2]}
            )

    file_path = os.path.join(get_assets_dir_path(), TEST_FILES_DIR, f"filter_json_{uuid.uuid4()}")
    with open(file_path, "w") as file:
        json.dump(trasformed_filters, file)
    return file_path


@allure.step("Search object")
def search_objectv2(
    rpc_endpoint: str,
    wallet: str,
    cid: str,
    shell: Shell,
    filters: Optional[list] = None,
    attributes: Optional[list] = None,
    count: Optional[int] = None,
    cursor: Optional[str] = None,
    address: Optional[str] = None,
    bearer: Optional[str] = None,
    oid: Optional[str] = None,
    phy: bool = False,
    root: bool = False,
    session: Optional[str] = None,
    ttl: Optional[int] = None,
    xhdr: Optional[dict] = None,
    timeout: Optional[str] = None,
    wallet_config: Optional[str] = None,
) -> tuple[list[dict], Union[str, None]]:
    """
    SEARCH an Object.

    Args:
        address: Address of wallet account.
        bearer: File with signed JSON or binary encoded bearer token.
        cid: Container ID.
        filters: Repeated filter expressions or files with protobuf JSON.
        attributes: Additional attributes to display for suitable objects
        count: Max number of resulting items. Must not exceed 1000
        cursor: Cursor to continue previous search
        oid: Object ID.
        phy: Search physically stored objects.
        root: Search for user objects.
        rpc_endpoint: Remote node address (as 'multiaddr' or '<host>:<port>').
        session: Filepath to a JSON- or binary-encoded token of the object SEARCH session.
        ttl: TTL value in request meta header (default 2).
        wallet: WIF (NEP-2) string or path to the wallet or binary key.
        xhdr: Dict with request X-Headers.
        timeout: Timeout for the operation (default 15s).

    Returns:
        list of found objects as a dict: [{'oid': '123', 'attrs': [{'attr1': '123'}]}]
    """

    cli = NeofsCli(shell, NEOFS_CLI_EXEC, wallet_config or WALLET_CONFIG)
    result = cli.object.searchv2(
        rpc_endpoint=rpc_endpoint,
        wallet=wallet,
        cid=cid,
        filters=",".join(filters) if filters else None,
        attributes=",".join(attributes) if attributes else None,
        count=count,
        cursor=cursor,
        address=address,
        bearer=bearer,
        oid=oid,
        phy=phy,
        root=root,
        session=session,
        ttl=ttl,
        xhdr=xhdr,
        timeout=timeout,
    )

    return parse_searchv2_output(result.stdout)


@allure.step("Get netmap netinfo")
def get_netmap_netinfo(
    wallet: str,
    shell: Shell,
    endpoint: str,
    wallet_config: Optional[str] = None,
    address: Optional[str] = None,
    ttl: Optional[int] = None,
    xhdr: Optional[dict] = None,
) -> dict[str, Any]:
    """
    Get netmap netinfo output from node

    Args:
        wallet (str): wallet on whose behalf request is done
        shell: executor for cli command
        endpoint (optional, str): NeoFS endpoint to send request to, appends to `--rpc-endpoint` key
        address: Address of wallet account
        ttl: TTL value in request meta header (default 2)
        wallet: Path to the wallet or binary key
        xhdr: Request X-Headers in form of Key=Value

    Returns:
        (dict): dict of parsed command output
    """

    cli = NeofsCli(shell, NEOFS_CLI_EXEC, wallet_config or WALLET_CONFIG)
    output = cli.netmap.netinfo(
        wallet=wallet,
        rpc_endpoint=endpoint,
        address=address,
        ttl=ttl,
        xhdr=xhdr,
    )

    settings = dict()

    def str_to_bool(val: str) -> bool:
        if val == "true":
            return True
        elif val == "false":
            return False
        else:
            raise ValueError(f"Invalid value: {val}")

    patterns = [
        (re.compile(r"(.*): (\d+)"), int),
        (re.compile("(.*): (false|true)"), str_to_bool),
        (re.compile(r"(.*): (\d+\.\d+)"), float),
    ]
    for pattern, func in patterns:
        for setting, value in re.findall(pattern, output.stdout):
            settings[setting.lower().strip().replace(" ", "_")] = func(value)

    return settings


@allure.step("Head object")
def head_object(
    wallet: str,
    cid: str,
    oid: str,
    shell: Shell,
    endpoint: str,
    bearer: str = "",
    xhdr: Optional[dict] = None,
    json_output: bool = True,
    is_raw: bool = False,
    is_direct: bool = False,
    wallet_config: Optional[str] = None,
    session: Optional[str] = None,
):
    """
    HEAD an Object.

    Args:
        wallet (str): wallet on whose behalf HEAD is done
        cid (str): ID of Container where we get the Object from
        oid (str): ObjectID to HEAD
        shell: executor for cli command
        bearer (optional, str): path to Bearer Token file, appends to `--bearer` key
        endpoint(optional, str): NeoFS endpoint to send request to
        json_output(optional, bool): return response in JSON format or not; this flag
                                    turns into `--json` key
        is_raw(optional, bool): send "raw" request or not; this flag
                                    turns into `--raw` key
        is_direct(optional, bool): send request directly to the node or not; this flag
                                    turns into `--ttl 1` key
        wallet_config(optional, str): path to the wallet config
        xhdr (optional, dict): Request X-Headers in form of Key=Value
        session (optional, dict): path to a JSON-encoded container session token
    Returns:
        depending on the `json_output` parameter value, the function returns
        (dict): HEAD response in JSON format
        or
        (str): HEAD response as a plain text
    """

    cli = NeofsCli(shell, NEOFS_CLI_EXEC, wallet_config or WALLET_CONFIG)
    result = cli.object.head(
        rpc_endpoint=endpoint,
        wallet=wallet,
        cid=cid,
        oid=oid,
        bearer=bearer,
        json_mode=json_output,
        raw=is_raw,
        ttl=1 if is_direct else None,
        xhdr=xhdr,
        session=session,
    )

    if not json_output:
        return result

    try:
        decoded = json.loads(result.stdout)
    except Exception as exc:
        # If we failed to parse output as JSON, the cause might be
        # the plain text string in the beginning of the output.
        # Here we cut off first string and try to parse again.
        logger.info(f"failed to parse output: {exc}")
        logger.info("parsing output in another way")
        fst_line_idx = result.stdout.find("\n")
        decoded = json.loads(result.stdout[fst_line_idx:])

    # If response is Complex Object header, it has `splitId` key
    if "splitId" in decoded.keys():
        logger.info("decoding split header")
        return json_transformers.decode_split_header(decoded)

    # If response is Last or Linking Object header,
    # it has `header` dictionary and non-null `split` dictionary
    if "split" in decoded["header"].keys():
        if decoded["header"]["split"]:
            logger.info("decoding linking object")
            return json_transformers.decode_linking_object(decoded)

    if decoded["header"]["objectType"] == "STORAGE_GROUP":
        logger.info("decoding storage group")
        return json_transformers.decode_storage_group(decoded)

    if decoded["header"]["objectType"] == "TOMBSTONE":
        logger.info("decoding tombstone")
        return json_transformers.decode_tombstone(decoded)

    logger.info("decoding simple header")
    return json_transformers.decode_simple_header(decoded)
