import json
import logging
import os
import random
import re
import uuid
from typing import Any, Optional

import allure
import json_transformers
from common import ASSETS_DIR, NEOFS_CLI_EXEC, NEOFS_ENDPOINT, NEOFS_NETMAP, WALLET_CONFIG
from neofs_testlib.cli import NeofsCli
from neofs_testlib.shell import Shell

logger = logging.getLogger("NeoLogger")


@allure.step("Get object")
def get_object(
    wallet: str,
    cid: str,
    oid: str,
    shell: Shell,
    bearer: Optional[str] = None,
    write_object: str = "",
    endpoint: str = "",
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
        bearer (optional, str): path to Bearer Token file, appends to `--bearer` key
        write_object (optional, str): path to downloaded file, appends to `--file` key
        endpoint (optional, str): NeoFS endpoint to send request to, appends to `--rpc-endpoint` key
        wallet_config(optional, str): path to the wallet config
        no_progress(optional, bool): do not show progress bar
        xhdr (optional, dict): Request X-Headers in form of Key=Value
        session (optional, dict): path to a JSON-encoded container session token
    Returns:
        (str): path to downloaded file
    """

    if not write_object:
        write_object = str(uuid.uuid4())
    file_path = os.path.join(ASSETS_DIR, write_object)

    if not endpoint:
        endpoint = random.sample(NEOFS_NETMAP, 1)[0]

    cli = NeofsCli(shell, NEOFS_CLI_EXEC, wallet_config or WALLET_CONFIG)
    cli.object.get(
        rpc_endpoint=endpoint or NEOFS_ENDPOINT,
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


@allure.step("Get Range Hash")
def get_range_hash(
    wallet: str,
    cid: str,
    oid: str,
    range_cut: str,
    shell: Shell,
    bearer: Optional[str] = None,
    endpoint: Optional[str] = None,
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
        rpc_endpoint=endpoint or NEOFS_ENDPOINT,
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


@allure.step("Put object")
def put_object(
    wallet: str,
    path: str,
    cid: str,
    shell: Shell,
    bearer: Optional[str] = None,
    attributes: Optional[dict] = None,
    xhdr: Optional[dict] = None,
    endpoint: Optional[str] = None,
    wallet_config: Optional[str] = None,
    expire_at: Optional[int] = None,
    no_progress: bool = True,
    session: Optional[str] = None,
):
    """
    PUT of given file.

    Args:
        wallet (str): wallet on whose behalf PUT is done
        path (str): path to file to be PUT
        cid (str): ID of Container where we get the Object from
        shell: executor for cli command
        bearer (optional, str): path to Bearer Token file, appends to `--bearer` key
        attributes (optional, str): User attributes in form of Key1=Value1,Key2=Value2
        endpoint(optional, str): NeoFS endpoint to send request to
        wallet_config(optional, str): path to the wallet config
        no_progress(optional, bool): do not show progress bar
        expire_at (optional, int): Last epoch in the life of the object
        xhdr (optional, dict): Request X-Headers in form of Key=Value
        session (optional, dict): path to a JSON-encoded container session token
    Returns:
        (str): ID of uploaded Object
    """
    if not endpoint:
        endpoint = random.sample(NEOFS_NETMAP, 1)[0]
        if not endpoint:
            logger.info(f"---DEB:\n{NEOFS_NETMAP}")

    cli = NeofsCli(shell, NEOFS_CLI_EXEC, wallet_config or WALLET_CONFIG)
    result = cli.object.put(
        rpc_endpoint=endpoint,
        wallet=wallet,
        file=path,
        cid=cid,
        attributes=attributes,
        bearer=bearer,
        expire_at=expire_at,
        no_progress=no_progress,
        xhdr=xhdr,
        session=session,
    )

    # splitting CLI output to lines and taking the penultimate line
    id_str = result.stdout.strip().split("\n")[-2]
    oid = id_str.split(":")[1]
    return oid.strip()


@allure.step("Delete object")
def delete_object(
    wallet: str,
    cid: str,
    oid: str,
    shell: Shell,
    endpoint: Optional[str] = None,
    bearer: str = "",
    wallet_config: Optional[str] = None,
    xhdr: Optional[dict] = None,
    session: Optional[str] = None,
):
    """
    DELETE an Object.

    Args:
        wallet (str): wallet on whose behalf DELETE is done
        cid (str): ID of Container where we get the Object from
        oid (str): ID of Object we are going to delete
        shell: executor for cli command
        bearer (optional, str): path to Bearer Token file, appends to `--bearer` key
        endpoint (optional, str): NeoFS endpoint to send request to, appends to `--rpc-endpoint` key
        wallet_config(optional, str): path to the wallet config
        xhdr (optional, dict): Request X-Headers in form of Key=Value
        session (optional, dict): path to a JSON-encoded container session token
    Returns:
        (str): Tombstone ID
    """
    cli = NeofsCli(shell, NEOFS_CLI_EXEC, wallet_config or WALLET_CONFIG)
    result = cli.object.delete(
        rpc_endpoint=endpoint or NEOFS_ENDPOINT,
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
    endpoint: Optional[str] = None,
    wallet_config: Optional[str] = None,
    bearer: str = "",
    xhdr: Optional[dict] = None,
    session: Optional[str] = None,
):
    """
    GETRANGE an Object.

    Args:
        wallet (str): wallet on whose behalf GETRANGE is done
        cid (str): ID of Container where we get the Object from
        oid (str): ID of Object we are going to request
        range_cut (str): range to take data from in the form offset:length
        shell: executor for cli command
        endpoint (optional, str): NeoFS endpoint to send request to, appends to `--rpc-endpoint` key
        bearer (optional, str): path to Bearer Token file, appends to `--bearer` key
        wallet_config(optional, str): path to the wallet config
        xhdr (optional, dict): Request X-Headers in form of Key=Value
        session (optional, dict): path to a JSON-encoded container session token
    Returns:
        (str, bytes) - path to the file with range content and content of this file as bytes
    """
    range_file_path = os.path.join(ASSETS_DIR, str(uuid.uuid4()))

    cli = NeofsCli(shell, NEOFS_CLI_EXEC, wallet_config or WALLET_CONFIG)
    cli.object.range(
        rpc_endpoint=endpoint or NEOFS_ENDPOINT,
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
    lifetime: Optional[int] = None,
    expire_at: Optional[int] = None,
    endpoint: Optional[str] = None,
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
        endpoint: Remote node address.
        session: Path to a JSON-encoded container session token.
        ttl: TTL value in request meta header (default 2).
        wallet: WIF (NEP-2) string or path to the wallet or binary key.
        xhdr: Dict with request X-Headers.

    Returns:
        Lock object ID
    """

    cli = NeofsCli(shell, NEOFS_CLI_EXEC, wallet_config or WALLET_CONFIG)
    result = cli.object.lock(
        rpc_endpoint=endpoint or NEOFS_ENDPOINT,
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
    bearer: str = "",
    endpoint: Optional[str] = None,
    filters: Optional[dict] = None,
    expected_objects_list: Optional[list] = None,
    wallet_config: Optional[str] = None,
    xhdr: Optional[dict] = None,
    session: Optional[str] = None,
    phy: bool = False,
    root: bool = False,
) -> list:
    """
    SEARCH an Object.

    Args:
        wallet (str): wallet on whose behalf SEARCH is done
        cid (str): ID of Container where we get the Object from
        shell: executor for cli command
        bearer (optional, str): path to Bearer Token file, appends to `--bearer` key
        endpoint (optional, str): NeoFS endpoint to send request to, appends to `--rpc-endpoint` key
        filters (optional, dict): key=value pairs to filter Objects
        expected_objects_list (optional, list): a list of ObjectIDs to compare found Objects with
        wallet_config(optional, str): path to the wallet config
        xhdr (optional, dict): Request X-Headers in form of Key=Value
        session (optional, dict): path to a JSON-encoded container session token
        phy: Search physically stored objects.
        root: Search for user objects.

    Returns:
        (list): list of found ObjectIDs
    """

    cli = NeofsCli(shell, NEOFS_CLI_EXEC, wallet_config or WALLET_CONFIG)
    result = cli.object.search(
        rpc_endpoint=endpoint or NEOFS_ENDPOINT,
        wallet=wallet,
        cid=cid,
        bearer=bearer,
        xhdr=xhdr,
        filters=[f"{filter_key} EQ {filter_val}" for filter_key, filter_val in filters.items()]
        if filters
        else None,
        session=session,
        phy=phy,
        root=root,
    )

    found_objects = re.findall(r"(\w{43,44})", result.stdout)

    if expected_objects_list:
        if sorted(found_objects) == sorted(expected_objects_list):
            logger.info(
                f"Found objects list '{found_objects}' "
                f"is equal for expected list '{expected_objects_list}'"
            )
        else:
            logger.warning(
                f"Found object list {found_objects} "
                f"is not equal to expected list '{expected_objects_list}'"
            )

    return found_objects


@allure.step("Get netmap netinfo")
def get_netmap_netinfo(
    wallet: str,
    shell: Shell,
    wallet_config: Optional[str] = None,
    endpoint: Optional[str] = None,
    address: Optional[str] = None,
    ttl: Optional[int] = None,
    xhdr: Optional[dict] = None,
) -> dict[str, Any]:
    """
    Get netmap netinfo output from node

    Args:
        wallet (str): wallet on whose behalf SEARCH is done
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
        rpc_endpoint=endpoint or NEOFS_ENDPOINT,
        address=address,
        ttl=ttl,
        xhdr=xhdr,
    )

    settings = dict()

    patterns = [
        (re.compile("(.*): (\d+)"), int),
        (re.compile("(.*): (false|true)"), bool),
        (re.compile("(.*): (\d+\.\d+)"), float),
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
    bearer: str = "",
    xhdr: Optional[dict] = None,
    endpoint: Optional[str] = None,
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
        rpc_endpoint=endpoint or NEOFS_ENDPOINT,
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
