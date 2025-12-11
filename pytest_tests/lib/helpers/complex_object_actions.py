import logging
import string
import time
from typing import Optional, Tuple

import allure
from helpers.grpc_responses import OBJECT_NOT_FOUND, error_matches_status
from helpers.neofs_verbs import get_object, head_object, search_objectv2
from helpers.storage_object_info import StorageObjectInfo
from neofs_testlib.env.env import NeoFSEnv, StorageNode
from neofs_testlib.shell import Shell

logger = logging.getLogger("NeoLogger")


def get_object_chunks(
    wallet_file_path: string,
    cid: string,
    oid: string,
    shell: Shell,
    neofs_env: NeoFSEnv,
    bearer: str = None,
) -> list[Tuple[str, int]]:
    """
    Get complex objects' IDs and sizes (no link object)

    Args:
    wallet_file_path: wallet to use
    cid: object's container
    oid: object's ID
    storage_object: storage_object to get it's chunks
    shell: client shell to do cmd requests
    bearer: bearer token to access chunks
    cluster: cluster object under test

    Returns:
    list of tuples (object_id, object_size) of complex object chunks
    """

    with allure.step(f"Get complex object chunks (f{oid})"):
        link_object_id = get_link_object(
            wallet_file_path,
            cid,
            oid,
            neofs_env,
        )

        link_obj_path = get_object(
            wallet=wallet_file_path, cid=cid, oid=link_object_id, shell=shell, endpoint=neofs_env.sn_rpc, bearer=bearer
        )

        resp = neofs_env.neofs_lens().object.link(link_obj_path)

        # exclude helper prompt
        raw_children = resp.stdout.splitlines()[1:]
        # exclude punctuation
        children = [x.translate(str.maketrans("", "", string.punctuation)) for x in raw_children]

        return [(x.split()[-1], int(x.split()[1])) for x in children]


def get_ec_object_chunks(
    wallet_file_path: string,
    cid: string,
    oid: string,
    neofs_env: NeoFSEnv,
) -> list[Tuple[str, int]]:
    """
    Get complex objects' IDs (no link object)

    Args:
    wallet_file_path: wallet to use
    cid: object's container
    oid: object's ID
    neofs_env: NeoFSEnv

    Returns:
    list of tuples (object_id, object_size) of ec object chunks
    object_size is left for backward compatibility with regular get_object_chunks
    """

    with allure.step(f"Get complex object chunks (f{oid})"):
        found_objects, _ = search_objectv2(
            rpc_endpoint=neofs_env.sn_rpc,
            wallet=wallet_file_path,
            cid=cid,
            shell=neofs_env.shell,
            filters=["$Object:objectType NE LINK"],
        )
        return [(found_obj["id"], 1) for found_obj in found_objects if found_obj["id"] != oid]


def get_complex_object_split_ranges(
    storage_object: StorageObjectInfo, shell: Shell, neofs_env: NeoFSEnv
) -> list[Tuple[int, int]]:
    """
    Get list of split ranges tuples (offset, length) of a complex object
    For example if object size if 100 and max object size in system is 30
    the returned list should be
    [(0, 30), (30, 30), (60, 30), (90, 10)]

    Args:
    storage_object: storage_object to get it's chunks
    shell: client shell to do cmd requests
    cluster: cluster object under test

    Returns:
    list of object ids of complex object chunks
    """

    ranges: list = []
    offset = 0
    chunks = get_object_chunks(
        storage_object.wallet_file_path, storage_object.cid, storage_object.oid, shell, neofs_env
    )
    for chunk in chunks:
        head = head_object(
            storage_object.wallet_file_path,
            storage_object.cid,
            chunk[0],
            shell,
            neofs_env.sn_rpc,
        )

        length = int(head["header"]["payloadLength"])
        ranges.append((offset, length))

        offset = offset + length

    return ranges


@allure.step("Get Link Object")
def get_link_object(
    wallet: str,
    cid: str,
    oid: str,
    neofs_env: NeoFSEnv,
):
    """
    Args:
        wallet (str): path to the wallet on whose behalf the Storage Nodes
                        are requested
        cid (str): Container ID which stores the Large Object
        oid (str): Large Object ID
        neofs_env: NeoFSEnv
    Returns:
        (str): Link Object ID
        When no Link Object ID is found after all Storage Nodes polling,
        the function throws an error.
    """
    found_objects, _ = search_objectv2(
        rpc_endpoint=neofs_env.sn_rpc,
        wallet=wallet,
        cid=cid,
        shell=neofs_env.shell,
        filters=["$Object:objectType EQ LINK", f"$Object:split.parent EQ {oid}"],
    )
    if not found_objects:
        raise AssertionError(f"No link object found for {cid=}; {oid=}")
    return found_objects[0]["id"]


@allure.step("Get Last Object")
def get_last_object(wallet: str, cid: str, oid: str, shell: Shell, nodes: list[StorageNode]) -> Optional[str]:
    """
    Args:
        wallet (str): path to the wallet on whose behalf the Storage Nodes
                        are requested
        cid (str): Container ID which stores the Large Object
        oid (str): Large Object ID
        shell: executor for cli command
        nodes: list of nodes to do search on
    Returns:
        (str): Last Object ID
        When no Last Object ID is found after all Storage Nodes polling,
        the function throws an error.
    """
    for node in nodes:
        endpoint = node.endpoint
        try:
            resp = head_object(wallet, cid, oid, shell=shell, endpoint=endpoint, is_raw=True, is_direct=True)
            if resp["lastPart"]:
                return resp["lastPart"]
        except Exception:
            logger.info(f"No Last Object found on {endpoint}; continue")
    logger.error(f"No Last Object for {cid}/{oid} found among all Storage Nodes")
    return None


@allure.step("Get Object Copies")
def get_object_copies(complexity: str, wallet: str, cid: str, oid: str, shell: Shell, nodes: list[StorageNode]) -> int:
    """
    The function performs requests to all nodes of the container and
    finds out if they store a copy of the object. The procedure is
    different for simple and complex object, so the function requires
    a sign of object complexity.
    Args:
        complexity (str): the tag of object size and complexity,
                            [Simple|Complex]
        wallet (str): the path to the wallet on whose behalf the
                            copies are got
        cid (str): ID of the container
        oid (str): ID of the Object
        shell: executor for cli command
    Returns:
        (int): the number of object copies in the container
    """
    return (
        get_simple_object_copies(wallet, cid, oid, shell, nodes)
        if complexity == "Simple"
        else get_complex_object_copies(wallet, cid, oid, shell, nodes)
    )


@allure.step("Get Simple Object Copies")
def get_simple_object_copies(wallet: str, cid: str, oid: str, shell: Shell, nodes: list[StorageNode]) -> int:
    """
    To figure out the number of a simple object copies, only direct
    HEAD requests should be made to the every node of the container.
    We consider non-empty HEAD response as a stored object copy.
    Args:
        wallet (str): the path to the wallet on whose behalf the
                            copies are got
        cid (str): ID of the container
        oid (str): ID of the Object
        shell: executor for cli command
        nodes: nodes to search on
    Returns:
        (int): the number of object copies in the container
    """
    copies = 0
    for node in nodes:
        try:
            response = head_object(wallet, cid, oid, shell=shell, endpoint=node.endpoint, is_direct=True)
            if response:
                logger.info(f"Found object {oid} on node {node}")
                copies += 1
        except Exception:
            logger.info(f"No {oid} object copy found on {node}, continue")
            continue
    return copies


@allure.step("Get Complex Object Copies")
def get_complex_object_copies(wallet: str, cid: str, oid: str, shell: Shell, nodes: list[StorageNode]) -> int:
    """
    To figure out the number of a complex object copies, we firstly
    need to retrieve its Last object. We consider that the number of
    complex object copies is equal to the number of its last object
    copies. When we have the Last object ID, the task is reduced
    to getting simple object copies.
    Args:
        wallet (str): the path to the wallet on whose behalf the
                            copies are got
        cid (str): ID of the container
        oid (str): ID of the Object
        shell: executor for cli command
    Returns:
        (int): the number of object copies in the container
    """
    last_oid = get_last_object(wallet, cid, oid, shell, nodes)
    assert last_oid, f"No Last Object for {cid}/{oid} found among all Storage Nodes"
    return get_simple_object_copies(wallet, cid, last_oid, shell, nodes)


@allure.step("Get Nodes With Object")
def get_nodes_with_object(
    cid: str, oid: str, shell: Shell, nodes: list[StorageNode], neofs_env: NeoFSEnv
) -> list[StorageNode]:
    """
    The function returns list of nodes which store
    the given object.
    Args:
         cid (str): ID of the container which store the object
         oid (str): object ID
         shell: executor for cli command
         nodes: nodes to find on
    Returns:
         (list): nodes which store the object
    """

    nodes_list = []
    for node in nodes:
        try:
            res = head_object(
                node.wallet.path,
                cid,
                oid,
                shell=shell,
                endpoint=node.endpoint,
                is_direct=True,
                wallet_config=node.cli_config,
            )
            if res is not None:
                logger.info(f"Found object {oid} on node {node}")
                nodes_list.append(node)
        except Exception:
            logger.info(f"No {oid} object copy found on {node}, continue")
            continue
    return nodes_list


@allure.step("Get Nodes Without Object")
def get_nodes_without_object(
    wallet: str, cid: str, oid: str, shell: Shell, nodes: list[StorageNode]
) -> list[StorageNode]:
    """
    The function returns list of nodes which do not store
    the given object.
    Args:
         wallet (str): the path to the wallet on whose behalf
                     we request the nodes
         cid (str): ID of the container which store the object
         oid (str): object ID
         shell: executor for cli command
    Returns:
         (list): nodes which do not store the object
    """
    nodes_list = []
    for node in nodes:
        try:
            res = head_object(wallet, cid, oid, shell=shell, endpoint=node.endpoint, is_direct=True)
            if res is None:
                nodes_list.append(node)
        except Exception as err:
            if error_matches_status(err, OBJECT_NOT_FOUND):
                nodes_list.append(node)
            else:
                raise Exception(f"Got error {err} on head object command") from err
    return nodes_list


@allure.step("Wait for object replication")
def wait_object_replication(
    cid: str,
    oid: str,
    expected_copies: int,
    shell: Shell,
    nodes: list[StorageNode],
    neofs_env: NeoFSEnv,
) -> list[StorageNode]:
    sleep_interval, attempts = 15, 20
    nodes_with_object = []
    for _ in range(attempts):
        nodes_with_object = get_nodes_with_object(cid, oid, shell=shell, nodes=nodes, neofs_env=neofs_env)
        if len(nodes_with_object) >= expected_copies:
            return nodes_with_object
        time.sleep(sleep_interval)
    raise AssertionError(
        f"Expected {expected_copies} copies of object, but found {len(nodes_with_object)}. "
        f"Waiting time {sleep_interval * attempts}"
    )
