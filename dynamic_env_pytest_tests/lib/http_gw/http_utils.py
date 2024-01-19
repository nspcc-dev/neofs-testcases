import random

import allure
import neofs_verbs
from grpc_responses import OBJECT_NOT_FOUND, error_matches_status
from neofs_testlib.env.env import StorageNode
from neofs_testlib.shell import Shell
from python_keywords.http_gate import assert_hashes_are_equal, get_via_http_gate
from python_keywords.neofs_verbs import get_object


def get_object_and_verify_hashes(
    oid: str,
    file_name: str,
    wallet: str,
    cid: str,
    shell: Shell,
    nodes: list[StorageNode],
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

    object_getter = object_getter or get_via_http_gate

    got_file_path = get_object(
        wallet=wallet,
        cid=cid,
        oid=oid,
        shell=shell,
        endpoint=random_node.endpoint,
    )
    got_file_path_http = object_getter(cid=cid, oid=oid, endpoint=endpoint)

    assert_hashes_are_equal(file_name, got_file_path, got_file_path_http)


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
            res = neofs_verbs.head_object(
                wallet, cid, oid, shell=shell, endpoint=node.endpoint, is_direct=True
            )
            if res is None:
                nodes_list.append(node)
        except Exception as err:
            if error_matches_status(err, OBJECT_NOT_FOUND):
                nodes_list.append(node)
            else:
                raise Exception(f"Got error {err} on head object command") from err
    return nodes_list
