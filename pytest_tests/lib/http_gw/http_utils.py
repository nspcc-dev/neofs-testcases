import random

from neofs_testlib.env.env import StorageNode
from neofs_testlib.shell import Shell
from helpers.http_gate import assert_hashes_are_equal, get_via_http_gate
from helpers.neofs_verbs import get_object
from helpers.complex_object_actions import get_nodes_without_object


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
