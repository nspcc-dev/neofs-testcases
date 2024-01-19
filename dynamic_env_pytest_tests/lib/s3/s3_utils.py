import logging

import allure
import neofs_verbs
from neofs_testlib.env.env import StorageNode
from neofs_testlib.shell import Shell

logger = logging.getLogger("NeoLogger")


@allure.step("Get Simple Object Copies")
def get_simple_object_copies(
    wallet: str, cid: str, oid: str, shell: Shell, nodes: list[StorageNode]
) -> int:
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
            response = neofs_verbs.head_object(
                wallet, cid, oid, shell=shell, endpoint=node.endpoint, is_direct=True
            )
            if response:
                logger.info(f"Found object {oid} on node {node}")
                copies += 1
        except Exception:
            logger.info(f"No {oid} object copy found on {node}, continue")
            continue
    return copies
