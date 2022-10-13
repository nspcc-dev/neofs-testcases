import logging
from time import sleep
from typing import Optional

import allure
from common import NEOFS_NETMAP_DICT
from neofs_testlib.shell import Shell
from python_keywords.node_management import node_healthcheck
from storage_policy import get_nodes_with_object

logger = logging.getLogger("NeoLogger")


@allure.step("Wait for object replication")
def wait_object_replication_on_nodes(
    wallet: str,
    cid: str,
    oid: str,
    expected_copies: int,
    shell: Shell,
    excluded_nodes: Optional[list[str]] = None,
) -> list[str]:
    excluded_nodes = excluded_nodes or []
    sleep_interval, attempts = 10, 18
    nodes = []
    for __attempt in range(attempts):
        nodes = get_nodes_with_object(wallet, cid, oid, shell=shell, skip_nodes=excluded_nodes)
        if len(nodes) == expected_copies:
            return nodes
        sleep(sleep_interval)
    raise AssertionError(
        f"Expected {expected_copies} copies of object, but found {len(nodes)}. "
        f"Waiting time {sleep_interval * attempts}"
    )


@allure.step("Wait for storage node returned to cluster")
def wait_all_storage_node_returned():
    sleep_interval, attempts = 15, 20
    for __attempt in range(attempts):
        if is_all_storage_node_returned():
            return
        sleep(sleep_interval)
    raise AssertionError("Storage node(s) is broken")


def is_all_storage_node_returned() -> bool:
    with allure.step("Run health check for all storage nodes"):
        for node_name in NEOFS_NETMAP_DICT.keys():
            try:
                health_check = node_healthcheck(node_name)
            except Exception as err:
                logger.warning(f"Node healthcheck fails with error {err}")
                return False
            if health_check.health_status != "READY" or health_check.network_status != "ONLINE":
                return False
    return True
