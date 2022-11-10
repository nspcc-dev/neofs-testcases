import logging
from time import sleep, time

import allure
import pytest
from common import STORAGE_NODE_SERVICE_NAME_REGEX
from epoch import tick_epoch
from grpc_responses import OBJECT_ALREADY_REMOVED
from neofs_testlib.hosting import Hosting
from neofs_testlib.shell import Shell
from python_keywords.neofs_verbs import delete_object, get_object, head_object
from storage_object_info import StorageObjectInfo
from tombstone import verify_head_tombstone

logger = logging.getLogger("NeoLogger")

CLEANUP_TIMEOUT = 10


@allure.step("Waiting until object will be available on all nodes")
def wait_until_objects_available_on_all_nodes(
    hosting: Hosting,
    storage_objects: list[StorageObjectInfo],
    shell: Shell,
    max_wait_time: int = 60,
) -> None:
    start = time()

    def wait_for_objects():
        for service_config in hosting.find_service_configs(STORAGE_NODE_SERVICE_NAME_REGEX):
            endpoint = service_config.attributes["rpc_endpoint"]
            for storage_object in storage_objects:
                head_object(
                    storage_object.wallet,
                    storage_object.cid,
                    storage_object.oid,
                    shell,
                    endpoint=endpoint,
                )

    while start + max_wait_time >= time():
        try:
            wait_for_objects()
            return
        except Exception as ex:
            logger.debug(ex)
            sleep(1)

    raise ex


def delete_objects(storage_objects: list[StorageObjectInfo], shell: Shell) -> None:
    """
    Deletes given storage objects.

    Args:
        storage_objects: list of objects to delete
        shell: executor for cli command
    """

    with allure.step("Delete objects"):
        for storage_object in storage_objects:
            storage_object.tombstone = delete_object(
                storage_object.wallet, storage_object.cid, storage_object.oid, shell
            )
            verify_head_tombstone(
                wallet_path=storage_object.wallet,
                cid=storage_object.cid,
                oid_ts=storage_object.tombstone,
                oid=storage_object.oid,
                shell=shell,
            )

    tick_epoch(shell=shell)
    sleep(CLEANUP_TIMEOUT)

    with allure.step("Get objects and check errors"):
        for storage_object in storage_objects:
            with pytest.raises(Exception, match=OBJECT_ALREADY_REMOVED):
                get_object(
                    storage_object.wallet,
                    storage_object.cid,
                    storage_object.oid,
                    shell=shell,
                )
