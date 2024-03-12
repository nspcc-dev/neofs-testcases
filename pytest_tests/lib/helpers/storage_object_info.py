import json
import logging
from dataclasses import dataclass
from time import sleep
from typing import Optional

import allure
import neofs_env.neofs_epoch as neofs_epoch
import pytest
from helpers.common import WALLET_PASS
from helpers.grpc_responses import OBJECT_ALREADY_REMOVED
from helpers.neofs_verbs import delete_object, get_object, head_object
from neo3.wallet import wallet
from neofs_testlib.env.env import NeoFSEnv
from neofs_testlib.shell import Shell

logger = logging.getLogger("NeoLogger")

CLEANUP_TIMEOUT = 10


@dataclass
class ObjectRef:
    cid: str
    oid: str


@dataclass
class LockObjectInfo(ObjectRef):
    lifetime: Optional[int] = None
    expire_at: Optional[int] = None


@dataclass
class StorageObjectInfo(ObjectRef):
    size: Optional[int] = None
    wallet_file_path: Optional[str] = None
    file_path: Optional[str] = None
    file_hash: Optional[str] = None
    attributes: Optional[list[dict[str, str]]] = None
    tombstone: Optional[str] = None
    locks: Optional[list[LockObjectInfo]] = None


@allure.step("Verify Head Tombstone")
def verify_head_tombstone(
    wallet_path: str, cid: str, oid_ts: str, oid: str, shell: Shell, endpoint: str
):
    header = head_object(wallet_path, cid, oid_ts, shell=shell, endpoint=endpoint)["header"]

    s_oid = header["sessionToken"]["body"]["object"]["target"]["objects"]
    logger.info(f"Header Session OIDs is {s_oid}")
    logger.info(f"OID is {oid}")

    assert header["containerID"] == cid, "Tombstone Header CID is wrong"

    with open(wallet_path, "r") as file:
        wlt_data = json.loads(file.read())
    wlt = wallet.Wallet.from_json(wlt_data, passwords=[WALLET_PASS])
    addr = wlt.accounts[0].address

    assert header["ownerID"] == addr, "Tombstone Owner ID is wrong"
    assert header["objectType"] == "TOMBSTONE", "Header Type isn't Tombstone"
    assert (
        header["sessionToken"]["body"]["object"]["verb"] == "DELETE"
    ), "Header Session Type isn't DELETE"
    assert (
        header["sessionToken"]["body"]["object"]["target"]["container"] == cid
    ), "Header Session ID is wrong"
    assert (
        oid in header["sessionToken"]["body"]["object"]["target"]["objects"]
    ), "Header Session OID is wrong"


@allure.step("Delete Objects")
def delete_objects(
    storage_objects: list[StorageObjectInfo], shell: Shell, neofs_env: NeoFSEnv
) -> None:
    """
    Deletes given storage objects.

    Args:
        storage_objects: list of objects to delete
        shell: executor for cli command
    """

    with allure.step("Delete objects"):
        for storage_object in storage_objects:
            storage_object.tombstone = delete_object(
                storage_object.wallet_file_path,
                storage_object.cid,
                storage_object.oid,
                shell=shell,
                endpoint=neofs_env.sn_rpc,
            )
            verify_head_tombstone(
                wallet_path=storage_object.wallet_file_path,
                cid=storage_object.cid,
                oid_ts=storage_object.tombstone,
                oid=storage_object.oid,
                shell=shell,
                endpoint=neofs_env.sn_rpc,
            )

    current_epoch = neofs_epoch.get_epoch(neofs_env)
    neofs_epoch.tick_epoch(neofs_env)
    neofs_epoch.wait_for_epochs_align(neofs_env, current_epoch)
    sleep(CLEANUP_TIMEOUT)

    with allure.step("Get objects and check errors"):
        for storage_object in storage_objects:
            with pytest.raises(Exception, match=OBJECT_ALREADY_REMOVED):
                get_object(
                    storage_object.wallet_file_path,
                    storage_object.cid,
                    storage_object.oid,
                    shell=shell,
                    endpoint=neofs_env.sn_rpc,
                )
