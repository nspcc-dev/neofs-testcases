import logging

import allure
from neofs_testlib.shell import Shell
from neofs_verbs import head_object
from neofs_testlib.utils.wallet import get_first_address_from_wallet

logger = logging.getLogger("NeoLogger")


@allure.step("Verify Head Tombstone")
def verify_head_tombstone(wallet_path: str, cid: str, oid_ts: str, oid: str, shell: Shell):
    header = head_object(wallet_path, cid, oid_ts, shell=shell)["header"]

    s_oid = header["sessionToken"]["body"]["object"]["target"]["objects"]
    logger.info(f"Header Session OIDs is {s_oid}")
    logger.info(f"OID is {oid}")

    assert header["containerID"] == cid, "Tombstone Header CID is wrong"

    addr = get_first_address_from_wallet(wallet_path)

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
