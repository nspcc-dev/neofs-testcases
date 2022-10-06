#!/usr/bin/python3

import json

import allure
import neofs_verbs
from neo3 import wallet


@allure.step("Verify Head Tombstone")
def verify_head_tombstone(wallet_path: str, cid: str, oid_ts: str, oid: str):
    header = neofs_verbs.head_object(wallet_path, cid, oid_ts)
    header = header["header"]
    assert header["containerID"] == cid, "Tombstone Header CID is wrong"

    wlt_data = dict()
    with open(wallet_path, "r") as fout:
        wlt_data = json.loads(fout.read())
    wlt = wallet.Wallet.from_json(wlt_data, password="")
    addr = wlt.accounts[0].address

    assert header["ownerID"] == addr, "Tombstone Owner ID is wrong"
    assert header["objectType"] == "TOMBSTONE", "Header Type isn't Tombstone"
    assert (
        header["sessionToken"]["body"]["object"]["verb"] == "DELETE"
    ), "Header Session Type isn't DELETE"
    assert (
        header["sessionToken"]["body"]["object"]["address"]["containerID"] == cid
    ), "Header Session ID is wrong"
    assert (
        header["sessionToken"]["body"]["object"]["address"]["objectID"] == oid
    ), "Header Session OID is wrong"
