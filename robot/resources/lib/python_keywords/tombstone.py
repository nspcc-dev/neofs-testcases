#!/usr/bin/python3

import allure
import json

import neofs_verbs
from neo3 import wallet
from robot.libraries.BuiltIn import BuiltIn

ROBOT_AUTO_KEYWORDS = False


@allure.step('Verify Head Tombstone')
def verify_head_tombstone(wallet_path: str, cid: str, oid_ts: str, oid: str):
    header = neofs_verbs.head_object(wallet_path, cid, oid_ts)
    header = header['header']

    BuiltIn().should_be_equal(header["containerID"], cid,
                              msg="Tombstone Header CID is wrong")

    wlt_data = dict()
    with open(wallet_path, 'r') as fout:
        wlt_data = json.loads(fout.read())
    wlt = wallet.Wallet.from_json(wlt_data, password='')
    addr = wlt.accounts[0].address

    BuiltIn().should_be_equal(header["ownerID"], addr,
                              msg="Tombstone Owner ID is wrong")

    BuiltIn().should_be_equal(header["objectType"], 'TOMBSTONE',
                              msg="Header Type isn't Tombstone")

    BuiltIn().should_be_equal(
        header["sessionToken"]["body"]["object"]["verb"], 'DELETE',
        msg="Header Session Type isn't DELETE"
    )

    BuiltIn().should_be_equal(
        header["sessionToken"]["body"]["object"]["address"]["containerID"],
        cid,
        msg="Header Session ID is wrong"
    )

    BuiltIn().should_be_equal(
        header["sessionToken"]["body"]["object"]["address"]["objectID"],
        oid,
        msg="Header Session OID is wrong"
    )
