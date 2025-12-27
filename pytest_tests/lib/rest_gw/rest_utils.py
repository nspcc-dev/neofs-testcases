import base64
import binascii
import hashlib
import os
import random

from helpers.complex_object_actions import get_nodes_without_object
from helpers.neofs_verbs import get_object
from helpers.rest_gate import assert_hashes_are_equal, get_container_token, get_via_rest_gate
from neo3.core import cryptography
from neo3.wallet.wallet import Wallet
from neofs_testlib.env.env import NodeWallet, StorageNode
from neofs_testlib.shell import Shell
from neofs_testlib.utils.converters import load_wallet


def generate_credentials(
    gw_endpoint: str, wallet: NodeWallet, verb="PUT", wallet_connect=False, bearer_for_all_users=None
) -> tuple:
    neo3_wallet: Wallet = load_wallet(wallet.path, wallet.password)
    acc = neo3_wallet.accounts[0]
    token = get_container_token(gw_endpoint, acc.address, verb=verb, bearer_for_all_users=bearer_for_all_users)
    private_key = acc.private_key

    if wallet_connect:
        prefix = b"\x01\x00\x01\xf0"
        postfix = b"\x00\x00"
        decoded_token_bytes = base64.standard_b64decode(token)
        encoded_token_bytes = base64.standard_b64encode(decoded_token_bytes)
        salt = os.urandom(16)
        hex_salt = binascii.hexlify(salt)
        msg_len = len(hex_salt) + len(encoded_token_bytes)
        msg = prefix + msg_len.to_bytes() + hex_salt + encoded_token_bytes + postfix
        signature = cryptography.sign(msg, private_key, hash_func=hashlib.sha256)
        signature = str(binascii.hexlify(signature))[2:-1]
        signature = f"{signature}{str(hex_salt)[2:-1]}"
    else:
        signature = cryptography.sign(base64.standard_b64decode(token), private_key, hash_func=hashlib.sha512)
        signature = str(binascii.hexlify(signature))[2:-1]
        signature = f"04{signature}"

    pub_key = str(binascii.hexlify(neo3_wallet.accounts[0].public_key.to_array()))[2:-1]
    return token, signature, pub_key


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

    object_getter = object_getter or get_via_rest_gate

    got_file_path = get_object(
        wallet=wallet,
        cid=cid,
        oid=oid,
        shell=shell,
        endpoint=random_node.endpoint,
    )
    got_file_path_http = object_getter(cid=cid, oid=oid, endpoint=endpoint)

    assert_hashes_are_equal(file_name, got_file_path, got_file_path_http)
