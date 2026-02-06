import base64
import binascii
import hashlib
import os
import random

from helpers.complex_object_actions import get_nodes_without_object
from helpers.neofs_verbs import get_object
from helpers.rest_gate import assert_hashes_are_equal, complete_session_token, get_container_token, get_via_rest_gate
from neo3.core import cryptography
from neo3.wallet.wallet import Wallet
from neofs_testlib.env.env import NodeWallet, StorageNode
from neofs_testlib.shell import Shell
from neofs_testlib.utils.converters import load_wallet


def generate_credentials(gw_endpoint: str, wallet: NodeWallet, verb="CONTAINER_PUT", wallet_connect=False) -> str:
    """
    Generate session token credentials for container operations.

    Args:
        gw_endpoint: REST gateway endpoint
        wallet: Node wallet to use for signing
        verb: Container operation verb (CONTAINER_PUT, CONTAINER_DELETE, etc.)
        wallet_connect: Use WalletConnect signature scheme (WALLETCONNECT),
                       otherwise use deterministic ECDSA with SHA256 (DETERMINISTIC_SHA256)

    Returns:
        str: Complete signed session token (base64 encoded)
    """

    neo3_wallet: Wallet = load_wallet(wallet.path, wallet.password)
    acc = neo3_wallet.accounts[0]

    unsigned_token, lock = get_container_token(gw_endpoint, acc.address, verb=verb)
    private_key = acc.private_key

    unsigned_token_bytes = base64.standard_b64decode(unsigned_token)

    if wallet_connect:
        prefix = b"\x01\x00\x01\xf0"
        postfix = b"\x00\x00"
        encoded_token_bytes = base64.standard_b64encode(unsigned_token_bytes)
        salt = os.urandom(16)
        hex_salt = binascii.hexlify(salt)
        msg_len = len(hex_salt) + len(encoded_token_bytes)
        msg = prefix + msg_len.to_bytes() + hex_salt + encoded_token_bytes + postfix
        signature_bytes = cryptography.sign(msg, private_key, hash_func=hashlib.sha256)

        signature_with_salt = signature_bytes + salt
        signature = base64.standard_b64encode(signature_with_salt).decode("utf-8")
        scheme = "WALLETCONNECT"
    else:
        signature_bytes = cryptography.sign(unsigned_token_bytes, private_key, hash_func=hashlib.sha256)
        signature = base64.standard_b64encode(signature_bytes).decode("utf-8")
        scheme = "DETERMINISTIC_SHA256"

    pub_key = str(binascii.hexlify(neo3_wallet.accounts[0].public_key.to_array()))[2:-1]

    return complete_session_token(
        gw_endpoint,
        unsigned_token,
        lock,
        signature,
        pub_key,
        scheme=scheme,
    )


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
