import base64
import binascii
import hashlib
import os
import random

from helpers.complex_object_actions import get_nodes_without_object
from helpers.neofs_verbs import get_object
from helpers.rest_gate import (
    assert_hashes_are_equal,
    complete_bearer_token,
    complete_session_token,
    get_unsigned_bearer_token,
    get_unsigned_session_token,
    get_via_rest_gate,
)
from neo3.core import cryptography
from neo3.wallet.wallet import Wallet
from neofs_testlib.env.env import NodeWallet, StorageNode
from neofs_testlib.shell import Shell
from neofs_testlib.utils.converters import load_wallet


def sign_session_token(
    unsigned_token: str,
    private_key,
    public_key_bytes: bytes,
    wallet_connect: bool = False,
) -> tuple[str, str, str]:
    """
    Sign an unsigned session token with the provided private key.

    Args:
        unsigned_token: Base64 encoded unsigned token
        private_key: Private key to sign with
        public_key_bytes: Public key bytes
        wallet_connect: Use WalletConnect signature scheme (WALLETCONNECT),
                       otherwise use deterministic ECDSA with SHA256 (DETERMINISTIC_SHA256)

    Returns:
        tuple: (signature, public_key_hex, scheme)
            - signature: Base64 encoded signature
            - public_key_hex: Hex encoded public key
            - scheme: Signature scheme name
    """
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

    pub_key = str(binascii.hexlify(public_key_bytes))[2:-1]

    return signature, pub_key, scheme


def sign_bearer_token_v2(
    unsigned_token: str,
    private_key,
    public_key_bytes: bytes,
    wallet_connect: bool = False,
) -> tuple[str, str, str]:
    """
    Sign an unsigned v2 bearer token returned from /v2/auth/bearer.

    Args:
        unsigned_token: Base64 encoded unsigned bearer token body.
        private_key: Private key to sign with.
        public_key_bytes: Public key bytes (uncompressed/compressed).
        wallet_connect: Use WalletConnect signature scheme (WALLETCONNECT),
                        otherwise use deterministic ECDSA with SHA256 (DETERMINISTIC_SHA256).

    Returns:
        tuple: (signature, public_key_hex, scheme)
            - signature: Hex encoded signature (with WalletConnect salt appended for that scheme).
            - public_key_hex: Hex encoded public key.
            - scheme: Signature scheme name accepted by /v2/auth/bearer/complete.

    Notes:
        Unlike v2 session tokens (where signature/key are base64-encoded), v2 bearer
        tokens expect hex-encoded signature and key in /v2/auth/bearer/complete.
    """
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
        signature = binascii.hexlify(signature_with_salt).decode("utf-8")
        scheme = "WALLETCONNECT"
    else:
        signature_bytes = cryptography.sign(unsigned_token_bytes, private_key, hash_func=hashlib.sha256)
        signature = binascii.hexlify(signature_bytes).decode("utf-8")
        scheme = "DETERMINISTIC_SHA256"

    pub_key = binascii.hexlify(public_key_bytes).decode("utf-8")

    return signature, pub_key, scheme


def generate_bearer_token_v2(
    gw_endpoint: str,
    issuer_wallet: NodeWallet,
    records: list[dict],
    owner: str = None,
    lifetime: int = 100,
    wallet_connect: bool = False,
    signer_wallet: NodeWallet = None,
) -> str:
    """
    Generate a complete signed v2 bearer token via REST API.

    Args:
        gw_endpoint: REST gateway endpoint
        issuer_wallet: Wallet that owns the container (declared as bearer issuer).
        records: List of EACL record dicts (FormBearerRequest.records schema).
        owner: Address of the account allowed to use this bearer (typically the
               REST gateway address). If None, the bearer will not be bound
               to a specific user.
        lifetime: Token lifetime in epochs.
        wallet_connect: Use WalletConnect signature scheme.
        signer_wallet: Optional wallet to sign the token with. Defaults to
                       ``issuer_wallet``. Use a different wallet to forge an
                       invalid signature for negative tests.

    Returns:
        str: Complete base64 encoded signed bearer token (suitable for the
             ``NeoFS-Bearer-Token`` HTTP header, ``Bearer`` cookie, or
             ``Authorization: Bearer ...`` header).
    """
    issuer_neo3_wallet: Wallet = load_wallet(issuer_wallet.path, issuer_wallet.password)
    issuer_acc = issuer_neo3_wallet.accounts[0]

    if signer_wallet is None:
        signer_acc = issuer_acc
    else:
        signer_neo3_wallet: Wallet = load_wallet(signer_wallet.path, signer_wallet.password)
        signer_acc = signer_neo3_wallet.accounts[0]

    unsigned_token = get_unsigned_bearer_token(
        gw_endpoint,
        issuer=issuer_acc.address,
        records=records,
        owner=owner,
        lifetime=lifetime,
    )

    signature, pub_key, scheme = sign_bearer_token_v2(
        unsigned_token,
        signer_acc.private_key,
        signer_acc.public_key.to_array(),
        wallet_connect=wallet_connect,
    )

    return complete_bearer_token(gw_endpoint, unsigned_token, signature, pub_key, scheme=scheme)


def extract_session_token_from_bearer(bearer_token: str) -> str:
    """
    Extract the session token part from a bearer token for use in delegation.

    Bearer tokens from /auth/session/complete are: lock (32 bytes) + session token
    For delegation origin parameter, we need only the session token part.

    Args:
        bearer_token: Base64 encoded bearer token (lock + session token)

    Returns:
        str: Base64 encoded session token (without lock)
    """
    bearer_bytes = base64.standard_b64decode(bearer_token)
    session_token_bytes = bearer_bytes[32:]
    return base64.standard_b64encode(session_token_bytes).decode("utf-8")


def generate_session_token_v2(
    gw_endpoint: str,
    wallet: NodeWallet,
    contexts: list[dict],
    lifetime: int = 1000,
    targets: list[str] = None,
    wallet_connect: bool = False,
    origin: str = None,
    final: bool = False,
) -> str:
    """
    Generate a complete signed session token via REST API.

    Args:
        gw_endpoint: REST gateway endpoint
        wallet: Wallet to use for signing
        contexts: List of context dicts with verbs (and optionally containerID).
                  Example: [{"containerID": "cid", "verbs": ["OBJECT_GET", "OBJECT_HEAD"]}]
                  or [{"verbs": ["CONTAINER_PUT"]}] for container operations.
        lifetime: Token lifetime in seconds (default: 1000)
        targets: List of target addresses (if None, uses wallet address)
        wallet_connect: Use WalletConnect signature scheme (WALLETCONNECT),
                       otherwise use deterministic ECDSA with SHA256 (DETERMINISTIC_SHA256)
        origin: Origin bearer token for delegation (base64 encoded bearer token).
                Automatically converted from bearer format to session token format.
        final: Mark token as final (prevents further delegation)

    Returns:
        str: Complete signed session token (base64 encoded)
    """
    neo3_wallet: Wallet = load_wallet(wallet.path, wallet.password)
    acc = neo3_wallet.accounts[0]

    if targets is None:
        targets = [acc.address]

    extracted_origin = extract_session_token_from_bearer(origin) if origin else None

    unsigned_token, lock = get_unsigned_session_token(
        gw_endpoint,
        issuer=acc.address,
        contexts=contexts,
        lifetime=lifetime,
        targets=targets,
        origin=extracted_origin,
        final=final,
    )

    signature, pub_key, scheme = sign_session_token(
        unsigned_token,
        acc.private_key,
        acc.public_key.to_array(),
        wallet_connect=wallet_connect,
    )

    return complete_session_token(gw_endpoint, unsigned_token, lock, signature, pub_key, scheme=scheme)


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
