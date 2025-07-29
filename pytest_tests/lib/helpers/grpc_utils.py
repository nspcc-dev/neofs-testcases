import hashlib

import base58
from ecdsa import NIST256p, SigningKey
from helpers.tzhash import TZHash
from helpers.utility import get_signature_slice, sign_ecdsa
from neo3.wallet.wallet import Wallet
from neofs_testlib.env.env import NodeWallet
from neofs_testlib.protobuf.generated.object import service_pb2 as object_service_pb2
from neofs_testlib.protobuf.generated.object import types_pb2 as object_types_pb2
from neofs_testlib.protobuf.generated.refs import types_pb2 as refs_types_pb2
from neofs_testlib.protobuf.generated.session import types_pb2 as session_types_pb2
from neofs_testlib.utils.converters import load_wallet


def _create_ecdsa_signature(data: bytes, public_key: bytes, private_key: bytes) -> refs_types_pb2.Signature:
    signature = refs_types_pb2.Signature()
    signature.key = public_key
    signing_key = SigningKey.from_string(private_key, curve=NIST256p)
    r, s = sign_ecdsa(signing_key, hashlib.sha256(data).digest(), hashlib.sha256)
    signature.sign = get_signature_slice(NIST256p, r, s)
    signature.scheme = refs_types_pb2.SignatureScheme.ECDSA_RFC6979_SHA256
    return signature


def create_object_components(container: str, default_wallet: NodeWallet, oid: str, test_payload: bytes):
    payload_hash = hashlib.sha256(test_payload).digest()

    object_id = refs_types_pb2.ObjectID()
    object_id.value = base58.b58decode(oid)

    container_id = refs_types_pb2.ContainerID()
    container_id.value = base58.b58decode(container)

    owner_id = refs_types_pb2.OwnerID()
    owner_id.value = base58.b58decode(default_wallet.address)

    version = refs_types_pb2.Version()
    version.major = 2
    version.minor = 18

    payload_checksum = refs_types_pb2.Checksum()
    payload_checksum.type = refs_types_pb2.ChecksumType.SHA256
    payload_checksum.sum = payload_hash

    homomorphic_checksum = refs_types_pb2.Checksum()
    homomorphic_checksum.type = refs_types_pb2.ChecksumType.TZ
    homomorphic_checksum.sum = TZHash.hash_data(test_payload)

    header = object_types_pb2.Header()
    header.version.CopyFrom(version)
    header.container_id.CopyFrom(container_id)
    header.owner_id.CopyFrom(owner_id)
    header.creation_epoch = 1
    header.payload_length = len(test_payload)
    header.payload_hash.CopyFrom(payload_checksum)
    header.object_type = object_types_pb2.ObjectType.STORAGE_GROUP
    header.homomorphic_hash.CopyFrom(homomorphic_checksum)

    return object_id, version, header


def get_wallet_keys(default_wallet: NodeWallet):
    neo3_wallet: Wallet = load_wallet(default_wallet.path, default_wallet.password)
    acc = neo3_wallet.accounts[0]
    public_key = acc.public_key.encode_point(True)
    private_key = acc.private_key_from_nep2(
        acc.encrypted_key.decode("utf-8"), default_wallet.password, _scrypt_parameters=acc.scrypt_parameters
    )
    return public_key, private_key


def _create_meta_header(version: refs_types_pb2.Version) -> session_types_pb2.RequestMetaHeader:
    meta_header = session_types_pb2.RequestMetaHeader()
    meta_header.version.CopyFrom(version)
    meta_header.epoch = 1
    meta_header.ttl = 1
    return meta_header


def _create_verification_header(
    request_body: bytes, request_meta: bytes, public_key: bytes, private_key: bytes
) -> session_types_pb2.RequestVerificationHeader:
    verify_header = session_types_pb2.RequestVerificationHeader()

    body_signature = _create_ecdsa_signature(request_body, public_key, private_key)
    verify_header.body_signature.CopyFrom(body_signature)

    meta_signature = _create_ecdsa_signature(request_meta, public_key, private_key)
    verify_header.meta_signature.CopyFrom(meta_signature)

    empty_verify_header = session_types_pb2.RequestVerificationHeader()
    origin_signature = _create_ecdsa_signature(
        empty_verify_header.SerializeToString(deterministic=True), public_key, private_key
    )
    verify_header.origin_signature.CopyFrom(origin_signature)

    return verify_header


def create_put_request(object_id, signature, header, version, test_payload, public_key, private_key, is_init=True):
    request = object_service_pb2.PutRequest()

    if is_init:
        request.body.init.object_id.CopyFrom(object_id)
        request.body.init.signature.CopyFrom(signature)
        request.body.init.header.CopyFrom(header)
    else:
        request.body.chunk = test_payload

    meta_header = _create_meta_header(version)
    request.meta_header.CopyFrom(meta_header)

    verify_header = _create_verification_header(
        request.body.SerializeToString(deterministic=True),
        request.meta_header.SerializeToString(deterministic=True),
        public_key,
        private_key,
    )
    request.verify_header.CopyFrom(verify_header)

    return request
