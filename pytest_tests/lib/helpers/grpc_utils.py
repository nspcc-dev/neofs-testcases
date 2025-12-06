import hashlib
import logging

import allure
import base58
import grpc
from ecdsa import NIST256p, SigningKey
from helpers.tzhash import TZHash
from helpers.utility import get_signature_slice, sign_ecdsa
from neo3.wallet.wallet import Wallet
from neofs_testlib.env.env import NodeWallet
from neofs_testlib.protobuf.generated.object import service_pb2 as object_service_pb2
from neofs_testlib.protobuf.generated.object import types_pb2 as object_types_pb2
from neofs_testlib.protobuf.generated.refs import types_pb2 as refs_types_pb2
from neofs_testlib.protobuf.generated.session import service_pb2 as session_service_pb2
from neofs_testlib.protobuf.generated.session import types_pb2 as session_types_pb2
from neofs_testlib.utils.converters import load_wallet

logger = logging.getLogger("NeoLogger")


def create_ecdsa_signature(data: bytes, public_key: bytes, private_key: bytes) -> refs_types_pb2.Signature:
    signature = refs_types_pb2.Signature()
    signature.key = public_key
    signing_key = SigningKey.from_string(private_key, curve=NIST256p)
    r, s = sign_ecdsa(signing_key, hashlib.sha256(data).digest(), hashlib.sha256)
    signature.sign = get_signature_slice(NIST256p, r, s)
    signature.scheme = refs_types_pb2.SignatureScheme.ECDSA_RFC6979_SHA256
    return signature


def get_wallet_keys(default_wallet: NodeWallet):
    neo3_wallet: Wallet = load_wallet(default_wallet.path, default_wallet.password)
    acc = neo3_wallet.accounts[0]
    public_key = acc.public_key.encode_point(True)
    private_key = acc.private_key_from_nep2(
        acc.encrypted_key.decode("utf-8"), default_wallet.password, _scrypt_parameters=acc.scrypt_parameters
    )
    return public_key, private_key


def create_verification_header(
    request_body: bytes, request_meta: bytes, public_key: bytes, private_key: bytes
) -> session_types_pb2.RequestVerificationHeader:
    verify_header = session_types_pb2.RequestVerificationHeader()

    body_signature = create_ecdsa_signature(request_body, public_key, private_key)
    verify_header.body_signature.CopyFrom(body_signature)

    meta_signature = create_ecdsa_signature(request_meta, public_key, private_key)
    verify_header.meta_signature.CopyFrom(meta_signature)

    empty_verify_header = session_types_pb2.RequestVerificationHeader()
    origin_signature = create_ecdsa_signature(
        empty_verify_header.SerializeToString(deterministic=True), public_key, private_key
    )
    verify_header.origin_signature.CopyFrom(origin_signature)

    return verify_header


def create_put_request(
    object_id: refs_types_pb2.ObjectID,
    header: object_types_pb2.Header,
    version: refs_types_pb2.Version,
    test_payload: bytes,
    wallet: NodeWallet,
    session_token: session_types_pb2.SessionToken,
    object_type: object_types_pb2.ObjectType,
    is_init=True,
    epoch=1,
    ttl=1,
):
    public_key, private_key = get_wallet_keys(wallet)

    request = object_service_pb2.PutRequest()

    if is_init:
        request.body.init.object_id.CopyFrom(object_id)
        request.body.init.header.CopyFrom(header)
    else:
        request.body.chunk = test_payload

    if object_type != object_types_pb2.ObjectType.TOMBSTONE and object_id:
        signature = create_ecdsa_signature(object_id.SerializeToString(deterministic=True), public_key, private_key)
        request.body.init.signature.CopyFrom(signature)

    meta_header = session_types_pb2.RequestMetaHeader()
    meta_header.version.CopyFrom(version)
    meta_header.epoch = epoch
    meta_header.ttl = ttl
    meta_header.session_token.CopyFrom(session_token)
    request.meta_header.CopyFrom(meta_header)

    verify_header = create_verification_header(
        request.body.SerializeToString(deterministic=True),
        request.meta_header.SerializeToString(deterministic=True),
        public_key,
        private_key,
    )
    request.verify_header.CopyFrom(verify_header)

    return request


def init_session_token_for_object_put(
    session_id: bytes,
    session_key: bytes,
    wallet: NodeWallet,
    owner_id: refs_types_pb2.OwnerID,
    cid: refs_types_pb2.ContainerID,
    oid: refs_types_pb2.ObjectID,
    object_context_verb: session_types_pb2.ObjectSessionContext,
    lifetime_exp=1000,
    lifetime_nbf=1,
    lifetime_iat=1,
) -> session_types_pb2.SessionToken:
    public_key, private_key = get_wallet_keys(wallet)

    session_token = session_types_pb2.SessionToken()
    session_token.body.id = session_id
    session_token.body.owner_id.CopyFrom(owner_id)
    session_token.body.lifetime.exp = lifetime_exp
    session_token.body.lifetime.nbf = lifetime_nbf
    session_token.body.lifetime.iat = lifetime_iat
    session_token.body.session_key = session_key

    object_context = session_types_pb2.ObjectSessionContext()
    object_context.verb = object_context_verb
    target = session_types_pb2.ObjectSessionContext.Target()
    target.container.CopyFrom(cid)
    target.objects.append(oid)
    object_context.target.CopyFrom(target)

    session_token.body.object.CopyFrom(object_context)
    session_token.signature.CopyFrom(
        create_ecdsa_signature(session_token.body.SerializeToString(deterministic=True), public_key, private_key)
    )
    return session_token


@allure.step("Create session via SessionService")
def get_session_token(wallet: NodeWallet, endpoint: str, epoch=1, expiration=1000) -> tuple[bytes, bytes]:
    public_key, private_key = get_wallet_keys(wallet)

    session_create_request = session_service_pb2.CreateRequest()

    owner_id = refs_types_pb2.OwnerID()
    owner_id.value = base58.b58decode(wallet.address)
    session_create_request.body.owner_id.CopyFrom(owner_id)

    session_create_request.body.expiration = expiration

    meta_header = session_types_pb2.RequestMetaHeader()
    meta_header.version.major = 2
    meta_header.version.minor = 18
    meta_header.epoch = epoch
    session_create_request.meta_header.CopyFrom(meta_header)

    verify_header = create_verification_header(
        session_create_request.body.SerializeToString(deterministic=True),
        session_create_request.meta_header.SerializeToString(deterministic=True),
        public_key,
        private_key,
    )
    session_create_request.verify_header.CopyFrom(verify_header)

    channel = grpc.insecure_channel(endpoint)
    try:
        session_response = channel.unary_unary(
            "/neo.fs.v2.session.SessionService/Create",
            request_serializer=session_service_pb2.CreateRequest.SerializeToString,
            response_deserializer=session_service_pb2.CreateResponse.FromString,
        )(session_create_request)
        allure.attach(str(session_response), "SessionService Create Response", allure.attachment_type.TEXT)
        logger.info(f"SessionService Create response: {session_response}")

        return session_response.body.id, session_response.body.session_key

    except grpc.RpcError as e:
        raise AssertionError(f"SessionService Create gRPC error: {e}")
    finally:
        channel.close()


def get_put_object_request_header(
    owner_id: refs_types_pb2.OwnerID,
    version: refs_types_pb2.Version,
    container_id: refs_types_pb2.ContainerID,
    object_type: object_types_pb2.ObjectType,
    test_payload: bytes,
    oid: str = None,
    homo_hash: bool = True,
    creation_epoch=1,
    expiration_epoch="1000",
) -> object_types_pb2.Header:
    payload_checksum = refs_types_pb2.Checksum()
    payload_checksum.type = refs_types_pb2.ChecksumType.SHA256
    payload_checksum.sum = hashlib.sha256(test_payload).digest()

    header = object_types_pb2.Header()
    header.version.CopyFrom(version)
    header.container_id.CopyFrom(container_id)
    header.owner_id.CopyFrom(owner_id)
    header.creation_epoch = creation_epoch
    header.payload_length = len(test_payload)
    header.payload_hash.CopyFrom(payload_checksum)
    header.object_type = object_type

    if homo_hash:
        homomorphic_checksum = refs_types_pb2.Checksum()
        homomorphic_checksum.type = refs_types_pb2.ChecksumType.TZ
        homomorphic_checksum.sum = TZHash.hash_data(test_payload)
        header.homomorphic_hash.CopyFrom(homomorphic_checksum)

    if object_type == object_types_pb2.ObjectType.TOMBSTONE:
        associate_attribute = object_types_pb2.Header.Attribute()
        associate_attribute.key = "__NEOFS__ASSOCIATE"
        associate_attribute.value = oid
        header.attributes.append(associate_attribute)

        expiration_epoch_attr = object_types_pb2.Header.Attribute()
        expiration_epoch_attr.key = "__NEOFS__EXPIRATION_EPOCH"
        expiration_epoch_attr.value = expiration_epoch
        header.attributes.append(expiration_epoch_attr)
    return header


@allure.step("Put object via gRPC")
def put_object_via_grpc(
    endpoint: str, init_request: object_service_pb2.PutRequest, chunk_request: object_service_pb2.PutRequest
) -> object_service_pb2.PutResponse:
    channel = grpc.insecure_channel(endpoint)

    def request_stream():
        yield init_request
        yield chunk_request

    try:
        response = channel.stream_unary(
            "/neo.fs.v2.object.ObjectService/Put",
            request_serializer=object_service_pb2.PutRequest.SerializeToString,
            response_deserializer=object_service_pb2.PutResponse.FromString,
        )(request_stream())
        allure.attach(str(response), "grpc response", allure.attachment_type.TEXT)
        logger.info(f"gRPC Put response: {response}")
        return response
    except grpc.RpcError as e:
        raise AssertionError(f"gRPC error: {e}")
    finally:
        channel.close()


def put_object(
    object_type: object_types_pb2.ObjectType,
    wallet: NodeWallet,
    sn_endpoint: str,
    payload: bytes,
    cid: str,
    oid: str = None,
    homo_hash: bool = True,
    object_context_verb: session_types_pb2.ObjectSessionContext = session_types_pb2.ObjectSessionContext.PUT,
) -> object_service_pb2.PutResponse:
    with allure.step("Create put request for an object"):
        owner_id = refs_types_pb2.OwnerID()
        owner_id.value = base58.b58decode(wallet.address)

        container_id = refs_types_pb2.ContainerID()
        container_id.value = base58.b58decode(cid)

        version = refs_types_pb2.Version()
        version.major = 2
        version.minor = 18

        session_id, session_key = get_session_token(wallet, sn_endpoint)

        header = get_put_object_request_header(
            owner_id, version, container_id, object_type, payload, oid, homo_hash=homo_hash
        )

        object_id = refs_types_pb2.ObjectID()
        object_id.value = hashlib.sha256(header.SerializeToString(deterministic=True)).digest()

        session_token = init_session_token_for_object_put(
            session_id, session_key, wallet, owner_id, container_id, object_id, object_context_verb
        )

        init_request = create_put_request(
            object_id, header, version, payload, wallet, session_token, object_type, is_init=True
        )
        allure.attach(str(init_request), "Init Request", allure.attachment_type.TEXT)

        chunk_request = create_put_request(
            None, None, version, payload, wallet, session_token, object_type, is_init=False
        )

        allure.attach(str(chunk_request), "Chunk Request", allure.attachment_type.TEXT)

    with allure.step("Send put request via grpc to a storage node"):
        response = put_object_via_grpc(sn_endpoint, init_request, chunk_request)
        return response
