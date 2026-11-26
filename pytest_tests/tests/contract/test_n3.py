import hashlib
import logging
import os
import time
import uuid

import allure
import base58
import grpc
from ecdsa import NIST256p, SigningKey
from helpers.grpc_utils import (
    REQUEST_LIFETIME_SEC,
    STATUS_CONTAINER_AWAIT_TIMEOUT,
    STATUS_CONTAINER_NOT_FOUND,
    STATUS_OK,
    STATUS_SIGNATURE_VERIFICATION_FAIL,
    call_unary_rpc,
    create_ecdsa_signature,
    create_request_verification_header,
    get_container_via_grpc,
    wait_for_container_via_grpc,
)
from helpers.utility import get_signature_slice, parse_version, sign_ecdsa
from neo3.wallet.wallet import Wallet
from neofs_env.neofs_env_test_base import TestNeofsBase
from neofs_testlib.cli import NeoGo
from neofs_testlib.env.env import NeoFSEnv
from neofs_testlib.protobuf.generated.container import service_pb2 as container_service_pb2
from neofs_testlib.protobuf.generated.container import types_pb2 as container_types_pb2
from neofs_testlib.protobuf.generated.refs import types_pb2 as refs_types_pb2
from neofs_testlib.utils.converters import load_wallet

logger = logging.getLogger("NeoLogger")

CONTRACT_ADDR = "1b4012d2aba18230a8ada77540f64d190480cbb0"
EACL_PUBLIC_READ_WRITE = 0x0FBFBFFF
CONTAINER_PUT_METHOD = "/neo.fs.v2.container.ContainerService/Put"


def build_container(owner_address: str, version: tuple[int, int]) -> container_types_pb2.Container:
    container = container_types_pb2.Container()
    container.version.major, container.version.minor = version
    container.nonce = uuid.uuid4().bytes
    container.owner_id.value = base58.b58decode(owner_address)
    container.basic_acl = EACL_PUBLIC_READ_WRITE

    for key, value in (("Type", "Test"), ("Timestamp", str(int(time.time())))):
        attr = container.attributes.add()
        attr.key = key
        attr.value = value

    container.placement_policy.container_backup_factor = 2
    sel = container.placement_policy.selectors.add()
    sel.name = "X"
    sel.count = 2
    sel.filter = "*"
    replica = container.placement_policy.replicas.add()
    replica.count = 2
    replica.selector = "X"

    return container


def calculate_verification_script(username: str, method: str) -> bytes:
    verification_script = bytearray()
    verification_script += bytes([0x0C, len(username)]) + username.encode()
    verification_script += bytes([0x10 + 0x02])
    verification_script += bytes([0xC0])
    verification_script += bytes([0x10 + 0x05])
    verification_script += bytes([0x0C, len(method)]) + method.encode()

    contract = bytes.fromhex(CONTRACT_ADDR)[::-1]
    verification_script += bytes([0x0C, len(contract)]) + contract

    syscall_name = "System.Contract.Call"
    syscall_hash = hashlib.sha256(syscall_name.encode()).digest()
    verification_script += bytes([0x41]) + syscall_hash[:4]

    return bytes(verification_script)


def hash160(data: bytes) -> bytes:
    sha256_hash = hashlib.sha256(data).digest()
    ripemd160 = hashlib.new("ripemd160")
    ripemd160.update(sha256_hash)
    return ripemd160.digest()


def neofs_n3_user(verif_script: bytes) -> str:
    user_payload = bytes([0x35]) + hash160(verif_script)
    checksum = hashlib.sha256(hashlib.sha256(user_payload).digest()).digest()[:4]
    owner_bytes = user_payload + checksum

    return base58.b58encode(owner_bytes).decode()


def calculate_invocation_script(datadir: str, data: bytes) -> bytes:
    network_magic = 15405
    signed_data = network_magic.to_bytes(4, byteorder="little", signed=False) + hashlib.sha256(data).digest()

    neo3_wallet: Wallet = load_wallet(os.path.join(datadir, "user_wallet.json"), "user")
    acc = neo3_wallet.accounts[0]
    private_key = acc.private_key
    signing_key = SigningKey.from_string(private_key, curve=NIST256p)
    r, s = sign_ecdsa(signing_key, hashlib.sha256(signed_data).digest(), hashlib.sha256)
    signature = get_signature_slice(NIST256p, r, s)
    return bytes([0x0C, 0x40]) + signature


def generate_ephemeral_ecdsa_keys() -> tuple[bytes, bytes]:
    signing_key = SigningKey.generate(curve=NIST256p)
    return signing_key.verifying_key.to_string("compressed"), signing_key.to_string()


def build_put_container_request(
    datadir: str,
    owner_address: str,
    verification_script: bytes,
    use_request_signature: bool,
    ecdsa_pub: bytes,
    ecdsa_priv: bytes,
    corrupt_signature: bool = False,
) -> tuple[container_service_pb2.PutRequest, bytes]:
    version = (2, 29) if use_request_signature else (2, 28)
    container = build_container(owner_address, version)
    container_bytes = container.SerializeToString(deterministic=True)
    container_id = hashlib.sha256(container_bytes).digest()

    request = container_service_pb2.PutRequest()
    request.body.container.CopyFrom(container)
    request.meta_header.version.major, request.meta_header.version.minor = version
    request.meta_header.valid_until_time = int(time.time()) + REQUEST_LIFETIME_SEC

    if use_request_signature:
        # Storage Node ContainerService.Put still requires a non-nil body.signature on API 2.29.
        request.body.signature.SetInParent()
        sign_data = request.SerializeToString(deterministic=True)
        if corrupt_signature:
            sign_data = bytes(~b & 0xFF for b in sign_data)
        invocation_script = calculate_invocation_script(datadir, sign_data)
        n3_signature = refs_types_pb2.Signature(
            key=verification_script,
            sign=invocation_script,
            scheme=refs_types_pb2.SignatureScheme.N3,
        )
        request.verify_header.CopyFrom(create_request_verification_header(n3_signature))
    else:
        sign_data = bytes(~b & 0xFF for b in container_bytes) if corrupt_signature else container_bytes
        request.body.signature.key = verification_script
        request.body.signature.sign = calculate_invocation_script(datadir, sign_data)
        ecdsa_sig = create_ecdsa_signature(request.SerializeToString(deterministic=True), ecdsa_pub, ecdsa_priv)
        request.verify_header.CopyFrom(create_request_verification_header(ecdsa_sig))

    return request, container_id


def assert_put_container_rejected(
    endpoint: str,
    request: container_service_pb2.PutRequest,
    container_id: bytes,
    use_request_signature: bool,
    ecdsa_pub: bytes,
    ecdsa_priv: bytes,
) -> None:
    if use_request_signature:
        response = call_unary_rpc(
            endpoint,
            CONTAINER_PUT_METHOD,
            request,
            container_service_pb2.PutRequest,
            container_service_pb2.PutResponse,
        )
        status = response.meta_header.status
        assert status.code == STATUS_SIGNATURE_VERIFICATION_FAIL, (
            f"Expected SIGNATURE_VERIFICATION_FAIL ({STATUS_SIGNATURE_VERIFICATION_FAIL}), "
            f"got code={status.code} message={status.message!r}"
        )
    else:
        try:
            response = call_unary_rpc(
                endpoint,
                CONTAINER_PUT_METHOD,
                request,
                container_service_pb2.PutRequest,
                container_service_pb2.PutResponse,
            )
        except grpc.RpcError as exc:
            assert exc.code() == grpc.StatusCode.DEADLINE_EXCEEDED, f"Unexpected gRPC error: {exc}"
        else:
            status = response.meta_header.status
            assert status.code == STATUS_CONTAINER_AWAIT_TIMEOUT, (
                f"Expected CONTAINER_AWAIT_TIMEOUT ({STATUS_CONTAINER_AWAIT_TIMEOUT}), "
                f"got code={status.code} message={status.message!r}"
            )

    get_response = get_container_via_grpc(endpoint, container_id, ecdsa_pub, ecdsa_priv)
    get_status = get_response.meta_header.status
    assert get_status.code == STATUS_CONTAINER_NOT_FOUND, (
        f"Expected rejected container not to be persisted (code={STATUS_CONTAINER_NOT_FOUND}), "
        f"got code={get_status.code} message={get_status.message!r}"
    )


class TestN3(TestNeofsBase):
    @allure.title("Test N3 contract witnesses in container ops")
    def test_n3_contract_witnesses_in_container_ops(self, datadir, neofs_env: NeoFSEnv):
        neogo = NeoGo(neofs_env.shell, neo_go_exec_path=neofs_env.neo_go_path)
        neofsadm = neofs_env.neofs_adm()
        endpoint = neofs_env.storage_nodes[0].endpoint
        node_version = neofs_env.get_binary_version(neofs_env.neofs_node_path)
        use_request_signature = parse_version(node_version) >= parse_version("0.58.1")
        ecdsa_pub, ecdsa_priv = generate_ephemeral_ecdsa_keys()

        neofsadm.fschain.refill_gas(
            rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
            alphabet_wallets=neofs_env.alphabet_wallets_dir,
            storage_wallet=os.path.join(datadir, "deployer_wallet.json"),
            gas="100",
        )

        with allure.step("Deploy user management contract"):
            result = neogo.contract.deploy(
                input_file=os.path.join(datadir, "contract", "usermgt.nef"),
                manifest=os.path.join(datadir, "contract", "usermgt.manifest.json"),
                rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
                wallet=os.path.join(datadir, "deployer_wallet.json"),
                wallet_password="deployer",
                force=True,
                await_mode=True,
            )
            assert CONTRACT_ADDR in result.stdout, "Expected contract address not found in deployment output"

        with allure.step("Calculate verification script"):
            verification_script = calculate_verification_script("Bob", "verifySignature")

        owner_address = neofs_n3_user(verification_script)

        with allure.step("Create container"):
            request, container_id = build_put_container_request(
                datadir,
                owner_address,
                verification_script,
                use_request_signature,
                ecdsa_pub,
                ecdsa_priv,
            )
            response = call_unary_rpc(
                endpoint,
                CONTAINER_PUT_METHOD,
                request,
                container_service_pb2.PutRequest,
                container_service_pb2.PutResponse,
            )
            status = response.meta_header.status
            assert status.code == STATUS_OK, f"Container creation failed: code={status.code} message={status.message!r}"
            assert response.body.container_id.value == container_id, "Returned container ID does not match container"
            wait_for_container_via_grpc(endpoint, container_id, ecdsa_pub, ecdsa_priv)

        with allure.step("Create container with wrong user"):
            verification_script_wrong_user = calculate_verification_script("Alice", "verifySignature")
            request, container_id = build_put_container_request(
                datadir,
                owner_address,
                verification_script_wrong_user,
                use_request_signature,
                ecdsa_pub,
                ecdsa_priv,
            )
            assert_put_container_rejected(endpoint, request, container_id, use_request_signature, ecdsa_pub, ecdsa_priv)

        with allure.step("Create container with wrong contract method"):
            verification_script_wrong_method = calculate_verification_script("Bob", "validateSignature")
            request, container_id = build_put_container_request(
                datadir,
                owner_address,
                verification_script_wrong_method,
                use_request_signature,
                ecdsa_pub,
                ecdsa_priv,
            )
            assert_put_container_rejected(endpoint, request, container_id, use_request_signature, ecdsa_pub, ecdsa_priv)

        with allure.step("Create container with wrong signature"):
            request, container_id = build_put_container_request(
                datadir,
                owner_address,
                verification_script,
                use_request_signature,
                ecdsa_pub,
                ecdsa_priv,
                corrupt_signature=True,
            )
            assert_put_container_rejected(endpoint, request, container_id, use_request_signature, ecdsa_pub, ecdsa_priv)
