import base64
import hashlib
import json
import logging
import os
import uuid
from pathlib import Path
from typing import Callable, Tuple

import allure
import base58
import jinja2
import pytest
from ecdsa import NIST256p, SigningKey
from ecdsa.ellipticcurve import Point
from ecdsa.rfc6979 import generate_k
from helpers.utility import parse_version
from neo3.wallet.wallet import Wallet
from neofs_env.neofs_env_test_base import TestNeofsBase
from neofs_testlib.cli import NeoGo
from neofs_testlib.env.env import NeoFSEnv
from neofs_testlib.protobuf.generated.container import types_pb2 as container_types_pb2
from neofs_testlib.utils import wallet as wallet_utils
from neofs_testlib.utils.converters import load_wallet

logger = logging.getLogger("NeoLogger")

CONTRACT_ADDR = "1b4012d2aba18230a8ada77540f64d190480cbb0"
CONTAINER_REQUEST_JSON_FILE_NAME = "create_container_request.json"
CONTAINER_REQUEST_TEMPLATE_JSON_FILE_NAME = "create_container_request_template.json"
CONTAINER_INVALID_REQUEST_TEMPLATE_JSON_FILE_NAME = "create_container_invalid_request_template.json"


def sign_ecdsa(priv_key: SigningKey, hash_bytes: bytes, hashfunc: Callable[[], "hashlib._Hash"]) -> Tuple[int, int]:
    curve = priv_key.curve
    order = curve.order
    secexp = priv_key.privkey.secret_multiplier

    e = int.from_bytes(hash_bytes, byteorder="big")

    def attempt_sign():
        k = generate_k(order, secexp, hashfunc, hash_bytes)
        p: Point = curve.generator * k
        r = p.x() % order
        if r == 0:
            return None

        inv_k = pow(k, -1, order)
        s = (inv_k * (e + r * secexp)) % order
        if s == 0:
            return None

        return r, s

    signature = attempt_sign()
    if signature is None:
        raise ValueError("Failed to generate valid signature")

    return signature


def get_signature_slice(curve, r: int, s: int) -> bytes:
    p_bitlen = curve.curve.p().bit_length()
    byte_len = p_bitlen // 8
    r_bytes = r.to_bytes(byte_len, byteorder="big")
    s_bytes = s.to_bytes(byte_len, byteorder="big")
    return r_bytes + s_bytes


def get_container_marshalled_bytes(datadir: str, template_file_name: str, owner_address: str) -> bytes:
    """
    There is no reason to reimplement the parsing logic for PlacementPolicy here, so
    we hardcode the values for the PlacementPolicy fields based on the template. If
    it is needed to update the template, this function should be updated accordingly.
    """
    ACL_MAP = {
        "eacl-public-read-write": 0x0FBFBFFF,
    }

    with open(os.path.join(datadir, template_file_name)) as f:
        request_json = json.load(f)

    container = container_types_pb2.Container()

    version_str = request_json["Container"]["Version"]
    major, minor = map(int, version_str.split("."))
    container.version.major = major
    container.version.minor = minor

    container.nonce = uuid.UUID(request_json["Container"]["Nonce"]).bytes

    owner_bytes = base58.b58decode(owner_address)
    container.owner_id.value = owner_bytes

    acl_name = request_json["Container"]["BasicACL"]
    container.basic_acl = ACL_MAP[acl_name]

    for attr in request_json["Container"]["Attributes"]:
        a = container.attributes.add()
        a.key = attr["key"]
        a.value = attr["value"]

    # CBF 2
    container.placement_policy.container_backup_factor = 2

    # SELECT 2 FROM * AS X
    sel = container.placement_policy.selectors.add()
    sel.name = "X"
    sel.count = 2
    sel.filter = "*"

    # REP 2 IN X
    replica = container.placement_policy.replicas.add()
    replica.count = 2
    replica.selector = "X"

    return container.SerializeToString(deterministic=True)


def calculate_verification_script(username: str, method: str) -> str:
    verification_script = bytearray()
    verification_script += bytes([0x0C, len(username)]) + username.encode()
    verification_script += bytes([0x10 + 0x02])
    verification_script += bytes([0xC0])
    verification_script += bytes([0x10 + 0x05])
    verification_script += bytes([0x0C, len(method)]) + method.encode()

    contract = bytes.fromhex(CONTRACT_ADDR)
    contract = contract[::-1]

    verification_script += bytes([0x0C, len(contract)]) + contract

    syscall_name = "System.Contract.Call"
    syscall_hash = hashlib.sha256(syscall_name.encode()).digest()
    verification_script += bytes([0x41]) + syscall_hash[:4]

    return base64.b64encode(verification_script).decode()


def calculate_invocation_script(datadir: str, container: bytes) -> str:
    network_magic = 15405
    signed_data = network_magic.to_bytes(4, byteorder="little", signed=False) + hashlib.sha256(container).digest()

    neo3_wallet: Wallet = load_wallet(os.path.join(datadir, "user_wallet.json"), "user")
    acc = neo3_wallet.accounts[0]
    private_key = acc.private_key_from_nep2(
        acc.encrypted_key.decode("utf-8"), "user", _scrypt_parameters=acc.scrypt_parameters
    )
    signing_key = SigningKey.from_string(private_key, curve=NIST256p)
    r, s = sign_ecdsa(signing_key, hashlib.sha256(signed_data).digest(), hashlib.sha256)
    signature = get_signature_slice(NIST256p, r, s)
    invocation_script = bytes([0x0C, 0x40]) + signature
    return base64.b64encode(invocation_script).decode()


def render_create_container_json(
    datadir: str,
    template_file_name: str,
    verification_script_b64: str,
    invocation_script_b64: str,
    owner_address: str,
) -> None:
    jinja_env = jinja2.Environment()
    template = Path(os.path.join(datadir, template_file_name)).read_text()
    jinja_template = jinja_env.from_string(template)

    rendered_config = jinja_template.render(
        owner=owner_address, verification_script=verification_script_b64, invocation_script=invocation_script_b64
    )

    with open(os.path.join(datadir, CONTAINER_REQUEST_JSON_FILE_NAME), mode="w") as fp:
        fp.write(rendered_config)

    allure.attach.file(
        os.path.join(datadir, CONTAINER_REQUEST_JSON_FILE_NAME), name="create_container_request", extension="json"
    )


class TestN3(TestNeofsBase):
    @allure.title("Test N3 contract witnesses in container ops")
    def test_n3_contract_witnesses_in_container_ops(self, datadir, neofs_env: NeoFSEnv):
        if parse_version(self.neofs_env.get_binary_version(self.neofs_env.neofs_node_path)) == parse_version("0.47.1"):
            pytest.skip("Test doesn't work on 0.47.1 - https://github.com/nspcc-dev/neofs-node/issues/3433")

        neogo = NeoGo(neofs_env.shell, neo_go_exec_path=neofs_env.neo_go_path)

        owner_address = wallet_utils.get_last_address_from_wallet(os.path.join(datadir, "user_wallet.json"), "user")

        neofsadm = neofs_env.neofs_adm()

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

        with allure.step("Calculate invocation script"):
            container_marshalled_bytes = get_container_marshalled_bytes(
                datadir, CONTAINER_REQUEST_TEMPLATE_JSON_FILE_NAME, owner_address
            )
            invocation_script_b64 = calculate_invocation_script(datadir, container_marshalled_bytes)

        with allure.step("Calculate verification script"):
            verification_script_b64 = calculate_verification_script("Bob", "verifySignature")

        with allure.step("Create container"):
            render_create_container_json(
                datadir,
                CONTAINER_REQUEST_TEMPLATE_JSON_FILE_NAME,
                verification_script_b64,
                invocation_script_b64,
                owner_address,
            )

            result = neofs_env.neofs_cli(None).request.create_container(
                body=os.path.join(datadir, CONTAINER_REQUEST_JSON_FILE_NAME),
                endpoint=neofs_env.storage_nodes[0].endpoint,
            )
            assert "Status OK. Operation succeeded." in result.stdout, "Container was not created successfully."

        with allure.step("Create container with wrong user"):
            invalid_container_marshalled_bytes = get_container_marshalled_bytes(
                datadir, CONTAINER_INVALID_REQUEST_TEMPLATE_JSON_FILE_NAME, owner_address
            )
            invalid_container_invocation_script_b64 = calculate_invocation_script(
                datadir, invalid_container_marshalled_bytes
            )
            verification_script_wrong_user_b64 = calculate_verification_script("Alice", "verifySignature")
            render_create_container_json(
                datadir,
                CONTAINER_INVALID_REQUEST_TEMPLATE_JSON_FILE_NAME,
                verification_script_wrong_user_b64,
                invalid_container_invocation_script_b64,
                owner_address,
            )
            with pytest.raises(Exception, match=".*container not saved within 10s.*"):
                result = neofs_env.neofs_cli(None).request.create_container(
                    body=os.path.join(datadir, CONTAINER_REQUEST_JSON_FILE_NAME),
                    endpoint=neofs_env.storage_nodes[0].endpoint,
                )

        with allure.step("Create container with wrong contract method"):
            verification_script_wrong_method_b64 = calculate_verification_script("Bob", "validateSignature")
            render_create_container_json(
                datadir,
                CONTAINER_INVALID_REQUEST_TEMPLATE_JSON_FILE_NAME,
                verification_script_wrong_method_b64,
                invalid_container_invocation_script_b64,
                owner_address,
            )
            with pytest.raises(Exception, match=".*container not saved within 10s.*"):
                result = neofs_env.neofs_cli(None).request.create_container(
                    body=os.path.join(datadir, CONTAINER_REQUEST_JSON_FILE_NAME),
                    endpoint=neofs_env.storage_nodes[0].endpoint,
                )

        with allure.step("Create container with wrong signature"):
            invocation_script_wrong_signature_container_b64 = calculate_invocation_script(
                datadir, bytes(~b & 0xFF for b in invalid_container_marshalled_bytes)
            )
            render_create_container_json(
                datadir,
                CONTAINER_INVALID_REQUEST_TEMPLATE_JSON_FILE_NAME,
                verification_script_b64,
                invocation_script_wrong_signature_container_b64,
                owner_address,
            )
            with pytest.raises(Exception, match=".*container not saved within 10s.*"):
                result = neofs_env.neofs_cli(None).request.create_container(
                    body=os.path.join(datadir, CONTAINER_REQUEST_JSON_FILE_NAME),
                    endpoint=neofs_env.storage_nodes[0].endpoint,
                )
