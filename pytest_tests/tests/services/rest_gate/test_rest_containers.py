import base64
import binascii
import hashlib
import logging
import os

import allure
import pytest
from helpers.file_helper import generate_file
from helpers.rest_gate import (
    create_container,
    delete_container,
    get_container_info,
    get_container_token,
    upload_via_rest_gate,
)
from helpers.wellknown_acl import EACL_PUBLIC_READ_WRITE, PUBLIC_ACL
from neo3.core import cryptography
from neo3.wallet.wallet import Wallet
from neofs_env.neofs_env_test_base import NeofsEnvTestBase
from neofs_testlib.env.env import NodeWallet
from neofs_testlib.utils.converters import load_wallet

logger = logging.getLogger("NeoLogger")


@pytest.mark.sanity
@pytest.mark.rest_gate
class TestRestContainers(NeofsEnvTestBase):
    PLACEMENT_RULE = "REP 2 IN X CBF 1 SELECT 4 FROM * AS X"

    @pytest.fixture(scope="class", autouse=True)
    @allure.title("[Class/Autouse]: Prepare wallet and deposit")
    def prepare_wallet(self, default_wallet: NodeWallet):
        TestRestContainers.wallet = default_wallet

    def generate_credentials(self, gw_endpoint: str, verb="PUT", wallet_connect=False) -> tuple:
        neo3_wallet: Wallet = load_wallet(self.wallet.path, self.wallet.password)
        acc = neo3_wallet.accounts[0]
        token = get_container_token(gw_endpoint, acc.address, verb=verb)
        private_key = acc.private_key_from_nep2(
            acc.encrypted_key.decode("utf-8"), self.wallet.password, _scrypt_parameters=acc.scrypt_parameters
        )

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

    @pytest.mark.parametrize("wallet_connect", [True, False])
    def test_rest_gw_containers_sanity(self, simple_object_size: int, gw_endpoint: str, wallet_connect: bool):
        session_token, signature, pub_key = self.generate_credentials(gw_endpoint, wallet_connect=wallet_connect)
        cid = create_container(
            gw_endpoint,
            "rest_gw_container",
            self.PLACEMENT_RULE,
            PUBLIC_ACL,
            session_token,
            signature,
            pub_key,
            wallet_connect=wallet_connect,
        )

        resp = get_container_info(gw_endpoint, cid)

        assert resp["containerId"] == cid, "Invalid containerId"
        assert resp["basicAcl"] == PUBLIC_ACL.lower().strip("0"), "Invalid ACL"
        assert resp["placementPolicy"].replace("\n", " ") == self.PLACEMENT_RULE, "Invalid placementPolicy"
        assert resp["cannedAcl"] == EACL_PUBLIC_READ_WRITE, "Invalid cannedAcl"

        upload_via_rest_gate(
            cid=cid,
            path=generate_file(simple_object_size),
            endpoint=gw_endpoint,
        )

        session_token, signature, pub_key = self.generate_credentials(
            gw_endpoint, verb="DELETE", wallet_connect=wallet_connect
        )
        delete_container(gw_endpoint, cid, session_token, signature, pub_key, wallet_connect=wallet_connect)
