import logging

import allure
import pytest
from helpers.file_helper import generate_file
from helpers.rest_gate import (
    create_container,
    delete_container,
    get_container_info,
    upload_via_rest_gate,
)
from helpers.wellknown_acl import EACL_PUBLIC_READ_WRITE, PUBLIC_ACL
from neofs_testlib.env.env import NodeWallet
from rest_gw.rest_base import TestNeofsRestBase
from rest_gw.rest_utils import generate_credentials

logger = logging.getLogger("NeoLogger")


@pytest.mark.sanity
class TestRestContainers(TestNeofsRestBase):
    PLACEMENT_RULE = "REP 2 IN X CBF 1 SELECT 4 FROM * AS X"

    @pytest.fixture(scope="class", autouse=True)
    @allure.title("[Class/Autouse]: Prepare wallet and deposit")
    def prepare_wallet(self, default_wallet: NodeWallet):
        TestRestContainers.wallet = default_wallet

    @pytest.mark.parametrize("wallet_connect", [True, False])
    @pytest.mark.parametrize("new_api", [True, False])
    @pytest.mark.parametrize("bearer_for_all_users", [True, False, None])
    @pytest.mark.simple
    def test_rest_gw_containers_sanity(
        self, gw_endpoint: str, wallet_connect: bool, new_api: bool, bearer_for_all_users: bool
    ):
        session_token, signature, pub_key = generate_credentials(
            gw_endpoint, self.wallet, wallet_connect=wallet_connect, bearer_for_all_users=bearer_for_all_users
        )
        cid = create_container(
            gw_endpoint,
            "rest_gw_container",
            self.PLACEMENT_RULE,
            PUBLIC_ACL,
            session_token,
            signature,
            pub_key,
            wallet_connect=wallet_connect,
            new_api=new_api,
        )

        resp = get_container_info(gw_endpoint, cid)

        assert resp["containerId"] == cid, "Invalid containerId"
        assert resp["basicAcl"] == PUBLIC_ACL.lower().strip("0"), "Invalid ACL"
        assert resp["placementPolicy"].replace("\n", " ") == self.PLACEMENT_RULE, "Invalid placementPolicy"
        assert resp["cannedAcl"] == EACL_PUBLIC_READ_WRITE, "Invalid cannedAcl"

        upload_via_rest_gate(
            cid=cid,
            path=generate_file(self.neofs_env.get_object_size("simple_object_size")),
            endpoint=gw_endpoint,
        )

        session_token, signature, pub_key = generate_credentials(
            gw_endpoint, self.wallet, verb="DELETE", wallet_connect=wallet_connect
        )
        delete_container(gw_endpoint, cid, session_token, signature, pub_key, wallet_connect=wallet_connect)
