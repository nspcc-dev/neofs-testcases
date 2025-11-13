import logging

import allure
import pytest
from helpers.container import create_container
from helpers.file_helper import generate_file
from helpers.rest_gate import upload_via_rest_gate, verify_options_request
from helpers.wellknown_acl import PUBLIC_ACL
from neofs_testlib.env.env import NodeWallet
from rest_gw.rest_base import TestNeofsRestBase

logger = logging.getLogger("NeoLogger")


@pytest.mark.sanity
class TestRestOptions(TestNeofsRestBase):
    PLACEMENT_RULE = "REP 2 IN X CBF 1 SELECT 4 FROM * AS X"

    @pytest.fixture(scope="class", autouse=True)
    @allure.title("[Class/Autouse]: Prepare wallet and deposit")
    def prepare_wallet(self, default_wallet: NodeWallet):
        TestRestOptions.wallet = default_wallet

    @pytest.fixture(scope="class")
    def user_container(self) -> str:
        return create_container(
            wallet=self.wallet.path,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            rule=self.PLACEMENT_RULE,
            basic_acl=PUBLIC_ACL,
        )

    @pytest.fixture(scope="class")
    @pytest.mark.simple
    def user_object(self, gw_endpoint, user_container) -> str:
        return upload_via_rest_gate(
            cid=user_container,
            path=generate_file(self.neofs_env.get_object_size("simple_object_size")),
            endpoint=gw_endpoint,
        )

    def test_rest_options_requests(self, gw_endpoint, user_container, user_object):
        for rest_gw_path in (
            "/auth",
            "/auth/bearer",
            f"/accounting/balance/{self.wallet.address}",
            "/containers",
            f"/containers/{user_container}",
            f"/containers/{user_container}/eacl",
        ):
            verify_options_request(f"{gw_endpoint}{rest_gw_path}")
