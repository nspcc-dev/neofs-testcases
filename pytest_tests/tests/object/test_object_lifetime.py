import logging

import allure
import pytest
from helpers.container import create_container
from helpers.file_helper import generate_file, get_file_hash
from helpers.grpc_responses import OBJECT_NOT_FOUND
from helpers.neofs_verbs import get_object_from_random_node, put_object_to_random_node
from helpers.utility import wait_for_gc_pass_on_storage_nodes
from neofs_env.neofs_env_test_base import NeofsEnvTestBase
from neofs_testlib.env.env import NodeWallet
from pytest import FixtureRequest
from pytest_lazy_fixtures import lf

logger = logging.getLogger("NeoLogger")


class TestObjectApiLifetime(NeofsEnvTestBase):
    @allure.title("Test object life time")
    @pytest.mark.parametrize(
        "object_size,expiration_flag",
        [
            (lf("simple_object_size"), "lifetime"),
            (lf("complex_object_size"), "expire_at"),
        ],
        ids=["simple object, lifetime", "complex object, expire_at"],
    )
    def test_object_api_lifetime(
        self,
        default_wallet: NodeWallet,
        request: FixtureRequest,
        object_size: int,
        expiration_flag: str,
    ):
        """
        Test object deleted after expiration epoch.
        """

        allure.dynamic.title(f"Test object life time for {request.node.callspec.id}")

        wallet = default_wallet
        endpoint = self.neofs_env.sn_rpc
        cid = create_container(wallet.path, self.shell, endpoint)

        file_path = generate_file(object_size)
        file_hash = get_file_hash(file_path)
        epoch = self.get_epoch()

        oid = put_object_to_random_node(
            wallet.path,
            file_path,
            cid,
            self.shell,
            neofs_env=self.neofs_env,
            expire_at=epoch + 1 if expiration_flag == "expire_at" else None,
            lifetime=1 if expiration_flag == "lifetime" else None,
        )
        got_file = get_object_from_random_node(wallet.path, cid, oid, self.shell, neofs_env=self.neofs_env)
        assert get_file_hash(got_file) == file_hash

        with allure.step("Tick two epochs"):
            self.tick_epochs_and_wait(2)

        # Wait for GC, because object with expiration is counted as alive until GC removes it
        wait_for_gc_pass_on_storage_nodes()

        with allure.step("Check object deleted because it expires-on epoch"):
            with pytest.raises(Exception, match=OBJECT_NOT_FOUND):
                get_object_from_random_node(wallet.path, cid, oid, self.shell, neofs_env=self.neofs_env)
