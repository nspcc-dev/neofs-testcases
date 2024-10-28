import logging

import allure
import pytest
import requests
from helpers.common import DEFAULT_OBJECT_OPERATION_TIMEOUT
from helpers.container import create_container
from helpers.file_helper import generate_file
from helpers.wellknown_acl import PUBLIC_ACL
from neofs_env.neofs_env_test_base import NeofsEnvTestBase
from pytest_lazy_fixtures import lf
from rest_gw.rest_utils import get_object_and_verify_hashes

logger = logging.getLogger("NeoLogger")


@pytest.mark.sanity
class Test_rest_streaming(NeofsEnvTestBase):
    PLACEMENT_RULE = "REP 2 IN X CBF 1 SELECT 4 FROM * AS X"

    @pytest.fixture(scope="class", autouse=True)
    @allure.title("[Class/Autouse]: Prepare wallet and deposit")
    def prepare_wallet(self, default_wallet):
        Test_rest_streaming.wallet = default_wallet

    @allure.title("Test Put via pipe (steaming), Get over HTTP and verify hashes")
    @pytest.mark.parametrize(
        "object_size",
        [lf("complex_object_size")],
        ids=["complex object"],
    )
    def test_object_can_be_put_get_by_streaming(self, object_size: int, gw_endpoint):
        """
        Test that object can be put using gRPC interface and get using HTTP.

        Steps:
        1. Create big object;
        2. Put object using curl with pipe (streaming);
        3. Download object using HTTP gate (https://github.com/nspcc-dev/neofs-http-gw#downloading);
        4. Compare hashes between original and downloaded object;

        Expected result:
        Hashes must be the same.
        """
        with allure.step("Create public container and verify container creation"):
            cid = create_container(
                self.wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                rule=self.PLACEMENT_RULE,
                basic_acl=PUBLIC_ACL,
            )
        with allure.step("Allocate big object"):
            # Generate file
            file_path = generate_file(object_size)

        with allure.step("Put objects and Get object and verify hashes [ get/$CID/$OID ]"):
            # https://docs.python-requests.org/en/latest/user/advanced/#streaming-uploads
            with open(file_path, "rb") as file:
                resp = requests.post(
                    f"{gw_endpoint}/objects/{cid}", data=file, timeout=DEFAULT_OBJECT_OPERATION_TIMEOUT
                )

            if not resp.ok:
                raise Exception(
                    f"""Failed to stream object via REST gate:
                        request: {resp.request.path_url},
                        response: {resp.text},
                        status code: {resp.status_code} {resp.reason}"""
                )

            assert resp.json().get("object_id"), f"OID found in response {resp}"

            oid = resp.json().get("object_id")

            get_object_and_verify_hashes(
                oid=oid,
                file_name=file_path,
                wallet=self.wallet.path,
                cid=cid,
                shell=self.shell,
                nodes=self.neofs_env.storage_nodes,
                endpoint=gw_endpoint,
            )
