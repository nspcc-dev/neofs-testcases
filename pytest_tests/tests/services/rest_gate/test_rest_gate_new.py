import json
import logging
import os
import time

import allure
import neofs_env.neofs_epoch as neofs_epoch
import pytest
from helpers.container import create_container
from helpers.file_helper import generate_file, generate_file_with_content
from helpers.neofs_verbs import put_object_to_random_node
from helpers.rest_gate import (
    get_epoch_duration_via_rest_gate,
    get_object_by_attr_and_verify_hashes,
    get_via_rest_gate,
    new_attr_into_header,
    new_upload_via_rest_gate,
    try_to_get_object_and_expect_error,
)
from helpers.utility import wait_for_gc_pass_on_storage_nodes
from helpers.wellknown_acl import PUBLIC_ACL
from rest_gw.rest_base import TestNeofsRestBase
from rest_gw.rest_utils import get_object_and_verify_hashes

logger = logging.getLogger("NeoLogger")
OBJECT_NOT_FOUND_ERROR = "not found"


@allure.link(
    "https://github.com/nspcc-dev/neofs-rest-gw?tab=readme-ov-file#neofs-rest-gw",
    name="neofs-rest-gateway",
)
@pytest.mark.sanity
class TestRestGate(TestNeofsRestBase):
    PLACEMENT_RULE_1 = "EC 2/2 IN X CBF 1 SELECT 1 FROM * AS X"
    PLACEMENT_RULE_2 = "REP 2 IN X CBF 2 SELECT 2 FROM * AS X"

    @pytest.fixture(scope="class", autouse=True)
    @allure.title("[Class/Autouse]: Prepare wallet and deposit")
    def prepare_wallet(self, default_wallet):
        TestRestGate.wallet = default_wallet

    @pytest.fixture(scope="class")
    def gw_params(self):
        return {
            "endpoint": f"http://{self.neofs_env.rest_gw.endpoint}/v1",
            "wallet_path": self.neofs_env.rest_gw.wallet.path,
        }

    @pytest.fixture
    def fresh_epoch(self):
        self.ensure_fresh_epoch()
        yield

    @allure.title("Test Put over gRPC, Get over REST")
    @pytest.mark.complex
    @pytest.mark.simple
    def test_put_grpc_get_rest(self, gw_endpoint):
        """
        Test that object can be put using gRPC interface and get using REST gateway.

        Steps:
        1. Create simple and large objects.
        2. Put objects using gRPC (neofs-cli).
        3. Download objects using REST gate (https://github.com/nspcc-dev/neofs-rest-gw).
        4. Get objects using gRPC (neofs-cli).
        5. Compare hashes for got objects.
        6. Compare hashes for got and original objects.

        Expected result:
        Hashes must be the same.
        """
        cid = create_container(
            self.wallet.path,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            rule=self.PLACEMENT_RULE_1,
            basic_acl=PUBLIC_ACL,
        )
        file_path_simple, file_path_large = (
            generate_file(self.neofs_env.get_object_size("simple_object_size")),
            generate_file(self.neofs_env.get_object_size("complex_object_size")),
        )

        with allure.step("Put objects using gRPC"):
            oid_simple = put_object_to_random_node(
                wallet=self.wallet.path,
                path=file_path_simple,
                cid=cid,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )
            oid_large = put_object_to_random_node(
                wallet=self.wallet.path,
                path=file_path_large,
                cid=cid,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        for oid, file_path in ((oid_simple, file_path_simple), (oid_large, file_path_large)):
            get_object_and_verify_hashes(
                oid=oid,
                file_name=file_path,
                wallet=self.wallet.path,
                cid=cid,
                shell=self.shell,
                nodes=self.neofs_env.storage_nodes,
                endpoint=gw_endpoint,
            )

    @allure.title("Verify Content-Disposition header")
    @pytest.mark.simple
    def test_put_http_get_http_content_disposition(self, gw_params):
        cid = create_container(
            self.wallet.path,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            rule=self.PLACEMENT_RULE_2,
            basic_acl=PUBLIC_ACL,
        )

        with allure.step("Verify Content-Disposition"):
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))

            oid = new_upload_via_rest_gate(
                cid=cid,
                path=file_path,
                endpoint=gw_params["endpoint"],
            )
            resp = get_via_rest_gate(
                cid=cid,
                oid=oid,
                endpoint=gw_params["endpoint"],
                return_response=True,
            )

            assert "inline" in resp.headers["Content-Disposition"]

        with allure.step("Verify Content-Disposition with download=true"):
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))

            oid = new_upload_via_rest_gate(
                cid=cid,
                path=file_path,
                endpoint=gw_params["endpoint"],
            )
            resp = get_via_rest_gate(
                cid=cid,
                oid=oid,
                endpoint=gw_params["endpoint"],
                return_response=True,
                download=True,
            )

            assert "attachment" in resp.headers["Content-Disposition"]

    @allure.title("Verify Content-Type if uploaded without any Content-Type specified")
    @pytest.mark.simple
    def test_put_http_get_http_without_content_type(self, gw_params):
        cid = create_container(
            self.wallet.path,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            rule=self.PLACEMENT_RULE_2,
            basic_acl=PUBLIC_ACL,
        )

        with allure.step("Upload binary object"):
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))

            oid = new_upload_via_rest_gate(
                cid=cid,
                path=file_path,
                endpoint=gw_params["endpoint"],
            )

            resp = get_via_rest_gate(
                cid=cid,
                oid=oid,
                endpoint=gw_params["endpoint"],
                return_response=True,
            )
            assert resp.headers["Content-Type"] == "application/octet-stream"

        with allure.step("Upload text object"):
            file_path = generate_file_with_content(self.neofs_env.get_object_size("simple_object_size"), content="123")

            oid = new_upload_via_rest_gate(
                cid=cid,
                path=file_path,
                endpoint=gw_params["endpoint"],
            )

            resp = get_via_rest_gate(
                cid=cid,
                oid=oid,
                endpoint=gw_params["endpoint"],
                return_response=True,
            )
            assert resp.headers["Content-Type"] == "text/plain; charset=utf-8"

    @allure.title("Verify Content-Type if uploaded with X-Attribute-Content-Type")
    @pytest.mark.simple
    def test_put_http_get_http_with_x_atribute_content_type(self, gw_params):
        cid = create_container(
            self.wallet.path,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            rule=self.PLACEMENT_RULE_2,
            basic_acl=PUBLIC_ACL,
        )

        with allure.step("Upload object with X-Attribute-Content-Type"):
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))

            headers = {"X-Attributes": '{"Content-Type":"CoolContentType"}'}
            oid = new_upload_via_rest_gate(
                cid=cid,
                path=file_path,
                headers=headers,
                endpoint=gw_params["endpoint"],
            )

            resp = get_via_rest_gate(
                cid=cid,
                oid=oid,
                endpoint=gw_params["endpoint"],
                return_response=True,
            )
            assert resp.headers["Content-Type"] == "CoolContentType"

    @allure.title("Verify Content-Type if uploaded with Content-Type")
    def test_put_http_get_http_with_content_type(self, gw_params):
        cid = create_container(
            self.wallet.path,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            rule=self.PLACEMENT_RULE_2,
            basic_acl=PUBLIC_ACL,
        )

        with allure.step("Upload object with content type"):
            file_path = generate_file_with_content(0, content="123")

            oid = new_upload_via_rest_gate(
                cid=cid,
                path=file_path,
                endpoint=gw_params["endpoint"],
                file_content_type="application/json",
            )

            resp = get_via_rest_gate(
                cid=cid,
                oid=oid,
                endpoint=gw_params["endpoint"],
                return_response=True,
            )
            assert resp.headers["Content-Type"] == "application/json"

    @allure.title("Verify special HTTP headers")
    @pytest.mark.simple
    def test_put_http_get_http_special_attributes(self, gw_params):
        cid = create_container(
            self.wallet.path,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            rule=self.PLACEMENT_RULE_2,
            basic_acl=PUBLIC_ACL,
        )

        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))

        oid = new_upload_via_rest_gate(
            cid=cid,
            path=file_path,
            endpoint=gw_params["endpoint"],
        )
        resp = get_via_rest_gate(
            cid=cid,
            oid=oid,
            endpoint=gw_params["endpoint"],
            return_response=True,
        )
        with open(gw_params["wallet_path"]) as wallet_file:
            wallet_json = json.load(wallet_file)

        assert resp.headers["X-Owner-Id"] == wallet_json["accounts"][0]["address"]
        assert resp.headers["X-Object-Id"] == oid
        assert resp.headers["X-Container-Id"] == cid

    @allure.link("https://github.com/nspcc-dev/neofs-rest-gw#docs", name="docs")
    @allure.title("Test Put over REST, Get over REST")
    @pytest.mark.complex
    @pytest.mark.simple
    def test_put_rest_get_rest(self, gw_params):
        """
        Test that object can be put and get using REST interface.

        Steps:
        1. Create simple and large objects.
        2. Upload objects using REST (https://github.com/nspcc-dev/neofs-rest-gw#docs).
        3. Download objects using REST gate (https://github.com/nspcc-dev/neofs-rest-gw#docs).
        4. Compare hashes for got and original objects.

        Expected result:
        Hashes must be the same.
        """
        cid = create_container(
            self.wallet.path,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            rule=self.PLACEMENT_RULE_2,
            basic_acl=PUBLIC_ACL,
        )
        file_path_simple, file_path_large = (
            generate_file(self.neofs_env.get_object_size("simple_object_size")),
            generate_file(self.neofs_env.get_object_size("complex_object_size")),
        )

        with allure.step("Put objects using REST"):
            oid_simple = new_upload_via_rest_gate(cid=cid, path=file_path_simple, endpoint=gw_params["endpoint"])
            oid_large = new_upload_via_rest_gate(cid=cid, path=file_path_large, endpoint=gw_params["endpoint"])

        for oid, file_path in ((oid_simple, file_path_simple), (oid_large, file_path_large)):
            get_object_and_verify_hashes(
                oid=oid,
                file_name=file_path,
                wallet=self.wallet.path,
                cid=cid,
                shell=self.shell,
                nodes=self.neofs_env.storage_nodes,
                endpoint=gw_params["endpoint"],
            )

    @allure.link("https://github.com/nspcc-dev/neofs-rest-gw#docs", name="docs")
    @allure.title("Test Put over REST, Get over REST with headers")
    @pytest.mark.parametrize(
        "attributes",
        [
            {"File-Name": "simple obj filename"},
            {"FileName": "simple obj filename"},
            {"Filename": "simple_obj_filename"},
            {"Filename": "simple_obj_filename\n"},
            {"Filename\n": "simple_obj_filename"},
            {"\n": "\n"},
            {
                """
            key
            """: """
            value
            """
            },
            {"\x01" * 8: "simple_obj_filename"},
            {"Some key": "\x01" * 8},
        ],
        ids=[
            "simple",
            "hyphen",
            "special",
            "linebreak in value",
            "linebreak in key",
            "linebreaks in key and value",
            "other linebreaks in key and value",
            "not-so-zero bytes key",
            "not-so-zero bytes value",
        ],
    )
    @pytest.mark.simple
    def test_put_rest_get_rest_with_headers(self, attributes: dict, gw_params):
        """
        Test that object can be downloaded using different attributes in HTTP header.

        Steps:
        1. Create simple and large objects.
        2. Upload objects using REST gate with particular attributes in the header.
        3. Download objects by attributes using REST gate (https://github.com/nspcc-dev/neofs-rest-gw#docs).
        4. Compare hashes for got and original objects.

        Expected result:
        Hashes must be the same.
        """
        cid = create_container(
            self.wallet.path,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            rule=self.PLACEMENT_RULE_2,
            basic_acl=PUBLIC_ACL,
        )
        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))

        with allure.step("Put objects using REST with attribute"):
            headers = new_attr_into_header(attributes)
            oid = new_upload_via_rest_gate(
                cid=cid,
                path=file_path,
                headers=headers,
                endpoint=gw_params["endpoint"],
            )

        get_object_by_attr_and_verify_hashes(
            oid=oid,
            file_name=file_path,
            cid=cid,
            attrs=attributes,
            endpoint=gw_params["endpoint"],
        )

    @allure.title("Test Expiration-Epoch in HTTP header")
    @pytest.mark.simple
    def test_expiration_epoch_in_rest(self, gw_params, fresh_epoch):
        endpoint = self.neofs_env.sn_rpc

        cid = create_container(
            self.wallet.path,
            shell=self.shell,
            endpoint=endpoint,
            rule=self.PLACEMENT_RULE_2,
            basic_acl=PUBLIC_ACL,
        )
        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        oids = []

        curr_epoch = neofs_epoch.get_epoch(self.neofs_env)
        epochs = (curr_epoch, curr_epoch + 1, curr_epoch + 2, curr_epoch + 100)

        for epoch in epochs:
            headers = {"X-Attributes": '{"__NEOFS__EXPIRATION_EPOCH": "' + str(epoch) + '"}'}

            with allure.step("Put objects using REST with attribute Expiration-Epoch"):
                oids.append(
                    new_upload_via_rest_gate(cid=cid, path=file_path, headers=headers, endpoint=gw_params["endpoint"])
                )

        assert len(oids) == len(epochs), "Expected all objects have been put successfully"

        with allure.step("All objects can be get"):
            for oid in oids:
                get_via_rest_gate(cid=cid, oid=oid, endpoint=gw_params["endpoint"])

        for expired_objects, not_expired_objects in [(oids[:1], oids[1:]), (oids[:2], oids[2:])]:
            self.tick_epochs_and_wait(1)

            # Wait for GC, because object with expiration is counted as alive until GC removes it
            wait_for_gc_pass_on_storage_nodes()

            for oid in expired_objects:
                try_to_get_object_and_expect_error(
                    cid=cid,
                    oid=oid,
                    error_pattern=OBJECT_NOT_FOUND_ERROR,
                    endpoint=gw_params["endpoint"],
                )

            with allure.step("Other objects can be get"):
                for oid in not_expired_objects:
                    get_via_rest_gate(cid=cid, oid=oid, endpoint=gw_params["endpoint"])

    @allure.title("Test other Expiration-Epoch settings in HTTP header")
    @pytest.mark.simple
    def test_expiration_headers_in_rest(self, gw_params, fresh_epoch):
        endpoint = self.neofs_env.sn_rpc

        cid = create_container(
            self.wallet.path,
            shell=self.shell,
            endpoint=endpoint,
            rule=self.PLACEMENT_RULE_2,
            basic_acl=PUBLIC_ACL,
        )
        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        oids = []

        epoch_duration = get_epoch_duration_via_rest_gate(endpoint=gw_params["endpoint"])
        current_ts = int(time.time())
        expiration_ts = current_ts + epoch_duration
        expiration_rfc3339 = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(expiration_ts))

        headers = (
            {"X-Neofs-Expiration-Timestamp": str(expiration_ts)},
            {"X-Neofs-Expiration-Duration": str(epoch_duration) + "s"},
            {"X-Neofs-Expiration-RFC3339": expiration_rfc3339},
        )

        for header in headers:
            keys_list = ",".join(header.keys())
            with allure.step("Put objects using REST with attributes: " + keys_list):
                oids.append(
                    new_upload_via_rest_gate(cid=cid, path=file_path, headers=header, endpoint=gw_params["endpoint"])
                )

        assert len(oids) == len(headers), "Expected all objects have been put successfully"

        with allure.step("All objects can be get"):
            for oid in oids:
                get_via_rest_gate(cid=cid, oid=oid, endpoint=gw_params["endpoint"])

        # Wait 2 epochs because REST gate rounds time to the next epoch.
        self.tick_epochs_and_wait(2)
        wait_for_gc_pass_on_storage_nodes()

        for oid in oids:
            try_to_get_object_and_expect_error(
                cid=cid,
                oid=oid,
                error_pattern=OBJECT_NOT_FOUND_ERROR,
                endpoint=gw_params["endpoint"],
            )

    @allure.title("Test Put over REST, Get over REST for large object")
    @pytest.mark.complex
    def test_put_rest_get_rest_large_file(self, gw_params):
        """
        This test checks upload and download with 'large' object.
        Large is object with size up to 20Mb.
        """
        cid = create_container(
            self.wallet.path,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            rule=self.PLACEMENT_RULE_2,
            basic_acl=PUBLIC_ACL,
        )

        obj_size = int(os.getenv("BIG_OBJ_SIZE", self.neofs_env.get_object_size("complex_object_size")))
        file_path = generate_file(obj_size)

        with allure.step("Put objects using REST"):
            oid_gate = new_upload_via_rest_gate(cid=cid, path=file_path, endpoint=gw_params["endpoint"])

        get_object_and_verify_hashes(
            oid=oid_gate,
            file_name=file_path,
            wallet=self.wallet.path,
            cid=cid,
            shell=self.shell,
            nodes=self.neofs_env.storage_nodes,
            endpoint=gw_params["endpoint"],
        )
