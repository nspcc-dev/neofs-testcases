import json
import logging
import os

import allure
import neofs_env.neofs_epoch as neofs_epoch
import pytest
from helpers.container import create_container
from helpers.file_helper import generate_file, generate_file_with_content
from helpers.http_gate import (
    attr_into_header,
    get_object_by_attr_and_verify_hashes,
    get_via_rest_gate,
    try_to_get_object_and_expect_error,
    upload_via_rest_gate,
)
from helpers.neofs_verbs import put_object_to_random_node
from helpers.utility import wait_for_gc_pass_on_storage_nodes
from helpers.wellknown_acl import PUBLIC_ACL
from http_gw.http_utils import get_object_and_verify_hashes
from neofs_env.neofs_env_test_base import NeofsEnvTestBase

logger = logging.getLogger("NeoLogger")
OBJECT_NOT_FOUND_ERROR = "not found"


@allure.link(
    "https://github.com/nspcc-dev/neofs-rest-gw?tab=readme-ov-file#neofs-rest-gw",
    name="neofs-rest-gateway",
)
@pytest.mark.sanity
@pytest.mark.rest_gate
class TestRestGate(NeofsEnvTestBase):
    PLACEMENT_RULE_1 = "REP 1 IN X CBF 1 SELECT 1 FROM * AS X"
    PLACEMENT_RULE_2 = "REP 2 IN X CBF 2 SELECT 2 FROM * AS X"

    @pytest.fixture(scope="class", autouse=True)
    @allure.title("[Class/Autouse]: Prepare wallet and deposit")
    def prepare_wallet(self, default_wallet):
        TestRestGate.wallet = default_wallet

    @pytest.fixture(scope="class")
    def gw_params(self):
        return {
            "endpoint": f"http://{self.neofs_env.rest_gw.address}/v1",
            "wallet_path": self.neofs_env.rest_gw.wallet.path,
        }

    @allure.title("Test Put over gRPC, Get over HTTP")
    def test_put_grpc_get_http(self, complex_object_size, simple_object_size, gw_endpoint):
        """
        Test that object can be put using gRPC interface and get using HTTP.

        Steps:
        1. Create simple and large objects.
        2. Put objects using gRPC (neofs-cli).
        3. Download objects using HTTP gate (https://github.com/nspcc-dev/neofs-http-gw#downloading).
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
        file_path_simple, file_path_large = generate_file(simple_object_size), generate_file(complex_object_size)

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
    def test_put_http_get_http_content_disposition(self, simple_object_size, gw_params):
        cid = create_container(
            self.wallet.path,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            rule=self.PLACEMENT_RULE_2,
            basic_acl=PUBLIC_ACL,
        )

        with allure.step("Verify Content-Disposition"):
            file_path = generate_file(simple_object_size)

            oid = upload_via_rest_gate(
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
            content_disposition_type, filename = resp.headers["Content-Disposition"].split(";")
            assert content_disposition_type.strip() == "inline"
            assert filename.strip().split("=")[1] == file_path.split("/")[-1]

        with allure.step("Verify Content-Disposition with download=true"):
            file_path = generate_file(simple_object_size)

            oid = upload_via_rest_gate(
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
            content_disposition_type, filename = resp.headers["Content-Disposition"].split(";")
            assert content_disposition_type.strip() == "attachment"
            assert filename.strip().split("=")[1] == file_path.split("/")[-1]

    @allure.title("Verify Content-Type if uploaded without any Content-Type specified")
    def test_put_http_get_http_without_content_type(self, simple_object_size, gw_params):
        cid = create_container(
            self.wallet.path,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            rule=self.PLACEMENT_RULE_2,
            basic_acl=PUBLIC_ACL,
        )

        with allure.step("Upload binary object"):
            file_path = generate_file(simple_object_size)

            oid = upload_via_rest_gate(
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
            file_path = generate_file_with_content(simple_object_size, content="123")

            oid = upload_via_rest_gate(
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
    def test_put_http_get_http_with_x_atribute_content_type(self, simple_object_size, gw_params):
        cid = create_container(
            self.wallet.path,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            rule=self.PLACEMENT_RULE_2,
            basic_acl=PUBLIC_ACL,
        )

        with allure.step("Upload object with X-Attribute-Content-Type"):
            file_path = generate_file(simple_object_size)

            headers = {"X-Attribute-Content-Type": "CoolContentType"}
            oid = upload_via_rest_gate(
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

    @allure.title("Verify Content-Type if uploaded with multipart Content-Type")
    def test_put_http_get_http_with_multipart_content_type(self, gw_params):
        cid = create_container(
            self.wallet.path,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            rule=self.PLACEMENT_RULE_2,
            basic_acl=PUBLIC_ACL,
        )

        with allure.step("Upload object with multipart content type"):
            file_path = generate_file_with_content(0, content="123")

            oid = upload_via_rest_gate(
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
    def test_put_http_get_http_special_attributes(self, simple_object_size, gw_params):
        cid = create_container(
            self.wallet.path,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            rule=self.PLACEMENT_RULE_2,
            basic_acl=PUBLIC_ACL,
        )

        file_path = generate_file(simple_object_size)

        oid = upload_via_rest_gate(
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

    @allure.link("https://github.com/nspcc-dev/neofs-http-gw#uploading", name="uploading")
    @allure.link("https://github.com/nspcc-dev/neofs-http-gw#downloading", name="downloading")
    @allure.title("Test Put over HTTP, Get over HTTP")
    @pytest.mark.smoke
    def test_put_http_get_http(self, complex_object_size, simple_object_size, gw_params):
        """
        Test that object can be put and get using HTTP interface.

        Steps:
        1. Create simple and large objects.
        2. Upload objects using HTTP (https://github.com/nspcc-dev/neofs-http-gw#uploading).
        3. Download objects using HTTP gate (https://github.com/nspcc-dev/neofs-http-gw#downloading).
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
        file_path_simple, file_path_large = generate_file(simple_object_size), generate_file(complex_object_size)

        with allure.step("Put objects using HTTP"):
            oid_simple = upload_via_rest_gate(cid=cid, path=file_path_simple, endpoint=gw_params["endpoint"])
            oid_large = upload_via_rest_gate(cid=cid, path=file_path_large, endpoint=gw_params["endpoint"])

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

    @allure.link("https://github.com/nspcc-dev/neofs-http-gw#by-attributes", name="download by attributes")
    @allure.title("Test Put over HTTP, Get over HTTP with headers")
    @pytest.mark.parametrize(
        "attributes",
        [
            {"File-Name": "simple obj filename"},
            {"FileName": "simple obj filename"},
            pytest.param(
                {"cat%jpeg": "cat%jpeg"},
                marks=pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-rest-gw/issues/195"),
            ),
            pytest.param(
                {"Filename": "simple_obj_filename"},
                marks=pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-rest-gw/issues/168"),
            ),
        ],
        ids=["simple", "hyphen", "special", "percent"],
    )
    def test_put_http_get_http_with_headers(self, attributes: dict, simple_object_size, gw_params):
        """
        Test that object can be downloaded using different attributes in HTTP header.

        Steps:
        1. Create simple and large objects.
        2. Upload objects using HTTP with particular attributes in the header.
        3. Download objects by attributes using HTTP gate (https://github.com/nspcc-dev/neofs-http-gw#by-attributes).
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
        file_path = generate_file(simple_object_size)

        with allure.step("Put objects using HTTP with attribute"):
            headers = attr_into_header(attributes)
            oid = upload_via_rest_gate(
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
    def test_expiration_epoch_in_http(self, simple_object_size, gw_params):
        endpoint = self.neofs_env.sn_rpc

        cid = create_container(
            self.wallet.path,
            shell=self.shell,
            endpoint=endpoint,
            rule=self.PLACEMENT_RULE_2,
            basic_acl=PUBLIC_ACL,
        )
        file_path = generate_file(simple_object_size)
        oids = []

        curr_epoch = neofs_epoch.get_epoch(self.neofs_env)
        epochs = (curr_epoch, curr_epoch + 1, curr_epoch + 2, curr_epoch + 100)

        for epoch in epochs:
            headers = {"X-Attribute-Neofs-Expiration-Epoch": str(epoch)}

            with allure.step("Put objects using HTTP with attribute Expiration-Epoch"):
                oids.append(
                    upload_via_rest_gate(cid=cid, path=file_path, headers=headers, endpoint=gw_params["endpoint"])
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

    @pytest.mark.long
    @allure.title("Test Put over HTTP, Get over HTTP for large object")
    def test_put_http_get_http_large_file(self, complex_object_size, gw_params):
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

        obj_size = int(os.getenv("BIG_OBJ_SIZE", complex_object_size))
        file_path = generate_file(obj_size)

        with allure.step("Put objects using HTTP"):
            oid_gate = upload_via_rest_gate(cid=cid, path=file_path, endpoint=gw_params["endpoint"])

        get_object_and_verify_hashes(
            oid=oid_gate,
            file_name=file_path,
            wallet=self.wallet.path,
            cid=cid,
            shell=self.shell,
            nodes=self.neofs_env.storage_nodes,
            endpoint=gw_params["endpoint"],
        )
