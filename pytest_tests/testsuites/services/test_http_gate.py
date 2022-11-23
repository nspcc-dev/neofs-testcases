import logging
import os
from random import choice
from time import sleep

import allure
import pytest
from common import COMPLEX_OBJ_SIZE
from container import create_container
from epoch import get_epoch, tick_epoch
from file_helper import generate_file, get_file_hash
from neofs_testlib.hosting import Hosting
from neofs_testlib.shell import Shell
from python_keywords.http_gate import (
    get_via_http_curl,
    get_via_http_gate,
    get_via_http_gate_by_attribute,
    get_via_zip_http_gate,
    upload_via_http_gate,
    upload_via_http_gate_curl,
)
from python_keywords.neofs_verbs import get_object, put_object
from python_keywords.storage_policy import get_nodes_without_object
from utility import wait_for_gc_pass_on_storage_nodes
from wellknown_acl import PUBLIC_ACL

logger = logging.getLogger("NeoLogger")
OBJECT_NOT_FOUND_ERROR = "not found"

# For some reason object uploaded via http gateway is not immediately available for downloading
# Until this issue is resolved we are waiting for some time before attempting to read an object
# TODO: remove after https://github.com/nspcc-dev/neofs-http-gw/issues/176 is fixed
OBJECT_UPLOAD_DELAY = 10


@allure.link(
    "https://github.com/nspcc-dev/neofs-http-gw#neofs-http-gateway", name="neofs-http-gateway"
)
@allure.link("https://github.com/nspcc-dev/neofs-http-gw#uploading", name="uploading")
@allure.link("https://github.com/nspcc-dev/neofs-http-gw#downloading", name="downloading")
@pytest.mark.http_gate
class TestHttpGate:
    PLACEMENT_RULE = "REP 1 IN X CBF 1 SELECT 1 FROM * AS X"

    @pytest.fixture(scope="class", autouse=True)
    @allure.title("[Class/Autouse]: Prepare wallet and deposit")
    def prepare_wallet(self, prepare_wallet_and_deposit):
        TestHttpGate.wallet = prepare_wallet_and_deposit

    @allure.title("Test Put over gRPC, Get over HTTP")
    def test_put_grpc_get_http(self, client_shell):
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
            self.wallet, shell=client_shell, rule=self.PLACEMENT_RULE, basic_acl=PUBLIC_ACL
        )
        file_path_simple, file_path_large = generate_file(), generate_file(COMPLEX_OBJ_SIZE)

        with allure.step("Put objects using gRPC"):
            oid_simple = put_object(
                wallet=self.wallet, path=file_path_simple, cid=cid, shell=client_shell
            )
            oid_large = put_object(
                wallet=self.wallet, path=file_path_large, cid=cid, shell=client_shell
            )

        for oid, file_path in ((oid_simple, file_path_simple), (oid_large, file_path_large)):
            self.get_object_and_verify_hashes(oid, file_path, self.wallet, cid, shell=client_shell)

    @allure.link("https://github.com/nspcc-dev/neofs-http-gw#uploading", name="uploading")
    @allure.link("https://github.com/nspcc-dev/neofs-http-gw#downloading", name="downloading")
    @pytest.mark.sanity
    @allure.title("Test Put over HTTP, Get over HTTP")
    def test_put_http_get_http(self, client_shell):
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
            self.wallet, shell=client_shell, rule=self.PLACEMENT_RULE, basic_acl=PUBLIC_ACL
        )
        file_path_simple, file_path_large = generate_file(), generate_file(COMPLEX_OBJ_SIZE)

        with allure.step("Put objects using HTTP"):
            oid_simple = upload_via_http_gate(cid=cid, path=file_path_simple)
            oid_large = upload_via_http_gate(cid=cid, path=file_path_large)

        for oid, file_path in ((oid_simple, file_path_simple), (oid_large, file_path_large)):
            self.get_object_and_verify_hashes(oid, file_path, self.wallet, cid, shell=client_shell)

    @allure.link(
        "https://github.com/nspcc-dev/neofs-http-gw#by-attributes", name="download by attributes"
    )
    @allure.title("Test Put over HTTP, Get over HTTP with headers")
    @pytest.mark.parametrize(
        "attributes",
        [
            {"fileName": "simple_obj_filename"},
            {"file-Name": "simple obj filename"},
            {"cat%jpeg": "cat%jpeg"},
        ],
        ids=["simple", "hyphen", "percent"],
    )
    def test_put_http_get_http_with_headers(self, client_shell, attributes: dict):
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
            self.wallet, shell=client_shell, rule=self.PLACEMENT_RULE, basic_acl=PUBLIC_ACL
        )
        file_path = generate_file()

        with allure.step("Put objects using HTTP with attribute"):
            headers = self._attr_into_header(attributes)
            oid = upload_via_http_gate(cid=cid, path=file_path, headers=headers)

        sleep(OBJECT_UPLOAD_DELAY)

        self.get_object_by_attr_and_verify_hashes(oid, file_path, cid, attributes)

    @allure.title("Test Expiration-Epoch in HTTP header")
    def test_expiration_epoch_in_http(self, client_shell, hosting):
        cid = create_container(
            self.wallet, shell=client_shell, rule=self.PLACEMENT_RULE, basic_acl=PUBLIC_ACL
        )
        file_path = generate_file()
        oids = []

        curr_epoch = get_epoch(client_shell)
        epochs = (curr_epoch, curr_epoch + 1, curr_epoch + 2, curr_epoch + 100)

        for epoch in epochs:
            headers = {"X-Attribute-Neofs-Expiration-Epoch": str(epoch)}

            with allure.step("Put objects using HTTP with attribute Expiration-Epoch"):
                oids.append(upload_via_http_gate(cid=cid, path=file_path, headers=headers))

        assert len(oids) == len(epochs), "Expected all objects have been put successfully"

        with allure.step("All objects can be get"):
            for oid in oids:
                get_via_http_gate(cid=cid, oid=oid)

        for expired_objects, not_expired_objects in [(oids[:1], oids[1:]), (oids[:2], oids[2:])]:
            tick_epoch(shell=client_shell, hosting=hosting)

            # Wait for GC, because object with expiration is counted as alive until GC removes it
            wait_for_gc_pass_on_storage_nodes()

            for oid in expired_objects:
                self.try_to_get_object_and_expect_error(
                    cid=cid, oid=oid, error_pattern=OBJECT_NOT_FOUND_ERROR
                )

            with allure.step("Other objects can be get"):
                for oid in not_expired_objects:
                    get_via_http_gate(cid=cid, oid=oid)

    @allure.title("Test Zip in HTTP header")
    def test_zip_in_http(self, client_shell):
        cid = create_container(
            self.wallet, shell=client_shell, rule=self.PLACEMENT_RULE, basic_acl=PUBLIC_ACL
        )
        file_path_simple, file_path_large = generate_file(), generate_file(COMPLEX_OBJ_SIZE)
        common_prefix = "my_files"

        headers1 = {"X-Attribute-FilePath": f"{common_prefix}/file1"}
        headers2 = {"X-Attribute-FilePath": f"{common_prefix}/file2"}

        upload_via_http_gate(cid=cid, path=file_path_simple, headers=headers1)
        upload_via_http_gate(cid=cid, path=file_path_large, headers=headers2)

        sleep(OBJECT_UPLOAD_DELAY)

        dir_path = get_via_zip_http_gate(cid=cid, prefix=common_prefix)

        with allure.step("Verify hashes"):
            assert get_file_hash(f"{dir_path}/file1") == get_file_hash(file_path_simple)
            assert get_file_hash(f"{dir_path}/file2") == get_file_hash(file_path_large)

    @pytest.mark.curl
    @pytest.mark.long
    @allure.title("Test Put over HTTP/Curl, Get over HTTP/Curl for large object")
    def test_put_http_get_http_large_file(self, client_shell):
        """
        This test checks upload and download using curl with 'large' object. Large is object with size up to 20Mb.
        """
        cid = create_container(
            self.wallet, shell=client_shell, rule=self.PLACEMENT_RULE, basic_acl=PUBLIC_ACL
        )

        obj_size = int(os.getenv("BIG_OBJ_SIZE", COMPLEX_OBJ_SIZE))
        file_path = generate_file(obj_size)

        with allure.step("Put objects using HTTP"):
            oid_gate = upload_via_http_gate(cid=cid, path=file_path)
            oid_curl = upload_via_http_gate_curl(cid=cid, filepath=file_path, large_object=True)

        self.get_object_and_verify_hashes(oid_gate, file_path, self.wallet, cid, shell=client_shell)
        self.get_object_and_verify_hashes(
            oid_curl,
            file_path,
            self.wallet,
            cid,
            shell=client_shell,
            object_getter=get_via_http_curl,
        )

    @pytest.mark.curl
    @allure.title("Test Put/Get over HTTP using Curl utility")
    def test_put_http_get_http_curl(self, client_shell):
        """
        Test checks upload and download over HTTP using curl utility.
        """
        cid = create_container(
            self.wallet, shell=client_shell, rule=self.PLACEMENT_RULE, basic_acl=PUBLIC_ACL
        )
        file_path_simple, file_path_large = generate_file(), generate_file(COMPLEX_OBJ_SIZE)

        with allure.step("Put objects using curl utility"):
            oid_simple = upload_via_http_gate_curl(cid=cid, filepath=file_path_simple)
            oid_large = upload_via_http_gate_curl(cid=cid, filepath=file_path_large)

        for oid, file_path in ((oid_simple, file_path_simple), (oid_large, file_path_large)):
            self.get_object_and_verify_hashes(
                oid,
                file_path,
                self.wallet,
                cid,
                shell=client_shell,
                object_getter=get_via_http_curl,
            )

    @staticmethod
    @allure.step("Try to get object and expect error")
    def try_to_get_object_and_expect_error(cid: str, oid: str, error_pattern: str) -> None:
        try:
            get_via_http_gate(cid=cid, oid=oid)
            raise AssertionError(f"Expected error on getting object with cid: {cid}")
        except Exception as err:
            match = error_pattern.casefold() in str(err).casefold()
            assert match, f"Expected {err} to match {error_pattern}"

    @staticmethod
    @allure.step("Verify object can be get using HTTP header attribute")
    def get_object_by_attr_and_verify_hashes(
        oid: str, file_name: str, cid: str, attrs: dict
    ) -> None:
        got_file_path_http = get_via_http_gate(cid=cid, oid=oid)
        got_file_path_http_attr = get_via_http_gate_by_attribute(cid=cid, attribute=attrs)

        TestHttpGate._assert_hashes_are_equal(
            file_name, got_file_path_http, got_file_path_http_attr
        )

    @staticmethod
    @allure.step("Verify object can be get using HTTP")
    def get_object_and_verify_hashes(
        oid: str, file_name: str, wallet: str, cid: str, shell: Shell, object_getter=None
    ) -> None:
        nodes = get_nodes_without_object(wallet=wallet, cid=cid, oid=oid, shell=shell)
        random_node = choice(nodes)
        object_getter = object_getter or get_via_http_gate

        got_file_path = get_object(
            wallet=wallet, cid=cid, oid=oid, shell=shell, endpoint=random_node
        )
        got_file_path_http = object_getter(cid=cid, oid=oid)

        TestHttpGate._assert_hashes_are_equal(file_name, got_file_path, got_file_path_http)

    @staticmethod
    def _assert_hashes_are_equal(orig_file_name: str, got_file_1: str, got_file_2: str) -> None:
        msg = "Expected hashes are equal for files {f1} and {f2}"
        got_file_hash_http = get_file_hash(got_file_1)
        assert get_file_hash(got_file_2) == got_file_hash_http, msg.format(
            f1=got_file_2, f2=got_file_1
        )
        assert get_file_hash(orig_file_name) == got_file_hash_http, msg.format(
            f1=orig_file_name, f2=got_file_1
        )

    @staticmethod
    def _attr_into_header(attrs: dict) -> dict:
        return {f"X-Attribute-{_key}": _value for _key, _value in attrs.items()}
