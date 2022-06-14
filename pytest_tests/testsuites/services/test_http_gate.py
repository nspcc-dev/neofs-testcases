import logging
from random import choice
from time import sleep

import allure
import pytest
from contract_keywords import get_epoch, tick_epoch
from python_keywords.http_gate import (get_via_http_curl, get_via_http_gate,
                                       get_via_http_gate_by_attribute,
                                       get_via_zip_http_gate,
                                       upload_via_http_gate,
                                       upload_via_http_gate_curl)
from python_keywords.neofs_verbs import get_object, put_object
from python_keywords.storage_policy import get_nodes_without_object
from python_keywords.utility_keywords import get_file_hash

logger = logging.getLogger('NeoLogger')

CLEANUP_TIMEOUT = 10


@allure.link('https://github.com/nspcc-dev/neofs-http-gw#neofs-http-gateway', name='neofs-http-gateway')
@allure.link('https://github.com/nspcc-dev/neofs-http-gw#uploading', name='uploading')
@allure.link('https://github.com/nspcc-dev/neofs-http-gw#downloading', name='downloading')
@pytest.mark.http_gate
class TestHttpGate:

    @allure.title('Test Put over gRPC, Get over HTTP')
    def test_put_grpc_get_http(self, prepare_public_container, generate_files):
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
        cid, wallet = prepare_public_container
        file_name_simple, large_file_name = generate_files

        with allure.step('Put objects using gRPC'):
            oid_simple = put_object(wallet=wallet, path=file_name_simple, cid=cid)
            oid_large = put_object(wallet=wallet, path=large_file_name, cid=cid)

        for oid, file_name in ((oid_simple, file_name_simple), (oid_large, large_file_name)):
            self.get_object_and_verify_hashes(oid, file_name, wallet, cid)

    @allure.link('https://github.com/nspcc-dev/neofs-http-gw#uploading', name='uploading')
    @allure.link('https://github.com/nspcc-dev/neofs-http-gw#downloading', name='downloading')
    @allure.title('Test Put over HTTP, Get over HTTP')
    def test_put_http_get_http(self, prepare_public_container, generate_files):
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
        cid, wallet = prepare_public_container
        file_name_simple, large_file_name = generate_files

        with allure.step('Put objects using HTTP'):
            oid_simple = upload_via_http_gate(cid=cid, path=file_name_simple)
            oid_large = upload_via_http_gate(cid=cid, path=large_file_name)

        for oid, file_name in ((oid_simple, file_name_simple), (oid_large, large_file_name)):
            self.get_object_and_verify_hashes(oid, file_name, wallet, cid)

    @allure.link('https://github.com/nspcc-dev/neofs-http-gw#by-attributes', name='download by attributes')
    @allure.title('Test Put over HTTP, Get over HTTP with headers')
    @pytest.mark.parametrize('attributes',
                             [
                                 {'fileName': 'simple_obj_filename'},
                                 {'file-Name': 'simple obj filename'},
                                 {'cat%jpeg': 'cat%jpeg'}
                             ], ids=['simple', 'hyphen', 'percent']
                             )
    def test_put_http_get_http_with_headers(self, prepare_public_container, generate_files, attributes):
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
        cid, wallet = prepare_public_container
        file_name_simple, _ = generate_files

        with allure.step('Put objects using HTTP with attribute'):
            oid_simple = upload_via_http_gate(cid=cid, path=file_name_simple,
                                              headers=self._attr_into_header(attributes))

        self.get_object_by_attr_and_verify_hashes(oid_simple, file_name_simple, cid, attributes)

    @allure.title('Test Expiration-Epoch in HTTP header')
    def test_expiration_epoch_in_http(self, prepare_public_container, generate_file):
        cid, wallet = prepare_public_container
        file_name_simple = generate_file
        object_not_found_err = 'object not found'
        oids = []

        curr_epoch = get_epoch()
        epochs = (curr_epoch, curr_epoch + 1, curr_epoch + 2, curr_epoch + 100)

        for epoch in epochs:
            headers = {'X-Attribute-Neofs-Expiration-Epoch': str(epoch)}

            with allure.step('Put objects using HTTP with attribute Expiration-Epoch'):
                oids.append(upload_via_http_gate(cid=cid, path=file_name_simple, headers=headers))

        assert len(oids) == len(epochs), 'Expected all objects has been put successfully'

        with allure.step('All objects can be get'):
            for oid in oids:
                get_via_http_gate(cid=cid, oid=oid)

        for expired_objects, not_expired_objects in [(oids[:1], oids[1:]), (oids[:2], oids[2:])]:
            tick_epoch()
            sleep(CLEANUP_TIMEOUT)

            for oid in expired_objects:
                self.try_to_get_object_and_expect_error(cid=cid, oid=oid, expected_err=object_not_found_err)

            with allure.step('Other objects can be get'):
                for oid in not_expired_objects:
                    get_via_http_gate(cid=cid, oid=oid)

    @allure.title('Test Zip in HTTP header')
    def test_zip_in_http(self, prepare_public_container, generate_files):
        cid, wallet = prepare_public_container
        file_name_simple, file_name_complex = generate_files
        common_prefix = 'my_files'

        headers1 = {'X-Attribute-FilePath': f'{common_prefix}/file1'}
        headers2 = {'X-Attribute-FilePath': f'{common_prefix}/file2'}

        upload_via_http_gate(cid=cid, path=file_name_simple, headers=headers1)
        upload_via_http_gate(cid=cid, path=file_name_complex, headers=headers2)

        dir_path = get_via_zip_http_gate(cid=cid, prefix=common_prefix)

        with allure.step('Verify hashes'):
            assert get_file_hash(f'{dir_path}/file1') == get_file_hash(file_name_simple)
            assert get_file_hash(f'{dir_path}/file2') == get_file_hash(file_name_complex)

    @pytest.mark.curl
    @pytest.mark.long
    @allure.title('Test Put over HTTP/Curl, Get over HTTP/Curl for large object')
    def test_put_http_get_http_large_file(self, prepare_public_container, generate_large_file):
        """
        This test checks upload and download using curl with 'large' object. Large is object with size up to 20Mb.
        """
        cid, wallet = prepare_public_container
        file_path, file_hash = generate_large_file

        with allure.step('Put objects using HTTP'):
            oid_simple = upload_via_http_gate(cid=cid, path=file_path)
            oid_curl = upload_via_http_gate_curl(cid=cid, filepath=file_path, large_object=True)

        self.get_object_and_verify_hashes(oid_simple, file_path, wallet, cid)
        self.get_object_and_verify_hashes(oid_curl, file_path, wallet, cid, object_getter=get_via_http_curl)

    @pytest.mark.curl
    @allure.title('Test Put/Get over HTTP using Curl utility')
    def test_put_http_get_http_curl(self, prepare_public_container, generate_files):
        """
        Test checks upload and download over HTTP using curl utility.
        """
        cid, wallet = prepare_public_container
        file_name_simple, large_file_name = generate_files

        with allure.step('Put objects using curl utility'):
            oid_simple = upload_via_http_gate_curl(cid=cid, filepath=file_name_simple)
            oid_large = upload_via_http_gate_curl(cid=cid, filepath=large_file_name)

        for oid, file_name in ((oid_simple, file_name_simple), (oid_large, large_file_name)):
            self.get_object_and_verify_hashes(oid, file_name, wallet, cid, object_getter=get_via_http_curl)

    @staticmethod
    @allure.step('Try to get object and expect error')
    def try_to_get_object_and_expect_error(cid: str, oid: str, expected_err: str):
        try:
            get_via_http_gate(cid=cid, oid=oid)
            raise AssertionError(f'Expected error on getting object with cid: {cid}')
        except Exception as err:
            assert expected_err in str(err), f'Expected error {expected_err} in {err}'

    @staticmethod
    @allure.step('Verify object can be get using HTTP header attribute')
    def get_object_by_attr_and_verify_hashes(oid: str, file_name: str, cid: str, attrs: dict):

        got_file_path_http = get_via_http_gate(cid=cid, oid=oid)
        got_file_path_http_attr = get_via_http_gate_by_attribute(cid=cid, attribute=attrs)

        TestHttpGate._assert_hashes_the_same(file_name, got_file_path_http, got_file_path_http_attr)

    @staticmethod
    @allure.step('Verify object can be get using HTTP')
    def get_object_and_verify_hashes(oid: str, file_name: str, wallet: str, cid: str, object_getter=None):
        nodes = get_nodes_without_object(wallet=wallet, cid=cid, oid=oid)
        random_node = choice(nodes)
        object_getter = object_getter or get_via_http_gate

        got_file_path = get_object(wallet=wallet, cid=cid, oid=oid, endpoint=random_node)
        got_file_path_http = object_getter(cid=cid, oid=oid)

        TestHttpGate._assert_hashes_the_same(file_name, got_file_path, got_file_path_http)

    @staticmethod
    def _assert_hashes_the_same(orig_file_name: str, got_file_1: str, got_file_2: str):
        msg = 'Expected hashes are equal for files {f1} and {f2}'
        got_file_hash_http = get_file_hash(got_file_1)
        assert get_file_hash(got_file_2) == got_file_hash_http, msg.format(f1=got_file_2, f2=got_file_1)
        assert get_file_hash(orig_file_name) == got_file_hash_http, msg.format(f1=orig_file_name, f2=got_file_1)

    @staticmethod
    def _attr_into_header(attrs: dict) -> dict:
        return {f'X-Attribute-{_key}': _value for _key, _value in attrs.items()}
