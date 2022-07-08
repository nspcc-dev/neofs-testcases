import os
from typing import Tuple

import allure
import pytest

import wallet
from common import ASSETS_DIR
from python_keywords.acl import set_eacl
from python_keywords.container import create_container
from python_keywords.neofs_verbs import (delete_object, get_object, get_range,
                                         get_range_hash, head_object,
                                         put_object, search_object)
from python_keywords.utility_keywords import generate_file, get_file_hash

RESOURCE_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    '../../../robot/resources/files/',
)


@pytest.mark.sanity
@pytest.mark.acl
class TestACL:
    @pytest.fixture(autouse=True)
    def create_two_wallets(self, prepare_wallet_and_deposit):
        self.main_wallet = prepare_wallet_and_deposit
        self.other_wallet = wallet.init_wallet(ASSETS_DIR)

    @allure.title('Test basic ACL')
    def test_basic_acl(self):
        """
        Test basic ACL set during container creation.
        """
        file_name = generate_file()

        with allure.step('Create public container and check access'):
            cid_public = create_container(self.main_wallet, basic_acl='public-read-write')
            self.check_full_access(cid_public, file_name)

        with allure.step('Create private container and check only owner has access'):
            cid_private = create_container(self.main_wallet, basic_acl='private')

            with allure.step('Check owner can put/get object into private container'):
                oid = put_object(wallet=self.main_wallet, path=file_name, cid=cid_private)

                got_file = get_object(self.main_wallet, cid_private, oid)
                assert get_file_hash(got_file) == get_file_hash(file_name)

            with allure.step('Check no one except owner has access to operations with container'):
                self.check_no_access_to_container(self.other_wallet, cid_private, oid, file_name)

            delete_object(self.main_wallet, cid_private, oid)

    @allure.title('Test extended ACL')
    def test_extended_acl(self):
        """
        Test basic extended ACL applied after container creation.
        """
        file_name = generate_file()
        deny_all_eacl = os.path.join(RESOURCE_DIR, 'eacl_tables/gen_eacl_deny_all_OTHERS')

        with allure.step('Create public container and check access'):
            cid_public = create_container(self.main_wallet, basic_acl='eacl-public-read-write')
            oid = self.check_full_access(cid_public, file_name)

        with allure.step('Set "deny all operations for other" for created container'):
            set_eacl(self.main_wallet, cid_public, deny_all_eacl)

        with allure.step('Check no one except owner has access to operations with container'):
            self.check_no_access_to_container(self.other_wallet, cid_public, oid, file_name)

        with allure.step('Check owner has access to operations with container'):
            self.check_full_access(cid_public, file_name, wallet_to_check=((self.main_wallet, 'owner'),))

            delete_object(self.main_wallet, cid_public, oid)

    @staticmethod
    def check_no_access_to_container(wallet: str, cid: str, oid: str, file_name: str):
        err_pattern = '.*access to object operation denied.*'
        with pytest.raises(Exception, match=err_pattern):
            get_object(wallet, cid, oid)

        with pytest.raises(Exception, match=err_pattern):
            put_object(wallet, file_name, cid)

        with pytest.raises(Exception, match=err_pattern):
            delete_object(wallet, cid, oid)

        with pytest.raises(Exception, match=err_pattern):
            head_object(wallet, cid, oid)

        with pytest.raises(Exception, match=err_pattern):
            get_range(wallet, cid, oid, file_path='s_get_range', bearer='', range_cut='0:10')

        with pytest.raises(Exception, match=err_pattern):
            get_range_hash(wallet, cid, oid, bearer_token='', range_cut='0:10')

        with pytest.raises(Exception, match=err_pattern):
            search_object(wallet, cid)

    def check_full_access(self, cid: str, file_name: str, wallet_to_check: Tuple = None) -> str:
        wallets = wallet_to_check or ((self.main_wallet, 'owner'), (self.other_wallet, 'not owner'))
        for current_wallet, desc in wallets:
            with allure.step(f'Check {desc} can put object into public container'):
                oid = put_object(current_wallet, file_name, cid)

            with allure.step(f'Check {desc} can execute operations on object from public container'):
                got_file = get_object(current_wallet, cid, oid)
                assert get_file_hash(got_file) == get_file_hash(file_name), 'Expected hashes are the same'

                head_object(current_wallet, cid, oid)
                get_range(current_wallet, cid, oid, file_path='s_get_range', bearer='', range_cut='0:10')
                get_range_hash(current_wallet, cid, oid, bearer_token='', range_cut='0:10')
                search_object(current_wallet, cid)

        return oid
