import logging
import os

import allure
import pytest
from contract_keywords import tick_epoch
from python_keywords.container import list_containers
from python_keywords.s3_gate import (config_s3_client, copy_object_s3,
                                     create_bucket_s3, delete_bucket_s3,
                                     delete_object_s3, get_object_s3,
                                     head_bucket, head_object_s3,
                                     init_s3_credentials, list_buckets_s3,
                                     list_objects_s3, list_objects_s3_v2,
                                     put_object_s3)
from python_keywords.utility_keywords import get_file_hash

logger = logging.getLogger('NeoLogger')


@allure.link('https://github.com/nspcc-dev/neofs-s3-gw#neofs-s3-gateway', name='neofs-s3-gateway')
@pytest.mark.s3_gate
class TestS3Gate:
    s3_client = None

    @pytest.fixture(scope='class', autouse=True)
    @allure.title('[Class/Autouse]: Create S3 client')
    def s3_client(self, prepare_wallet_and_deposit):
        wallet, addr, wif = prepare_wallet_and_deposit
        s3_bearer_rules_file = f"{os.getcwd()}/robot/resources/files/s3_bearer_rules.json"

        cid, bucket, access_key_id, secret_access_key, owner_private_key = \
            init_s3_credentials(wallet, s3_bearer_rules_file=s3_bearer_rules_file)
        containers_list = list_containers(wallet)
        assert cid in containers_list, f'Expected cid {cid} in {containers_list}'

        client = config_s3_client(access_key_id, secret_access_key)
        TestS3Gate.s3_client = client

    @allure.title('Test S3 Bucket API')
    @pytest.mark.current
    def test_s3_buckets(self, generate_files):
        """
        Test base S3 Bucket API.

        Steps:
        1. Create simple objects.
        2. Create two buckets.
        3. Check buckets are presented in the system with S3 list command
        4. Check buckets are empty.
        5. Check buckets are visible with S3 head command.
        6. Check we can put/head/list object with S3 commands.
        7. Try to delete not empty bucket and get error.
        8. Delete empty bucket.
        9. Check only empty bucket deleted.
        """

        file_name_simple, file_name_large = generate_files
        file_name = self.file_name(file_name_simple)

        with allure.step('Create buckets'):
            bucket_1 = create_bucket_s3(self.s3_client)
            bucket_2 = create_bucket_s3(self.s3_client)

        with allure.step('Check buckets are presented in the system'):
            buckets = list_buckets_s3(self.s3_client)
            assert bucket_1 in buckets, f'Expected bucket {bucket_1} is in the list'
            assert bucket_2 in buckets, f'Expected bucket {bucket_2} is in the list'

        with allure.step('Bucket must be empty'):
            for bucket in (bucket_1, bucket_2):
                objects_list = list_objects_s3(self.s3_client, bucket)
                assert not objects_list, f'Expected empty bucket, got {objects_list}'

        with allure.step('Check buckets are visible with S3 head command'):
            head_bucket(self.s3_client, bucket_1)
            head_bucket(self.s3_client, bucket_2)

        with allure.step('Check we can put/list object with S3 commands'):
            put_object_s3(self.s3_client, bucket_1, file_name_simple)
            head_object_s3(self.s3_client, bucket_1, file_name)

            bucket_objects = list_objects_s3(self.s3_client, bucket_1)
            assert file_name in bucket_objects, \
                f'Expected file {file_name} in objects list {bucket_objects}'

        with allure.step('Try to delete not empty bucket and get error'):
            with pytest.raises(Exception, match=r'.*The bucket you tried to delete is not empty.*'):
                delete_bucket_s3(self.s3_client, bucket_1)

            head_bucket(self.s3_client, bucket_1)

        with allure.step('Delete empty bucket'):
            delete_bucket_s3(self.s3_client, bucket_2)
            tick_epoch()

        with allure.step('Check bucket deleted'):
            with pytest.raises(Exception, match=r'.*Not Found.*'):
                head_bucket(self.s3_client, bucket_2)

            buckets = list_buckets_s3(self.s3_client)
            assert bucket_1 in buckets, f'Expected bucket {bucket_1} is in the list'
            assert bucket_2 not in buckets, f'Expected bucket {bucket_2} is not in the list'

    @allure.title('Test S3 Object API')
    @pytest.mark.parametrize('file_type', ['simple', 'large'], ids=['Simple object', 'Large object'])
    def test_s3_api(self, generate_files, file_type):
        """
        Test base S3 API.

        Steps:
        1. Create simple/large objects.
        2. Create two buckets.
        3. Check buckets are empty.
        4. Put object into buckets using S3 client.
        5. Check objects appear in bucket with ls and head S3 operations.
        6. Get object from bucket using S3 client.
        7. Delete object and check it is deleted from the bucket.
        """
        file_name_simple, file_name_large = generate_files
        file_name_path = file_name_simple if file_type == 'simple' else file_name_large
        file_name = self.file_name(file_name_path)

        bucket_1 = create_bucket_s3(self.s3_client)
        bucket_2 = create_bucket_s3(self.s3_client)

        for bucket in (bucket_1, bucket_2):
            with allure.step('Bucket must be empty'):
                objects_list = list_objects_s3(self.s3_client, bucket)
                assert not objects_list, f'Expected empty bucket, got {objects_list}'

            put_object_s3(self.s3_client, bucket, file_name_path)
            head_object_s3(self.s3_client, bucket, file_name)

            bucket_objects = list_objects_s3(self.s3_client, bucket)
            assert file_name in bucket_objects, \
                f'Expected file {file_name} in objects list {bucket_objects}'

            bucket_objects = list_objects_s3_v2(self.s3_client, bucket)
            assert file_name in bucket_objects, \
                f'Expected file {file_name} in objects list {bucket_objects}'

        with allure.step('Get objects and check they are the same as original ones'):
            for bucket in (bucket_1, bucket_2):
                got_file = get_object_s3(self.s3_client, bucket, file_name)
                assert get_file_hash(got_file) == get_file_hash(file_name_path), 'Hashes must be the same'

        with allure.step('Delete original object from bucket and check copy is presented'):
            for bucket in (bucket_1, bucket_2):
                delete_object_s3(self.s3_client, bucket, file_name)
                bucket_objects = list_objects_s3(self.s3_client, bucket)
                assert got_file not in bucket_objects, \
                    f'Expected file {file_name} not in objects list {bucket_objects}'

    @allure.title('Test S3: Copy object to the same bucket')
    def test_s3_copy_same_bucket(self, generate_files):
        """
        Test object can be copied to the same bucket.

        Steps:
        1. Create simple and large objects.
        2. Create bucket.
        3. Check bucket is empty.
        4. Put objects into buckets using S3 client.
        5. Copy one object to the same bucket.
        6. Check all objects shown in the bucket.
        7. Get copied object from bucket and compare with original.
        8. Delete original object from the bucket and check copied one is still presented.
        """
        file_simple, file_large = generate_files
        file_name_simple = self.file_name(file_simple)
        file_name_large = self.file_name(file_large)
        bucket_objects = [file_name_simple, file_name_large]

        bucket = create_bucket_s3(self.s3_client)

        with allure.step('Bucket must be empty'):
            objects_list = list_objects_s3(self.s3_client, bucket)
            assert not objects_list, f'Expected empty bucket, got {objects_list}'

        with allure.step('Put objects into bucket'):
            for obj in (file_simple, file_large):
                put_object_s3(self.s3_client, bucket, obj)

        with allure.step('Copy one object into the same bucket'):
            copy_obj_path = copy_object_s3(self.s3_client, bucket, file_name_simple)
            bucket_objects.append(copy_obj_path)

        self.check_objects_in_bucket(bucket, bucket_objects)

        with allure.step('Check copied object has the same content'):
            got_copied_file = get_object_s3(self.s3_client, bucket, copy_obj_path)
            assert get_file_hash(file_simple) == get_file_hash(got_copied_file), 'Hashes must be the same'

        with allure.step('Delete one object from bucket'):
            delete_object_s3(self.s3_client, bucket, file_name_simple)
            bucket_objects.remove(file_name_simple)

        self.check_objects_in_bucket(bucket, expected_objects=bucket_objects, unexpected_objects=[file_name_simple])

    @allure.title('Test S3: Copy object to another bucket')
    def test_s3_copy_another_bucket(self, generate_files):
        """
        Test object can be copied to another bucket.

        Steps:
        1. Create simple and large objects.
        2. Create two buckets.
        3. Check buckets are empty.
        4. Put objects into one bucket using S3 client.
        5. Copy object from first bucket into second.
        6. Check copied object has the same content.
        7. Delete 'source' object from first bucket.
        8. Check buckets content.
        9. Delete copied object from second bucket and check it is empty.
        """
        file_simple, file_large = generate_files
        file_name_simple = self.file_name(file_simple)
        file_name_large = self.file_name(file_large)
        bucket_1_objects = [file_name_simple, file_name_large]

        bucket_1 = create_bucket_s3(self.s3_client)
        bucket_2 = create_bucket_s3(self.s3_client)

        with allure.step('Buckets must be empty'):
            for bucket in (bucket_1, bucket_2):
                objects_list = list_objects_s3(self.s3_client, bucket)
                assert not objects_list, f'Expected empty bucket, got {objects_list}'

        with allure.step('Put objects into one bucket'):
            for obj in (file_simple, file_large):
                put_object_s3(self.s3_client, bucket_1, obj)

        with allure.step('Copy object from first bucket into second'):
            copy_obj_path_b2 = copy_object_s3(self.s3_client, bucket_1, file_name_large, bucket_dst=bucket_2)
        self.check_objects_in_bucket(bucket_1, expected_objects=bucket_1_objects)
        self.check_objects_in_bucket(bucket_2, expected_objects=[copy_obj_path_b2])

        with allure.step('Check copied object has the same content'):
            got_copied_file_b2 = get_object_s3(self.s3_client, bucket_2, copy_obj_path_b2)
            assert get_file_hash(file_large) == get_file_hash(got_copied_file_b2), 'Hashes must be the same'

        with allure.step('Delete one object from first bucket'):
            delete_object_s3(self.s3_client, bucket_1, file_name_simple)
            bucket_1_objects.remove(file_name_simple)

        self.check_objects_in_bucket(bucket_1, expected_objects=bucket_1_objects)
        self.check_objects_in_bucket(bucket_2, expected_objects=[copy_obj_path_b2])

        with allure.step('Delete one object from second bucket and check it is empty'):
            delete_object_s3(self.s3_client, bucket_2, copy_obj_path_b2)
            self.check_objects_in_bucket(bucket_2, expected_objects=[])

    @allure.step('Expected all objects are presented in the bucket')
    def check_objects_in_bucket(self, bucket, expected_objects: list, unexpected_objects: list = None):
        unexpected_objects = unexpected_objects or []
        bucket_objects = list_objects_s3(self.s3_client, bucket)
        assert len(bucket_objects) == len(expected_objects), f'Expected {len(expected_objects)} objects in the bucket'

        for bucket_object in expected_objects:
            assert bucket_object in bucket_objects, \
                f'Expected object {bucket_object} in objects list {bucket_objects}'

        for bucket_object in unexpected_objects:
            assert bucket_object not in bucket_objects, \
                f'Expected object {bucket_object} not in objects list {bucket_objects}'

    @staticmethod
    def file_name(full_path: str) -> str:
        return full_path.split('/')[-1]
