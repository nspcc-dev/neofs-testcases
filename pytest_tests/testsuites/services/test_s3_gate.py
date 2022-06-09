import logging
import os
from random import choice, choices

import allure
import pytest
from common import COMPLEX_OBJ_SIZE, SIMPLE_OBJ_SIZE
from contract_keywords import tick_epoch
from python_keywords.container import list_containers
from python_keywords.s3_gate import (VersioningStatus, config_s3_client,
                                     copy_object_s3, create_bucket_s3,
                                     delete_bucket_s3, delete_object_s3,
                                     delete_objects_s3,
                                     get_bucket_versioning_status,
                                     get_object_s3, head_bucket,
                                     head_object_s3, init_s3_credentials,
                                     list_buckets_s3, list_objects_s3,
                                     list_objects_s3_v2,
                                     list_objects_versions_s3, put_object_s3,
                                     set_bucket_versioning)
from python_keywords.utility_keywords import (generate_file_and_file_hash,
                                              get_file_hash)
from utility import create_file_with_content, get_file_content

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

    @pytest.fixture
    @allure.title('Create two buckets')
    def create_buckets(self):
        bucket_1 = create_bucket_s3(self.s3_client)
        bucket_2 = create_bucket_s3(self.s3_client)
        return bucket_1, bucket_2

    @allure.title('Test S3 Bucket API')
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
            put_object_s3(self.s3_client, bucket, file_name_large)
            head_object_s3(self.s3_client, bucket, file_name)

            bucket_objects = list_objects_s3(self.s3_client, bucket)
            assert file_name in bucket_objects, \
                f'Expected file {file_name} in objects list {bucket_objects}'

    @allure.title('Test S3 Object versioning')
    @pytest.mark.current
    def test_s3_api_versioning(self):
        """
        Test checks basic versioning functionality for S3 bucket.
        """
        version_1_content = 'Version 1'
        version_2_content = 'Version 2'
        file_name_simple = create_file_with_content(content=version_1_content)
        obj_key = os.path.basename(file_name_simple)

        bucket = create_bucket_s3(self.s3_client)

        with allure.step('Set versioning enable for bucket'):
            status = get_bucket_versioning_status(self.s3_client, bucket)
            assert status == VersioningStatus.SUSPENDED.value, f'Expected suspended status. Got {status}'

            set_bucket_versioning(self.s3_client, bucket, status=VersioningStatus.ENABLED)
            status = get_bucket_versioning_status(self.s3_client, bucket)
            assert status == VersioningStatus.ENABLED.value, f'Expected enabled status. Got {status}'

        with allure.step('Put several versions of object into bucket'):
            version_id_1 = put_object_s3(self.s3_client, bucket, file_name_simple)
            create_file_with_content(file_path=file_name_simple, content=version_2_content)
            version_id_2 = put_object_s3(self.s3_client, bucket, file_name_simple)

        with allure.step('Check bucket shows all versions'):
            versions = list_objects_versions_s3(self.s3_client, bucket)
            obj_versions = {version.get('VersionId') for version in versions if version.get('Key') == obj_key}
            assert obj_versions == {version_id_1, version_id_2}, \
                f'Expected object has versions: {version_id_1, version_id_2}'

        with allure.step('Delete object and check it was deleted'):
            response = delete_object_s3(self.s3_client, bucket, obj_key)
            version_id_delete = response.get('VersionId')

            with pytest.raises(Exception, match=r'.*Not Found.*'):
                head_object_s3(self.s3_client, bucket, obj_key)

        with allure.step('Get content for all versions and check it is correct'):
            for version, content in ((version_id_2, version_2_content), (version_id_1, version_1_content)):
                file_name = get_object_s3(self.s3_client, bucket, obj_key, version_id=version)
                got_content = get_file_content(file_name)
                assert got_content == content, f'Expected object content is\n{content}\nGot\n{got_content}'

        with allure.step('Restore previous object version'):
            delete_object_s3(self.s3_client, bucket, obj_key, version_id=version_id_delete)

            file_name = get_object_s3(self.s3_client, bucket, obj_key)
            got_content = get_file_content(file_name)
            assert got_content == version_2_content, \
                f'Expected object content is\n{version_2_content}\nGot\n{got_content}'

    @allure.title('Test delete object & delete objects S3 API')
    def test_s3_api_delete(self, create_buckets):
        """
        Check delete_object and delete_objects S3 API operation. From first bucket some objects deleted one by one.
        From second bucket some objects deleted all at once.
        """
        max_obj_count = 20
        max_delete_objects = 17
        put_objects = []
        file_paths = []
        obj_sizes = [SIMPLE_OBJ_SIZE, COMPLEX_OBJ_SIZE]

        bucket_1, bucket_2 = create_buckets

        with allure.step(f'Generate {max_obj_count} files'):
            for _ in range(max_obj_count):
                file_paths.append(generate_file_and_file_hash(choice(obj_sizes))[0])

        for bucket in (bucket_1, bucket_2):
            with allure.step(f'Bucket {bucket} must be empty as it just created'):
                objects_list = list_objects_s3_v2(self.s3_client, bucket)
                assert not objects_list, f'Expected empty bucket, got {objects_list}'

            for file_path in file_paths:
                put_object_s3(self.s3_client, bucket, file_path)
                put_objects.append(self.file_name(file_path))

            with allure.step(f'Check all objects put in bucket {bucket} successfully'):
                bucket_objects = list_objects_s3_v2(self.s3_client, bucket)
                assert set(put_objects) == set(bucket_objects), \
                    f'Expected all objects {put_objects} in objects list {bucket_objects}'

        with allure.step('Delete some objects from bucket_1 one by one'):
            objects_to_delete_b1 = choices(put_objects, k=max_delete_objects)
            for obj in objects_to_delete_b1:
                delete_object_s3(self.s3_client, bucket_1, obj)

        with allure.step('Check deleted objects are not visible in bucket bucket_1'):
            bucket_objects = list_objects_s3_v2(self.s3_client, bucket_1)
            assert set(put_objects).difference(set(objects_to_delete_b1)) == set(bucket_objects), \
                f'Expected all objects {put_objects} in objects list {bucket_objects}'
            self.try_to_get_object_and_got_error(bucket_1, objects_to_delete_b1)

        with allure.step('Delete some objects from bucket_2 at once'):
            objects_to_delete_b2 = choices(put_objects, k=max_delete_objects)
            delete_objects_s3(self.s3_client, bucket_2, objects_to_delete_b2)

        with allure.step('Check deleted objects are not visible in bucket bucket_2'):
            objects_list = list_objects_s3_v2(self.s3_client, bucket_2)
            assert set(put_objects).difference(set(objects_to_delete_b2)) == set(objects_list), \
                f'Expected all objects {put_objects} in objects list {bucket_objects}'
            self.try_to_get_object_and_got_error(bucket_2, objects_to_delete_b2)

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

    @allure.step('Try to get object and got error')
    def try_to_get_object_and_got_error(self, bucket: str, unexpected_objects: list):
        for obj in unexpected_objects:
            try:
                get_object_s3(self.s3_client, bucket, obj)
                raise AssertionError(f'Object {obj} found in bucket {bucket}')
            except Exception as err:
                assert 'The specified key does not exist' in str(err), f'Expected error in exception {err}'

    @staticmethod
    def file_name(full_path: str) -> str:
        return full_path.split('/')[-1]
