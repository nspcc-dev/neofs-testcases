import logging
import os
import time
from random import choice, choices

import allure
import pytest
from helpers.aws_cli_client import AwsCliClient
from helpers.common import get_assets_dir_path
from helpers.file_helper import (
    generate_file,
    generate_file_with_content,
    get_file_content,
    get_file_hash,
    split_file,
)
from helpers.s3_helper import (
    NO_SUCH_TAGS_ERROR,
    check_objects_in_bucket,
    check_tags_by_bucket,
    check_tags_by_object,
    parametrize_clients,
    set_bucket_versioning,
    try_to_get_objects_and_expect_error,
)
from s3 import s3_bucket, s3_object
from s3.s3_base import TestNeofsS3Base

logger = logging.getLogger("NeoLogger")


def pytest_generate_tests(metafunc):
    parametrize_clients(metafunc)


@allure.link("https://github.com/nspcc-dev/neofs-s3-gw#neofs-s3-gateway", name="neofs-s3-gateway")
class TestS3(TestNeofsS3Base):
    @allure.title("Test S3 Bucket API")
    @pytest.mark.simple
    def test_s3_buckets(self):
        """
        Test base S3 Bucket API (Create/List/Head/Delete).
        """

        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        file_name = self.object_key_from_file_path(file_path)

        with allure.step("Create buckets"):
            bucket_1 = s3_bucket.create_bucket_s3(
                self.s3_client, object_lock_enabled_for_bucket=True, bucket_configuration="rep-2"
            )
            set_bucket_versioning(self.s3_client, bucket_1, s3_bucket.VersioningStatus.ENABLED)
            bucket_2 = s3_bucket.create_bucket_s3(self.s3_client, bucket_configuration="rep-2")

        with allure.step("Check buckets are presented in the system"):
            buckets = s3_bucket.list_buckets_s3(self.s3_client)
            assert bucket_1 in buckets, f"Expected bucket {bucket_1} is in the list"
            assert bucket_2 in buckets, f"Expected bucket {bucket_2} is in the list"

        with allure.step("Bucket must be empty"):
            for bucket in (bucket_1, bucket_2):
                objects_list = s3_object.list_objects_s3(self.s3_client, bucket)
                assert not objects_list, f"Expected empty bucket, got {objects_list}"

        with allure.step("Check buckets are visible with S3 API HeadBucket op"):
            s3_bucket.head_bucket(self.s3_client, bucket_1)
            s3_bucket.head_bucket(self.s3_client, bucket_2)

        with allure.step("Check we can put/list object with S3 commands"):
            version_id = s3_object.put_object_s3(self.s3_client, bucket_1, file_path)
            s3_object.head_object_s3(self.s3_client, bucket_1, file_name)

            bucket_objects = s3_object.list_objects_s3(self.s3_client, bucket_1)
            assert file_name in bucket_objects, f"Expected file {file_name} in objects list {bucket_objects}"

        with allure.step("Try to delete not empty bucket and expect error"):
            with pytest.raises(Exception, match=r".*The bucket you tried to delete is not empty.*"):
                s3_bucket.delete_bucket_s3(self.s3_client, bucket_1)

            s3_bucket.head_bucket(self.s3_client, bucket_1)

        with allure.step(f"Delete empty bucket {bucket_2}"):
            s3_bucket.delete_bucket_s3(self.s3_client, bucket_2)
            self.tick_epochs_and_wait(1)

        with allure.step(f"Check bucket {bucket_2} deleted"):
            with pytest.raises(Exception, match=r".*Not Found.*"):
                s3_bucket.head_bucket(self.s3_client, bucket_2)

            buckets = s3_bucket.list_buckets_s3(self.s3_client)
            assert bucket_1 in buckets, f"Expected bucket {bucket_1} is in the list"
            assert bucket_2 not in buckets, f"Expected bucket {bucket_2} is not in the list"

        with allure.step(f"Delete object from {bucket_1}"):
            s3_object.delete_object_s3(self.s3_client, bucket_1, file_name, version_id)
            check_objects_in_bucket(self.s3_client, bucket_1, expected_objects=[])

        with allure.step(f"Delete bucket {bucket_1}"):
            s3_bucket.delete_bucket_s3(self.s3_client, bucket_1)
            self.tick_epochs_and_wait(1)

        with allure.step(f"Check bucket {bucket_1} deleted"):
            with pytest.raises(Exception, match=r".*Not Found.*"):
                s3_bucket.head_bucket(self.s3_client, bucket_1)

    @allure.title("Test S3 Object API")
    @pytest.mark.parametrize(
        "file_type",
        [
            pytest.param("simple_object_size", id="simple object", marks=pytest.mark.simple),
            pytest.param("complex_object_size", id="complex object", marks=pytest.mark.complex),
        ],
    )
    def test_s3_api_object(self, file_type, two_buckets):
        """
        Test base S3 Object API (Put/Head/List) for simple and large objects.
        """
        file_path = generate_file(self.neofs_env.get_object_size(file_type))
        file_name = self.object_key_from_file_path(file_path)

        bucket_1, bucket_2 = two_buckets

        for bucket in (bucket_1, bucket_2):
            with allure.step("Bucket must be empty"):
                objects_list = s3_object.list_objects_s3(self.s3_client, bucket)
                assert not objects_list, f"Expected empty bucket, got {objects_list}"

            s3_object.put_object_s3(self.s3_client, bucket, file_path)
            s3_object.head_object_s3(self.s3_client, bucket, file_name)

            bucket_objects = s3_object.list_objects_s3(self.s3_client, bucket)
            assert file_name in bucket_objects, f"Expected file {file_name} in objects list {bucket_objects}"

        with allure.step("Check object's attributes"):
            for attrs in (["ETag"], ["ObjectSize", "StorageClass"]):
                s3_object.get_object_attributes(self.s3_client, bucket, file_name, *attrs)

    @allure.title("Test S3 Sync directory")
    @pytest.mark.aws_cli_only
    @pytest.mark.simple
    def test_s3_sync_dir(self, bucket):
        """
        Test checks sync directory with AWS CLI utility.
        """
        file_path_1 = os.path.join(get_assets_dir_path(), "test_sync", "test_file_1")
        file_path_2 = os.path.join(get_assets_dir_path(), "test_sync", "test_file_2")
        key_to_path = {"test_file_1": file_path_1, "test_file_2": file_path_2}

        generate_file_with_content(self.neofs_env.get_object_size("simple_object_size"), file_path=file_path_1)
        generate_file_with_content(self.neofs_env.get_object_size("simple_object_size"), file_path=file_path_2)

        self.s3_client.sync(bucket_name=bucket, dir_path=os.path.dirname(file_path_1))

        with allure.step("Check objects are synced"):
            objects = s3_object.list_objects_s3(self.s3_client, bucket)

        with allure.step("Check these are the same objects"):
            assert set(key_to_path.keys()) == set(objects), f"Expected exact objects saved. Got {objects}"
            for obj_key in objects:
                got_object = s3_object.get_object_s3(self.s3_client, bucket, obj_key)
                assert get_file_hash(got_object) == get_file_hash(key_to_path.get(obj_key)), (
                    "Expected hashes are the same"
                )

    @allure.title("Test S3 Object versioning")
    @pytest.mark.simple
    def test_s3_api_versioning(self, bucket):
        """
        Test checks basic versioning functionality for S3 bucket.
        """
        version_1_content = "Version 1"
        version_2_content = "Version 2"
        file_name_simple = generate_file_with_content(
            self.neofs_env.get_object_size("simple_object_size"), content=version_1_content
        )
        obj_key = os.path.basename(file_name_simple)
        set_bucket_versioning(self.s3_client, bucket, s3_bucket.VersioningStatus.ENABLED)

        with allure.step("Put several versions of object into bucket"):
            version_id_1 = s3_object.put_object_s3(self.s3_client, bucket, file_name_simple)
            time.sleep(1)
            generate_file_with_content(
                self.neofs_env.get_object_size("simple_object_size"),
                file_path=file_name_simple,
                content=version_2_content,
            )
            version_id_2 = s3_object.put_object_s3(self.s3_client, bucket, file_name_simple)
            time.sleep(1)

        with allure.step("Check bucket shows all versions"):
            versions = s3_object.list_objects_versions_s3(self.s3_client, bucket)
            obj_versions = {version.get("VersionId") for version in versions if version.get("Key") == obj_key}
            assert obj_versions == {
                version_id_1,
                version_id_2,
            }, f"Expected object has versions: {version_id_1, version_id_2}"

        with allure.step("Show information about particular version"):
            for version_id in (version_id_1, version_id_2):
                response = s3_object.head_object_s3(self.s3_client, bucket, obj_key, version_id=version_id)
                assert "LastModified" in response, "Expected LastModified field"
                assert "ETag" in response, "Expected ETag field"
                assert response.get("VersionId") == version_id, f"Expected VersionId is {version_id}"
                assert response.get("ContentLength") != 0, "Expected ContentLength is not zero"

        with allure.step("Check object's attributes"):
            for version_id in (version_id_1, version_id_2):
                got_attrs = s3_object.get_object_attributes(
                    self.s3_client, bucket, obj_key, "ETag", version_id=version_id
                )
                if got_attrs:
                    assert got_attrs.get("VersionId") == version_id, f"Expected VersionId is {version_id}"

        with allure.step("Delete object and check it was deleted"):
            response = s3_object.delete_object_s3(self.s3_client, bucket, obj_key)
            version_id_delete = response.get("VersionId")

            with pytest.raises(Exception, match=r".*Not Found.*"):
                s3_object.head_object_s3(self.s3_client, bucket, obj_key)

        with allure.step("Get content for all versions and check it is correct"):
            for version, content in (
                (version_id_2, version_2_content),
                (version_id_1, version_1_content),
            ):
                file_name = s3_object.get_object_s3(self.s3_client, bucket, obj_key, version_id=version)
                got_content = get_file_content(file_name)
                assert got_content == content, f"Expected object content is\n{content}\nGot\n{got_content}"

        with allure.step("Restore previous object version"):
            s3_object.delete_object_s3(self.s3_client, bucket, obj_key, version_id=version_id_delete)

            file_name = s3_object.get_object_s3(self.s3_client, bucket, obj_key)
            got_content = get_file_content(file_name)
            assert got_content == version_2_content, (
                f"Expected object content is\n{version_2_content}\nGot\n{got_content}"
            )

    @allure.title("Test S3 Object Multipart API")
    @pytest.mark.simple
    def test_s3_api_multipart(self, bucket):
        """
        Test checks S3 Multipart API (Create multipart upload/Abort multipart upload/List multipart upload/
        Upload part/List parts/Complete multipart upload).
        """
        parts_count = 3
        file_name_large = generate_file(
            self.neofs_env.get_object_size("simple_object_size") * 1024 * 6 * parts_count
        )  # 5Mb - min part
        object_key = self.object_key_from_file_path(file_name_large)
        part_files = split_file(file_name_large, parts_count)
        parts = []

        uploads = s3_object.list_multipart_uploads_s3(self.s3_client, bucket)
        assert not uploads, f"Expected there is no uploads in bucket {bucket}"

        with allure.step("Create and abort multipart upload"):
            upload_id = s3_object.create_multipart_upload_s3(self.s3_client, bucket, object_key)
            uploads = s3_object.list_multipart_uploads_s3(self.s3_client, bucket)
            assert uploads, f"Expected there one upload in bucket {bucket}"
            assert uploads[0].get("Key") == object_key, f"Expected correct key {object_key} in upload {uploads}"
            assert uploads[0].get("UploadId") == upload_id, f"Expected correct UploadId {upload_id} in upload {uploads}"

            s3_object.abort_multipart_uploads_s3(self.s3_client, bucket, object_key, upload_id)
            uploads = s3_object.list_multipart_uploads_s3(self.s3_client, bucket)
            assert not uploads, f"Expected there is no uploads in bucket {bucket}"

        with allure.step("Create new multipart upload and upload several parts"):
            upload_id = s3_object.create_multipart_upload_s3(self.s3_client, bucket, object_key)
            for part_id, file_path in enumerate(part_files, start=1):
                etag = s3_object.upload_part_s3(self.s3_client, bucket, object_key, upload_id, part_id, file_path)
                parts.append((part_id, etag))

        with allure.step("Check all parts are visible in bucket"):
            got_parts = s3_object.list_parts_s3(self.s3_client, bucket, object_key, upload_id)
            assert len(got_parts) == len(part_files), f"Expected {parts_count} parts, got\n{got_parts}"

        s3_object.complete_multipart_upload_s3(self.s3_client, bucket, object_key, upload_id, parts)

        uploads = s3_object.list_multipart_uploads_s3(self.s3_client, bucket)
        assert not uploads, f"Expected there is no uploads in bucket {bucket}"

        with allure.step("Check we can get whole object from bucket"):
            got_object = s3_object.get_object_s3(self.s3_client, bucket, object_key)
            assert get_file_hash(got_object) == get_file_hash(file_name_large)

        self.check_object_attributes(bucket, object_key, parts_count)

    @allure.title("Test S3 Bucket tagging API")
    def test_s3_api_bucket_tagging(self, bucket):
        """
        Test checks S3 Bucket tagging API (Put tag/Get tag).
        """
        key_value_pair = [("some-key", "some-value"), ("some-key-2", "some-value-2")]

        s3_bucket.put_bucket_tagging(self.s3_client, bucket, key_value_pair)
        check_tags_by_bucket(self.s3_client, bucket, key_value_pair)

        s3_bucket.delete_bucket_tagging(self.s3_client, bucket)

        with pytest.raises(Exception, match=NO_SUCH_TAGS_ERROR):
            check_tags_by_bucket(self.s3_client, bucket, [])

    @allure.title("Test S3 Object tagging API")
    @pytest.mark.simple
    def test_s3_api_object_tagging(self, bucket):
        """
        Test checks S3 Object tagging API (Put tag/Get tag/Update tag).
        """
        key_value_pair_obj = [
            ("some-key-obj", "some-value-obj"),
            ("some-key--obj2", "some-value--obj2"),
        ]
        key_value_pair_obj_new = [("some-key-obj-new", "some-value-obj-new")]
        file_name_simple = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        obj_key = self.object_key_from_file_path(file_name_simple)

        s3_object.put_object_s3(self.s3_client, bucket, file_name_simple)

        for tags in (key_value_pair_obj, key_value_pair_obj_new):
            s3_object.put_object_tagging(self.s3_client, bucket, obj_key, tags)
            time.sleep(1)
            check_tags_by_object(
                self.s3_client,
                bucket,
                obj_key,
                tags,
            )

        s3_object.delete_object_tagging(self.s3_client, bucket, obj_key)
        check_tags_by_object(self.s3_client, bucket, obj_key, [])

    @allure.title("Test S3: Delete object & delete objects S3 API")
    @pytest.mark.complex
    @pytest.mark.simple
    def test_s3_api_delete(self, two_buckets):
        """
        Check DeleteObject and DeleteObjects S3 API operations. From first bucket some objects deleted one by one.
        From second bucket some objects deleted all at once.
        """
        max_obj_count = 20
        max_delete_objects = 17
        put_objects = []
        file_paths = []
        obj_sizes = [
            self.neofs_env.get_object_size("simple_object_size"),
            self.neofs_env.get_object_size("complex_object_size"),
        ]

        bucket_1, bucket_2 = two_buckets

        with allure.step(f"Generate {max_obj_count} files"):
            for _ in range(max_obj_count):
                file_paths.append(generate_file(choice(obj_sizes)))

        for bucket in (bucket_1, bucket_2):
            with allure.step(f"Bucket {bucket} must be empty as it just created"):
                objects_list = s3_object.list_objects_s3_v2(self.s3_client, bucket)
                assert not objects_list, f"Expected empty bucket, got {objects_list}"

            for file_path in file_paths:
                s3_object.put_object_s3(self.s3_client, bucket, file_path)
                put_objects.append(self.object_key_from_file_path(file_path))

            with allure.step(f"Check all objects put in bucket {bucket} successfully"):
                bucket_objects = s3_object.list_objects_s3_v2(self.s3_client, bucket)
                assert set(put_objects) == set(bucket_objects), (
                    f"Expected all objects {put_objects} in objects list {bucket_objects}"
                )

        with allure.step("Delete some objects from bucket_1 one by one"):
            objects_to_delete_b1 = choices(put_objects, k=max_delete_objects)
            for obj in objects_to_delete_b1:
                s3_object.delete_object_s3(self.s3_client, bucket_1, obj)

        with allure.step("Check deleted objects are not visible in bucket bucket_1"):
            bucket_objects = s3_object.list_objects_s3_v2(self.s3_client, bucket_1)
            assert set(put_objects).difference(set(objects_to_delete_b1)) == set(bucket_objects), (
                f"Expected all objects {put_objects} in objects list {bucket_objects}"
            )
            try_to_get_objects_and_expect_error(self.s3_client, bucket_1, objects_to_delete_b1)

        with allure.step("Delete some objects from bucket_2 at once"):
            objects_to_delete_b2 = choices(put_objects, k=max_delete_objects)
            s3_object.delete_objects_s3(self.s3_client, bucket_2, objects_to_delete_b2)

        with allure.step("Check deleted objects are not visible in bucket bucket_2"):
            objects_list = s3_object.list_objects_s3_v2(self.s3_client, bucket_2)
            assert set(put_objects).difference(set(objects_to_delete_b2)) == set(objects_list), (
                f"Expected all objects {put_objects} in objects list {bucket_objects}"
            )
            try_to_get_objects_and_expect_error(self.s3_client, bucket_2, objects_to_delete_b2)

    @allure.title("Test S3: Copy object to the same bucket")
    @pytest.mark.complex
    @pytest.mark.simple
    def test_s3_copy_same_bucket(self, bucket):
        """
        Test object can be copied to the same bucket.
        #TODO: delete after test_s3_copy_object will be merge
        """
        file_path_simple, file_path_large = (
            generate_file(self.neofs_env.get_object_size("simple_object_size")),
            generate_file(self.neofs_env.get_object_size("complex_object_size")),
        )
        file_name_simple = self.object_key_from_file_path(file_path_simple)
        file_name_large = self.object_key_from_file_path(file_path_large)
        bucket_objects = [file_name_simple, file_name_large]

        with allure.step("Bucket must be empty"):
            objects_list = s3_object.list_objects_s3(self.s3_client, bucket)
            assert not objects_list, f"Expected empty bucket, got {objects_list}"

        with allure.step("Put objects into bucket"):
            for file_path in (file_path_simple, file_path_large):
                s3_object.put_object_s3(self.s3_client, bucket, file_path)

        with allure.step("Copy one object into the same bucket"):
            copy_obj_path = s3_object.copy_object_s3(self.s3_client, bucket, file_name_simple)
            bucket_objects.append(copy_obj_path)

        check_objects_in_bucket(self.s3_client, bucket, bucket_objects)

        with allure.step("Check copied object has the same content"):
            got_copied_file = s3_object.get_object_s3(self.s3_client, bucket, copy_obj_path)
            assert get_file_hash(file_path_simple) == get_file_hash(got_copied_file), "Hashes must be the same"

        with allure.step("Delete one object from bucket"):
            s3_object.delete_object_s3(self.s3_client, bucket, file_name_simple)
            bucket_objects.remove(file_name_simple)

        check_objects_in_bucket(
            self.s3_client,
            bucket,
            expected_objects=bucket_objects,
            unexpected_objects=[file_name_simple],
        )

    @allure.title("Test S3: Copy object to another bucket")
    @pytest.mark.complex
    @pytest.mark.simple
    def test_s3_copy_to_another_bucket(self, two_buckets):
        """
        Test object can be copied to another bucket.
        #TODO: delete after test_s3_copy_object will be merge
        """
        file_path_simple, file_path_large = (
            generate_file(self.neofs_env.get_object_size("simple_object_size")),
            generate_file(self.neofs_env.get_object_size("complex_object_size")),
        )
        file_name_simple = self.object_key_from_file_path(file_path_simple)
        file_name_large = self.object_key_from_file_path(file_path_large)
        bucket_1_objects = [file_name_simple, file_name_large]

        bucket_1, bucket_2 = two_buckets

        with allure.step("Buckets must be empty"):
            for bucket in (bucket_1, bucket_2):
                objects_list = s3_object.list_objects_s3(self.s3_client, bucket)
                assert not objects_list, f"Expected empty bucket, got {objects_list}"

        with allure.step("Put objects into one bucket"):
            for file_path in (file_path_simple, file_path_large):
                s3_object.put_object_s3(self.s3_client, bucket_1, file_path)

        with allure.step("Copy object from first bucket into second"):
            copy_obj_path_b2 = s3_object.copy_object_s3(self.s3_client, bucket_1, file_name_large, bucket_dst=bucket_2)
        check_objects_in_bucket(self.s3_client, bucket_1, expected_objects=bucket_1_objects)
        check_objects_in_bucket(self.s3_client, bucket_2, expected_objects=[copy_obj_path_b2])

        with allure.step("Check copied object has the same content"):
            got_copied_file_b2 = s3_object.get_object_s3(self.s3_client, bucket_2, copy_obj_path_b2)
            assert get_file_hash(file_path_large) == get_file_hash(got_copied_file_b2), "Hashes must be the same"

        with allure.step("Delete one object from first bucket"):
            s3_object.delete_object_s3(self.s3_client, bucket_1, file_name_simple)
            bucket_1_objects.remove(file_name_simple)

        check_objects_in_bucket(self.s3_client, bucket_1, expected_objects=bucket_1_objects)
        check_objects_in_bucket(self.s3_client, bucket_2, expected_objects=[copy_obj_path_b2])

        with allure.step("Delete one object from second bucket and check it is empty"):
            s3_object.delete_object_s3(self.s3_client, bucket_2, copy_obj_path_b2)
            check_objects_in_bucket(self.s3_client, bucket_2, expected_objects=[])

    @allure.title("Test S3: bucket cors")
    def test_s3_cors(self):
        with allure.step("Create bucket without cors"):
            bucket = s3_bucket.create_bucket_s3(self.s3_client)
            set_bucket_versioning(self.s3_client, bucket, s3_bucket.VersioningStatus.ENABLED)

        with pytest.raises(Exception):
            bucket_cors = s3_bucket.get_bucket_cors(self.s3_client, bucket)

        with allure.step("Put bucket cors"):
            cors = {
                "CORSRules": [
                    {
                        "AllowedOrigins": ["http://www.example.com"],
                        "AllowedHeaders": ["*"],
                        "AllowedMethods": ["PUT", "POST", "DELETE"],
                        "MaxAgeSeconds": 3000,
                        "ExposeHeaders": ["x-amz-server-side-encryption"],
                    },
                    {
                        "AllowedOrigins": ["*"],
                        "AllowedHeaders": ["Authorization"],
                        "AllowedMethods": ["GET"],
                        "MaxAgeSeconds": 3000,
                    },
                ]
            }
            s3_bucket.put_bucket_cors(self.s3_client, bucket, cors)
            bucket_cors = s3_bucket.get_bucket_cors(self.s3_client, bucket)
            assert bucket_cors == cors.get("CORSRules"), f"Expected corsrules must be {cors.get('CORSRules')}"

        with allure.step("delete bucket cors"):
            s3_bucket.delete_bucket_cors(self.s3_client, bucket)

        with pytest.raises(Exception):
            bucket_cors = s3_bucket.get_bucket_cors(self.s3_client, bucket)

    def check_object_attributes(self, bucket: str, object_key: str, parts_count: int):
        if not isinstance(self.s3_client, AwsCliClient):
            logger.warning("Attributes check is not supported for boto3 implementation")
            return

        with allure.step("Check object's attributes"):
            obj_parts = s3_object.get_object_attributes(
                self.s3_client, bucket, object_key, "ObjectParts", get_full_resp=False
            )
            assert obj_parts.get("TotalPartsCount") == parts_count, f"Expected TotalPartsCount is {parts_count}"
            assert len(obj_parts.get("Parts")) == parts_count, f"Expected Parts cunt is {parts_count}"

        with allure.step("Check object's attribute max-parts"):
            max_parts = 2
            obj_parts = s3_object.get_object_attributes(
                self.s3_client,
                bucket,
                object_key,
                "ObjectParts",
                max_parts=max_parts,
                get_full_resp=False,
            )
            assert obj_parts.get("TotalPartsCount") == parts_count, f"Expected TotalPartsCount is {parts_count}"
            assert obj_parts.get("MaxParts") == max_parts, f"Expected MaxParts is {parts_count}"
            assert len(obj_parts.get("Parts")) == max_parts, f"Expected Parts count is {parts_count}"

        with allure.step("Check object's attribute part-number-marker"):
            part_number_marker = 3
            obj_parts = s3_object.get_object_attributes(
                self.s3_client,
                bucket,
                object_key,
                "ObjectParts",
                part_number=part_number_marker,
                get_full_resp=False,
            )
            assert obj_parts.get("TotalPartsCount") == parts_count, f"Expected TotalPartsCount is {parts_count}"
            assert obj_parts.get("PartNumberMarker") == part_number_marker, (
                f"Expected PartNumberMarker is {part_number_marker}"
            )
            assert len(obj_parts.get("Parts")) == 1, f"Expected Parts count is {parts_count}"

    @staticmethod
    def object_key_from_file_path(full_path: str) -> str:
        return os.path.basename(full_path)
