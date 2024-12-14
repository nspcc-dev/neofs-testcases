import random
from collections import defaultdict

import allure
import pytest
from helpers.file_helper import generate_file, get_file_hash, split_file
from helpers.s3_helper import (
    check_objects_in_bucket,
    object_key_from_file_path,
    set_bucket_versioning,
)
from s3 import s3_bucket, s3_object
from s3.s3_base import TestNeofsS3Base

PART_SIZE = 5 * 1024 * 1024


def pytest_generate_tests(metafunc):
    if "s3_client" in metafunc.fixturenames:
        metafunc.parametrize("s3_client", ["aws cli", "boto3"], indirect=True)


class TestS3Multipart(TestNeofsS3Base):
    @allure.title("Test S3 Object Multipart API")
    def test_s3_object_multipart(self):
        bucket = s3_bucket.create_bucket_s3(self.s3_client, bucket_configuration="rep-1")
        set_bucket_versioning(self.s3_client, bucket, s3_bucket.VersioningStatus.ENABLED)
        parts_count = 5
        file_name_large = generate_file(PART_SIZE * parts_count)  # 5Mb - min part
        object_key = object_key_from_file_path(file_name_large)
        part_files = split_file(file_name_large, parts_count)
        parts = []

        with allure.step("Upload first part"):
            upload_id = s3_object.create_multipart_upload_s3(self.s3_client, bucket, object_key)
            uploads = s3_object.list_multipart_uploads_s3(self.s3_client, bucket)
            etag = s3_object.upload_part_s3(self.s3_client, bucket, object_key, upload_id, 1, part_files[0])
            parts.append((1, etag))
            got_parts = s3_object.list_parts_s3(self.s3_client, bucket, object_key, upload_id)
            assert len(got_parts) == 1, f"Expected {1} parts, got\n{got_parts}"

        with allure.step("Upload last parts"):
            for part_id, file_path in enumerate(part_files[1:], start=2):
                etag = s3_object.upload_part_s3(self.s3_client, bucket, object_key, upload_id, part_id, file_path)
                parts.append((part_id, etag))
            got_parts = s3_object.list_parts_s3(self.s3_client, bucket, object_key, upload_id)
            s3_object.complete_multipart_upload_s3(self.s3_client, bucket, object_key, upload_id, parts)
            assert len(got_parts) == len(part_files), f"Expected {parts_count} parts, got\n{got_parts}"

        with allure.step("Check upload list is empty"):
            uploads = s3_object.list_multipart_uploads_s3(self.s3_client, bucket)
            assert not uploads, f"Expected there is no uploads in bucket {bucket}"

        with allure.step("Check we can get whole object from bucket"):
            got_object = s3_object.get_object_s3(self.s3_client, bucket, object_key)
            assert get_file_hash(got_object) == get_file_hash(file_name_large)

    def test_s3_object_multipart_non_sequential(self):
        if self.neofs_env.s3_gw._get_version() <= "0.32.0":
            pytest.skip("This test runs only on post 0.32.0 S3 gw version")
        bucket = s3_bucket.create_bucket_s3(self.s3_client, bucket_configuration="rep-1")
        set_bucket_versioning(self.s3_client, bucket, s3_bucket.VersioningStatus.ENABLED)
        parts_count = 5
        file_name_large = generate_file(PART_SIZE * parts_count)  # 5Mb - min part
        object_key = object_key_from_file_path(file_name_large)
        part_files = split_file(file_name_large, parts_count)
        parts = []

        with allure.step("Upload second part"):
            upload_id = s3_object.create_multipart_upload_s3(self.s3_client, bucket, object_key)
            uploads = s3_object.list_multipart_uploads_s3(self.s3_client, bucket)
            etag = s3_object.upload_part_s3(self.s3_client, bucket, object_key, upload_id, 2, part_files[1])
            parts.append((2, etag))

        with allure.step("Upload first part"):
            etag = s3_object.upload_part_s3(self.s3_client, bucket, object_key, upload_id, 1, part_files[0])
            parts.append((1, etag))
            got_parts = s3_object.list_parts_s3(self.s3_client, bucket, object_key, upload_id)
            assert len(got_parts) == 2, f"Expected {1} parts, got\n{got_parts}"

        with allure.step("Upload last parts"):
            for part_id, file_path in enumerate(part_files[2:], start=3):
                etag = s3_object.upload_part_s3(self.s3_client, bucket, object_key, upload_id, part_id, file_path)
                parts.append((part_id, etag))
            got_parts = s3_object.list_parts_s3(self.s3_client, bucket, object_key, upload_id)
            sorted_parts = sorted(parts, key=lambda x: x[0])
            s3_object.complete_multipart_upload_s3(self.s3_client, bucket, object_key, upload_id, sorted_parts)
            assert len(got_parts) == len(part_files), f"Expected {parts_count} parts, got\n{got_parts}"

        with allure.step("Check upload list is empty"):
            uploads = s3_object.list_multipart_uploads_s3(self.s3_client, bucket)
            assert not uploads, f"Expected there is no uploads in bucket {bucket}"

        with allure.step("Check we can get whole object from bucket"):
            got_object = s3_object.get_object_s3(self.s3_client, bucket, object_key)
            assert get_file_hash(got_object) == get_file_hash(file_name_large)

    def test_s3_object_multipart_random(self):
        if self.neofs_env.s3_gw._get_version() <= "0.32.0":
            pytest.skip("This test runs only on post 0.32.0 S3 gw version")

        bucket = s3_bucket.create_bucket_s3(self.s3_client, bucket_configuration="rep-1")
        set_bucket_versioning(self.s3_client, bucket, s3_bucket.VersioningStatus.ENABLED)
        parts_count = 7
        files_num = 5
        total_num_of_uploads = parts_count * files_num
        uploads = {}
        parts = defaultdict(list)

        with allure.step(f"Generate {files_num} of multipart upload requests"):
            for _ in range(files_num):
                file_name_large = generate_file(PART_SIZE * parts_count)
                object_key = object_key_from_file_path(file_name_large)
                upload_id = s3_object.create_multipart_upload_s3(self.s3_client, bucket, object_key)
                part_files = split_file(file_name_large, parts_count)
                uploads[upload_id] = [
                    (part_id, file_path, object_key, file_name_large)
                    for part_id, file_path in enumerate(part_files, start=1)
                ]
                random.shuffle(uploads[upload_id])

        with allure.step("Upload all parts randomly"):
            for _ in range(total_num_of_uploads):
                random_upload_id = random.choice(list(uploads.keys()))
                part_id, file_path, object_key, file_name_large = uploads[random_upload_id].pop()
                if len(uploads[random_upload_id]) == 0:
                    del uploads[random_upload_id]
                etag = s3_object.upload_part_s3(
                    self.s3_client, bucket, object_key, random_upload_id, part_id, file_path
                )
                parts[(random_upload_id, object_key, file_name_large)].append((part_id, etag))

        with allure.step("Complete all multipart upload requests"):
            for upload_id, object_key, file_name_large in parts.keys():
                got_parts = s3_object.list_parts_s3(self.s3_client, bucket, object_key, upload_id)
                sorted_parts = sorted(parts[(upload_id, object_key, file_name_large)], key=lambda x: x[0])
                s3_object.complete_multipart_upload_s3(self.s3_client, bucket, object_key, upload_id, sorted_parts)
                assert len(got_parts) == parts_count, f"Expected {parts_count} parts, got\n{got_parts}"

                with allure.step("Check we can get whole object from bucket"):
                    got_object = s3_object.get_object_s3(self.s3_client, bucket, object_key)
                    assert get_file_hash(got_object) == get_file_hash(file_name_large)

        with allure.step("Check upload list is empty"):
            uploads = s3_object.list_multipart_uploads_s3(self.s3_client, bucket)
            assert not uploads, f"Expected there is no uploads in bucket {bucket}"

    @allure.title("Test S3 Multipart abord")
    def test_s3_abort_multipart(self):
        bucket = s3_bucket.create_bucket_s3(self.s3_client, bucket_configuration="rep-1")
        set_bucket_versioning(self.s3_client, bucket, s3_bucket.VersioningStatus.ENABLED)
        parts_count = 5
        file_name_large = generate_file(PART_SIZE * parts_count)  # 5Mb - min part
        object_key = object_key_from_file_path(file_name_large)
        part_files = split_file(file_name_large, parts_count)
        parts = []

        with allure.step("Upload first part"):
            upload_id = s3_object.create_multipart_upload_s3(self.s3_client, bucket, object_key)
            uploads = s3_object.list_multipart_uploads_s3(self.s3_client, bucket)
            etag = s3_object.upload_part_s3(self.s3_client, bucket, object_key, upload_id, 1, part_files[0])
            parts.append((1, etag))
            got_parts = s3_object.list_parts_s3(self.s3_client, bucket, object_key, upload_id)
            assert len(got_parts) == 1, f"Expected {1} parts, got\n{got_parts}"

        with allure.step("Abort multipart upload"):
            s3_object.abort_multipart_uploads_s3(self.s3_client, bucket, object_key, upload_id)
            uploads = s3_object.list_multipart_uploads_s3(self.s3_client, bucket)
            assert not uploads, f"Expected there is no uploads in bucket {bucket}"

    @allure.title("Test S3 Upload Part Copy")
    def test_s3_multipart_copy(self):
        bucket = s3_bucket.create_bucket_s3(self.s3_client, bucket_configuration="rep-1")
        set_bucket_versioning(self.s3_client, bucket, s3_bucket.VersioningStatus.ENABLED)
        parts_count = 3
        file_name_large = generate_file(PART_SIZE * parts_count)  # 5Mb - min part
        object_key = object_key_from_file_path(file_name_large)
        part_files = split_file(file_name_large, parts_count)
        parts = []
        objs = []

        with allure.step(f"Put {parts_count} objec in bucket"):
            for part in part_files:
                s3_object.put_object_s3(self.s3_client, bucket, part)
                objs.append(object_key_from_file_path(part))
            check_objects_in_bucket(self.s3_client, bucket, objs)

        with allure.step("Create multipart upload object"):
            upload_id = s3_object.create_multipart_upload_s3(self.s3_client, bucket, object_key)
            uploads = s3_object.list_multipart_uploads_s3(self.s3_client, bucket)
            assert uploads, f"Expected there are uploads in bucket {bucket}"

        with allure.step("Start multipart upload"):
            for part_id, obj_key in enumerate(objs, start=1):
                etag = s3_object.upload_part_copy_s3(
                    self.s3_client, bucket, object_key, upload_id, part_id, f"{bucket}/{obj_key}"
                )
                parts.append((part_id, etag))
            got_parts = s3_object.list_parts_s3(self.s3_client, bucket, object_key, upload_id)
            s3_object.complete_multipart_upload_s3(self.s3_client, bucket, object_key, upload_id, parts)
            assert len(got_parts) == len(part_files), f"Expected {parts_count} parts, got\n{got_parts}"

        with allure.step("Check we can get whole object from bucket"):
            got_object = s3_object.get_object_s3(self.s3_client, bucket, object_key)
            assert get_file_hash(got_object) == get_file_hash(file_name_large)
