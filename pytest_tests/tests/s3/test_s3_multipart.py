import os
import random
from collections import defaultdict

import allure
import pytest
from helpers.file_helper import concat_files, generate_file, get_file_hash, split_file
from helpers.s3_helper import (
    check_objects_in_bucket,
    object_key_from_file_path,
    parametrize_clients,
    set_bucket_versioning,
)
from helpers.utility import parse_version
from s3 import s3_bucket, s3_object
from s3.s3_base import TestNeofsS3Base

PART_SIZE = 5 * 1024 * 1024
INVALID_PART_ORDER_ERROR = r".*The list of parts was not in ascending order.*"
INVALID_PART_ERROR = r".*One or more of the specified parts could not be found.*"


def pytest_generate_tests(metafunc):
    parametrize_clients(metafunc)


class TestS3Multipart(TestNeofsS3Base):
    def _skip_if_arbitrary_parts_not_supported(self):
        s3_gw_version = self.neofs_env.get_binary_version(self.neofs_env.neofs_s3_gw_path)
        if parse_version(s3_gw_version) <= parse_version("0.47.0"):
            pytest.skip(f"Arbitrary multipart parts are not supported by neofs-s3-gw {s3_gw_version} (<= 0.47.0)")

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
        bucket = s3_bucket.create_bucket_s3(self.s3_client, bucket_configuration="rep-1")
        set_bucket_versioning(self.s3_client, bucket, s3_bucket.VersioningStatus.ENABLED)
        parts_count = 11
        file_name_large = generate_file(PART_SIZE * parts_count)  # 5Mb - min part
        object_key = object_key_from_file_path(file_name_large)
        part_files = split_file(file_name_large, parts_count)
        parts = []

        with allure.step("Create multipart upload"):
            upload_id = s3_object.create_multipart_upload_s3(self.s3_client, bucket, object_key)
            uploads = s3_object.list_multipart_uploads_s3(self.s3_client, bucket)

        with allure.step("Upload all parts in random order"):
            part_orders = list(enumerate(part_files, start=1))
            random.shuffle(part_orders)

            for part_id, file_path in part_orders:
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

    @allure.title("Test S3 Multipart with gaps in part numbers")
    def test_s3_object_multipart_with_gaps(self):
        self._skip_if_arbitrary_parts_not_supported()
        bucket = s3_bucket.create_bucket_s3(self.s3_client, bucket_configuration="rep-1")
        part_numbers = [1, 5, 14]
        file_name_large = generate_file(PART_SIZE * len(part_numbers))
        object_key = object_key_from_file_path(file_name_large)
        part_files = split_file(file_name_large, len(part_numbers))
        parts = []

        with allure.step(f"Upload parts with numbers {part_numbers}"):
            upload_id = s3_object.create_multipart_upload_s3(self.s3_client, bucket, object_key)
            for part_num, file_path in zip(part_numbers, part_files):
                etag = s3_object.upload_part_s3(self.s3_client, bucket, object_key, upload_id, part_num, file_path)
                parts.append((part_num, etag))

            got_parts = s3_object.list_parts_s3(self.s3_client, bucket, object_key, upload_id)
            got_part_numbers = [part["PartNumber"] for part in got_parts]
            assert got_part_numbers == part_numbers, f"Expected parts {part_numbers}, got {got_part_numbers}"

        with allure.step("Complete multipart upload"):
            s3_object.complete_multipart_upload_s3(self.s3_client, bucket, object_key, upload_id, parts)
            uploads = s3_object.list_multipart_uploads_s3(self.s3_client, bucket)
            assert not uploads, f"Expected there is no uploads in bucket {bucket}"

        with allure.step("Check we can get whole object from bucket"):
            got_object = s3_object.get_object_s3(self.s3_client, bucket, object_key)
            assert get_file_hash(got_object) == get_file_hash(file_name_large)

    @allure.title("Test S3 Multipart completion with a subset of uploaded parts")
    def test_s3_object_multipart_complete_subset_of_parts(self):
        self._skip_if_arbitrary_parts_not_supported()
        bucket = s3_bucket.create_bucket_s3(self.s3_client, bucket_configuration="rep-1")
        parts_count = 5
        completed_part_numbers = [1, 3, 5]
        file_name_large = generate_file(PART_SIZE * parts_count)
        object_key = object_key_from_file_path(file_name_large)
        part_files = split_file(file_name_large, parts_count)
        parts = {}

        with allure.step(f"Upload {parts_count} parts"):
            upload_id = s3_object.create_multipart_upload_s3(self.s3_client, bucket, object_key)
            for part_num, file_path in enumerate(part_files, start=1):
                etag = s3_object.upload_part_s3(self.s3_client, bucket, object_key, upload_id, part_num, file_path)
                parts[part_num] = etag

        with allure.step(f"Complete multipart upload with parts {completed_part_numbers}"):
            s3_object.complete_multipart_upload_s3(
                self.s3_client,
                bucket,
                object_key,
                upload_id,
                [(part_num, parts[part_num]) for part_num in completed_part_numbers],
            )
            uploads = s3_object.list_multipart_uploads_s3(self.s3_client, bucket)
            assert not uploads, f"Expected there is no uploads in bucket {bucket}"

        with allure.step("Check object consists only of completed parts"):
            expected_file = concat_files([part_files[part_num - 1] for part_num in completed_part_numbers])
            got_object = s3_object.get_object_s3(self.s3_client, bucket, object_key)
            assert os.path.getsize(got_object) == os.path.getsize(expected_file)
            assert get_file_hash(got_object) == get_file_hash(expected_file)

    @allure.title("Test S3 Multipart completion with invalid parts list")
    def test_s3_object_multipart_complete_invalid_parts(self):
        bucket = s3_bucket.create_bucket_s3(self.s3_client, bucket_configuration="rep-1")
        parts_count = 3
        file_name_large = generate_file(PART_SIZE * parts_count)
        object_key = object_key_from_file_path(file_name_large)
        part_files = split_file(file_name_large, parts_count)
        parts = []

        with allure.step(f"Upload {parts_count} parts"):
            upload_id = s3_object.create_multipart_upload_s3(self.s3_client, bucket, object_key)
            for part_num, file_path in enumerate(part_files, start=1):
                etag = s3_object.upload_part_s3(self.s3_client, bucket, object_key, upload_id, part_num, file_path)
                parts.append((part_num, etag))

        invalid_parts_lists = {
            "descending order": (INVALID_PART_ORDER_ERROR, [parts[1], parts[0], parts[2]]),
            "duplicated part": (INVALID_PART_ORDER_ERROR, [parts[0], parts[0], parts[1], parts[2]]),
            "missing part": (INVALID_PART_ERROR, [parts[0], parts[1], (parts_count + 1, parts[2][1])]),
            "wrong ETag": (INVALID_PART_ERROR, [parts[0], (parts[1][0], parts[0][1]), parts[2]]),
        }
        for case, (error, invalid_parts) in invalid_parts_lists.items():
            with allure.step(f"Complete multipart upload with {case}"):
                with pytest.raises(Exception, match=error):
                    s3_object.complete_multipart_upload_s3(self.s3_client, bucket, object_key, upload_id, invalid_parts)

        with allure.step("Check upload is still in progress"):
            uploads = s3_object.list_multipart_uploads_s3(self.s3_client, bucket)
            assert uploads, f"Expected there are uploads in bucket {bucket}"
            got_parts = s3_object.list_parts_s3(self.s3_client, bucket, object_key, upload_id)
            assert len(got_parts) == parts_count, f"Expected {parts_count} parts, got\n{got_parts}"

        with allure.step("Complete multipart upload with valid parts list"):
            s3_object.complete_multipart_upload_s3(self.s3_client, bucket, object_key, upload_id, parts)
            got_object = s3_object.get_object_s3(self.s3_client, bucket, object_key)
            assert get_file_hash(got_object) == get_file_hash(file_name_large)

    def test_s3_object_multipart_random(self):
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

    @allure.title("Test S3 Object List Multipart Uploads Pagination via boto3")
    @pytest.mark.boto3_only
    def test_s3_object_multipart_upload_pagination_boto3(self):
        bucket = s3_bucket.create_bucket_s3(self.s3_client, bucket_configuration="rep-1")
        small_object_size = 8
        number_of_uploads = 20

        with allure.step("Create multiple uploads"):
            for _ in range(number_of_uploads):
                file_name = generate_file(small_object_size)
                object_key = object_key_from_file_path(file_name)
                s3_object.create_multipart_upload_s3(self.s3_client, bucket, object_key)

        with allure.step("Check pagination with paginator"):
            paginator = self.s3_client.get_paginator("list_multipart_uploads")

            pages = paginator.paginate(Bucket=bucket, PaginationConfig={"PageSize": 5, "MaxItems": number_of_uploads})

            results = []
            for page in pages:
                results.extend(page.get("Uploads", []))

            assert len(results) == number_of_uploads, f"Expected {number_of_uploads} uploads, got {len(results)}"

    @allure.title("Test S3 Object List Multipart Uploads Pagination via aws cli")
    @pytest.mark.aws_cli_only
    def test_s3_object_multipart_upload_pagination_aws_cli(self, bucket):
        bucket = s3_bucket.create_bucket_s3(self.s3_client, bucket_configuration="rep-1")
        small_object_size = 8
        number_of_uploads = 20

        with allure.step("Create multiple uploads"):
            for _ in range(number_of_uploads):
                file_name = generate_file(small_object_size)
                object_key = object_key_from_file_path(file_name)
                s3_object.create_multipart_upload_s3(self.s3_client, bucket, object_key)

        with allure.step("Check pagination with max-items and page-size"):
            response = self.s3_client.list_multipart_uploads(Bucket=bucket, MaxItems=number_of_uploads, PageSize=5)
            uploads = response.get("Uploads", [])
            assert len(uploads) == number_of_uploads, f"Expected {number_of_uploads} uploads, got {len(uploads)}"

        with allure.step("Check pagination with starting token loop"):
            all_uploads = []
            starting_token = None

            while True:
                response = self.s3_client.list_multipart_uploads(
                    Bucket=bucket, PageSize=5, StartingToken=starting_token
                )
                uploads = response.get("Uploads", [])
                all_uploads.extend(uploads)

                starting_token = response.get("NextToken")
                if not starting_token:
                    break

            assert len(all_uploads) == number_of_uploads, (
                f"Expected {number_of_uploads} uploads, got {len(all_uploads)}"
            )

    @allure.title("Test S3 Object Multipart List Parts Pagination via boto3")
    @pytest.mark.boto3_only
    def test_s3_object_multipart_list_parts_boto3(self):
        bucket = s3_bucket.create_bucket_s3(self.s3_client, bucket_configuration="rep-1")
        PART_SIZE = 16
        file_name = generate_file(PART_SIZE * 20)
        object_key = object_key_from_file_path(file_name)
        part_files = split_file(file_name, 20)
        parts_count = len(part_files)
        parts = []

        with allure.step("Upload all parts"):
            upload_id = s3_object.create_multipart_upload_s3(self.s3_client, bucket, object_key)
            for part_id, file_path in enumerate(part_files, start=1):
                etag = s3_object.upload_part_s3(self.s3_client, bucket, object_key, upload_id, part_id, file_path)
                parts.append((part_id, etag))

        with allure.step("Check pagination with paginator"):
            paginator = self.s3_client.get_paginator("list_parts")

            pages = paginator.paginate(
                Bucket=bucket,
                Key=object_key,
                UploadId=upload_id,
                PaginationConfig={"PageSize": 5, "MaxItems": parts_count},
            )

            results = []
            for page in pages:
                results.extend(page.get("Parts", []))

            assert len(results) == parts_count, f"Expected {parts_count} parts, got {len(results)}"

    @allure.title("Test S3 Object Multipart List Parts Pagination via aws cli")
    @pytest.mark.aws_cli_only
    def test_s3_object_multipart_list_parts_aws_cli(self, bucket):
        bucket = s3_bucket.create_bucket_s3(self.s3_client, bucket_configuration="rep-1")
        PART_SIZE = 16
        parts_count = 20
        file_name = generate_file(PART_SIZE * parts_count)
        object_key = object_key_from_file_path(file_name)
        part_files = split_file(file_name, parts_count)
        parts_count = len(part_files)
        parts = []

        with allure.step("Upload all parts"):
            upload_id = s3_object.create_multipart_upload_s3(self.s3_client, bucket, object_key)
            for part_id, file_path in enumerate(part_files, start=1):
                etag = s3_object.upload_part_s3(self.s3_client, bucket, object_key, upload_id, part_id, file_path)
                parts.append((part_id, etag))

        with allure.step("Check pagination with max-items and page-size"):
            response = self.s3_client.list_parts(
                Bucket=bucket, Key=object_key, UploadId=upload_id, MaxItems=parts_count, PageSize=5
            )
            parts = response.get("Parts", [])
            assert len(parts) == parts_count, f"Expected {parts_count} parts, got {len(parts)}"

        with allure.step("Check pagination with starting token loop"):
            all_parts = []
            starting_token = None

            while True:
                response = self.s3_client.list_parts(
                    Bucket=bucket, Key=object_key, UploadId=upload_id, PageSize=5, StartingToken=starting_token
                )
                parts = response.get("Parts", [])
                all_parts.extend(parts)

                starting_token = response.get("NextToken")
                if not starting_token:
                    break

            assert len(all_parts) == parts_count, f"Expected {parts_count} parts, got {len(all_parts)}"

    @allure.title("Test S3 Get Specific Part of Multipart Object")
    @pytest.mark.boto3_only
    def test_s3_get_multipart_object_part(self):
        bucket = s3_bucket.create_bucket_s3(self.s3_client)
        parts_count = 5
        file_name_large = generate_file(PART_SIZE * parts_count)
        object_key = object_key_from_file_path(file_name_large)
        part_files = split_file(file_name_large, parts_count)
        parts = []

        with allure.step("Upload multipart object"):
            upload_id = s3_object.create_multipart_upload_s3(self.s3_client, bucket, object_key)
            for part_id, file_path in enumerate(part_files, start=1):
                etag = s3_object.upload_part_s3(self.s3_client, bucket, object_key, upload_id, part_id, file_path)
                parts.append((part_id, etag))
            s3_object.complete_multipart_upload_s3(self.s3_client, bucket, object_key, upload_id, parts)

        with allure.step("Verify parts can be retrieved individually and reconstruct the full object"):
            first_part_response = s3_object.get_object_s3(
                self.s3_client, bucket, object_key, part_number=1, full_output=True
            )

            actual_parts_count = int(first_part_response.get("PartsCount"))

            assert actual_parts_count == parts_count, (
                f"Invalid parts count, expected {parts_count}, got {actual_parts_count}"
            )

            reconstructed_file = os.path.join(os.path.dirname(file_name_large), f"reconstructed_{object_key}")
            with open(reconstructed_file, "wb") as outfile:
                for part_number in range(1, actual_parts_count + 1):
                    part_file = s3_object.get_object_s3(self.s3_client, bucket, object_key, part_number=part_number)

                    with open(part_file, "rb") as infile:
                        outfile.write(infile.read())

                    os.remove(part_file)

            reconstructed_hash = get_file_hash(reconstructed_file)
            original_hash = get_file_hash(file_name_large)
            reconstructed_size = os.path.getsize(reconstructed_file)
            original_size = os.path.getsize(file_name_large)

            assert reconstructed_hash == original_hash, (
                f"Reconstructed object doesn't match original. "
                f"Reconstructed size: {reconstructed_size}, Original size: {original_size}"
            )

            os.remove(reconstructed_file)
