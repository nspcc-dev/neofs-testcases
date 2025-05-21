import os
import threading
import time

import allure
import pytest
from helpers.file_helper import generate_file, generate_file_with_content
from helpers.s3_helper import set_bucket_versioning
from helpers.utility import parse_version
from s3 import s3_bucket, s3_object
from s3.s3_base import TestNeofsS3Base


def pytest_generate_tests(metafunc):
    if "s3_client" in metafunc.fixturenames:
        clients = ["aws cli", "boto3"]
        for mark in metafunc.definition.own_markers:
            if mark.name == "aws_cli_only":
                clients = ["aws cli"]
                break
            if mark.name == "boto3_only":
                clients = ["boto3"]
                break
        metafunc.parametrize("s3_client", clients, indirect=True)


class TestS3Versioning(TestNeofsS3Base):
    @staticmethod
    def object_key_from_file_path(full_path: str) -> str:
        return os.path.basename(full_path)

    @allure.title("Test S3: try to disable versioning")
    def test_s3_version_off(self):
        bucket = s3_bucket.create_bucket_s3(
            self.s3_client, object_lock_enabled_for_bucket=True, bucket_configuration="rep-1"
        )
        with pytest.raises(Exception):
            set_bucket_versioning(self.s3_client, bucket, s3_bucket.VersioningStatus.SUSPENDED)

    @pytest.fixture(scope="class")
    def prepare_versioned_objects(self) -> tuple:
        bucket = s3_bucket.create_bucket_s3(
            self.s3_client, object_lock_enabled_for_bucket=False, bucket_configuration="rep-1"
        )

        set_bucket_versioning(self.s3_client, bucket, s3_bucket.VersioningStatus.ENABLED)

        num_of_versions = 2
        num_of_objects = 10
        small_object_size = 8

        def put_objects_with_versions():
            file_path = generate_file(small_object_size)
            file_name = self.object_key_from_file_path(file_path)
            for _ in range(num_of_versions):
                file_name = generate_file_with_content(small_object_size, file_path=file_path)
                s3_object.put_object_s3(self.s3_client, bucket, file_name)

        put_object_threads = [
            threading.Thread(
                target=put_objects_with_versions,
            )
            for _ in range(num_of_objects)
        ]

        for t in put_object_threads:
            t.start()
        for t in put_object_threads:
            t.join()
        yield bucket, num_of_versions * num_of_objects

    @allure.title("Test S3: Enable and disable versioning")
    def test_s3_version(self):
        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        file_name = self.object_key_from_file_path(file_path)
        bucket_objects = [file_name]
        bucket = s3_bucket.create_bucket_s3(
            self.s3_client, object_lock_enabled_for_bucket=False, bucket_configuration="rep-1"
        )
        set_bucket_versioning(self.s3_client, bucket, s3_bucket.VersioningStatus.SUSPENDED)

        with allure.step("Put object into bucket"):
            s3_object.put_object_s3(self.s3_client, bucket, file_path)
            time.sleep(1)
            objects_list = s3_object.list_objects_s3(self.s3_client, bucket)
            assert objects_list == bucket_objects, f"Expected list with single objects in bucket, got {objects_list}"
            object_version = s3_object.list_objects_versions_s3(self.s3_client, bucket)
            actual_version = [version.get("VersionId") for version in object_version if version.get("Key") == file_name]
            assert actual_version == ["null"], f"Expected version is null in list-object-versions, got {object_version}"
            object_0 = s3_object.head_object_s3(self.s3_client, bucket, file_name)
            assert object_0.get("VersionId") == "null", (
                f"Expected version is null in head-object, got {object_0.get('VersionId')}"
            )

        set_bucket_versioning(self.s3_client, bucket, s3_bucket.VersioningStatus.ENABLED)

        with allure.step("Put several versions of object into bucket"):
            version_id_1 = s3_object.put_object_s3(self.s3_client, bucket, file_path)
            time.sleep(1)
            file_name_1 = generate_file_with_content(
                self.neofs_env.get_object_size("simple_object_size"), file_path=file_path
            )
            version_id_2 = s3_object.put_object_s3(self.s3_client, bucket, file_name_1)

        with allure.step("Check bucket shows all versions"):
            versions = s3_object.list_objects_versions_s3(self.s3_client, bucket)
            obj_versions = [version.get("VersionId") for version in versions if version.get("Key") == file_name]
            assert obj_versions.sort() == [version_id_1, version_id_2, "null"].sort(), (
                f"Expected object has versions: {version_id_1, version_id_2, 'null'}"
            )

        with allure.step("Get object"):
            object_1 = s3_object.get_object_s3(self.s3_client, bucket, file_name, full_output=True)
            assert object_1.get("VersionId") == version_id_2, f"Get object with version {version_id_2}"

        with allure.step("Get first version of object"):
            object_2 = s3_object.get_object_s3(self.s3_client, bucket, file_name, version_id_1, full_output=True)
            assert object_2.get("VersionId") == version_id_1, f"Get object with version {version_id_1}"

        with allure.step("Get second version of object"):
            object_3 = s3_object.get_object_s3(self.s3_client, bucket, file_name, version_id_2, full_output=True)
            assert object_3.get("VersionId") == version_id_2, f"Get object with version {version_id_2}"

    @allure.title("Test for duplicate objects in S3 listings")
    def test_s3_duplicates_in_object_listing(self):
        bucket = s3_bucket.create_bucket_s3(
            self.s3_client, object_lock_enabled_for_bucket=False, bucket_configuration="rep-1"
        )

        set_bucket_versioning(self.s3_client, bucket, s3_bucket.VersioningStatus.ENABLED)

        num_of_versions = 51
        num_of_objects = 20
        small_object_size = 8

        def put_objects_with_versions():
            file_path = generate_file(small_object_size)
            file_name = self.object_key_from_file_path(file_path)
            for _ in range(num_of_versions):
                file_name = generate_file_with_content(small_object_size, file_path=file_path)
                s3_object.put_object_s3(self.s3_client, bucket, file_name)

        put_object_threads = [
            threading.Thread(
                target=put_objects_with_versions,
            )
            for _ in range(num_of_objects)
        ]

        for t in put_object_threads:
            t.start()
        for t in put_object_threads:
            t.join()

        with allure.step("Check all versions are presented and unique"):
            versions = s3_object.list_objects_versions_s3(self.s3_client, bucket, max_keys=2000)
            assert len(versions) == num_of_versions * num_of_objects, (
                f"Expected {num_of_versions * num_of_objects} versions, got {len(versions)}"
            )
            version_ids = [obj["VersionId"] for obj in versions]
            assert len(version_ids) == len(set(version_ids)), "Duplicate VersionId found!"

    @allure.title("Test prefix in list object versions")
    def test_s3_prefix_in_object_listing(self):
        bucket = s3_bucket.create_bucket_s3(
            self.s3_client, object_lock_enabled_for_bucket=False, bucket_configuration="rep-1"
        )

        set_bucket_versioning(self.s3_client, bucket, s3_bucket.VersioningStatus.ENABLED)

        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        file_name = self.object_key_from_file_path(file_path)
        for _ in range(5):
            file_name = generate_file_with_content(
                self.neofs_env.get_object_size("simple_object_size"), file_path=file_path
            )
            s3_object.put_object_s3(self.s3_client, bucket, file_name)

        with allure.step("Check prefix in list object versions"):
            response = self.s3_client.list_object_versions(Bucket=bucket, Prefix=file_name[:5])
            assert response.get("Prefix", "") == file_name[:5], (
                f"Expected prefix {file_name[:5]}, got {response.get('Prefix', '')}"
            )

    @allure.title("Test pagination for list_objects_versions_s3 using boto3")
    @pytest.mark.boto3_only
    def test_s3_pagination_in_objects_versions_listing_via_boto3(self, prepare_versioned_objects: tuple):
        if parse_version(self.neofs_env.get_binary_version(self.neofs_env.neofs_s3_gw_path)) <= parse_version("0.37.0"):
            pytest.skip("Supported only on post-0.37.0 s3 gw")

        bucket, objects_count = prepare_versioned_objects
        with allure.step("Check basic pagination with MaxKeys"):
            max_keys = 5
            response = self.s3_client.list_object_versions(Bucket=bucket, MaxKeys=max_keys)
            versions = response.get("Versions", [])
            assert len(versions) <= max_keys, f"Expected {max_keys} versions, got {len(versions)}"
            assert "IsTruncated" in response, "Expected 'IsTruncated' key in response"

        with allure.step("Check pagination with paginator"):
            paginator = self.s3_client.get_paginator("list_object_versions")

            pages = paginator.paginate(Bucket=bucket, PaginationConfig={"PageSize": 5, "MaxItems": objects_count})

            results = []
            for page in pages:
                results.extend(page.get("Versions", []))

            assert len(results) == objects_count, f"Expected {objects_count} versions, got {len(results)}"

    @allure.title("Test pagination for list_object_versions using AWS CLI")
    @pytest.mark.aws_cli_only
    def test_s3_pagination_in_objects_versions_listing_via_cli(self, prepare_versioned_objects: tuple):
        if parse_version(self.neofs_env.get_binary_version(self.neofs_env.neofs_s3_gw_path)) <= parse_version("0.37.0"):
            pytest.skip("Supported only on post-0.37.0 s3 gw")

        bucket, objects_count = prepare_versioned_objects

        with allure.step("Check basic pagination with MaxKeys"):
            max_keys = 5
            response = self.s3_client.paginated_list_object_versions(Bucket=bucket, MaxKeys=max_keys)
            versions = response.get("Versions", [])
            assert len(versions) <= max_keys, f"Expected {max_keys} versions, got {len(versions)}"
            assert response.get("IsTruncated", False), "Expected 'IsTruncated' key in response"

        with allure.step("Check pagination with max-items and page-size"):
            response = self.s3_client.paginated_list_object_versions(Bucket=bucket, MaxItems=objects_count, PageSize=5)
            versions = response.get("Versions", [])
            assert len(versions) == objects_count, f"Expected {objects_count} versions, got {len(versions)}"

        with allure.step("Check pagination with starting token loop"):
            all_versions = []
            starting_token = None

            while True:
                response = self.s3_client.paginated_list_object_versions(
                    Bucket=bucket, PageSize=5, StartingToken=starting_token
                )
                versions = response.get("Versions", [])
                all_versions.extend(versions)

                starting_token = response.get("NextToken")
                if not starting_token:
                    break

            assert len(all_versions) == objects_count, f"Expected {objects_count} versions, got {len(all_versions)}"
