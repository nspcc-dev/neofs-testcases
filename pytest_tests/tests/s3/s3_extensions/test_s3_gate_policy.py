import os

import allure
import pytest
from helpers.complex_object_actions import get_simple_object_copies
from helpers.container import get_container, search_container_by_name
from helpers.file_helper import generate_file
from helpers.s3_helper import (
    check_objects_in_bucket,
    object_key_from_file_path,
    set_bucket_versioning,
)
from helpers.utility import placement_policy_from_container
from s3 import s3_bucket, s3_object
from s3.s3_base import TestNeofsS3Base


def pytest_generate_tests(metafunc):
    policy = f"{os.getcwd()}/pytest_tests/data/policy.json"
    if "s3_client" in metafunc.fixturenames:
        metafunc.parametrize(
            "s3_client",
            [("aws cli", policy), ("boto3", policy)],
            indirect=True,
            ids=["aws cli", "boto3"],
        )


class TestS3GatePolicy(TestNeofsS3Base):
    def check_container_policy(self, bucket_name: str, expected_policy: str):
        cid = search_container_by_name(self.wallet.path, bucket_name, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
        container_info: str = get_container(
            self.wallet.path,
            cid,
            json_mode=False,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )
        container_info = container_info.casefold()
        expected_policy = expected_policy.casefold()
        actual_policy = placement_policy_from_container(container_info)
        assert actual_policy == expected_policy, f"Expected policy\n{expected_policy} but got policy\n{actual_policy}"

    @allure.title("Test S3: Verify bucket creation with retention policy applied")
    def test_s3_bucket_location(self, simple_object_size):
        file_path_1 = generate_file(simple_object_size)
        file_name_1 = object_key_from_file_path(file_path_1)
        file_path_2 = generate_file(simple_object_size)
        file_name_2 = object_key_from_file_path(file_path_2)

        with allure.step("Create two buckets with different bucket configuration"):
            bucket_1 = s3_bucket.create_bucket_s3(self.s3_client, bucket_configuration="complex")
            set_bucket_versioning(self.s3_client, bucket_1, s3_bucket.VersioningStatus.ENABLED)
            bucket_2 = s3_bucket.create_bucket_s3(self.s3_client, bucket_configuration="rep-3")
            set_bucket_versioning(self.s3_client, bucket_2, s3_bucket.VersioningStatus.ENABLED)
            list_buckets = s3_bucket.list_buckets_s3(self.s3_client)
            assert bucket_1 in list_buckets and bucket_2 in list_buckets, (
                f"Expected two buckets {bucket_1, bucket_2}, got {list_buckets}"
            )

            # with allure.step("Check head buckets"):
            head_1 = s3_bucket.head_bucket(self.s3_client, bucket_1)
            head_2 = s3_bucket.head_bucket(self.s3_client, bucket_2)
            assert head_1 == {} or head_1.get("HEAD") is None, "Expected head is empty"
            assert head_2 == {} or head_2.get("HEAD") is None, "Expected head is empty"

        with allure.step("Put objects into buckets"):
            version_id_1 = s3_object.put_object_s3(self.s3_client, bucket_1, file_path_1)
            version_id_2 = s3_object.put_object_s3(self.s3_client, bucket_2, file_path_2)
            check_objects_in_bucket(self.s3_client, bucket_1, [file_name_1])
            check_objects_in_bucket(self.s3_client, bucket_2, [file_name_2])

        with allure.step("Check bucket location"):
            bucket_loc_1 = s3_bucket.get_bucket_location(self.s3_client, bucket_1)
            bucket_loc_2 = s3_bucket.get_bucket_location(self.s3_client, bucket_2)
            assert bucket_loc_1 == "complex"
            assert bucket_loc_2 == "rep-3"

        with allure.step("Check object policy"):
            cid_1 = search_container_by_name(
                self.wallet.path, bucket_1, shell=self.shell, endpoint=self.neofs_env.sn_rpc
            )
            copies_1 = get_simple_object_copies(
                wallet=self.wallet.path,
                cid=cid_1,
                oid=version_id_1,
                shell=self.shell,
                nodes=self.neofs_env.storage_nodes,
            )
            assert copies_1 == 1
            cid_2 = search_container_by_name(
                self.wallet.path, bucket_2, shell=self.shell, endpoint=self.neofs_env.sn_rpc
            )
            copies_2 = get_simple_object_copies(
                wallet=self.wallet.path,
                cid=cid_2,
                oid=version_id_2,
                shell=self.shell,
                nodes=self.neofs_env.storage_nodes,
            )
            assert copies_2 == 3

    @allure.title("Test S3: Verify bucket creation with policies from config file")
    def test_s3_bucket_location_from_config_file(self, simple_object_size):
        if self.neofs_env.s3_gw._get_version() <= "0.31.1":
            pytest.skip("This test runs only on post 0.31.1 S3 gw version")
        file_path_1 = generate_file(simple_object_size)
        file_name_1 = object_key_from_file_path(file_path_1)
        file_path_2 = generate_file(simple_object_size)
        file_name_2 = object_key_from_file_path(file_path_2)

        with allure.step("Create two buckets with different bucket configuration"):
            bucket_1 = s3_bucket.create_bucket_s3(self.s3_client, bucket_configuration="select")
            set_bucket_versioning(self.s3_client, bucket_1, s3_bucket.VersioningStatus.ENABLED)
            bucket_2 = s3_bucket.create_bucket_s3(self.s3_client, bucket_configuration="rep-2")
            set_bucket_versioning(self.s3_client, bucket_2, s3_bucket.VersioningStatus.ENABLED)
            list_buckets = s3_bucket.list_buckets_s3(self.s3_client)
            assert bucket_1 in list_buckets and bucket_2 in list_buckets, (
                f"Expected two buckets {bucket_1, bucket_2}, got {list_buckets}"
            )

            head_1 = s3_bucket.head_bucket(self.s3_client, bucket_1)
            head_2 = s3_bucket.head_bucket(self.s3_client, bucket_2)
            assert head_1 == {} or head_1.get("HEAD") is None, "Expected head is empty"
            assert head_2 == {} or head_2.get("HEAD") is None, "Expected head is empty"

        with allure.step("Put objects into buckets"):
            version_id_1 = s3_object.put_object_s3(self.s3_client, bucket_1, file_path_1)
            version_id_2 = s3_object.put_object_s3(self.s3_client, bucket_2, file_path_2)
            check_objects_in_bucket(self.s3_client, bucket_1, [file_name_1])
            check_objects_in_bucket(self.s3_client, bucket_2, [file_name_2])

        with allure.step("Check bucket location"):
            bucket_loc_1 = s3_bucket.get_bucket_location(self.s3_client, bucket_1)
            bucket_loc_2 = s3_bucket.get_bucket_location(self.s3_client, bucket_2)
            assert bucket_loc_1 == "select"
            assert bucket_loc_2 == "rep-2"

        with allure.step("Check containers policy"):
            self.check_container_policy(bucket_1, "REP 1 IN X CBF 1 SELECT 1 FROM * AS X")
            self.check_container_policy(bucket_2, "REP 2")

        with allure.step("Check object policy"):
            cid_1 = search_container_by_name(
                self.wallet.path, bucket_1, shell=self.shell, endpoint=self.neofs_env.sn_rpc
            )
            copies_1 = get_simple_object_copies(
                wallet=self.wallet.path,
                cid=cid_1,
                oid=version_id_1,
                shell=self.shell,
                nodes=self.neofs_env.storage_nodes,
            )
            assert copies_1 == 1
            cid_2 = search_container_by_name(
                self.wallet.path, bucket_2, shell=self.shell, endpoint=self.neofs_env.sn_rpc
            )
            copies_2 = get_simple_object_copies(
                wallet=self.wallet.path,
                cid=cid_2,
                oid=version_id_2,
                shell=self.shell,
                nodes=self.neofs_env.storage_nodes,
            )
            assert copies_2 == 2
