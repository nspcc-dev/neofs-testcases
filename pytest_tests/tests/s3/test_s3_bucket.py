from datetime import UTC, datetime, timedelta

import allure
import pytest
from helpers.file_helper import generate_file
from helpers.s3_helper import (
    ACLType,
    assert_object_lock_mode,
    check_objects_in_bucket,
    object_key_from_file_path,
    set_bucket_versioning,
    verify_acls,
)
from s3 import s3_bucket, s3_object
from s3.s3_base import TestNeofsS3Base


def pytest_generate_tests(metafunc):
    if "s3_client" in metafunc.fixturenames:
        metafunc.parametrize("s3_client", ["aws cli", "boto3"], indirect=True)


class TestS3Bucket(TestNeofsS3Base):
    @pytest.mark.sanity
    @allure.title("Test S3: Create Bucket with various ACL")
    def test_s3_create_bucket_with_ACL(self):
        with allure.step("Create bucket with ACL = private"):
            acl = "private"
            bucket = s3_bucket.create_bucket_s3(
                self.s3_client,
                object_lock_enabled_for_bucket=True,
                acl=acl,
                bucket_configuration="rep-1",
            )
            bucket_acl = s3_bucket.get_bucket_acl(self.s3_client, bucket)
            verify_acls(bucket_acl, ACLType.PRIVATE)

        with allure.step("Create bucket with ACL = public-read"):
            acl = "public-read"
            bucket_1 = s3_bucket.create_bucket_s3(
                self.s3_client,
                object_lock_enabled_for_bucket=True,
                acl=acl,
                bucket_configuration="rep-1",
            )
            bucket_acl_1 = s3_bucket.get_bucket_acl(self.s3_client, bucket_1)
            verify_acls(bucket_acl_1, ACLType.PUBLIC_READ)

        with allure.step("Create bucket with ACL = public-read-write"):
            acl = "public-read-write"
            bucket_2 = s3_bucket.create_bucket_s3(
                self.s3_client,
                object_lock_enabled_for_bucket=True,
                acl=acl,
                bucket_configuration="rep-1",
            )
            bucket_acl_2 = s3_bucket.get_bucket_acl(self.s3_client, bucket_2)
            verify_acls(bucket_acl_2, ACLType.PUBLIC_READ_WRITE)

        with allure.step("Create bucket with ACL = authenticated-read"):
            acl = "authenticated-read"
            bucket_3 = s3_bucket.create_bucket_s3(
                self.s3_client,
                object_lock_enabled_for_bucket=True,
                acl=acl,
                bucket_configuration="rep-1",
            )
            bucket_acl_3 = s3_bucket.get_bucket_acl(self.s3_client, bucket_3)
            verify_acls(bucket_acl_3, ACLType.PUBLIC_READ)

    @allure.title("Test S3: Create Bucket with different ACL by grand")
    def test_s3_create_bucket_with_grands(self):
        with allure.step("Create bucket with  --grant-read"):
            bucket = s3_bucket.create_bucket_s3(
                self.s3_client,
                object_lock_enabled_for_bucket=True,
                grant_read="uri=http://acs.amazonaws.com/groups/global/AllUsers",
                bucket_configuration="rep-1",
            )
            bucket_acl = s3_bucket.get_bucket_acl(self.s3_client, bucket)
            verify_acls(bucket_acl, ACLType.PUBLIC_READ)

        with allure.step("Create bucket with --grant-write"):
            bucket_1 = s3_bucket.create_bucket_s3(
                self.s3_client,
                object_lock_enabled_for_bucket=True,
                grant_write="uri=http://acs.amazonaws.com/groups/global/AllUsers",
                bucket_configuration="rep-1",
            )
            bucket_acl_1 = s3_bucket.get_bucket_acl(self.s3_client, bucket_1)
            verify_acls(bucket_acl_1, ACLType.PUBLIC_WRITE)

        with allure.step("Create bucket with --grant-full-control"):
            bucket_2 = s3_bucket.create_bucket_s3(
                self.s3_client,
                object_lock_enabled_for_bucket=True,
                grant_full_control="uri=http://acs.amazonaws.com/groups/global/AllUsers",
                bucket_configuration="rep-1",
            )
            bucket_acl_2 = s3_bucket.get_bucket_acl(self.s3_client, bucket_2)
            verify_acls(bucket_acl_2, ACLType.PUBLIC_READ_WRITE)

    @allure.title("Test S3: create bucket with object lock")
    def test_s3_bucket_object_lock(self):
        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        file_name = object_key_from_file_path(file_path)

        with allure.step("Create bucket with --no-object-lock-enabled-for-bucket"):
            bucket = s3_bucket.create_bucket_s3(
                self.s3_client, object_lock_enabled_for_bucket=False, bucket_configuration="rep-1"
            )
            date_obj = datetime.now(UTC) + timedelta(days=1)
            with pytest.raises(Exception, match=r".*Object Lock configuration does not exist for this bucket.*"):
                # An error occurred (ObjectLockConfigurationNotFoundError) when calling the PutObject operation (reached max retries: 0):
                # Object Lock configuration does not exist for this bucket
                s3_object.put_object_s3(
                    self.s3_client,
                    bucket,
                    file_path,
                    ObjectLockMode="COMPLIANCE",
                    ObjectLockRetainUntilDate=date_obj.strftime("%Y-%m-%dT%H:%M:%S"),
                )
        with allure.step("Create bucket with --object-lock-enabled-for-bucket"):
            bucket_1 = s3_bucket.create_bucket_s3(
                self.s3_client, object_lock_enabled_for_bucket=True, bucket_configuration="rep-1"
            )
            date_obj_1 = datetime.now(UTC) + timedelta(days=1)
            s3_object.put_object_s3(
                self.s3_client,
                bucket_1,
                file_path,
                ObjectLockMode="COMPLIANCE",
                ObjectLockRetainUntilDate=date_obj_1.strftime("%Y-%m-%dT%H:%M:%S"),
                ObjectLockLegalHoldStatus="ON",
            )
            assert_object_lock_mode(self.s3_client, bucket_1, file_name, "COMPLIANCE", date_obj_1, "ON")

    @allure.title("Test S3: delete bucket")
    def test_s3_delete_bucket(self):
        file_path_1 = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        file_name_1 = object_key_from_file_path(file_path_1)
        file_path_2 = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        file_name_2 = object_key_from_file_path(file_path_2)
        bucket = s3_bucket.create_bucket_s3(self.s3_client, bucket_configuration="rep-1")

        with allure.step("Put two objects into bucket"):
            s3_object.put_object_s3(self.s3_client, bucket, file_path_1)
            s3_object.put_object_s3(self.s3_client, bucket, file_path_2)
            check_objects_in_bucket(self.s3_client, bucket, [file_name_1, file_name_2])

        with allure.step("Try to delete not empty bucket and expect error"):
            with pytest.raises(Exception, match=r".*The bucket you tried to delete is not empty.*"):
                s3_bucket.delete_bucket_s3(self.s3_client, bucket)

        with allure.step("Delete all objects in bucket"):
            s3_object.delete_object_s3(self.s3_client, bucket, file_name_1)
            s3_object.delete_object_s3(self.s3_client, bucket, file_name_2)
            check_objects_in_bucket(self.s3_client, bucket, [])

        with allure.step("Delete empty bucket"):
            s3_bucket.delete_bucket_s3(self.s3_client, bucket)
            with pytest.raises(Exception, match=r".*Not Found.*"):
                s3_bucket.head_bucket(self.s3_client, bucket)

    @allure.title("Test S3: bucket policy ")
    def test_s3_bucket_policy(self):
        with allure.step("Create bucket with default policy"):
            bucket = s3_bucket.create_bucket_s3(self.s3_client)
            set_bucket_versioning(self.s3_client, bucket, s3_bucket.VersioningStatus.ENABLED)

        with allure.step("GetBucketPolicy"):
            s3_bucket.get_bucket_policy(self.s3_client, bucket)

        with allure.step("Put new policy"):
            custom_policy = {
                "Version": "2008-10-17",
                "Id": "aaaa-bbbb-cccc-dddd",
                "Statement": [
                    {
                        "Sid": "AddPerm",
                        "Effect": "Allow",
                        "Principal": {"AWS": "*"},
                        "Action": ["s3:GetObject"],
                        "Resource": [f"arn:aws:s3:::{bucket}/*"],
                    }
                ],
            }

            s3_bucket.put_bucket_policy(self.s3_client, bucket, custom_policy)
        with allure.step("GetBucketPolicy"):
            s3_bucket.get_bucket_policy(self.s3_client, bucket)
