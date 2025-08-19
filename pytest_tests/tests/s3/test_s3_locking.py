import time
from datetime import UTC, datetime, timedelta

import allure
import pytest
from helpers.file_helper import generate_file, generate_file_with_content
from helpers.s3_helper import (
    assert_object_lock_mode,
    check_objects_in_bucket,
    object_key_from_file_path,
)
from s3 import s3_bucket, s3_object
from s3.s3_base import TestNeofsS3Base


def pytest_generate_tests(metafunc):
    if "s3_client" in metafunc.fixturenames:
        metafunc.parametrize("s3_client", ["aws cli", "boto3"], indirect=True)


class TestS3Locking(TestNeofsS3Base):
    @allure.title("Test S3: Checking the operation of retention period & legal lock on the object")
    @pytest.mark.simple
    def test_s3_object_locking(self):
        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        file_name = object_key_from_file_path(file_path)
        retention_period = 30

        bucket = s3_bucket.create_bucket_s3(
            self.s3_client, object_lock_enabled_for_bucket=True, bucket_configuration="rep-1"
        )

        for version_id in [None, "second"]:
            with allure.step("Put several versions of object into bucket"):
                s3_object.put_object_s3(self.s3_client, bucket, file_path)
                time.sleep(1)
                file_name_1 = generate_file_with_content(
                    self.neofs_env.get_object_size("simple_object_size"), file_path=file_path
                )
                version_id_2 = s3_object.put_object_s3(self.s3_client, bucket, file_name_1)
                time.sleep(1)
                check_objects_in_bucket(self.s3_client, bucket, [file_name])
                if version_id:
                    version_id = version_id_2

            with allure.step(f"Put retention period {retention_period}min to object {file_name}"):
                date_obj = datetime.now(UTC) + timedelta(seconds=retention_period)
                retention = {
                    "Mode": "COMPLIANCE",
                    "RetainUntilDate": date_obj,
                }
                s3_object.put_object_retention(self.s3_client, bucket, file_name, retention, version_id)
                time.sleep(1)
                assert_object_lock_mode(self.s3_client, bucket, file_name, "COMPLIANCE", date_obj, "OFF")

            with allure.step(f"Put legal hold to object {file_name}"):
                s3_object.put_object_legal_hold(self.s3_client, bucket, file_name, "ON", version_id)
                time.sleep(1)
                assert_object_lock_mode(self.s3_client, bucket, file_name, "COMPLIANCE", date_obj, "ON")

            with allure.step("Fail with deleting object with legal hold and retention period"):
                if version_id:
                    with pytest.raises(Exception):
                        # An error occurred (AccessDenied) when calling the DeleteObject operation (reached max retries: 0): Access Denied.
                        s3_object.delete_object_s3(self.s3_client, bucket, file_name, version_id)

            with allure.step("Check retention period is no longer set on the uploaded object"):
                time.sleep(retention_period * 2)
                assert_object_lock_mode(self.s3_client, bucket, file_name, "COMPLIANCE", date_obj, "ON")

            with allure.step("Fail with deleting object with legal hold and retention period"):
                time.sleep(1)
                if version_id:
                    with pytest.raises(Exception):
                        # An error occurred (AccessDenied) when calling the DeleteObject operation (reached max retries: 0): Access Denied.
                        s3_object.delete_object_s3(self.s3_client, bucket, file_name, version_id)
                else:
                    s3_object.delete_object_s3(self.s3_client, bucket, file_name, version_id)

    @allure.title("Test S3: Checking the impossibility to change the retention mode COMPLIANCE")
    @pytest.mark.simple
    def test_s3_mode_compliance(self):
        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        file_name = object_key_from_file_path(file_path)
        retention_period = 2
        retention_period_1 = 1

        bucket = s3_bucket.create_bucket_s3(
            self.s3_client, object_lock_enabled_for_bucket=True, bucket_configuration="rep-1"
        )

        for version_id in [None, "second"]:
            with allure.step("Put object into bucket"):
                obj_version = s3_object.put_object_s3(self.s3_client, bucket, file_path)
                if version_id:
                    version_id = obj_version
                check_objects_in_bucket(self.s3_client, bucket, [file_name])

            with allure.step(f"Put retention period {retention_period}min to object {file_name}"):
                date_obj = datetime.now(UTC) + timedelta(minutes=retention_period)
                retention = {
                    "Mode": "COMPLIANCE",
                    "RetainUntilDate": date_obj,
                }
                s3_object.put_object_retention(self.s3_client, bucket, file_name, retention, version_id)
                time.sleep(1)
                assert_object_lock_mode(self.s3_client, bucket, file_name, "COMPLIANCE", date_obj, "OFF")

            with allure.step(f"Try to change retention period {retention_period_1}min to object {file_name}"):
                date_obj = datetime.now(UTC) + timedelta(minutes=retention_period_1)
                retention = {
                    "Mode": "COMPLIANCE",
                    "RetainUntilDate": date_obj,
                }
                with pytest.raises(Exception):
                    s3_object.put_object_retention(self.s3_client, bucket, file_name, retention, version_id)
                    time.sleep(1)

    @allure.title("Test S3: Checking the ability to change retention mode GOVERNANCE")
    @pytest.mark.simple
    def test_s3_mode_governance(self):
        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        file_name = object_key_from_file_path(file_path)
        retention_period = 3
        retention_period_1 = 2
        retention_period_2 = 5

        bucket = s3_bucket.create_bucket_s3(
            self.s3_client, object_lock_enabled_for_bucket=True, bucket_configuration="rep-1"
        )

        for version_id in [None, "second"]:
            with allure.step("Put object into bucket"):
                obj_version = s3_object.put_object_s3(self.s3_client, bucket, file_path)
                if version_id:
                    version_id = obj_version
                check_objects_in_bucket(self.s3_client, bucket, [file_name])

            with allure.step(f"Put retention period {retention_period}min to object {file_name}"):
                date_obj = datetime.now(UTC) + timedelta(minutes=retention_period)
                retention = {
                    "Mode": "GOVERNANCE",
                    "RetainUntilDate": date_obj,
                }
                s3_object.put_object_retention(self.s3_client, bucket, file_name, retention, version_id)
                time.sleep(1)
                assert_object_lock_mode(self.s3_client, bucket, file_name, "GOVERNANCE", date_obj, "OFF")

            with allure.step(f"Try to change retention period {retention_period_1}min to object {file_name}"):
                date_obj = datetime.now(UTC) + timedelta(minutes=retention_period_1)
                retention = {
                    "Mode": "GOVERNANCE",
                    "RetainUntilDate": date_obj,
                }
                with pytest.raises(Exception):
                    s3_object.put_object_retention(self.s3_client, bucket, file_name, retention, version_id)
                    time.sleep(1)

            with allure.step(f"Try to change retention period {retention_period_1}min to object {file_name}"):
                date_obj = datetime.now(UTC) + timedelta(minutes=retention_period_1)
                retention = {
                    "Mode": "GOVERNANCE",
                    "RetainUntilDate": date_obj,
                }
                with pytest.raises(Exception):
                    s3_object.put_object_retention(self.s3_client, bucket, file_name, retention, version_id)
                    time.sleep(1)

            with allure.step(f"Put new retention period {retention_period_2}min to object {file_name}"):
                date_obj = datetime.now(UTC) + timedelta(minutes=retention_period_2)
                retention = {
                    "Mode": "GOVERNANCE",
                    "RetainUntilDate": date_obj,
                }
                s3_object.put_object_retention(self.s3_client, bucket, file_name, retention, version_id, True)
                assert_object_lock_mode(self.s3_client, bucket, file_name, "GOVERNANCE", date_obj, "OFF")

    @allure.title("Test S3: Checking if an Object Cannot Be Locked")
    @pytest.mark.simple
    def test_s3_legal_hold(self):
        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        file_name = object_key_from_file_path(file_path)

        bucket = s3_bucket.create_bucket_s3(
            self.s3_client, object_lock_enabled_for_bucket=False, bucket_configuration="rep-1"
        )

        for version_id in [None, "second"]:
            with allure.step("Put object into bucket"):
                obj_version = s3_object.put_object_s3(self.s3_client, bucket, file_path)
                if version_id:
                    version_id = obj_version
                check_objects_in_bucket(self.s3_client, bucket, [file_name])

            with allure.step(f"Put legal hold to object {file_name}"):
                with pytest.raises(Exception):
                    s3_object.put_object_legal_hold(self.s3_client, bucket, file_name, "ON", version_id)

    @allure.title("Test S3: Checking that Legal Hold cannot be turned off once it has been enabled")
    @pytest.mark.simple
    def test_object_lock_set_legal_hold_off_not_supported(self):
        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        file_name = object_key_from_file_path(file_path)

        bucket = s3_bucket.create_bucket_s3(
            self.s3_client, object_lock_enabled_for_bucket=True, bucket_configuration="rep-1"
        )

        for version_id in [None, "second"]:
            with allure.step("Put object into bucket"):
                obj_version = s3_object.put_object_s3(self.s3_client, bucket, file_path)
                if version_id:
                    version_id = obj_version
                check_objects_in_bucket(self.s3_client, bucket, [file_name])
                s3_object.put_object_legal_hold(self.s3_client, bucket, file_name, "ON", version_id)

            with allure.step(f"Put legal hold to object {file_name}"):
                with pytest.raises(Exception):
                    s3_object.put_object_legal_hold(self.s3_client, bucket, file_name, "OFF", version_id)

            time.sleep(1)


class TestS3LockingBucket(TestNeofsS3Base):
    @allure.title("Test S3: Bucket Lock")
    @pytest.mark.simple
    def test_s3_bucket_lock(self):
        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        file_name = object_key_from_file_path(file_path)
        configuration = {"Rule": {"DefaultRetention": {"Mode": "COMPLIANCE", "Days": 1}}}

        bucket = s3_bucket.create_bucket_s3(
            self.s3_client, object_lock_enabled_for_bucket=True, bucket_configuration="rep-1"
        )

        with allure.step("PutObjectLockConfiguration with ObjectLockEnabled=False"):
            s3_bucket.put_object_lock_configuration(self.s3_client, bucket, configuration)

        with allure.step("PutObjectLockConfiguration with ObjectLockEnabled=True"):
            configuration["ObjectLockEnabled"] = "Enabled"
            s3_bucket.put_object_lock_configuration(self.s3_client, bucket, configuration)

        with allure.step("GetObjectLockConfiguration"):
            config = s3_bucket.get_object_lock_configuration(self.s3_client, bucket)
            configuration["Rule"]["DefaultRetention"]["Years"] = 0
            assert config == configuration, f"Configurations must be equal {configuration}"

        with allure.step("Put object into bucket"):
            s3_object.put_object_s3(self.s3_client, bucket, file_path)
            assert_object_lock_mode(self.s3_client, bucket, file_name, "COMPLIANCE", None, "OFF", 1)
