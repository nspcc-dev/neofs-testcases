from typing import Optional

import allure

from steps import s3_gate_bucket, s3_gate_object


@allure.step("Expected all objects are presented in the bucket")
def check_objects_in_bucket(
    s3_client, bucket, expected_objects: list, unexpected_objects: Optional[list] = None
) -> None:
    unexpected_objects = unexpected_objects or []
    bucket_objects = s3_gate_object.list_objects_s3(s3_client, bucket)
    assert len(bucket_objects) == len(
        expected_objects
    ), f"Expected {len(expected_objects)} objects in the bucket"
    for bucket_object in expected_objects:
        assert (
            bucket_object in bucket_objects
        ), f"Expected object {bucket_object} in objects list {bucket_objects}"

    for bucket_object in unexpected_objects:
        assert (
            bucket_object not in bucket_objects
        ), f"Expected object {bucket_object} not in objects list {bucket_objects}"


@allure.step("Try to get object and got error")
def try_to_get_objects_and_expect_error(s3_client, bucket: str, object_keys: list) -> None:
    for obj in object_keys:
        try:
            s3_gate_object.get_object_s3(s3_client, bucket, obj)
            raise AssertionError(f"Object {obj} found in bucket {bucket}")
        except Exception as err:
            assert "The specified key does not exist" in str(
                err
            ), f"Expected error in exception {err}"


@allure.step("Set versioning enable for bucket")
def set_bucket_versioning(s3_client, bucket: str, status: s3_gate_bucket.VersioningStatus):
    s3_gate_bucket.get_bucket_versioning_status(s3_client, bucket)
    s3_gate_bucket.set_bucket_versioning(s3_client, bucket, status=status)
    bucket_status = s3_gate_bucket.get_bucket_versioning_status(s3_client, bucket)
    assert bucket_status == status.value, f"Expected {bucket_status} status. Got {status.value}"
