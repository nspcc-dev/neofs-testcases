import time
from random import choice
from string import ascii_letters
from typing import Tuple

import allure
import pytest
from helpers.file_helper import generate_file
from helpers.s3_helper import (
    NO_SUCH_TAGS_ERROR,
    check_tags_by_bucket,
    check_tags_by_object,
    object_key_from_file_path,
)
from helpers.utility import parse_version
from s3 import s3_bucket, s3_object
from s3.s3_base import TestNeofsS3Base


def pytest_generate_tests(metafunc):
    if "s3_client" in metafunc.fixturenames:
        metafunc.parametrize("s3_client", ["aws cli", "boto3"], indirect=True)


class TestS3Tagging(TestNeofsS3Base):
    @staticmethod
    def create_tags(count: int) -> Tuple[list, list]:
        tags = []
        for _ in range(count):
            tag_key = "".join(choice(ascii_letters) for _ in range(8))
            tag_value = "".join(choice(ascii_letters) for _ in range(12))
            tags.append((tag_key, tag_value))
        return tags

    @allure.title("Test S3: Object tagging")
    @pytest.mark.simple
    def test_s3_object_tagging(self, bucket):
        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        file_name = object_key_from_file_path(file_path)

        with allure.step("Put with 3 tags object into bucket"):
            tag_1 = "Tag1=Value1"
            s3_object.put_object_s3(self.s3_client, bucket, file_path, Tagging=tag_1)
            time.sleep(1)
            got_tags = s3_object.get_object_tagging(self.s3_client, bucket, file_name)
            assert got_tags, f"Expected tags, got {got_tags}"
            assert got_tags == [{"Key": "Tag1", "Value": "Value1"}], "Tags must be the same"

        with allure.step("Put 10 new tags for object"):
            tags_2 = self.create_tags(10)
            s3_object.put_object_tagging(self.s3_client, bucket, file_name, tags=tags_2)
            time.sleep(1)
            check_tags_by_object(self.s3_client, bucket, file_name, tags_2, [("Tag1", "Value1")])

        with allure.step("Put 10 extra new tags for object"):
            tags_3 = self.create_tags(10)
            s3_object.put_object_tagging(self.s3_client, bucket, file_name, tags=tags_3)
            time.sleep(1)
            check_tags_by_object(self.s3_client, bucket, file_name, tags_3, tags_2)

        with allure.step("Copy one object with tag"):
            copy_obj_path_1 = s3_object.copy_object_s3(self.s3_client, bucket, file_name, tagging_directive="COPY")
            time.sleep(1)
            check_tags_by_object(self.s3_client, bucket, copy_obj_path_1, tags_3, tags_2)

        with allure.step("Put 11 new tags to object and expect an error"):
            tags_4 = self.create_tags(11)
            with pytest.raises(Exception, match=r".*Object tags cannot be greater than 10*"):
                # An error occurred (BadRequest) when calling the PutObjectTagging operation: Object tags cannot be greater than 10
                s3_object.put_object_tagging(self.s3_client, bucket, file_name, tags=tags_4)

        with allure.step("Put empty tag"):
            tags_5 = []
            s3_object.put_object_tagging(self.s3_client, bucket, file_name, tags=tags_5)
            time.sleep(1)
            check_tags_by_object(self.s3_client, bucket, file_name, [])

        with allure.step("Put 10 object tags"):
            tags_6 = self.create_tags(10)
            s3_object.put_object_tagging(self.s3_client, bucket, file_name, tags=tags_6)
            time.sleep(1)
            check_tags_by_object(self.s3_client, bucket, file_name, tags_6)

        with allure.step("Delete tags by delete-object-tagging"):
            s3_object.delete_object_tagging(self.s3_client, bucket, file_name)
            time.sleep(1)
            check_tags_by_object(self.s3_client, bucket, file_name, [])

    @allure.title("Test S3: bucket tagging")
    def test_s3_bucket_tagging(self, bucket):
        with allure.step("Put 10 bucket tags"):
            tags_1 = self.create_tags(10)
            s3_bucket.put_bucket_tagging(self.s3_client, bucket, tags_1)
            time.sleep(1)
            check_tags_by_bucket(self.s3_client, bucket, tags_1)

        with allure.step("Put new 10 bucket tags"):
            tags_2 = self.create_tags(10)
            s3_bucket.put_bucket_tagging(self.s3_client, bucket, tags_2)
            time.sleep(1)
            check_tags_by_bucket(self.s3_client, bucket, tags_2, tags_1)

        with allure.step("Put 11 new tags to bucket and expect an error"):
            tags_3 = self.create_tags(11)
            with pytest.raises(Exception, match=r".*Object tags cannot be greater than 10.*"):
                # An error occurred (BadRequest) when calling the PutBucketTagging operation (reached max retries: 0): Object tags cannot be greater than 10
                s3_bucket.put_bucket_tagging(self.s3_client, bucket, tags_3)
                time.sleep(1)

        with allure.step("Put empty tag"):
            tags_4 = []
            s3_bucket.put_bucket_tagging(self.s3_client, bucket, tags_4)
            time.sleep(1)
            check_tags_by_bucket(self.s3_client, bucket, tags_4)

        with allure.step("Put new 10 bucket tags"):
            tags_5 = self.create_tags(10)
            s3_bucket.put_bucket_tagging(self.s3_client, bucket, tags_5)
            time.sleep(1)
            check_tags_by_bucket(self.s3_client, bucket, tags_5, tags_2)

        with allure.step("Delete tags by delete-bucket-tagging"):
            s3_bucket.delete_bucket_tagging(self.s3_client, bucket)
            with pytest.raises(Exception, match=NO_SUCH_TAGS_ERROR):
                check_tags_by_bucket(self.s3_client, bucket, [])

    @allure.title("Test S3: unversioned bucket tagging")
    @pytest.mark.simple
    def test_s3_object_tagging_unversioned_bucket(self, bucket):
        if parse_version(self.neofs_env.get_binary_version(self.neofs_env.neofs_s3_gw_path)) <= parse_version("0.38.0"):
            pytest.skip("Test doesn't work on s3 gw 0.38.0 and earlier versions")
        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        file_name = object_key_from_file_path(file_path)

        with allure.step("Put with 3 tags object into bucket"):
            s3_object.put_object_s3(self.s3_client, bucket, file_path)
            time.sleep(1)
            tags = self.create_tags(1)
            s3_object.put_object_tagging(self.s3_client, bucket, file_name, tags=tags)
            check_tags_by_object(self.s3_client, bucket, file_name, tags)
            s3_object.put_object_s3(self.s3_client, bucket, file_path)
            time.sleep(1)
            check_tags_by_object(self.s3_client, bucket, file_name, [])
