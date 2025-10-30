import allure
import pytest
from helpers.file_helper import generate_file
from helpers.s3_helper import ACLType, object_key_from_file_path, verify_acls
from s3 import s3_bucket, s3_object
from s3.s3_base import TestNeofsS3Base


def pytest_generate_tests(metafunc):
    if "s3_client" in metafunc.fixturenames:
        metafunc.parametrize("s3_client", ["aws cli", "boto3"], indirect=True)


class TestS3ACL(TestNeofsS3Base):
    @pytest.mark.sanity
    @allure.title("Test S3: Object ACL")
    @pytest.mark.simple
    def test_s3_object_ACL(self, bucket):
        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        file_name = object_key_from_file_path(file_path)

        with allure.step("Put object into bucket, Check ACL is empty"):
            s3_object.put_object_s3(self.s3_client, bucket, file_path)
            obj_acl = s3_object.get_object_acl_s3(self.s3_client, bucket, file_name)
            verify_acls(obj_acl, ACLType.PRIVATE)

        with allure.step("Put object ACL = public-read"):
            acl = "public-read"
            with allure.step("By default ACLs are disabled"):
                with pytest.raises(Exception, match=r".*The bucket does not allow ACLs.*"):
                    s3_object.put_object_acl_s3(self.s3_client, bucket, file_name, acl)
                obj_acl = s3_object.get_object_acl_s3(self.s3_client, bucket, file_name)
                verify_acls(obj_acl, ACLType.PRIVATE)
            with allure.step("Enable ACLs"):
                s3_bucket.put_bucket_ownership_controls(
                    self.s3_client, bucket, s3_bucket.ObjectOwnership.BUCKET_OWNER_PREFERRED
                )
            s3_object.put_object_acl_s3(self.s3_client, bucket, file_name, acl)
            obj_acl = s3_object.get_object_acl_s3(self.s3_client, bucket, file_name)
            verify_acls(obj_acl, ACLType.PUBLIC_READ)

        with allure.step("Put object ACL = private"):
            acl = "private"
            s3_object.put_object_acl_s3(self.s3_client, bucket, file_name, acl)
            obj_acl = s3_object.get_object_acl_s3(self.s3_client, bucket, file_name)
            verify_acls(obj_acl, ACLType.PRIVATE)

        with allure.step("Put object with grant-read uri=http://acs.amazonaws.com/groups/global/AllUsers"):
            s3_object.put_object_acl_s3(
                self.s3_client,
                bucket,
                file_name,
                grant_read="uri=http://acs.amazonaws.com/groups/global/AllUsers",
            )
            obj_acl = s3_object.get_object_acl_s3(self.s3_client, bucket, file_name)
            verify_acls(obj_acl, ACLType.PUBLIC_READ)

        with allure.step("Disable ACL"):
            s3_bucket.put_bucket_ownership_controls(
                self.s3_client, bucket, s3_bucket.ObjectOwnership.BUCKET_OWNER_ENFORCED
            )

        with allure.step("Put object ACL = public-read"):
            acl = "public-read"
            with pytest.raises(Exception, match=r".*The bucket does not allow ACLs.*"):
                s3_object.put_object_acl_s3(self.s3_client, bucket, file_name, acl)
            obj_acl = s3_object.get_object_acl_s3(self.s3_client, bucket, file_name)
            verify_acls(obj_acl, ACLType.PRIVATE)

    @allure.title("Test S3: Object eligible ACLs")
    @pytest.mark.simple
    def test_s3_object_eligible_acls(self, bucket):
        """
        By default with disabled ACLs, user should be able to set object 'private'
        and 'bucket-owner-full-control' ACLs
        """
        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        file_name = object_key_from_file_path(file_path)

        with allure.step("Put object into bucket, Check ACL is empty"):
            s3_object.put_object_s3(self.s3_client, bucket, file_path)
            obj_acl = s3_object.get_object_acl_s3(self.s3_client, bucket, file_name)
            verify_acls(obj_acl, ACLType.PRIVATE)

        with allure.step("Put object ACL = bucket-owner-full-control"):
            acl = "bucket-owner-full-control"
            s3_object.put_object_acl_s3(self.s3_client, bucket, file_name, acl)
            obj_acl = s3_object.get_object_acl_s3(self.s3_client, bucket, file_name)
            assert len(obj_acl) == 1, f"Invalid number of grantee entries for {acl}"
            assert obj_acl[0]["Permission"] == "FULL_CONTROL", f"Invalid permissions for {acl}"

        with allure.step("Put object ACL = private"):
            acl = "private"
            s3_object.put_object_acl_s3(self.s3_client, bucket, file_name, acl)
            obj_acl = s3_object.get_object_acl_s3(self.s3_client, bucket, file_name)
            verify_acls(obj_acl, ACLType.PRIVATE)

    @pytest.mark.sanity
    @allure.title("Test S3: Bucket ACL")
    def test_s3_bucket_ACL(self):
        with allure.step("Create bucket with ACL = public-read-write"):
            acl = "public-read-write"
            bucket = s3_bucket.create_bucket_s3(
                self.s3_client,
                object_lock_enabled_for_bucket=True,
                acl=acl,
                bucket_configuration="rep-2",
            )
            bucket_acl = s3_bucket.get_bucket_acl(self.s3_client, bucket)
            verify_acls(bucket_acl, ACLType.PUBLIC_READ_WRITE)

        with allure.step("Change bucket ACL to private"):
            acl = "private"
            s3_bucket.put_bucket_acl_s3(self.s3_client, bucket, acl=acl)
            bucket_acl = s3_bucket.get_bucket_acl(self.s3_client, bucket)
            verify_acls(bucket_acl, ACLType.PRIVATE)

        with allure.step("Change bucket acl to --grant-write uri=http://acs.amazonaws.com/groups/global/AllUsers"):
            s3_bucket.put_bucket_acl_s3(
                self.s3_client,
                bucket,
                grant_write="uri=http://acs.amazonaws.com/groups/global/AllUsers",
            )
            bucket_acl = s3_bucket.get_bucket_acl(self.s3_client, bucket)
            verify_acls(bucket_acl, ACLType.PUBLIC_WRITE)

    @allure.title("Test S3: Bucket Enable Disable ACL")
    def test_s3_bucket_disable_enable_ACL(self):
        with allure.step("Create bucket"):
            bucket = s3_bucket.create_bucket_s3(
                self.s3_client,
                bucket_configuration="rep-2",
            )
            bucket_acl = s3_bucket.get_bucket_acl(self.s3_client, bucket)
            verify_acls(bucket_acl, ACLType.PRIVATE)

        with allure.step("Try to change bucket acl to public-read-write"):
            acl = "public-read-write"
            with pytest.raises(Exception, match=r".*The bucket does not allow ACLs..*"):
                s3_bucket.put_bucket_acl_s3(
                    self.s3_client,
                    bucket,
                    acl=acl,
                )
            bucket_acl = s3_bucket.get_bucket_acl(self.s3_client, bucket)
            verify_acls(bucket_acl, ACLType.PRIVATE)

        with allure.step("Enable ACLs"):
            s3_bucket.put_bucket_ownership_controls(
                self.s3_client, bucket, s3_bucket.ObjectOwnership.BUCKET_OWNER_PREFERRED
            )

        with allure.step("Change bucket acl to public-read-write"):
            acl = "public-read-write"
            s3_bucket.put_bucket_acl_s3(
                self.s3_client,
                bucket,
                acl=acl,
            )
            bucket_acl = s3_bucket.get_bucket_acl(self.s3_client, bucket)
            verify_acls(bucket_acl, ACLType.PUBLIC_READ_WRITE)

        with allure.step("Disable ACL"):
            s3_bucket.put_bucket_ownership_controls(
                self.s3_client, bucket, s3_bucket.ObjectOwnership.BUCKET_OWNER_ENFORCED
            )

        with allure.step("Try to change bucket acl to public-read-write"):
            acl = "public-read"
            with pytest.raises(Exception, match=r".*The bucket does not allow ACLs..*"):
                s3_bucket.put_bucket_acl_s3(
                    self.s3_client,
                    bucket,
                    acl=acl,
                )
            bucket_acl = s3_bucket.get_bucket_acl(self.s3_client, bucket)
            verify_acls(bucket_acl, ACLType.PUBLIC_READ_WRITE)
