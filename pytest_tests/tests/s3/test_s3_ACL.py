import allure
import pytest
from helpers.s3_helper import ACLType, verify_acls
from s3 import s3_bucket
from s3.s3_base import TestNeofsS3Base


def pytest_generate_tests(metafunc):
    if "s3_client" in metafunc.fixturenames:
        metafunc.parametrize("s3_client", ["aws cli", "boto3"], indirect=True)


class TestS3ACL(TestNeofsS3Base):
    @pytest.mark.sanity
    @allure.title("Test S3: Bucket ACL")
    def test_s3_bucket_ACL(self):
        with allure.step("Create bucket with ACL = public-read-write"):
            acl = "public-read-write"
            bucket = s3_bucket.create_bucket_s3(
                self.s3_client,
                object_lock_enabled_for_bucket=True,
                acl=acl,
                bucket_configuration="rep-1",
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
                bucket_configuration="rep-1",
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
