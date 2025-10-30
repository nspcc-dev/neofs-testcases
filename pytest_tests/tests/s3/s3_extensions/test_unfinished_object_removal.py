import logging

import allure
from helpers.container import search_container_by_name
from helpers.file_helper import generate_file, split_file
from helpers.neofs_verbs import delete_object, search_object
from helpers.s3_helper import object_key_from_file_path, set_bucket_versioning
from s3 import s3_bucket, s3_object
from s3.s3_base import TestNeofsS3Base

PART_SIZE = 5 * 1024 * 1024

logger = logging.getLogger("NeoLogger")


def pytest_generate_tests(metafunc):
    if "s3_client" in metafunc.fixturenames:
        metafunc.parametrize("s3_client", ["boto3"], indirect=True)


class TestUnfinishedObjectRemoval(TestNeofsS3Base):
    def test_unfinished_object_removal(self):
        with allure.step("Create s3 bucket"):
            bucket = s3_bucket.create_bucket_s3(self.s3_client, bucket_configuration="rep-2")
            cid = search_container_by_name(self.wallet.path, bucket, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
            set_bucket_versioning(self.s3_client, bucket, s3_bucket.VersioningStatus.ENABLED)
            parts_count = 5
            file_name_large = generate_file(PART_SIZE * parts_count)  # 5Mb - min part
            object_key = object_key_from_file_path(file_name_large)
            part_files = split_file(file_name_large, parts_count)

        with allure.step("Initiate multipart upload and upload first part"):
            upload_id = s3_object.create_multipart_upload_s3(self.s3_client, bucket, object_key)
            s3_object.upload_part_s3(self.s3_client, bucket, object_key, upload_id, 1, part_files[0])
            got_parts = s3_object.list_parts_s3(self.s3_client, bucket, object_key, upload_id)
            assert len(got_parts) == 1, f"Expected {1} parts, got\n{got_parts}"

        with allure.step(f"Find all created objects related to {upload_id}"):
            oids = search_object(
                self.wallet.path,
                cid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                filters=[f"$Object:split.first EQ {upload_id}"],
            )

        with allure.step(f"Delete all parts related to {upload_id}"):
            for oid in oids:
                delete_object(
                    self.wallet.path,
                    cid,
                    oid,
                    self.shell,
                    self.neofs_env.sn_rpc,
                )

        with allure.step(f"Delete {upload_id} itself"):
            delete_object(
                self.wallet.path,
                cid,
                upload_id,
                self.shell,
                self.neofs_env.sn_rpc,
            )
