import json
import logging
import os
import shutil
import uuid

import allure
import pytest
import requests
from helpers.common import TEST_FILES_DIR, get_assets_dir_path
from helpers.file_helper import generate_file, get_file_hash
from helpers.s3_helper import object_key_from_file_path
from neofs_testlib.cli import NeofsAuthmate
from s3 import s3_object
from s3.s3_base import TestNeofsS3Base

logger = logging.getLogger("NeoLogger")


def pytest_generate_tests(metafunc):
    if "s3_client" in metafunc.fixturenames:
        metafunc.parametrize("s3_client", ["aws cli", "boto3"], indirect=True)


class TestS3Presigned(TestNeofsS3Base):
    @allure.title("Test S3: Get Object With Presigned Url")
    @pytest.mark.parametrize("url_from", ["neofs_authmate", "s3"])
    def test_s3_get_object_with_presigned_url(self, bucket, simple_object_size, url_from: str):
        file_path = generate_file(simple_object_size)
        file_name = object_key_from_file_path(file_path)

        with allure.step("Put object into Bucket"):
            s3_object.put_object_s3(self.s3_client, bucket, file_path)

        with allure.step(f"Get presigned URL from {url_from}"):
            if url_from == "neofs_authmate":
                neofs_authmate = NeofsAuthmate(
                    shell=self.shell, neofs_authmate_exec_path=self.neofs_env.neofs_s3_authmate_path
                )
                raw_url = json.loads(
                    neofs_authmate.presigned.generate_presigned_url(
                        endpoint=f"https://{self.neofs_env.s3_gw.address}",
                        method="GET",
                        bucket=bucket,
                        object=file_name,
                        lifetime="30s",
                        aws_secret_access_key=self.secret_access_key,
                        aws_access_key_id=self.access_key_id,
                    ).stdout
                )
                presigned_url = raw_url["URL"]
            else:
                presigned_url = self.s3_client.generate_presigned_url(
                    ClientMethod="get_object",
                    Params={"Bucket": bucket, "Key": file_name},
                    ExpiresIn=30,
                    HttpMethod="GET",
                ).strip()
                logger.info(f"Presigned URL: {presigned_url}")

        with allure.step("Get object with generated presigned url"):
            resp = requests.get(presigned_url, stream=True, timeout=30, verify=False)

            if not resp.ok:
                raise Exception(
                    f"""Failed to get object via presigned url:
                        request: {resp.request.path_url},
                        response: {resp.text},
                        status code: {resp.status_code} {resp.reason}"""
                )

            new_file_path = os.path.join(get_assets_dir_path(), TEST_FILES_DIR, f"temp_file_{uuid.uuid4()}")
            with open(new_file_path, "wb") as file:
                shutil.copyfileobj(resp.raw, file)

            assert get_file_hash(file_path) == get_file_hash(new_file_path), "Files hashes are different"
