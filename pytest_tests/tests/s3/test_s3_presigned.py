import logging
import os
import shutil
import uuid

import allure
import pytest
import requests
from helpers.common import TEST_FILES_DIR, get_assets_dir_path
from helpers.file_helper import generate_file, get_file_hash
from s3 import s3_object
from s3.s3_base import TestNeofsS3Base

logger = logging.getLogger("NeoLogger")


def pytest_generate_tests(metafunc):
    if "s3_client" in metafunc.fixturenames:
        metafunc.parametrize("s3_client", ["boto3"], indirect=True)


class TestS3Presigned(TestNeofsS3Base):
    @allure.title("Test S3: Get Object With Presigned Url")
    @pytest.mark.parametrize("url_from", ["s3"])
    def test_s3_get_object_with_presigned_url(self, bucket, simple_object_size, url_from: str):
        file_path = generate_file(simple_object_size)
        file_names_to_check = [
            "temp_file_12345",
            "%40hashed/4e/07/temp_file_12345",
            "\\inter\\stingfile",
            ">cool<>name<",
            "&cool&&name&",
            "cool\\/name\\",
            "||cool||name||",
            ":cool::name",
            "@cool@name@",
            ";cool;name;",
            ";cool,name!",
        ]

        if self.neofs_env.get_binary_version(self.neofs_env.neofs_s3_gw_path) <= "0.33.0":
            file_names_to_check = ["temp_file_12345"]

        for file_name in file_names_to_check:
            with allure.step("Put object into Bucket"):
                s3_object.put_object_s3(self.s3_client, bucket, file_path, file_name)

            with allure.step(f"Get presigned URL from {url_from}"):
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
