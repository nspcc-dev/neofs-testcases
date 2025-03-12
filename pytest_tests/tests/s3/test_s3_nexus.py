import json
import logging
import sys
import time

import allure
import pytest
import requests
from neofs_testlib.env.env import NeoFSEnv
from neofs_testlib.shell import CommandOptions
from s3.s3_base import TestNeofsS3Base

logger = logging.getLogger("NeoLogger")

NEXUS_DOCKER_IMAGE = "sonatype/nexus3"
NEXUS_URL = "http://localhost:8081"
NEXUS_DOCKER_CONTAINER_NAME = "nexus"
NEXUS_DOCKER_VOLUME_NAME = "nexus-data"
ADMIN_USER = "admin"


def pytest_generate_tests(metafunc):
    if "s3_client" in metafunc.fixturenames:
        metafunc.parametrize("s3_client", ["boto3"], indirect=True)


def ensure_nexus_running(admin_password: str):
    retries = 20
    for attempt in range(retries):
        try:
            response = requests.get(f"{NEXUS_URL}/service/rest/v1/status", auth=(ADMIN_USER, admin_password))
            if response.status_code == 200:
                logger.info("Nexus is up and running.")
                return True
            logger.info(f"Status Code: {response.status_code}, Response: {response.text}")
        except requests.ConnectionError:
            pass
        logger.info(f"Attempt {attempt + 1}/{retries} failed. Retrying...")
        time.sleep(10)
    logger.info("Nexus is not running after several attempts.")
    return False


def configure_blob_store(
    admin_password: str,
    blob_store_name: str,
    s3_bucket_name: str,
    s3_endpoint: str,
    access_key: str,
    secret_key: str,
):
    logger.info("Configuring S3 blob store...")
    headers = {"Content-Type": "application/json"}
    data = {
        "name": blob_store_name,
        "type": "S3",
        "bucketConfiguration": {
            "bucket": {"region": "DEFAULT", "name": s3_bucket_name, "prefix": "", "expiration": -1},
            "bucketSecurity": {
                "accessKeyId": access_key,
                "secretAccessKey": secret_key,
            },
            "advancedBucketConnection": {"endpoint": s3_endpoint, "signerType": "AWSS3V4SignerType"},
        },
    }
    response = requests.post(
        f"{NEXUS_URL}/service/rest/v1/blobstores/s3",
        auth=(ADMIN_USER, admin_password),
        headers=headers,
        data=json.dumps(data),
    )
    if response.status_code == 204:
        logger.info("S3 blob store configured successfully.")
    else:
        raise AssertionError(
            f"Failed to configure blob store. Status code: {response.status_code}, Response: {response.text}"
        )


def get_nexus_admin_password(neofs_env: NeoFSEnv) -> str:
    nexus_password = ""
    retries = 20
    while retries > 0:
        nexus_password_raw = neofs_env.shell.exec(
            f"docker exec -it {NEXUS_DOCKER_CONTAINER_NAME} cat /nexus-data/admin.password",
            options=CommandOptions(check=False),
        ).stdout.strip()
        if "No such file or directory" not in nexus_password_raw and nexus_password_raw != "":
            nexus_password = nexus_password_raw
            break
        retries -= 1
        time.sleep(10)
    logger.info(f"{nexus_password=}")
    assert nexus_password, "Can not retrieve nexus admin password"
    return nexus_password


class TestS3Nexus(TestNeofsS3Base):
    @pytest.fixture(scope="class", autouse=True)
    def cleanup_nexus(self):
        yield
        nexus_logs_file = "nexus_logs.txt"
        self.neofs_env.shell.exec(f"docker logs {NEXUS_DOCKER_CONTAINER_NAME} > {nexus_logs_file} 2>&1")
        self.neofs_env.shell.exec(f"docker stop {NEXUS_DOCKER_CONTAINER_NAME}")
        self.neofs_env.shell.exec(f"docker rm {NEXUS_DOCKER_CONTAINER_NAME}")
        self.neofs_env.shell.exec(f"docker volume rm {NEXUS_DOCKER_VOLUME_NAME}")
        allure.attach.file(nexus_logs_file, name="nexus logs", extension="txt")

    @allure.title("Test S3: Nexus")
    @pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/1049")
    @pytest.mark.skipif(sys.platform == "darwin", reason="not supported on macos runners")
    def test_s3_nexus_configure_blob_store(self, bucket: str):
        self.neofs_env.shell.exec(
            f"docker run -d --network host --name {NEXUS_DOCKER_CONTAINER_NAME} -v {NEXUS_DOCKER_VOLUME_NAME}:/{NEXUS_DOCKER_VOLUME_NAME} "
            f'-e INSTALL4J_ADD_VM_PARAMS="-Dcom.amazonaws.sdk.disableCertChecking=true" {NEXUS_DOCKER_IMAGE}'
        )
        time.sleep(60)
        nexus_admin_password = get_nexus_admin_password(self.neofs_env)
        assert ensure_nexus_running(nexus_admin_password), "Nexus is not running"
        configure_blob_store(
            admin_password=nexus_admin_password,
            blob_store_name="s3-blob-store",
            s3_bucket_name=bucket,
            s3_endpoint=self.neofs_env.s3_gw.endpoint,
            access_key=self.access_key_id,
            secret_key=self.secret_access_key,
        )
