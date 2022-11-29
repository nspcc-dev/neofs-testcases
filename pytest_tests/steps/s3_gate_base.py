import json
import logging
import os
import re
import uuid
from typing import Optional

import allure
import boto3
import pytest
import urllib3
from botocore.config import Config
from botocore.exceptions import ClientError
from cli_helpers import _cmd_run, _configure_aws_cli, _run_with_passwd
from common import NEOFS_AUTHMATE_EXEC, NEOFS_ENDPOINT, S3_GATE, S3_GATE_WALLET_PATH
from data_formatters import get_wallet_public_key
from neofs_testlib.hosting import Hosting
from neofs_testlib.shell import Shell
from python_keywords.container import list_containers

from steps import s3_gate_bucket, s3_gate_object
from steps.aws_cli_client import AwsCliClient

# Disable warnings on self-signed certificate which the
# boto library produces on requests to S3-gate in dev-env
urllib3.disable_warnings()

logger = logging.getLogger("NeoLogger")
CREDENTIALS_CREATE_TIMEOUT = "1m"

# Number of attempts that S3 clients will attempt per each request (1 means single attempt
# without any retries)
MAX_REQUEST_ATTEMPTS = 1
RETRY_MODE = "standard"


class TestS3GateBase:
    s3_client = None

    @pytest.fixture(scope="class", autouse=True)
    @allure.title("[Class/Autouse]: Create S3 client")
    def s3_client(self, prepare_wallet_and_deposit, client_shell: Shell, request, hosting: Hosting):
        wallet = prepare_wallet_and_deposit
        s3_bearer_rules_file = f"{os.getcwd()}/robot/resources/files/s3_bearer_rules.json"
        policy = None if isinstance(request.param, str) else request.param[1]
        (
            cid,
            bucket,
            access_key_id,
            secret_access_key,
            owner_private_key,
        ) = init_s3_credentials(wallet, hosting, s3_bearer_rules_file=s3_bearer_rules_file)
        containers_list = list_containers(wallet, shell=client_shell)
        assert cid in containers_list, f"Expected cid {cid} in {containers_list}"

        if "aws cli" in request.param:
            client = configure_cli_client(access_key_id, secret_access_key)
        else:
            client = configure_boto3_client(access_key_id, secret_access_key)
        TestS3GateBase.s3_client = client
        TestS3GateBase.wallet = wallet

    @pytest.fixture
    @allure.title("Create/delete bucket")
    def bucket(self):
        bucket = s3_gate_bucket.create_bucket_s3(self.s3_client)
        yield bucket
        self.delete_all_object_in_bucket(bucket)

    @pytest.fixture
    @allure.title("Create two buckets")
    def two_buckets(self):
        bucket_1 = s3_gate_bucket.create_bucket_s3(self.s3_client)
        bucket_2 = s3_gate_bucket.create_bucket_s3(self.s3_client)
        yield bucket_1, bucket_2
        for bucket in [bucket_1, bucket_2]:
            self.delete_all_object_in_bucket(bucket)

    def delete_all_object_in_bucket(self, bucket):
        versioning_status = s3_gate_bucket.get_bucket_versioning_status(self.s3_client, bucket)
        if versioning_status == s3_gate_bucket.VersioningStatus.ENABLED.value:
            # From versioned bucket we should delete all versions of all objects
            objects_versions = s3_gate_object.list_objects_versions_s3(self.s3_client, bucket)
            if objects_versions:
                s3_gate_object.delete_object_versions_s3(self.s3_client, bucket, objects_versions)
        else:
            # From non-versioned bucket it's sufficient to delete objects by key
            objects = s3_gate_object.list_objects_s3(self.s3_client, bucket)
            if objects:
                s3_gate_object.delete_objects_s3(self.s3_client, bucket, objects)

        # Delete the bucket itself
        s3_gate_bucket.delete_bucket_s3(self.s3_client, bucket)


def get_wallet_password(hosting: Hosting, s3_service_name: str) -> str:
    service_config = hosting.get_service_config(s3_service_name)
    return service_config.attributes.get("wallet_password")


@allure.step("Init S3 Credentials")
def init_s3_credentials(
    wallet_path: str,
    hosting: Hosting,
    s3_bearer_rules_file: Optional[str] = None,
    policy: Optional[dict] = None,
    s3_service_name: str = "s3-gate01",
):
    bucket = str(uuid.uuid4())
    s3_bearer_rules = s3_bearer_rules_file or "robot/resources/files/s3_bearer_rules.json"
    s3_password = get_wallet_password(hosting, s3_service_name)
    gate_public_key = get_wallet_public_key(S3_GATE_WALLET_PATH, s3_password)
    cmd = (
        f"{NEOFS_AUTHMATE_EXEC} --debug --with-log --timeout {CREDENTIALS_CREATE_TIMEOUT} "
        f"issue-secret --wallet {wallet_path} --gate-public-key={gate_public_key} "
        f"--peer {NEOFS_ENDPOINT} --container-friendly-name {bucket} "
        f"--bearer-rules {s3_bearer_rules}"
    )
    if policy:
        cmd += f" --container-policy {policy}'"
    logger.info(f"Executing command: {cmd}")

    try:
        output = _run_with_passwd(cmd)
        logger.info(f"Command completed with output: {output}")

        # output contains some debug info and then several JSON structures, so we find each
        # JSON structure by curly brackets (naive approach, but works while JSON is not nested)
        # and then we take JSON containing secret_access_key
        json_blocks = re.findall(r"\{.*?\}", output, re.DOTALL)
        for json_block in json_blocks:
            try:
                parsed_json_block = json.loads(json_block)
                if "secret_access_key" in parsed_json_block:
                    return (
                        parsed_json_block["container_id"],
                        bucket,
                        parsed_json_block["access_key_id"],
                        parsed_json_block["secret_access_key"],
                        parsed_json_block["owner_private_key"],
                    )
            except json.JSONDecodeError:
                raise AssertionError(f"Could not parse info from output\n{output}")
        raise AssertionError(f"Could not find AWS credentials in output:\n{output}")

    except Exception as exc:
        raise RuntimeError(f"Failed to init s3 credentials because of error\n{exc}") from exc


@allure.step("Configure S3 client (boto3)")
def configure_boto3_client(access_key_id: str, secret_access_key: str):
    try:
        session = boto3.session.Session()
        config = Config(
            retries={
                "max_attempts": MAX_REQUEST_ATTEMPTS,
                "mode": RETRY_MODE,
            }
        )

        s3_client = session.client(
            service_name="s3",
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            config=config,
            endpoint_url=S3_GATE,
            verify=False,
        )
        return s3_client
    except ClientError as err:
        raise Exception(
            f'Error Message: {err.response["Error"]["Message"]}\n'
            f'Http status code: {err.response["ResponseMetadata"]["HTTPStatusCode"]}'
        ) from err


@allure.step("Configure S3 client (aws cli)")
def configure_cli_client(access_key_id: str, secret_access_key: str):
    try:
        client = AwsCliClient()
        _configure_aws_cli("aws configure", access_key_id, secret_access_key)
        _cmd_run(f"aws configure set max_attempts {MAX_REQUEST_ATTEMPTS}")
        _cmd_run(f"aws configure set retry_mode {RETRY_MODE}")
        return client
    except Exception as err:
        if "command was not found or was not executable" in str(err):
            pytest.skip("AWS CLI was not found")
        else:
            raise RuntimeError("Error while configuring AwsCliClient") from err
