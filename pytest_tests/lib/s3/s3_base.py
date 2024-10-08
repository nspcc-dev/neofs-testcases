import json
import logging
import os
import random
import re
import string
import sys
import uuid
from typing import Any, Optional

import allure
import boto3
import pexpect
import pytest
import urllib3
from botocore.config import Config
from botocore.exceptions import ClientError
from helpers.aws_cli_client import AwsCliClient
from helpers.cli_helpers import _cmd_run, _configure_aws_cli
from neofs_env.neofs_env_test_base import NeofsEnvTestBase
from neofs_testlib.env.env import NeoFSEnv, NodeWallet
from neofs_testlib.shell import Shell
from neofs_testlib.utils.wallet import get_last_public_key_from_wallet
from pytest import FixtureRequest
from s3 import s3_bucket, s3_object

# Disable warnings on self-signed certificate which the
# boto library produces on requests to S3-gate in dev-env
urllib3.disable_warnings()

logger = logging.getLogger("NeoLogger")
CREDENTIALS_CREATE_TIMEOUT = "1m"

# Number of attempts that S3 clients will attempt per each request (1 means single attempt
# without any retries)
MAX_REQUEST_ATTEMPTS = 1
RETRY_MODE = "standard"


def _run_with_passwd(cmd: str, password: str) -> str:
    child = pexpect.spawn(cmd)
    child.delaybeforesend = 1
    child.expect(".*")
    child.sendline(f"{password}\r")
    if sys.platform == "darwin":
        child.expect(pexpect.EOF)
        cmd = child.before
    else:
        child.wait()
        cmd = child.read()
    return cmd.decode()


class TestNeofsS3Base(NeofsEnvTestBase):
    s3_client: Any = None  # noqa
    access_key_id: str = None
    secret_access_key: str = None

    @pytest.fixture(scope="class", autouse=True)
    @allure.title("[Class/Autouse]: Create S3 client")
    def s3_client(  # noqa
        self,
        default_wallet: NodeWallet,
        client_shell: Shell,
        request: FixtureRequest,
        neofs_env: NeoFSEnv,
    ) -> Any:
        wallet = default_wallet
        s3_bearer_rules_file = f"{os.getcwd()}/pytest_tests/data/s3_bearer_rules.json"
        policy = None if isinstance(request.param, str) else request.param[1]
        (
            cid,
            bucket,
            access_key_id,
            secret_access_key,
            owner_private_key,
        ) = init_s3_credentials(wallet, neofs_env, s3_bearer_rules_file=s3_bearer_rules_file, policy=policy)

        cli = neofs_env.neofs_cli(neofs_env.generate_cli_config(wallet))
        result = cli.container.list(rpc_endpoint=neofs_env.sn_rpc, wallet=wallet.path)
        containers_list = result.stdout.split()
        assert cid in containers_list, f"Expected cid {cid} in {containers_list}"

        if "aws cli" in request.param:
            client = configure_cli_client(access_key_id, secret_access_key, f"https://{neofs_env.s3_gw.address}")
        else:
            client = configure_boto3_client(access_key_id, secret_access_key, f"https://{neofs_env.s3_gw.address}")
        TestNeofsS3Base.s3_client = client
        TestNeofsS3Base.wallet = wallet
        TestNeofsS3Base.access_key_id = access_key_id
        TestNeofsS3Base.secret_access_key = secret_access_key

    @pytest.fixture
    @allure.title("Create/delete bucket")
    def bucket(self):
        bucket = s3_bucket.create_bucket_s3(self.s3_client, bucket_configuration="rep-1")
        yield bucket
        self.delete_all_object_in_bucket(bucket)

    @pytest.fixture
    @allure.title("Create two buckets")
    def two_buckets(self):
        bucket_1 = s3_bucket.create_bucket_s3(self.s3_client, bucket_configuration="rep-1")
        bucket_2 = s3_bucket.create_bucket_s3(self.s3_client, bucket_configuration="rep-1")
        yield bucket_1, bucket_2
        for bucket in [bucket_1, bucket_2]:
            self.delete_all_object_in_bucket(bucket)

    def delete_all_object_in_bucket(self, bucket):
        versioning_status = s3_bucket.get_bucket_versioning_status(self.s3_client, bucket)
        if versioning_status == s3_bucket.VersioningStatus.ENABLED.value:
            # From versioned bucket we should delete all versions and delete markers of all objects
            objects_versions = s3_object.list_objects_versions_s3(self.s3_client, bucket)
            if objects_versions:
                s3_object.delete_object_versions_s3_without_dm(self.s3_client, bucket, objects_versions)
            objects_delete_markers = s3_object.list_objects_delete_markers_s3(self.s3_client, bucket)
            if objects_delete_markers:
                s3_object.delete_object_versions_s3_without_dm(self.s3_client, bucket, objects_delete_markers)

        else:
            # From non-versioned bucket it's sufficient to delete objects by key
            objects = s3_object.list_objects_s3(self.s3_client, bucket)
            if objects:
                s3_object.delete_objects_s3(self.s3_client, bucket, objects)
            objects_delete_markers = s3_object.list_objects_delete_markers_s3(self.s3_client, bucket)
            if objects_delete_markers:
                s3_object.delete_object_versions_s3_without_dm(self.s3_client, bucket, objects_delete_markers)

        # Delete the bucket itself
        s3_bucket.delete_bucket_s3(self.s3_client, bucket)


@allure.step("Init S3 Credentials")
def init_s3_credentials(
    wallet: NodeWallet,
    neofs_env: NeoFSEnv,
    s3_bearer_rules_file: Optional[str] = None,
    policy: Optional[dict] = None,
) -> tuple:
    bucket = str(uuid.uuid4())
    s3_bearer_rules = s3_bearer_rules_file or "pytest_tests/data/s3_bearer_rules.json"
    policy = policy or "pytest_tests/data/container_policy.json"

    gate_public_key = get_last_public_key_from_wallet(neofs_env.s3_gw.wallet.path, neofs_env.s3_gw.wallet.password)
    cmd = (
        f"{neofs_env.neofs_s3_authmate_path} --debug --with-log --timeout 1m "
        f"issue-secret --wallet {wallet.path} --gate-public-key={gate_public_key} "
        f"--peer {neofs_env.storage_nodes[0].endpoint} --container-friendly-name {bucket} "
        f"--bearer-rules {s3_bearer_rules} --container-placement-policy 'REP 1' "
        f"--container-policy {policy}"
    )

    logger.info(f"Executing command: {cmd}")

    try:
        output = _run_with_passwd(cmd, wallet.password)

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
def configure_boto3_client(access_key_id: str, secret_access_key: str, s3gate_endpoint: str):
    try:
        session = boto3.Session()
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
            endpoint_url=s3gate_endpoint,
            verify=False,
        )
        return s3_client
    except ClientError as err:
        raise Exception(
            f'Error Message: {err.response["Error"]["Message"]}\n'
            f'Http status code: {err.response["ResponseMetadata"]["HTTPStatusCode"]}'
        ) from err


@allure.step("Configure S3 client (aws cli)")
def configure_cli_client(access_key_id: str, secret_access_key: str, s3gate_endpoint: str):
    try:
        profile = _generate_random_profile()
        client = AwsCliClient(s3gate_endpoint, profile)
        _configure_aws_cli(f"aws configure --profile {profile}", access_key_id, secret_access_key)
        _cmd_run(f"aws configure set max_attempts {MAX_REQUEST_ATTEMPTS}")
        _cmd_run(f"aws configure set retry_mode {RETRY_MODE}")
        return client
    except Exception as err:
        if "command was not found or was not executable" in str(err):
            pytest.skip("AWS CLI was not found")
        else:
            raise RuntimeError("Error while configuring AwsCliClient") from err


def _generate_random_profile():
    random_postfix = "".join(random.choice(string.ascii_letters + string.digits) for _ in range(10))
    return f"profile__{random_postfix}"
