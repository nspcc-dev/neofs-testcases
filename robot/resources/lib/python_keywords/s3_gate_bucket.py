#!/usr/bin/python3

import json
import os
import re
import uuid
from enum import Enum
from time import sleep

import boto3
from data_formatters import pub_key_hex
from botocore.exceptions import ClientError
import urllib3
from robot.api import logger
from robot.api.deco import keyword

from cli_helpers import _run_with_passwd, log_command_execution
from common import NEOFS_ENDPOINT, S3_GATE, S3_GATE_WALLET_PATH, S3_GATE_WALLET_PASS

##########################################################
# Disabling warnings on self-signed certificate which the
# boto library produces on requests to S3-gate in dev-env.
urllib3.disable_warnings()
##########################################################

ROBOT_AUTO_KEYWORDS = False
CREDENTIALS_CREATE_TIMEOUT = '30s'
NEOFS_EXEC = os.getenv('NEOFS_EXEC', 'neofs-authmate')

# Artificial delay that we add after object deletion and container creation
# Delay is added because sometimes immediately after deletion object still appears
# to be existing (probably because tombstone object takes some time to replicate)
S3_SYNC_WAIT_TIME = 5

class VersioningStatus(Enum):
    ENABLED = 'Enabled'
    SUSPENDED = 'Suspended'


@keyword('Init S3 Credentials')
def init_s3_credentials(wallet_path, s3_bearer_rules_file: str = None):
    bucket = str(uuid.uuid4())
    s3_bearer_rules = s3_bearer_rules_file or 'robot/resources/files/s3_bearer_rules.json'
    gate_pub_key = pub_key_hex(S3_GATE_WALLET_PATH, S3_GATE_WALLET_PASS)
    cmd = (
        f'{NEOFS_EXEC} --debug --with-log --timeout {CREDENTIALS_CREATE_TIMEOUT} '
        f'issue-secret --wallet {wallet_path} --gate-public-key={gate_pub_key} '
        f'--peer {NEOFS_ENDPOINT} --container-friendly-name {bucket} '
        f'--bearer-rules {s3_bearer_rules}'
    )
    logger.info(f'Executing command: {cmd}')

    try:
        output = _run_with_passwd(cmd)
        logger.info(f'Command completed with output: {output}')

        # output contains some debug info and then several JSON structures, so we find each
        # JSON structure by curly brackets (naive approach, but works while JSON is not nested)
        # and then we take JSON containing secret_access_key
        json_blocks = re.findall(r'\{.*?\}', output, re.DOTALL)
        for json_block in json_blocks:
            try:
                parsed_json_block = json.loads(json_block)
                if 'secret_access_key' in parsed_json_block:
                    return (
                        parsed_json_block['container_id'],
                        bucket,
                        parsed_json_block['access_key_id'],
                        parsed_json_block['secret_access_key'],
                        parsed_json_block['owner_private_key']
                    )
            except json.JSONDecodeError:
                raise AssertionError(f'Could not parse info from output\n{output}')
        raise AssertionError(f'Could not find AWS credentials in output:\n{output}')

    except Exception as exc:
        raise RuntimeError(f'Failed to init s3 credentials because of error\n{exc}') from exc


@keyword('Config S3 client')
def config_s3_client(access_key_id: str, secret_access_key: str):
    try:

        session = boto3.session.Session()

        s3_client = session.client(
            service_name='s3',
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            endpoint_url=S3_GATE, verify=False
        )

        return s3_client

    except ClientError as err:
        raise Exception(f'Error Message: {err.response["Error"]["Message"]}\n'
                        f'Http status code: {err.response["ResponseMetadata"]["HTTPStatusCode"]}') from err


@keyword('Create bucket S3')
def create_bucket_s3(s3_client):
    bucket_name = str(uuid.uuid4())

    try:
        s3_bucket = s3_client.create_bucket(Bucket=bucket_name)
        log_command_execution(f'Created S3 bucket {bucket_name}', s3_bucket)
        sleep(S3_SYNC_WAIT_TIME)
        return bucket_name

    except ClientError as err:
        raise Exception(f'Error Message: {err.response["Error"]["Message"]}\n'
                        f'Http status code: {err.response["ResponseMetadata"]["HTTPStatusCode"]}') from err


@keyword('List buckets S3')
def list_buckets_s3(s3_client):
    found_buckets = []
    try:
        response = s3_client.list_buckets()
        log_command_execution('S3 List buckets result', response)

        for bucket in response['Buckets']:
            found_buckets.append(bucket['Name'])

        return found_buckets

    except ClientError as err:
        raise Exception(f'Error Message: {err.response["Error"]["Message"]}\n'
                        f'Http status code: {err.response["ResponseMetadata"]["HTTPStatusCode"]}') from err


@keyword('Delete bucket S3')
def delete_bucket_s3(s3_client, bucket: str):
    try:
        response = s3_client.delete_bucket(Bucket=bucket)
        log_command_execution('S3 Delete bucket result', response)
        sleep(S3_SYNC_WAIT_TIME)
        return response

    except ClientError as err:
        log_command_execution('S3 Delete bucket error', str(err))
        raise Exception(f'Error Message: {err.response["Error"]["Message"]}\n'
                        f'Http status code: {err.response["ResponseMetadata"]["HTTPStatusCode"]}') from err


@keyword('Head bucket S3')
def head_bucket(s3_client, bucket: str):
    try:
        response = s3_client.head_bucket(Bucket=bucket)
        log_command_execution('S3 Head bucket result', response)
        return response

    except ClientError as err:
        log_command_execution('S3 Head bucket error', str(err))
        raise Exception(f'Error Message: {err.response["Error"]["Message"]}\n'
                        f'Http status code: {err.response["ResponseMetadata"]["HTTPStatusCode"]}') from err


@keyword('Set bucket versioning status')
def set_bucket_versioning(s3_client, bucket_name: str, status: VersioningStatus):
    try:
        response = s3_client.put_bucket_versioning(Bucket=bucket_name, VersioningConfiguration={'Status': status.value})
        log_command_execution('S3 Set bucket versioning to', response)

    except ClientError as err:
        raise Exception(f'Got error during set bucket versioning: {err}') from err


@keyword('Get bucket versioning status')
def get_bucket_versioning_status(s3_client, bucket_name: str) -> str:
    try:
        response = s3_client.get_bucket_versioning(Bucket=bucket_name)
        status = response.get('Status')
        log_command_execution('S3 Got bucket versioning status', response)
        return status
    except ClientError as err:
        raise Exception(f'Got error during get bucket versioning status: {err}') from err


@keyword('Put bucket tagging')
def put_bucket_tagging(s3_client, bucket_name: str, tags: list):
    try:
        tags = [{'Key': tag_key, 'Value': tag_value} for tag_key, tag_value in tags]
        tagging = {'TagSet': tags}
        response = s3_client.put_bucket_tagging(Bucket=bucket_name, Tagging=tagging)
        log_command_execution('S3 Put bucket tagging', response)

    except ClientError as err:
        raise Exception(f'Got error during put bucket tagging: {err}') from err


@keyword('Get bucket tagging')
def get_bucket_tagging(s3_client, bucket_name: str) -> list:
    try:
        response = s3_client.get_bucket_tagging(Bucket=bucket_name)
        log_command_execution('S3 Get bucket tagging', response)
        return response.get('TagSet')

    except ClientError as err:
        raise Exception(f'Got error during get bucket tagging: {err}') from err


@keyword('Delete bucket tagging')
def delete_bucket_tagging(s3_client, bucket_name: str):
    try:
        response = s3_client.delete_bucket_tagging(Bucket=bucket_name)
        log_command_execution('S3 Delete bucket tagging', response)

    except ClientError as err:
        raise Exception(f'Got error during delete bucket tagging: {err}') from err
