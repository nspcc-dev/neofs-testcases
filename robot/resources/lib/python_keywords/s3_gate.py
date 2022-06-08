#!/usr/bin/python3

import json
import os
import uuid

import boto3
import botocore
import urllib3
from cli_helpers import _run_with_passwd
from common import GATE_PUB_KEY, NEOFS_ENDPOINT, S3_GATE
from robot.api import logger
from robot.api.deco import keyword

##########################################################
# Disabling warnings on self-signed certificate which the
# boto library produces on requests to S3-gate in dev-env.
urllib3.disable_warnings()
##########################################################

ROBOT_AUTO_KEYWORDS = False
CREDENTIALS_CREATE_TIMEOUT = '30s'

NEOFS_EXEC = os.getenv('NEOFS_EXEC', 'neofs-authmate')
ASSETS_DIR = os.getenv("ASSETS_DIR", "TemporaryDir/")


@keyword('Init S3 Credentials')
def init_s3_credentials(wallet, s3_bearer_rules_file: str = None):
    bucket = str(uuid.uuid4())
    s3_bearer_rules = s3_bearer_rules_file or "robot/resources/files/s3_bearer_rules.json"
    cmd = (
        f'{NEOFS_EXEC} --debug --with-log --timeout {CREDENTIALS_CREATE_TIMEOUT} '
        f'issue-secret --wallet {wallet} --gate-public-key={GATE_PUB_KEY} '
        f'--peer {NEOFS_ENDPOINT} --container-friendly-name {bucket} '
        f'--bearer-rules {s3_bearer_rules}'
    )
    logger.info(f"Executing command: {cmd}")

    try:
        output = _run_with_passwd(cmd)
        logger.info(f"Command completed with output: {output}")
        # first five string are log output, cutting them off and parse
        # the rest of the output as JSON
        output = '\n'.join(output.split('\n')[5:])
        output_dict = json.loads(output)

        return (output_dict['container_id'],
                bucket,
                output_dict['access_key_id'],
                output_dict['secret_access_key'],
                output_dict['owner_private_key'])

    except Exception as exc:
        raise RuntimeError(f"Failed to init s3 credentials because of error\n{exc}") from exc


@keyword('Config S3 client')
def config_s3_client(access_key_id, secret_access_key):
    try:

        session = boto3.session.Session()

        s3_client = session.client(
            service_name='s3',
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            endpoint_url=S3_GATE, verify=False
        )

        return s3_client

    except botocore.exceptions.ClientError as err:
        raise Exception(f"Error Message: {err.response['Error']['Message']}\n"
                        f"Http status code: {err.response['ResponseMetadata']['HTTPStatusCode']}") from err


@keyword('List objects S3 v2')
def list_objects_s3_v2(s3_client, bucket):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket)
        content = response.get('Contents', [])
        logger.info(f"S3 v2 List objects result: {response['Contents'] if content else response}")
        obj_list = []
        for obj in content:
            obj_list.append(obj['Key'])
        logger.info(f"Found s3 objects: {obj_list}")
        return obj_list

    except botocore.exceptions.ClientError as err:
        raise Exception(f"Error Message: {err.response['Error']['Message']}\n"
                        f"Http status code: {err.response['ResponseMetadata']['HTTPStatusCode']}") from err


@keyword('List objects S3')
def list_objects_s3(s3_client, bucket):
    try:
        response = s3_client.list_objects(Bucket=bucket)
        content = response.get('Contents', [])
        logger.info(f"S3 List objects result: {content}")
        obj_list = []
        for obj in content:
            obj_list.append(obj['Key'])
        logger.info(f"Found s3 objects: {obj_list}")
        return obj_list

    except botocore.exceptions.ClientError as err:
        raise Exception(f"Error Message: {err.response['Error']['Message']}\n"
                        f"Http status code: {err.response['ResponseMetadata']['HTTPStatusCode']}") from err


@keyword('Create bucket S3')
def create_bucket_s3(s3_client):
    bucket_name = str(uuid.uuid4())

    try:
        s3_bucket = s3_client.create_bucket(Bucket=bucket_name)
        logger.info(f"Created S3 bucket: {s3_bucket}")
        return bucket_name

    except botocore.exceptions.ClientError as err:
        raise Exception(f"Error Message: {err.response['Error']['Message']}\n"
                        f"Http status code: {err.response['ResponseMetadata']['HTTPStatusCode']}") from err


@keyword('List buckets S3')
def list_buckets_s3(s3_client):
    found_buckets = []
    try:
        response = s3_client.list_buckets()
        logger.info(f"S3 List buckets result: {response}")

        for bucket in response['Buckets']:
            found_buckets.append(bucket['Name'])

        return found_buckets

    except botocore.exceptions.ClientError as err:
        raise Exception(f"Error Message: {err.response['Error']['Message']}\n"
                        f"Http status code: {err.response['ResponseMetadata']['HTTPStatusCode']}") from err


@keyword('Delete bucket S3')
def delete_bucket_s3(s3_client, bucket):
    try:
        response = s3_client.delete_bucket(Bucket=bucket)
        logger.info(f"S3 Delete bucket result: {response}")

        return response

    except botocore.exceptions.ClientError as err:
        raise Exception(f"Error Message: {err.response['Error']['Message']}\n"
                        f"Http status code: {err.response['ResponseMetadata']['HTTPStatusCode']}") from err


@keyword('Head bucket S3')
def head_bucket(s3_client, bucket):
    try:
        response = s3_client.head_bucket(Bucket=bucket)
        logger.info(f"S3 Head bucket result: {response}")
        return response

    except botocore.exceptions.ClientError as err:
        raise Exception(f"Error Message: {err.response['Error']['Message']}\n"
                        f"Http status code: {err.response['ResponseMetadata']['HTTPStatusCode']}") from err


@keyword('Put object S3')
def put_object_s3(s3_client, bucket, filepath):
    filename = os.path.basename(filepath)

    with open(filepath, "rb") as put_file:
        file_content = put_file.read()

    try:
        response = s3_client.put_object(Body=file_content, Bucket=bucket, Key=filename)
        logger.info(f"S3 Put object result: {response}")
    except botocore.exceptions.ClientError as err:
        raise Exception(f"Error Message: {err.response['Error']['Message']}\n"
                        f"Http status code: {err.response['ResponseMetadata']['HTTPStatusCode']}") from err


@keyword('Head object S3')
def head_object_s3(s3_client, bucket, object_key):
    try:
        response = s3_client.head_object(Bucket=bucket, Key=object_key)
        logger.info(f"S3 Head object result: {response}")
        return response

    except botocore.exceptions.ClientError as err:
        raise Exception(f"Error Message: {err.response['Error']['Message']}\n"
                        f"Http status code: {err.response['ResponseMetadata']['HTTPStatusCode']}") from err


@keyword('Delete object S3')
def delete_object_s3(s3_client, bucket, object_key):
    try:
        response = s3_client.delete_object(Bucket=bucket, Key=object_key)
        logger.info(f"S3 Delete object result: {response}")
        return response

    except botocore.exceptions.ClientError as err:
        raise Exception(f"Error Message: {err.response['Error']['Message']}\n"
                        f"Http status code: {err.response['ResponseMetadata']['HTTPStatusCode']}") from err


@keyword('Delete objects S3')
def delete_objects_s3(s3_client, bucket: str, object_keys: list):
    try:
        response = s3_client.delete_objects(Bucket=bucket, Delete=_make_objs_dict(object_keys))
        logger.info(f"S3 Delete objects result: {response}")
        return response

    except botocore.exceptions.ClientError as err:
        raise Exception(f"Error Message: {err.response['Error']['Message']}\n"
                        f"Http status code: {err.response['ResponseMetadata']['HTTPStatusCode']}") from err


@keyword('Copy object S3')
def copy_object_s3(s3_client, bucket, object_key, bucket_dst=None):
    filename = f"{os.getcwd()}/{uuid.uuid4()}"
    try:
        response = s3_client.copy_object(Bucket=bucket_dst or bucket,
                                         CopySource=f"{bucket}/{object_key}",
                                         Key=filename)
        logger.info(f"S3 Copy object result: {response}")
        return filename

    except botocore.exceptions.ClientError as err:
        raise Exception(f"Error Message: {err.response['Error']['Message']}\n"
                        f"Http status code: {err.response['ResponseMetadata']['HTTPStatusCode']}") from err


@keyword('Get object S3')
def get_object_s3(s3_client, bucket, object_key):
    filename = f"{ASSETS_DIR}/{uuid.uuid4()}"
    try:
        response = s3_client.get_object(Bucket=bucket, Key=object_key)

        with open(f"{filename}", 'wb') as get_file:
            chunk = response['Body'].read(1024)
            while chunk:
                get_file.write(chunk)
                chunk = response['Body'].read(1024)

        return filename

    except botocore.exceptions.ClientError as err:
        raise Exception(f"Error Message: {err.response['Error']['Message']}\n"
                        f"Http status code: {err.response['ResponseMetadata']['HTTPStatusCode']}") from err


def _make_objs_dict(key_names):
    objs_list = []
    for key in key_names:
        obj_dict = {'Key': key}
        objs_list.append(obj_dict)
    objs_dict = {'Objects': objs_list}
    return objs_dict
