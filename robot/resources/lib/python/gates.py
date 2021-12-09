#!/usr/bin/python3.8

import os
import re
import shutil
import subprocess
import uuid
import requests
import botocore
import boto3

from common import *
from robot.api.deco import keyword
from robot.api import logger
from cli_helpers import _run_with_passwd


ROBOT_AUTO_KEYWORDS = False

NEOFS_EXEC = os.getenv('NEOFS_EXEC', 'neofs-authmate')

@keyword('Init S3 Credentials')
def init_s3_credentials(wallet):
    bucket = str(uuid.uuid4())
    records = ' \' {"records":[{"operation":"PUT","action":"ALLOW","filters":[],"targets":[{"role":"OTHERS","keys":[]}]}, {"operation":"SEARCH","action":"ALLOW","filters":[],"targets":[{"role":"OTHERS","keys":[]}]}, {"operation":"GET","action":"ALLOW","filters":[],"targets":[{"role":"OTHERS","keys":[]}]}]} \' '
    Cmd = (
        f'{NEOFS_EXEC} --debug --with-log issue-secret --wallet {wallet} '
        f'--gate-public-key={GATE_PUB_KEY} --peer {NEOFS_ENDPOINT} '
        f'--container-friendly-name {bucket} --create-session-token '
        f'--bearer-rules {records}'
    )
    logger.info(f"Executing command: {Cmd}")

    try:
        output = _run_with_passwd(Cmd)
        logger.info(f"Command completed with output: {output}")

        m = re.search(r'"container_id":\s+"(\w+)"', output)
        cid = m.group(1)
        logger.info("cid: %s" % cid)

        m = re.search(r'"access_key_id":\s+"([\w\/]+)"', output)
        access_key_id = m.group(1)
        logger.info("access_key_id: %s" % access_key_id)

        m = re.search(r'"secret_access_key":\s+"(\w+)"', output)
        secret_access_key = m.group(1)
        logger.info("secret_access_key: %s" % secret_access_key)

        m = re.search(r'"owner_private_key":\s+"(\w+)"', output)
        owner_private_key = m.group(1)
        logger.info("owner_private_key: %s" % owner_private_key)

        return cid, bucket, access_key_id, secret_access_key, owner_private_key

    except subprocess.CalledProcessError as e:
        raise Exception(f"Error: \nreturn code: {e.returncode}. \nOutput: {e.stderr}")


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
        logger.info("S3 v2 List objects result: %s" % response['Contents'])
        obj_list = []
        for obj in response['Contents']:
            obj_list.append(obj['Key'])
        logger.info("Found s3 objects: %s" % obj_list)
        return obj_list

    except botocore.exceptions.ClientError as err:
        raise Exception(f"Error Message: {err.response['Error']['Message']}\n"
        f"Http status code: {err.response['ResponseMetadata']['HTTPStatusCode']}") from err


@keyword('List objects S3')
def list_objects_s3(s3_client, bucket):
    try:
        response = s3_client.list_objects(Bucket=bucket)
        logger.info("S3 List objects result: %s" % response['Contents'])
        obj_list = []
        for obj in response['Contents']:
            obj_list.append(obj['Key'])
        logger.info("Found s3 objects: %s" % obj_list)
        return obj_list

    except botocore.exceptions.ClientError as err:
        raise Exception(f"Error Message: {err.response['Error']['Message']}\n"
        f"Http status code: {err.response['ResponseMetadata']['HTTPStatusCode']}") from err


@keyword('Create bucket S3')
def create_bucket_s3(s3_client):
    bucket_name = str(uuid.uuid4())

    try:
        s3_bucket = s3_client.create_bucket(Bucket=bucket_name)
        logger.info("Created S3 bucket: %s" % s3_bucket)
        return bucket_name

    except botocore.exceptions.ClientError as err:
        raise Exception(f"Error Message: {err.response['Error']['Message']}\n"
        f"Http status code: {err.response['ResponseMetadata']['HTTPStatusCode']}") from err 


@keyword('List buckets S3')
def list_buckets_s3(s3_client):
    found_buckets = []
    try:
        response = s3_client.list_buckets()
        logger.info("S3 List buckets result: %s" % response)

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


@keyword('HeadBucket S3')
def headbucket(bucket, s3_client):
    try:
        response = s3_client.head_bucket(Bucket=bucket)
        logger.info(f"S3 HeadBucket result: {response}")
        return response

    except botocore.exceptions.ClientError as err:
        raise Exception(f"Error Message: {err.response['Error']['Message']}\n"
        f"Http status code: {err.response['ResponseMetadata']['HTTPStatusCode']}") from err


@keyword('Put object S3')
def put_object_s3(s3_client, bucket, filepath):
    filename = os.path.basename(filepath)

    with open(filepath, "rb") as f:
        fileContent = f.read()

    try:
        response = s3_client.put_object(Body=fileContent, Bucket=bucket, Key=filename)
        logger.info("S3 Put object result: %s" % response)
    except botocore.exceptions.ClientError as err:
        raise Exception(f"Error Message: {err.response['Error']['Message']}\n"
        f"Http status code: {err.response['ResponseMetadata']['HTTPStatusCode']}") from err


@keyword('Head object S3')
def head_object_s3(s3_client, bucket, object_key):

    try:
        response = s3_client.head_object(Bucket=bucket, Key=object_key)
        logger.info("S3 Head object result: %s" % response)
        return response

    except botocore.exceptions.ClientError as err:
        raise Exception(f"Error Message: {err.response['Error']['Message']}\n"
        f"Http status code: {err.response['ResponseMetadata']['HTTPStatusCode']}") from err   


@keyword('Delete object S3')
def delete_object_s3(s3_client, bucket, object_key):
    try:
        response = s3_client.delete_object(Bucket=bucket, Key=object_key)
        logger.info("S3 Put object result: %s" % response)
        return response

    except botocore.exceptions.ClientError as err:
        raise Exception(f"Error Message: {err.response['Error']['Message']}\n"
        f"Http status code: {err.response['ResponseMetadata']['HTTPStatusCode']}") from err


@keyword('Copy object S3')
def copy_object_s3(s3_client, bucket, object_key, new_object):
    try:
        response = s3_client.copy_object(Bucket=bucket, CopySource=bucket+"/"+object_key, Key=new_object)
        logger.info("S3 Copy object result: %s" % response)
        return response

    except botocore.exceptions.ClientError as err:
        raise Exception(f"Error Message: {err.response['Error']['Message']}\n"
        f"Http status code: {err.response['ResponseMetadata']['HTTPStatusCode']}") from err


@keyword('Get object S3')
def get_object_s3(s3_client, bucket, object_key, target_file):
    try:
        response = s3_client.get_object(Bucket=bucket, Key=object_key)

        with open(f"{target_file}", 'wb') as f:
            chunk = response['Body'].read(1024)
            while chunk:
                f.write(chunk)
                chunk = response['Body'].read(1024)

        return target_file

    except botocore.exceptions.ClientError as err:
        raise Exception(f"Error Message: {err.response['Error']['Message']}\n"
        f"Http status code: {err.response['ResponseMetadata']['HTTPStatusCode']}") from err


@keyword('Get via HTTP Gate')
def get_via_http_gate(cid: str, oid: str):
    """
    This function gets given object from HTTP gate
    :param cid:      CID to get object from
    :param oid:      object OID
    """
    request = f'{HTTP_GATE}/get/{cid}/{oid}'
    resp = requests.get(request, stream=True)

    if not resp.ok:
        raise Exception(f"""Failed to get object via HTTP gate:
                request: {resp.request.path_url},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}""")
        return

    logger.info(f'Request: {request}')
    filename = os.path.curdir + f"/{cid}_{oid}"
    with open(filename, "wb") as f:
        shutil.copyfileobj(resp.raw, f)
    del resp
    return filename

