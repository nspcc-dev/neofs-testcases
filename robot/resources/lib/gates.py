#!/usr/bin/python3

import logging
import os
import re
import requests
import shutil
import subprocess
import boto3
import uuid
import io

from robot.api.deco import keyword
from robot.api import logger
import robot.errors
from robot.libraries.BuiltIn import BuiltIn

from common import *


ROBOT_AUTO_KEYWORDS = False

CDNAUTH_EXEC = os.getenv('CDNAUTH_EXEC', 'cdn-authmate')

@keyword('Init S3 Credentials')
def init_s3_credentials(private_key: str, s3_key):
    bucket = str(uuid.uuid4())
    Cmd = (
        f'{CDNAUTH_EXEC} --debug --with-log issue-secret --neofs-key {private_key} '
        f'--gate-public-key={s3_key} --peer {NEOFS_ENDPOINT} '
        f'--container-friendly-name {bucket}'
    )
    logger.info("Cmd: %s" % Cmd)
    try:
        complProc = subprocess.run(Cmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=360, shell=True)
        output = complProc.stdout
        logger.info("Output: %s" % output)

        m = re.search(r'"cid":\s+"(\w+)"', output)
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
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))


@keyword('Config S3 client')
def config_s3_client(access_key_id, secret_access_key):
    session = boto3.session.Session()

    s3_client = session.client(
        service_name='s3',
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        endpoint_url=S3_GATE, verify=False
    )

    return s3_client


@keyword('List objects S3 v2')
def list_objects_s3_v2(s3_client, bucket):
    response = s3_client.list_objects_v2(Bucket=bucket)
    logger.info("S3 v2 List objects result: %s" % response['Contents'])
    obj_list = []
    for obj in response['Contents']:
        obj_list.append(obj['Key'])
    logger.info("Found s3 objects: %s" % obj_list)
    return obj_list


@keyword('List objects S3')
def list_objects_s3(s3_client, bucket):
    response = s3_client.list_objects(Bucket=bucket)
    logger.info("S3 List objects result: %s" % response['Contents'])
    obj_list = []
    for obj in response['Contents']:
        obj_list.append(obj['Key'])
    logger.info("Found s3 objects: %s" % obj_list)
    return obj_list


@keyword('List buckets S3')
def list_buckets_s3(s3_client):
    found_buckets = []
    response = s3_client.list_buckets()
    logger.info("S3 List buckets result: %s" % response)

    for bucket in response['Buckets']:
        found_buckets.append(bucket['Name'])

    return found_buckets


@keyword('Put object S3')
def put_object_s3(s3_client, bucket, filepath):
    filename = os.path.basename(filepath)

    with open(filepath, "rb") as f:
        fileContent = f.read()

    response = s3_client.put_object(Body=fileContent, Bucket=bucket, Key=filename)
    logger.info("S3 Put object result: %s" % response)
    return response


@keyword('Head object S3')
def head_object_s3(s3_client, bucket, object_key):
    response = s3_client.head_object(Bucket=bucket, Key=object_key)
    logger.info("S3 Head object result: %s" % response)
    return response


@keyword('Delete object S3')
def delete_object_s3(s3_client, bucket, object_key):

    response = s3_client.delete_object(Bucket=bucket, Key=object_key)
    logger.info("S3 Put object result: %s" % response)
    return response


@keyword('Copy object S3')
def copy_object_s3(s3_client, bucket, object_key, new_object):

    response = s3_client.copy_object(Bucket=bucket, CopySource=bucket+"/"+object_key, Key=new_object)
    logger.info("S3 Copy object result: %s" % response)
    return response


@keyword('Get object S3')
def get_object_s3(s3_client, bucket, object_key, target_file):
    response = s3_client.get_object(Bucket=bucket, Key=object_key)

    with open(f"{target_file}", 'wb') as f:
        chunk = response['Body'].read(1024)
        while chunk:
            f.write(chunk)
            chunk = response['Body'].read(1024)

    return target_file


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
