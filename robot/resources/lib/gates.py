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


ROBOT_AUTO_KEYWORDS = False

if os.getenv('ROBOT_PROFILE') == 'selectel_smoke':
    from selectelcdn_smoke_vars import (NEOGO_CLI_PREFIX, NEO_MAINNET_ENDPOINT,
    NEOFS_NEO_API_ENDPOINT, NEOFS_ENDPOINT, HTTP_GATE)
else:
    from neofs_int_vars import (NEOGO_CLI_PREFIX, NEO_MAINNET_ENDPOINT,
    NEOFS_NEO_API_ENDPOINT, NEOFS_ENDPOINT, HTTP_GATE, S3_PUBLIC_KEY)


@keyword('Init S3 Credentials')
def init_s3_credentials(private_key: str):
    bucket = str(uuid.uuid4())
    Cmd = f'cdn-authmate --debug --with-log issue-secret --neofs-key ./user.key --gate-public-key=./hcs.pub.key --peer {NEOFS_ENDPOINT} --container-friendly-name {bucket}'
    logger.info("Cmd: %s" % Cmd)
    try:
        complProc = subprocess.run(Cmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=150, shell=True)
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

        
        '''
        s3.neofs.devenv

        "access_key_id": "7vfXNMQQvA4YsLYpnrY7FSRsw4Dof9Q4FQGBa5URFKjp/GYssVA6cNtu5XimidxxXrS6co6oKuKcWP97kqYkMstaD",
        "secret_access_key": "11f2d6dfce835bf68314cdc9b33390e8003dbfae192f65d9f0bb3eb37dc0e85b",
        "owner_private_key": "5e8be9d85f3b0c66c563af52cc73c5763b47fa72a70f437600dff13ca52427a0"
        '''

        return cid, bucket, access_key_id, secret_access_key, owner_private_key

    except subprocess.CalledProcessError as e:
        raise Exception("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))



########## Remove

@keyword('Config S3 resource')
def config_s3_resource(access_key_id, secret_access_key):
    session = boto3.session.Session()

    resource = boto3.resource(
        service_name='s3',
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        endpoint_url='https://s3.neofs.devenv:8080', verify=False
    )

    return s3_resource



@keyword('Config S3 client')
def config_s3_client(access_key_id, secret_access_key):
    session = boto3.session.Session()

    s3_client = session.client(
        service_name='s3',
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        endpoint_url='https://s3.neofs.devenv:8080', verify=False
    )

    return s3_client

# add CopyObject

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
    response = s3_client.list_buckets()
    logger.info("S3 List buckets result: %s" % response)
    return response

# Search?


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



@keyword('Get object S3')
def get_object_s3(s3_client, bucket, object_key, access_key_id, secret_access_key):
    
    #s3 = boto3.resource(
    #    service_name='s3',
    #    aws_access_key_id=access_key_id,
    #    aws_secret_access_key=secret_access_key,
    #    endpoint_url='https://s3.neofs.devenv:8080', verify=False
    #)
    
    #response = s3.Object(bucket_name=bucket, key=object_key).get()
    #body = response['Body'].read()  

    #s3 = boto3.resource("s3")

    #srcFileName=object_key
    #destFileName="s3_abc.txt"
    #bucketName=bucket
    #k = s3.Key(bucket,srcFileName)
    #k.get_contents_to_filename(destFileName)

    #s3.Bucket(bucket).download_file(object_key, 'my_local_image.jpg')


    response = s3_client.get_object(Bucket=bucket, Key=object_key)
    
    #s3_client.download_file(Bucket=bucket, Key=object_key, 'FILE_NAME')
    #logger.info("S3 Head object result: %s" % response)

    #bytes_buffer = io.BytesIO()
    #byte_value = bytes_buffer.getvalue(response['Body'])

    body = response['Body'].read()
#    with open(f"{object_key}", 'wb') as f:
#        #.write(response['Body'])
#        f.write(response['Body'].read())

#    bytes_buffer = io.BytesIO()
#    s3_client.download_fileobj(Bucket=bucket, Key=object_key, Fileobj=bytes_buffer)
#    byte_value = bytes_buffer.getvalue()
#    str_value = byte_value.decode() #python3, default decoding is utf-8

    return # object_key




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
