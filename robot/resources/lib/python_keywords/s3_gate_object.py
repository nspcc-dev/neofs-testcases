#!/usr/bin/python3

import os
import uuid
from enum import Enum
from typing import Optional, List

from botocore.exceptions import ClientError
import urllib3
from robot.api import logger
from robot.api.deco import keyword

##########################################################
# Disabling warnings on self-signed certificate which the
# boto library produces on requests to S3-gate in dev-env.
urllib3.disable_warnings()
##########################################################

ROBOT_AUTO_KEYWORDS = False
CREDENTIALS_CREATE_TIMEOUT = '30s'

ASSETS_DIR = os.getenv('ASSETS_DIR', 'TemporaryDir/')


class VersioningStatus(Enum):
    ENABLED = 'Enabled'
    SUSPENDED = 'Suspended'


@keyword('List objects S3 v2')
def list_objects_s3_v2(s3_client, bucket: str) -> list:
    try:
        response = s3_client.list_objects_v2(Bucket=bucket)
        content = response.get('Contents', [])
        logger.info(f'S3 v2 List objects result: {response["Contents"] if content else response}')
        obj_list = []
        for obj in content:
            obj_list.append(obj['Key'])
        logger.info(f'Found s3 objects: {obj_list}')
        return obj_list

    except ClientError as err:
        raise Exception(f'Error Message: {err.response["Error"]["Message"]}\n'
                        f'Http status code: {err.response["ResponseMetadata"]["HTTPStatusCode"]}') from err


@keyword('List objects S3')
def list_objects_s3(s3_client, bucket: str) -> list:
    try:
        response = s3_client.list_objects(Bucket=bucket)
        content = response.get('Contents', [])
        logger.info(f'S3 List objects result: {content}')
        obj_list = []
        for obj in content:
            obj_list.append(obj['Key'])
        logger.info(f'Found s3 objects: {obj_list}')
        return obj_list

    except ClientError as err:
        raise Exception(f'Error Message: {err.response["Error"]["Message"]}\n'
                        f'Http status code: {err.response["ResponseMetadata"]["HTTPStatusCode"]}') from err


@keyword('List objects versions S3')
def list_objects_versions_s3(s3_client, bucket: str) -> list:
    try:
        response = s3_client.list_object_versions(Bucket=bucket)
        versions = response.get('Versions', [])
        logger.info(f'S3 List objects versions result: {versions}')
        return versions

    except ClientError as err:
        raise Exception(f'Error Message: {err.response["Error"]["Message"]}\n'
                        f'Http status code: {err.response["ResponseMetadata"]["HTTPStatusCode"]}') from err


@keyword('Put object S3')
def put_object_s3(s3_client, bucket: str, filepath: str):
    filename = os.path.basename(filepath)

    with open(filepath, 'rb') as put_file:
        file_content = put_file.read()

    try:
        response = s3_client.put_object(Body=file_content, Bucket=bucket, Key=filename)
        logger.info(f'S3 Put object result: {response}')
        return response.get('VersionId')
    except ClientError as err:
        raise Exception(f'Error Message: {err.response["Error"]["Message"]}\n'
                        f'Http status code: {err.response["ResponseMetadata"]["HTTPStatusCode"]}') from err


@keyword('Head object S3')
def head_object_s3(s3_client, bucket: str, object_key: str, version_id: str = None):
    try:
        params = {'Bucket': bucket, 'Key': object_key}
        if version_id:
            params['VersionId'] = version_id
        response = s3_client.head_object(**params)
        logger.info(f'S3 Head object result: {response}')
        return response

    except ClientError as err:
        raise Exception(f'Error Message: {err.response["Error"]["Message"]}\n'
                        f'Http status code: {err.response["ResponseMetadata"]["HTTPStatusCode"]}') from err


@keyword('Delete object S3')
def delete_object_s3(s3_client, bucket, object_key, version_id: str = None):
    try:
        params = {'Bucket': bucket, 'Key': object_key}
        if version_id:
            params['VersionId'] = version_id
        response = s3_client.delete_object(**params)
        logger.info(f'S3 Delete object result: {response}')
        return response

    except ClientError as err:
        raise Exception(f'Error Message: {err.response["Error"]["Message"]}\n'
                        f'Http status code: {err.response["ResponseMetadata"]["HTTPStatusCode"]}') from err


@keyword('Delete objects S3')
def delete_objects_s3(s3_client, bucket: str, object_keys: list):
    try:
        response = s3_client.delete_objects(Bucket=bucket, Delete=_make_objs_dict(object_keys))
        logger.info(f'S3 Delete objects result: {response}')
        return response

    except ClientError as err:
        raise Exception(f'Error Message: {err.response["Error"]["Message"]}\n'
                        f'Http status code: {err.response["ResponseMetadata"]["HTTPStatusCode"]}') from err


@keyword('Copy object S3')
def copy_object_s3(s3_client, bucket, object_key, bucket_dst=None):
    filename = f'{os.getcwd()}/{uuid.uuid4()}'
    try:
        response = s3_client.copy_object(Bucket=bucket_dst or bucket,
                                         CopySource=f'{bucket}/{object_key}',
                                         Key=filename)
        logger.info(f'S3 Copy object result: {response}')
        return filename

    except ClientError as err:
        raise Exception(f'Error Message: {err.response["Error"]["Message"]}\n'
                        f'Http status code: {err.response["ResponseMetadata"]["HTTPStatusCode"]}') from err


@keyword('Get object S3')
def get_object_s3(s3_client, bucket: str, object_key: str, version_id: str = None):
    filename = f'{ASSETS_DIR}/{uuid.uuid4()}'
    try:
        params = {'Bucket': bucket, 'Key': object_key}
        if version_id:
            params['VersionId'] = version_id
        response = s3_client.get_object(**params)

        with open(f'{filename}', 'wb') as get_file:
            chunk = response['Body'].read(1024)
            while chunk:
                get_file.write(chunk)
                chunk = response['Body'].read(1024)

        return filename

    except ClientError as err:
        raise Exception(f'Error Message: {err.response["Error"]["Message"]}\n'
                        f'Http status code: {err.response["ResponseMetadata"]["HTTPStatusCode"]}') from err


@keyword('Create multipart upload S3')
def create_multipart_upload_s3(s3_client, bucket_name: str, object_key: str) -> str:
    try:
        response = s3_client.create_multipart_upload(Bucket=bucket_name, Key=object_key)
        logger.info(f'S3 Created multipart upload: {response}')
        assert response.get('UploadId'), f'Expected UploadId in response:\n{response}'

        return response.get('UploadId')

    except ClientError as err:
        raise Exception(f'Error Message: {err.response["Error"]["Message"]}\n'
                        f'Http status code: {err.response["ResponseMetadata"]["HTTPStatusCode"]}') from err


@keyword('List multipart uploads S3')
def list_multipart_uploads_s3(s3_client, bucket_name: str) -> Optional[List[dict]]:
    try:
        response = s3_client.list_multipart_uploads(Bucket=bucket_name)
        logger.info(f'S3 List multipart uploads: {response}')

        return response.get('Uploads')

    except ClientError as err:
        raise Exception(f'Error Message: {err.response["Error"]["Message"]}\n'
                        f'Http status code: {err.response["ResponseMetadata"]["HTTPStatusCode"]}') from err


@keyword('Abort multipart upload S3')
def abort_multipart_uploads_s3(s3_client, bucket_name: str, object_key: str, upload_id: str):
    try:
        response = s3_client.abort_multipart_upload(Bucket=bucket_name, Key=object_key, UploadId=upload_id)
        logger.info(f'S3 Abort multipart uploads: {response}')

    except ClientError as err:
        raise Exception(f'Error Message: {err.response["Error"]["Message"]}\n'
                        f'Http status code: {err.response["ResponseMetadata"]["HTTPStatusCode"]}') from err


@keyword('Upload part S3')
def upload_part_s3(s3_client, bucket_name: str, object_key: str, upload_id: str, part_num: int, filepath: str) -> str:
    with open(filepath, 'rb') as put_file:
        file_content = put_file.read()

    try:
        response = s3_client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=object_key, PartNumber=part_num,
                                         Body=file_content)
        logger.info(f'S3 Upload part: {response}')
        assert response.get('ETag'), f'Expected ETag in response:\n{response}'

        return response.get('ETag')
    except ClientError as err:
        raise Exception(f'Error Message: {err.response["Error"]["Message"]}\n'
                        f'Http status code: {err.response["ResponseMetadata"]["HTTPStatusCode"]}') from err


@keyword('List parts S3')
def list_parts_s3(s3_client, bucket_name: str, object_key: str, upload_id: str) -> List[dict]:
    try:
        response = s3_client.list_parts(UploadId=upload_id, Bucket=bucket_name, Key=object_key)
        logger.info(f'S3 List parts: {response}')
        assert response.get('Parts'), f'Expected Parts in response:\n{response}'

        return response.get('Parts')
    except ClientError as err:
        raise Exception(f'Error Message: {err.response["Error"]["Message"]}\n'
                        f'Http status code: {err.response["ResponseMetadata"]["HTTPStatusCode"]}') from err


@keyword('Complete multipart upload S3')
def complete_multipart_upload_s3(s3_client, bucket_name: str, object_key: str, upload_id: str,
                                 parts: list):
    try:
        parts = [{'ETag': etag, 'PartNumber': part_num} for part_num, etag in parts]
        response = s3_client.complete_multipart_upload(Bucket=bucket_name, Key=object_key, UploadId=upload_id,
                                                       MultipartUpload={'Parts': parts})
        logger.info(f'S3 Complete multipart upload: {response}')

    except ClientError as err:
        raise Exception(f'Error Message: {err.response["Error"]["Message"]}\n'
                        f'Http status code: {err.response["ResponseMetadata"]["HTTPStatusCode"]}') from err


@keyword('Put object tagging')
def put_object_tagging(s3_client, bucket_name: str, object_key: str, tags: list):
    try:
        tags = [{'Key': tag_key, 'Value': tag_value} for tag_key, tag_value in tags]
        tagging = {'TagSet': tags}
        s3_client.put_object_tagging(Bucket=bucket_name, Key=object_key, Tagging=tagging)
        logger.info(f'S3 Put object tagging: {tags}')

    except ClientError as err:
        raise Exception(f'Got error during put object tagging: {err}') from err


@keyword('Get object tagging')
def get_object_tagging(s3_client, bucket_name: str, object_key: str) -> list:
    try:
        response = s3_client.get_object_tagging(Bucket=bucket_name, Key=object_key)
        logger.info(f'S3 Get object tagging: {response}')
        return response.get('TagSet')

    except ClientError as err:
        raise Exception(f'Got error during get object tagging: {err}') from err


@keyword('Delete object tagging')
def delete_object_tagging(s3_client, bucket_name: str, object_key: str):
    try:
        response = s3_client.delete_object_tagging(Bucket=bucket_name, Key=object_key)
        logger.info(f'S3 Delete object tagging: {response}')

    except ClientError as err:
        raise Exception(f'Got error during delete object tagging: {err}') from err


@keyword('Get object attributes')
def get_object_attributes(s3_client, bucket_name: str, object_key: str):
    try:
        response = s3_client.delete_object_tagging(Bucket=bucket_name, Key=object_key)
        logger.info(f'S3 Delete object tagging: {response}')

    except ClientError as err:
        raise Exception(f'Got error during delete object tagging: {err}') from err


def _make_objs_dict(key_names):
    objs_list = []
    for key in key_names:
        obj_dict = {'Key': key}
        objs_list.append(obj_dict)
    objs_dict = {'Objects': objs_list}
    return objs_dict
