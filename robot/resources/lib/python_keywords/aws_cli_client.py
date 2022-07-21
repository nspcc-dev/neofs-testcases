import json
import logging
import os

import allure
from cli_helpers import _cmd_run, _configure_aws_cli
from common import ASSETS_DIR, S3_GATE

logger = logging.getLogger('NeoLogger')


class AwsCliClient:

    def __init__(self, access_key_id: str, secret_access_key: str):
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.config_aws_client()

    def config_aws_client(self):
        cmd = 'aws configure'
        logger.info(f'Executing command: {cmd}')
        _configure_aws_cli(cmd, self.access_key_id, self.secret_access_key)

    def create_bucket(self, Bucket: str):
        cmd = f'aws --no-verify-ssl s3api create-bucket --bucket {Bucket} --endpoint-url {S3_GATE}'
        _cmd_run(cmd, timeout=90)

    def list_buckets(self) -> dict:
        cmd = f'aws --no-verify-ssl s3api list-buckets --endpoint {S3_GATE}'
        output = _cmd_run(cmd)
        return self._to_json(output)

    def get_bucket_versioning(self, Bucket: str) -> dict:
        cmd = f'aws --no-verify-ssl s3api get-bucket-versioning --bucket {Bucket}' \
              f' --endpoint {S3_GATE}'
        output = _cmd_run(cmd)
        return self._to_json(output)

    def put_bucket_versioning(self, Bucket: str, VersioningConfiguration: dict) -> dict:
        cmd = f'aws --no-verify-ssl s3api put-bucket-versioning --bucket {Bucket} ' \
              f'--versioning-configuration Status={VersioningConfiguration.get("Status")}' \
              f' --endpoint {S3_GATE}'
        output = _cmd_run(cmd)
        return self._to_json(output)

    def list_objects(self, Bucket: str) -> dict:
        cmd = f'aws --no-verify-ssl s3api list-objects --bucket {Bucket}' \
              f' --endpoint {S3_GATE}'
        output = _cmd_run(cmd)
        return self._to_json(output)

    def list_objects_v2(self, Bucket: str) -> dict:
        cmd = f'aws --no-verify-ssl s3api list-objects-v2 --bucket {Bucket}' \
              f' --endpoint {S3_GATE}'
        output = _cmd_run(cmd)
        return self._to_json(output)

    def list_object_versions(self, Bucket: str) -> dict:
        cmd = f'aws --no-verify-ssl s3api list-object-versions --bucket {Bucket}' \
              f' --endpoint {S3_GATE}'
        output = _cmd_run(cmd)
        return self._to_json(output)

    def copy_object(self, Bucket: str, CopySource: str, Key: str) -> dict:
        cmd = f'aws --no-verify-ssl s3api copy-object --copy-source {CopySource} --bucket {Bucket} --key {Key}' \
              f' --endpoint {S3_GATE}'
        output = _cmd_run(cmd)
        return self._to_json(output)

    def head_bucket(self, Bucket: str) -> dict:
        cmd = f'aws --no-verify-ssl s3api head-bucket --bucket {Bucket} --endpoint {S3_GATE}'
        output = _cmd_run(cmd)
        return self._to_json(output)

    def put_object(self, Body: str, Bucket: str, Key: str) -> dict:
        cmd = f' aws --no-verify-ssl s3api put-object --bucket {Bucket} --key {Key} --body {Body}' \
              f' --endpoint {S3_GATE}'
        output = _cmd_run(cmd)
        return self._to_json(output)

    def head_object(self, Bucket: str, Key: str, VersionId: str = None) -> dict:
        version = f' --version-id {VersionId}' if VersionId else ''
        cmd = f' aws --no-verify-ssl s3api head-object --bucket {Bucket} --key {Key} {version}' \
              f' --endpoint {S3_GATE}'
        output = _cmd_run(cmd)
        return self._to_json(output)

    def get_object(self, Bucket: str, Key: str, file_path: str, VersionId: str = None) -> dict:
        version = f' --version-id {VersionId}' if VersionId else ''
        cmd = f' aws --no-verify-ssl s3api get-object --bucket {Bucket} ' \
              f'--key {Key} {version} {file_path} --endpoint {S3_GATE}'
        output = _cmd_run(cmd, timeout=90)
        return self._to_json(output)

    def delete_objects(self, Bucket: str, Delete: dict) -> dict:
        file_path = f"{os.getcwd()}/{ASSETS_DIR}/delete.json"
        with open(file_path, 'w') as out_file:
            out_file.write(json.dumps(Delete))

        cmd = f'aws --no-verify-ssl s3api delete-objects --bucket {Bucket} --delete file://{file_path} ' \
              f'--endpoint {S3_GATE}'
        output = _cmd_run(cmd, timeout=90)
        return self._to_json(output)

    def delete_object(self, Bucket: str, Key: str, VersionId: str = None) -> dict:
        version = f' --version-id {VersionId}' if VersionId else ''
        cmd = f'aws --no-verify-ssl s3api delete-object --bucket {Bucket} --key {Key} {version}' \
              f' --endpoint {S3_GATE}'
        output = _cmd_run(cmd, timeout=90)
        return self._to_json(output)

    def get_object_attributes(self, bucket: str, key: str, *attributes: str, version_id: str = None,
                              max_parts: int = None, part_number: int = None) -> dict:
        attrs = ','.join(attributes)
        version = f' --version-id {version_id}' if version_id else ''
        parts = f'--max-parts {max_parts}' if max_parts else ''
        part_number = f'--part-number-marker {part_number}' if part_number else ''
        cmd = f'aws --no-verify-ssl s3api get-object-attributes --bucket {bucket} --key {key} {version}' \
              f' {parts} {part_number} --object-attributes {attrs} --endpoint {S3_GATE}'
        output = _cmd_run(cmd)
        return self._to_json(output)

    def delete_bucket(self, Bucket: str) -> dict:
        cmd = f'aws --no-verify-ssl s3api delete-bucket --bucket {Bucket} --endpoint {S3_GATE}'
        output = _cmd_run(cmd, timeout=90)
        return self._to_json(output)

    def get_bucket_tagging(self, Bucket: str) -> dict:
        cmd = f'aws --no-verify-ssl s3api get-bucket-tagging --bucket {Bucket} --endpoint {S3_GATE}'
        output = _cmd_run(cmd)
        return self._to_json(output)

    def put_bucket_tagging(self, Bucket: str, Tagging: dict) -> dict:
        cmd = f'aws --no-verify-ssl s3api put-bucket-tagging --bucket {Bucket} --tagging \'{json.dumps(Tagging)}\'' \
              f' --endpoint {S3_GATE}'
        output = _cmd_run(cmd)
        return self._to_json(output)

    def delete_bucket_tagging(self, Bucket: str) -> dict:
        cmd = f'aws --no-verify-ssl s3api delete-bucket-tagging --bucket {Bucket} --endpoint {S3_GATE}'
        output = _cmd_run(cmd)
        return self._to_json(output)

    def put_object_tagging(self, Bucket: str, Key: str, Tagging: dict) -> dict:
        cmd = f'aws --no-verify-ssl s3api put-object-tagging --bucket {Bucket} --key {Key}' \
              f' --tagging \'{json.dumps(Tagging)}\'' \
              f' --endpoint {S3_GATE}'
        output = _cmd_run(cmd)
        return self._to_json(output)

    def get_object_tagging(self, Bucket: str, Key: str) -> dict:
        cmd = f'aws --no-verify-ssl s3api get-object-tagging --bucket {Bucket} --key {Key} --endpoint {S3_GATE}'
        output = _cmd_run(cmd)
        return self._to_json(output)

    def delete_object_tagging(self, Bucket: str, Key: str) -> dict:
        cmd = f'aws --no-verify-ssl s3api delete-object-tagging --bucket {Bucket} --key {Key} --endpoint {S3_GATE}'
        output = _cmd_run(cmd)
        return self._to_json(output)

    @allure.step('Sync directory S3')
    def sync(self, bucket_name: str, dir_path: str) -> dict:
        cmd = f'aws --no-verify-ssl s3 sync {dir_path}  s3://{bucket_name} --endpoint-url {S3_GATE}'
        output = _cmd_run(cmd)
        return self._to_json(output)

    def create_multipart_upload(self, Bucket: str, Key: str) -> dict:
        cmd = f'aws  --no-verify-ssl s3api create-multipart-upload --bucket {Bucket} --key {Key}' \
              f' --endpoint-url {S3_GATE}'
        output = _cmd_run(cmd)
        return self._to_json(output)

    def list_multipart_uploads(self, Bucket: str) -> dict:
        cmd = f'aws --no-verify-ssl s3api list-multipart-uploads --bucket {Bucket}' \
              f' --endpoint-url {S3_GATE}'
        output = _cmd_run(cmd)
        return self._to_json(output)

    def abort_multipart_upload(self, Bucket: str, Key: str, UploadId: str) -> dict:
        cmd = f'aws --no-verify-ssl s3api abort-multipart-upload  --bucket {Bucket} --key {Key}' \
              f' --upload-id {UploadId} --endpoint-url {S3_GATE}'
        output = _cmd_run(cmd)
        return self._to_json(output)

    def upload_part(self, UploadId: str, Bucket: str, Key: str, PartNumber: int, Body: str) -> dict:
        cmd = f'aws --no-verify-ssl s3api upload-part --bucket {Bucket} --key {Key} --upload-id {UploadId} ' \
              f'--part-number {PartNumber}  --body {Body} --endpoint-url {S3_GATE}'
        output = _cmd_run(cmd)
        return self._to_json(output)

    def list_parts(self, UploadId: str, Bucket: str, Key: str) -> dict:
        cmd = f'aws --no-verify-ssl s3api list-parts --bucket {Bucket} --key {Key} --upload-id {UploadId} ' \
              f' --endpoint-url {S3_GATE}'
        output = _cmd_run(cmd)
        return self._to_json(output)

    def complete_multipart_upload(self, Bucket: str, Key: str, UploadId: str, MultipartUpload: dict) -> dict:
        file_path = f"{os.getcwd()}/{ASSETS_DIR}/parts.json"
        with open(file_path, 'w') as out_file:
            out_file.write(json.dumps(MultipartUpload))

        cmd = f'aws --no-verify-ssl s3api complete-multipart-upload --bucket {Bucket} --key {Key}' \
              f' --upload-id {UploadId} --multipart-upload file://{file_path}' \
              f' --endpoint-url {S3_GATE}'
        output = _cmd_run(cmd)
        return self._to_json(output)

    @staticmethod
    def _to_json(output: str) -> dict:
        json_output = {}
        try:
            json_output = json.loads(output)
        except Exception:
            if '{' not in output and '}' not in output:
                logger.warning(f'Could not parse json from output {output}')
                return json_output
            json_output = json.loads(output[output.index('{'):])

        return json_output
