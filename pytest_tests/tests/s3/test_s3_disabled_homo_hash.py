import os
from typing import Any

import allure
import pytest
from helpers.file_helper import (
    generate_file,
    get_file_hash,
    split_file,
)
from helpers.neofs_verbs import get_netmap_netinfo
from neofs_testlib.env.env import NeoFSEnv, NodeWallet
from s3 import s3_bucket, s3_object
from s3.s3_base import configure_boto3_client, init_s3_credentials


def set_homomorphic_hash_disabled(neofs_env: NeoFSEnv, value: bool):
    ir_node = neofs_env.inner_ring_nodes[0]
    neofsadm = neofs_env.neofs_adm()
    neofsadm.fschain.set_config(
        rpc_endpoint=f"http://{ir_node.endpoint}",
        alphabet_wallets=neofs_env.alphabet_wallets_dir,
        post_data=f"HomomorphicHashingDisabled={str(value).lower()}",
    )
    get_netmap_netinfo(
        wallet=neofs_env.storage_nodes[0].wallet.path,
        wallet_config=neofs_env.storage_nodes[0].cli_config,
        endpoint=neofs_env.storage_nodes[0].endpoint,
        shell=neofs_env.shell,
    )


@pytest.fixture
def s3_client(default_wallet: NodeWallet, neofs_env_single_sn: NeoFSEnv) -> Any:
    set_homomorphic_hash_disabled(neofs_env_single_sn, True)

    s3_bearer_rules_file = f"{os.getcwd()}/pytest_tests/data/s3_bearer_rules.json"
    (
        cid,
        bucket,
        access_key_id,
        secret_access_key,
        owner_private_key,
    ) = init_s3_credentials(default_wallet, neofs_env_single_sn, s3_bearer_rules_file=s3_bearer_rules_file)

    cli = neofs_env_single_sn.neofs_cli(neofs_env_single_sn.generate_cli_config(default_wallet))
    result = cli.container.list(rpc_endpoint=neofs_env_single_sn.sn_rpc, wallet=default_wallet.path)
    containers_list = result.stdout.split()
    assert cid in containers_list, f"Expected cid {cid} in {containers_list}"

    client = configure_boto3_client(access_key_id, secret_access_key, f"https://{neofs_env_single_sn.s3_gw.endpoint}")
    yield neofs_env_single_sn, client


@allure.title("Test S3 Object Multipart API with disabled homomorphic hashing")
@pytest.mark.simple
def test_s3_api_multipart_disabled_homo_hash(s3_client):
    neofs_env, client = s3_client

    bucket = s3_bucket.create_bucket_s3(client, bucket_configuration="rep-2")

    parts_count = 3
    file_name_large = generate_file(neofs_env.get_object_size("simple_object_size") * 1024 * 6 * parts_count)
    object_key = os.path.basename(file_name_large)
    part_files = split_file(file_name_large, parts_count)
    parts = []

    uploads = s3_object.list_multipart_uploads_s3(client, bucket)
    assert not uploads, f"Expected there is no uploads in bucket {bucket}"

    with allure.step("Create and abort multipart upload"):
        upload_id = s3_object.create_multipart_upload_s3(client, bucket, object_key)
        uploads = s3_object.list_multipart_uploads_s3(client, bucket)
        assert uploads, f"Expected there one upload in bucket {bucket}"
        assert uploads[0].get("Key") == object_key, f"Expected correct key {object_key} in upload {uploads}"
        assert uploads[0].get("UploadId") == upload_id, f"Expected correct UploadId {upload_id} in upload {uploads}"

        s3_object.abort_multipart_uploads_s3(client, bucket, object_key, upload_id)
        uploads = s3_object.list_multipart_uploads_s3(client, bucket)
        assert not uploads, f"Expected there is no uploads in bucket {bucket}"

    with allure.step("Create new multipart upload and upload several parts"):
        upload_id = s3_object.create_multipart_upload_s3(client, bucket, object_key)
        for part_id, file_path in enumerate(part_files, start=1):
            etag = s3_object.upload_part_s3(client, bucket, object_key, upload_id, part_id, file_path)
            parts.append((part_id, etag))

    with allure.step("Check all parts are visible in bucket"):
        got_parts = s3_object.list_parts_s3(client, bucket, object_key, upload_id)
        assert len(got_parts) == len(part_files), f"Expected {parts_count} parts, got\n{got_parts}"

    s3_object.complete_multipart_upload_s3(client, bucket, object_key, upload_id, parts)

    uploads = s3_object.list_multipart_uploads_s3(client, bucket)
    assert not uploads, f"Expected there is no uploads in bucket {bucket}"

    with allure.step("Check we can get whole object from bucket"):
        got_object = s3_object.get_object_s3(client, bucket, object_key)
        assert get_file_hash(got_object) == get_file_hash(file_name_large)
