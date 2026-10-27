import os

import allure
import pytest
from helpers.file_helper import generate_file, get_file_hash
from neofs_testlib.env.env import S3_GW, NeoFSEnv, NodeWallet
from s3 import s3_bucket, s3_object
from s3.s3_base import configure_boto3_client, init_s3_credentials

_UNREACHABLE_PEER = "127.0.0.1:1"


@allure.title("S3 GW with peers_from_netmap dials storage nodes from the network map")
@pytest.mark.simple
def test_s3_peers_from_netmap(neofs_env: NeoFSEnv, default_wallet: NodeWallet):
    with allure.step("Deploy an S3 GW that takes peers from the network map"):
        s3_gw = S3_GW(neofs_env, peer_addresses=[_UNREACHABLE_PEER], peers_from_netmap=True)
        s3_gw.start()

    try:
        with allure.step("Gateway log lists online storage nodes and ignores the static peers list"):
            with open(s3_gw.stderr) as log_file:
                log = log_file.read()
            assert "using nodes from network map" in log, "Gateway did not log network-map peers"
            assert "added connection peer" not in log, "Gateway used the static peers list"
            for storage_node in neofs_env.storage_nodes:
                assert storage_node.netmap_endpoint in log, (
                    f"Network map peer {storage_node.netmap_endpoint} missing from S3 GW log"
                )

        with allure.step("Issue S3 credentials for the netmap-backed gateway"):
            _, _, access_key_id, secret_access_key = init_s3_credentials(
                default_wallet, neofs_env, s3_gw=s3_gw, placement_policy="REP 1"
            )
            client = configure_boto3_client(access_key_id, secret_access_key, f"https://{s3_gw.endpoint}")

        bucket = s3_bucket.create_bucket_s3(client, bucket_configuration="rep-1")
        file_path = generate_file(neofs_env.get_object_size("simple_object_size"))
        file_name = os.path.basename(file_path)

        with allure.step("Put and get the object back through the netmap-backed gateway"):
            s3_object.put_object_s3(client, bucket, file_path)
            got_object = s3_object.get_object_s3(client, bucket, file_name)
            assert get_file_hash(got_object) == get_file_hash(file_path), (
                "Object payload differs via netmap-backed S3 GW"
            )

        with allure.step("Cleanup"):
            s3_object.delete_object_s3(client, bucket, file_name)
            s3_bucket.delete_bucket_s3(client, bucket)
    finally:
        s3_gw.stop()
