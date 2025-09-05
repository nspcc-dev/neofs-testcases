import logging
import os
import time
from typing import Any

import pytest
from helpers.container import list_containers
from helpers.rest_gate import create_container, get_containers_list
from helpers.wellknown_acl import PUBLIC_ACL
from neofs_testlib.env.env import NeoFSEnv, NodeWallet
from rest_gw.rest_utils import generate_credentials
from s3 import s3_bucket
from s3.s3_base import configure_boto3_client, init_s3_credentials

logger = logging.getLogger("NeoLogger")


@pytest.fixture
def s3_client(default_wallet: NodeWallet, neofs_env_single_sn: NeoFSEnv) -> Any:
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


@pytest.mark.long
def test_10k_containers_creation(default_wallet: NodeWallet, neofs_env_single_sn: NeoFSEnv):
    gw_endpoint = f"http://{neofs_env_single_sn.rest_gw.endpoint}/v1"
    containers_count = 10000

    session_token, signature, pub_key = generate_credentials(
        gw_endpoint, default_wallet, wallet_connect=False, bearer_for_all_users=False
    )
    cids = []
    for index in range(containers_count):
        cids.append(
            create_container(
                gw_endpoint,
                "rest_gw_container",
                "REP 1",
                PUBLIC_ACL,
                session_token,
                signature,
                pub_key,
                wallet_connect=False,
                new_api=True,
            )
        )
        logger.info(f"Container {index + 1} successfully created.")
        time.sleep(0.1)

    containers = get_containers_list(gw_endpoint)
    assert len(containers) == containers_count

    containers = list_containers(default_wallet.path, neofs_env_single_sn.shell, neofs_env_single_sn.sn_rpc)
    assert len(containers) == containers_count


@pytest.mark.long
def test_s3_api_10k_buckets(s3_client):
    neofs_env, client = s3_client
    buckets_count = 10000

    for _ in range(buckets_count):
        s3_bucket.create_bucket_s3(client, bucket_configuration="rep-1")

    buckets = s3_bucket.list_buckets_s3(client)
    assert len(buckets) == buckets_count
