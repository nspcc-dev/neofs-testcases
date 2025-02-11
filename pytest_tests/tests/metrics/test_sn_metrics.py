import json
import logging
import os
import sys
from importlib.resources import files

import allure
import neofs_env.neofs_epoch as neofs_epoch
import pytest
import yaml
from helpers.common import SIMPLE_OBJECT_SIZE
from helpers.container import create_container, delete_container
from helpers.file_helper import generate_file
from helpers.metrics import get_metrics, wait_for_metric_to_arrive
from helpers.neofs_verbs import (
    delete_object,
    get_object,
    get_range,
    get_range_hash,
    head_object,
    put_object,
    search_object,
)
from helpers.node_management import drop_object
from helpers.wallet_helpers import create_wallet
from neofs_testlib.env.env import NeoFSEnv, NodeWallet
from s3 import s3_bucket, s3_object
from s3.s3_base import configure_boto3_client, init_s3_credentials

logger = logging.getLogger("NeoLogger")


def parse_node_height(stdout: str) -> tuple[float, float]:
    lines = stdout.strip().split("\n")
    block_height = float(lines[0].split(": ")[1].strip())
    state = float(lines[1].split(": ")[1].strip())
    return block_height, state


@pytest.fixture()
def single_noded_env():
    neofs_env_config = yaml.safe_load(
        files("neofs_testlib.env.templates").joinpath("neofs_env_config.yaml").read_text()
    )
    neofs_env = NeoFSEnv(neofs_env_config=neofs_env_config)
    neofs_env.download_binaries()
    neofs_env.deploy_inner_ring_nodes()
    neofs_env.deploy_storage_nodes(
        count=1,
        node_attrs={
            0: ["UN-LOCODE:RU MOW", "Price:22"],
        },
    )
    neofs_env.neofs_adm().fschain.set_config(
        rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
        alphabet_wallets=neofs_env.alphabet_wallets_dir,
        post_data="ContainerFee=0 ContainerAliasFee=0 MaxObjectSize=524288",
    )
    neofs_env.deploy_s3_gw()
    neofs_env.deploy_rest_gw()
    yield neofs_env
    neofs_env.kill()


@pytest.fixture()
def s3_boto_client(temp_directory, single_noded_env: NeoFSEnv):
    wallet = create_wallet()
    s3_bearer_rules_file = f"{os.getcwd()}/pytest_tests/data/s3_bearer_rules.json"
    _, _, access_key_id, secret_access_key, _ = init_s3_credentials(
        wallet, single_noded_env, s3_bearer_rules_file=s3_bearer_rules_file
    )
    client = configure_boto3_client(access_key_id, secret_access_key, f"https://{single_noded_env.s3_gw.address}")
    yield client


def test_sn_ir_metrics(single_noded_env: NeoFSEnv, default_wallet: NodeWallet):
    simple_object_size = 1000
    sn = single_noded_env.storage_nodes[0]
    ir = single_noded_env.inner_ring_nodes[0]

    cid = create_container(
        default_wallet.path, shell=single_noded_env.shell, endpoint=single_noded_env.sn_rpc, rule="REP 1"
    )

    file_path = generate_file(simple_object_size)

    oid = put_object(
        default_wallet.path,
        file_path,
        cid,
        single_noded_env.shell,
        single_noded_env.sn_rpc,
    )

    get_object(
        default_wallet.path,
        cid,
        oid,
        single_noded_env.shell,
        single_noded_env.sn_rpc,
    )

    head_object(
        default_wallet.path,
        cid,
        oid,
        shell=single_noded_env.shell,
        endpoint=single_noded_env.sn_rpc,
    )

    search_object(
        default_wallet.path,
        cid,
        shell=single_noded_env.shell,
        endpoint=single_noded_env.sn_rpc,
        expected_objects_list=[oid],
        root=True,
    )

    get_range(
        default_wallet.path,
        cid,
        oid,
        shell=single_noded_env.shell,
        endpoint=single_noded_env.sn_rpc,
        range_cut="0:1",
    )

    get_range_hash(
        default_wallet.path,
        cid,
        oid,
        range_cut="0:1",
        shell=single_noded_env.shell,
        endpoint=single_noded_env.sn_rpc,
    )

    block_height, validated_state = parse_node_height(
        single_noded_env.neo_go().query.height(rpc_endpoint=f"http://{ir.rpc_address}").stdout
    )

    with allure.step("Get metrics"):
        after_metrics_sn = get_metrics(sn)
        allure.attach(json.dumps(dict(after_metrics_sn)), "sn metrics", allure.attachment_type.JSON)
        after_metrics_ir = get_metrics(ir)
        allure.attach(json.dumps(dict(after_metrics_ir)), "ir metrics", allure.attachment_type.JSON)

    assert float(after_metrics_ir["neogo_current_block_height"][0]["value"]) >= block_height, (
        "invalid value for neogo_current_block_height"
    )
    assert float(after_metrics_ir["neogo_current_header_height"][0]["value"]) >= block_height, (
        "invalid value for neogo_current_header_height"
    )
    assert float(after_metrics_ir["neogo_current_persisted_height"][0]["value"]) >= block_height - 1, (
        "invalid value for neogo_current_persisted_height"
    )
    assert float(after_metrics_ir["neogo_current_state_height"][0]["value"]) >= validated_state, (
        "invalid value for neogo_current_state_height"
    )

    size_metrics_for_container = next(
        (c for c in after_metrics_sn["neofs_node_engine_container_size"] if c["params"]["cid"] == cid), None
    )
    assert size_metrics_for_container, "no metrics for the created container"
    assert size_metrics_for_container["value"] == simple_object_size, (
        "invalid value for neofs_node_engine_container_size"
    )
    assert after_metrics_sn["neofs_node_object_get_payload"][0]["value"] == simple_object_size, (
        "invalid value for neofs_node_object_get_payload"
    )
    assert after_metrics_sn["neofs_node_object_put_payload"][0]["value"] == simple_object_size, (
        "invalid value for neofs_node_object_put_payload"
    )
    assert after_metrics_sn["neofs_node_state_health"][0]["value"] == 2.0, "invalid value for neofs_node_state_health"
    assert after_metrics_ir["neofs_ir_state_health"][0]["value"] == 2.0, "invalid value for neofs_ir_state_health"

    for metric in (
        "neofs_node_engine_put_time_count",
        "neofs_node_engine_get_time_count",
        "neofs_node_engine_search_time_count",
        "neofs_node_object_get_req_count",
        "neofs_node_object_get_req_count_success",
        "neofs_node_object_rpc_get_time_count",
        "neofs_node_object_rpc_range_time_count",
        "neofs_node_object_rpc_search_time_count",
        "neofs_node_object_search_req_count",
        "neofs_node_object_search_req_count_success",
        "neofs_node_engine_head_time_count",
        "neofs_node_object_head_req_count",
        "neofs_node_object_head_req_count_success",
        "neofs_node_object_rpc_head_time_count",
        "neofs_node_object_put_req_count",
        "neofs_node_object_put_req_count_success",
        "neofs_node_object_range_hash_req_count",
        "neofs_node_object_range_hash_req_count_success",
        "neofs_node_object_range_req_count",
        "neofs_node_object_range_req_count_success",
    ):
        assert after_metrics_sn[metric][0]["value"] == 1, f"invalid value for {metric}"

    assert after_metrics_sn["neofs_node_engine_range_time_count"][0]["value"] == 2, (
        "invalid value for neofs_node_engine_range_time_count"
    )

    for metric in (
        "neofs_node_engine_put_time_bucket",
        "neofs_node_engine_get_time_bucket",
        "neofs_node_engine_range_time_bucket",
        "neofs_node_engine_search_time_bucket",
        "neofs_node_object_rpc_get_time_bucket",
        "neofs_node_object_rpc_range_time_bucket",
        "neofs_node_object_rpc_search_time_bucket",
        "neofs_node_object_counter",
        "neofs_node_engine_head_time_bucket",
        "neofs_node_object_rpc_head_time_bucket",
        "neofs_node_object_rpc_range_hash_time_bucket",
        "neofs_node_object_rpc_put_time_bucket",
        "neofs_node_engine_list_objects_time_bucket",
    ):
        assert len([m for _, m in enumerate(after_metrics_sn[metric]) if m["value"] >= 1]) >= 1, (
            f"invalid value for {metric}"
        )

    node_version = single_noded_env.get_binary_version(single_noded_env.neofs_node_path)
    assert after_metrics_sn["neofs_node_version"][0]["params"]["version"] == node_version, (
        "invalid value for neofs_node_version"
    )
    ir_version = single_noded_env.get_binary_version(single_noded_env.neofs_ir_path)
    assert after_metrics_ir["neofs_ir_version"][0]["params"]["version"] == ir_version, (
        "invalid value for neofs_ir_version"
    )

    fs_size = 0
    if sys.platform == "darwin":
        fs_size = single_noded_env.shell.exec("df -k . | awk 'NR==2 {print $2 * 1024}'").stdout.strip()
    else:
        fs_size = single_noded_env.shell.exec("df --block-size=1 . | awk 'NR==2 {print $2}'").stdout.strip()

    assert after_metrics_sn["neofs_node_engine_capacity"][0]["value"] == float(fs_size), (
        "invalid value for neofs_node_engine_capacity"
    )

    fresh_epoch = neofs_epoch.ensure_fresh_epoch(single_noded_env)
    wait_for_metric_to_arrive(sn, "neofs_node_state_epoch", float(fresh_epoch))
    wait_for_metric_to_arrive(ir, "neofs_ir_state_epoch", float(fresh_epoch))

    delete_object(
        default_wallet.path,
        cid,
        oid,
        shell=single_noded_env.shell,
        endpoint=single_noded_env.sn_rpc,
    )

    with allure.step("Get metrics after object deletion"):
        after_metrics_sn = get_metrics(sn)
        allure.attach(json.dumps(dict(after_metrics_sn)), "sn metrics object delete", allure.attachment_type.JSON)

    for metric in ("neofs_node_object_delete_req_count", "neofs_node_object_delete_req_count_success"):
        assert after_metrics_sn[metric][0]["value"] == 1, f"invalid value for {metric}"

    for metric in (
        "neofs_node_object_rpc_delete_time_bucket",
        "neofs_node_engine_inhume_time_bucket",
    ):
        assert len([m for _, m in enumerate(after_metrics_sn[metric]) if m["value"] >= 1]) >= 1, (
            f"invalid value for {metric}"
        )

    file_path = generate_file(simple_object_size)

    oid = put_object(
        default_wallet.path,
        file_path,
        cid,
        single_noded_env.shell,
        single_noded_env.sn_rpc,
    )

    drop_object(sn, cid, oid)

    with allure.step("Get metrics after object drop"):
        after_metrics_sn = get_metrics(sn)
        allure.attach(json.dumps(dict(after_metrics_sn)), "sn metrics object drop", allure.attachment_type.JSON)

    for metric in ("neofs_node_engine_delete_time_bucket",):
        assert len([m for _, m in enumerate(after_metrics_sn[metric]) if m["value"] >= 1]) >= 1, (
            f"invalid value for {metric}"
        )

    delete_container(
        default_wallet.path, cid, shell=single_noded_env.shell, endpoint=single_noded_env.sn_rpc, await_mode=True
    )


def test_s3_gw_metrics(single_noded_env: NeoFSEnv, s3_boto_client):
    simple_object_size = int(SIMPLE_OBJECT_SIZE)
    bucket = s3_bucket.create_bucket_s3(
        s3_boto_client,
        acl="public-read",
        bucket_configuration="rep-1",
    )
    file_path = generate_file(simple_object_size)
    file_name = os.path.basename(file_path)

    s3_object.put_object_s3(s3_boto_client, bucket, file_path, filename=file_name)
    s3_object.list_objects_s3(s3_boto_client, bucket)
    s3_object.copy_object_s3(s3_boto_client, bucket, file_name)
    s3_object.get_object_acl_s3(s3_boto_client, bucket, file_name)
    s3_bucket.put_bucket_ownership_controls(s3_boto_client, bucket, s3_bucket.ObjectOwnership.BUCKET_OWNER_PREFERRED)
    s3_object.put_object_acl_s3(s3_boto_client, bucket, file_name, "public-read")
    s3_bucket.get_bucket_acl(s3_boto_client, bucket)

    with allure.step("Get metrics"):
        after_metrics_s3_gw = get_metrics(single_noded_env.s3_gw)
        allure.attach(json.dumps(dict(after_metrics_s3_gw)), "s3 gw metrics", allure.attachment_type.JSON)

    assert after_metrics_s3_gw["neofs_s3_gw_pool_overall_node_requests"][0]["value"] >= 10, (
        "invalid value for neofs_s3_gw_pool_overall_node_requests"
    )

    expected_params = {"getbucketacl", "getobjectacl", "listbuckets", "listobjectsv1"}
    for metric in after_metrics_s3_gw["neofs_s3_request_seconds_bucket"]:
        if metric["params"]["api"] in expected_params:
            expected_params.remove(metric["params"]["api"])
    assert len(expected_params) == 0, (
        f"invalid value for neofs_s3_request_seconds_bucket, these params are not present: {expected_params=}"
    )

    expected_params = {
        "copyobject",
        "createbucket",
        "getbucketacl",
        "getobjectacl",
        "listbuckets",
        "listobjectsv1",
        "putbucketownershipcontrols",
        "putobject",
        "putobjectacl",
    }
    for metric in after_metrics_s3_gw["neofs_s3_requests_total"]:
        if metric["params"]["api"] in expected_params:
            expected_params.remove(metric["params"]["api"])
    assert len(expected_params) == 0, (
        f"invalid value for neofs_s3_request_seconds_bucket, these params are not present: {expected_params=}"
    )

    assert after_metrics_s3_gw["neofs_s3_rx_bytes_total"][0]["value"] >= int(SIMPLE_OBJECT_SIZE), (
        "invalid value for neofs_s3_rx_bytes_total"
    )
    assert after_metrics_s3_gw["neofs_s3_tx_bytes_total"][0]["value"] >= int(SIMPLE_OBJECT_SIZE), (
        "invalid value for neofs_s3_rx_bytes_total"
    )
    neofs_s3_version = single_noded_env.get_binary_version(single_noded_env.neofs_s3_gw_path)
    assert after_metrics_s3_gw["neofs_s3_version"][0]["params"]["version"] == neofs_s3_version, (
        "invalid value for neofs_s3_version"
    )
