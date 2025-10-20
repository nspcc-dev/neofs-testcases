import json
import logging
import os
import sys

import allure
import neofs_env.neofs_epoch as neofs_epoch
import pytest
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
from helpers.rest_gate import create_container as create_container_rest_gw
from helpers.rest_gate import delete_container as delete_container_rest_gw
from helpers.rest_gate import (
    get_container_info,
    get_epoch_duration_via_rest_gate,
    get_via_rest_gate,
    new_upload_via_rest_gate,
)
from helpers.wallet_helpers import create_wallet
from helpers.wellknown_acl import PUBLIC_ACL
from neofs_testlib.env.env import NeoFSEnv, NodeWallet
from rest_gw.rest_utils import generate_credentials
from s3 import s3_bucket, s3_object
from s3.s3_base import configure_boto3_client, init_s3_credentials

logger = logging.getLogger("NeoLogger")


def parse_node_height(stdout: str) -> tuple[float, float]:
    lines = stdout.strip().split("\n")
    block_height = float(lines[0].split(": ")[1].strip())
    state = float(lines[1].split(": ")[1].strip())
    return block_height, state


@pytest.fixture()
def s3_boto_client(temp_directory, neofs_env_single_sn: NeoFSEnv):
    wallet = create_wallet()
    s3_bearer_rules_file = f"{os.getcwd()}/pytest_tests/data/s3_bearer_rules.json"
    _, _, access_key_id, secret_access_key, _ = init_s3_credentials(
        wallet, neofs_env_single_sn, s3_bearer_rules_file=s3_bearer_rules_file
    )
    client = configure_boto3_client(access_key_id, secret_access_key, f"https://{neofs_env_single_sn.s3_gw.endpoint}")
    yield client


def test_sn_ir_metrics(neofs_env_single_sn: NeoFSEnv, default_wallet: NodeWallet):
    simple_object_size = 1000
    sn = neofs_env_single_sn.storage_nodes[0]
    ir = neofs_env_single_sn.inner_ring_nodes[0]

    cid = create_container(
        default_wallet.path, shell=neofs_env_single_sn.shell, endpoint=neofs_env_single_sn.sn_rpc, rule="EC 2/2"
    )

    file_path = generate_file(simple_object_size)

    oid = put_object(
        default_wallet.path,
        file_path,
        cid,
        neofs_env_single_sn.shell,
        neofs_env_single_sn.sn_rpc,
    )

    get_object(
        default_wallet.path,
        cid,
        oid,
        neofs_env_single_sn.shell,
        neofs_env_single_sn.sn_rpc,
    )

    head_object(
        default_wallet.path,
        cid,
        oid,
        shell=neofs_env_single_sn.shell,
        endpoint=neofs_env_single_sn.sn_rpc,
    )

    search_object(
        default_wallet.path,
        cid,
        shell=neofs_env_single_sn.shell,
        endpoint=neofs_env_single_sn.sn_rpc,
        expected_objects_list=[oid],
        root=True,
    )

    get_range(
        default_wallet.path,
        cid,
        oid,
        shell=neofs_env_single_sn.shell,
        endpoint=neofs_env_single_sn.sn_rpc,
        range_cut="0:1",
    )

    get_range_hash(
        default_wallet.path,
        cid,
        oid,
        range_cut="0:1",
        shell=neofs_env_single_sn.shell,
        endpoint=neofs_env_single_sn.sn_rpc,
    )

    block_height, validated_state = parse_node_height(
        neofs_env_single_sn.neo_go().query.height(rpc_endpoint=f"http://{ir.endpoint}").stdout
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

    metrics_to_verify = [
        "neofs_node_engine_put_time_count",
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
    ]

    metrics_to_verify.append("neofs_node_engine_get_stream_time_count")

    for metric in metrics_to_verify:
        assert after_metrics_sn[metric][0]["value"] == 1, f"invalid value for {metric}"

    assert after_metrics_sn["neofs_node_engine_range_time_count"][0]["value"] == 2, (
        "invalid value for neofs_node_engine_range_time_count"
    )

    metrics_to_verify = [
        "neofs_node_engine_put_time_bucket",
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
    ]

    metrics_to_verify.append("neofs_node_engine_get_stream_time_bucket")

    for metric in metrics_to_verify:
        assert len([m for _, m in enumerate(after_metrics_sn[metric]) if m["value"] >= 1]) >= 1, (
            f"invalid value for {metric}"
        )

    node_version = neofs_env_single_sn.get_binary_version(neofs_env_single_sn.neofs_node_path)
    assert after_metrics_sn["neofs_node_version"][0]["params"]["version"] == node_version, (
        "invalid value for neofs_node_version"
    )
    ir_version = neofs_env_single_sn.get_binary_version(neofs_env_single_sn.neofs_ir_path)
    assert after_metrics_ir["neofs_ir_version"][0]["params"]["version"] == ir_version, (
        "invalid value for neofs_ir_version"
    )

    fs_size = 0
    if sys.platform == "darwin":
        fs_size = neofs_env_single_sn.shell.exec("df -k . | awk 'NR==2 {print $2 * 1024}'").stdout.strip()
    else:
        fs_size = neofs_env_single_sn.shell.exec("df --block-size=1 . | awk 'NR==2 {print $2}'").stdout.strip()

    assert after_metrics_sn["neofs_node_engine_capacity"][0]["value"] == float(fs_size), (
        "invalid value for neofs_node_engine_capacity"
    )

    fresh_epoch = neofs_epoch.ensure_fresh_epoch(neofs_env_single_sn)
    wait_for_metric_to_arrive(sn, "neofs_node_state_epoch", float(fresh_epoch))
    wait_for_metric_to_arrive(ir, "neofs_ir_state_epoch", float(fresh_epoch))

    delete_object(
        default_wallet.path,
        cid,
        oid,
        shell=neofs_env_single_sn.shell,
        endpoint=neofs_env_single_sn.sn_rpc,
    )

    with allure.step("Get metrics after object deletion"):
        after_metrics_sn = get_metrics(sn)
        allure.attach(json.dumps(dict(after_metrics_sn)), "sn metrics object delete", allure.attachment_type.JSON)

    for metric in ("neofs_node_object_delete_req_count", "neofs_node_object_delete_req_count_success"):
        assert after_metrics_sn[metric][0]["value"] == 1, f"invalid value for {metric}"

    for metric in ("neofs_node_object_rpc_delete_time_bucket",):
        assert len([m for _, m in enumerate(after_metrics_sn[metric]) if m["value"] >= 1]) >= 1, (
            f"invalid value for {metric}"
        )

    file_path = generate_file(simple_object_size)

    oid = put_object(
        default_wallet.path,
        file_path,
        cid,
        neofs_env_single_sn.shell,
        neofs_env_single_sn.sn_rpc,
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
        default_wallet.path, cid, shell=neofs_env_single_sn.shell, endpoint=neofs_env_single_sn.sn_rpc, await_mode=True
    )


def test_s3_gw_metrics(neofs_env_single_sn: NeoFSEnv, s3_boto_client):
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
        after_metrics_s3_gw = get_metrics(neofs_env_single_sn.s3_gw)
        allure.attach(json.dumps(dict(after_metrics_s3_gw)), "s3 gw metrics", allure.attachment_type.JSON)

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

    neofs_s3_version = neofs_env_single_sn.get_binary_version(neofs_env_single_sn.neofs_s3_gw_path)
    assert "neofs_s3_version" in after_metrics_s3_gw, "no neofs_s3_version in metrics"
    assert after_metrics_s3_gw["neofs_s3_version"][0]["params"]["version"] == neofs_s3_version, (
        "invalid value for neofs_s3_version"
    )

    for metric in ("eacl", "get", "list", "put", "set_eacl"):
        assert (
            len(
                [
                    m
                    for _, m in enumerate(after_metrics_s3_gw[f"neofs_s3_pool_container_{metric}_bucket"])
                    if m["value"] >= 1
                ]
            )
            >= 1
        ), f"invalid value for neofs_s3_pool_container_{metric}_duration_bucket"

        assert after_metrics_s3_gw[f"neofs_s3_pool_container_{metric}_sum"][0]["value"] > 0, (
            f"invalid value for neofs_s3_pool_container_{metric}_sum"
        )
        assert after_metrics_s3_gw[f"neofs_s3_pool_container_{metric}_count"][0]["value"] > 0, (
            f"invalid value for neofs_s3_pool_container_{metric}_count"
        )

    for metric in (
        "network_info",
        "object_get_init",
        "object_get_stream",
        "object_head",
        "object_put_init",
        "object_put_stream",
        "object_search_v2",
        "session_create",
    ):
        assert (
            len([m for _, m in enumerate(after_metrics_s3_gw[f"neofs_s3_pool_{metric}_bucket"]) if m["value"] >= 1])
            >= 1
        ), f"invalid value for neofs_s3_pool_{metric}_duration_bucket"

        assert after_metrics_s3_gw[f"neofs_s3_pool_{metric}_sum"][0]["value"] > 0, (
            f"invalid value for neofs_s3_pool_{metric}_sum"
        )
        assert after_metrics_s3_gw[f"neofs_s3_pool_{metric}_count"][0]["value"] > 0, (
            f"invalid value for neofs_s3_pool_{metric}_count"
        )


def test_rest_gw_metrics(neofs_env_single_sn: NeoFSEnv, default_wallet: NodeWallet):
    simple_object_size = int(SIMPLE_OBJECT_SIZE)
    gw_endpoint = f"http://{neofs_env_single_sn.rest_gw.endpoint}/v1"

    session_token, signature, pub_key = generate_credentials(gw_endpoint, default_wallet, wallet_connect=True)
    cid = create_container_rest_gw(
        gw_endpoint,
        "rest_gw_container",
        "EC 2/2",
        PUBLIC_ACL,
        session_token,
        signature,
        pub_key,
        wallet_connect=True,
        new_api=False,
    )

    get_container_info(gw_endpoint, cid)
    get_epoch_duration_via_rest_gate(gw_endpoint)
    oid = new_upload_via_rest_gate(
        cid=cid,
        path=generate_file(simple_object_size),
        endpoint=gw_endpoint,
    )
    get_via_rest_gate(
        cid=cid,
        oid=oid,
        endpoint=gw_endpoint,
        return_response=True,
    )

    with allure.step("Get metrics"):
        after_metrics_rest_gw = get_metrics(neofs_env_single_sn.rest_gw)
        with open("rest_gw_metrics", "w") as f:
            json.dump(dict(after_metrics_rest_gw), f)
        allure.attach(json.dumps(dict(after_metrics_rest_gw)), "rest gw metrics", allure.attachment_type.JSON)

    rest_gw_version = neofs_env_single_sn.get_binary_version(neofs_env_single_sn.neofs_rest_gw_path)
    assert "neofs_rest_gw_version" in after_metrics_rest_gw, "no neofs_rest_gw_version in metrics"
    assert after_metrics_rest_gw["neofs_rest_gw_version"][0]["params"]["version"] == rest_gw_version, (
        "invalid value for neofs_rest_gw_version"
    )

    for metric in (
        "auth",
        "get_container",
        "get_network_info",
        "new_upload_container_object",
        "put_container",
    ):
        assert (
            len(
                [
                    m
                    for _, m in enumerate(after_metrics_rest_gw[f"neofs_rest_gw_api_{metric}_duration_bucket"])
                    if m["value"] >= 1
                ]
            )
            >= 1
        ), f"invalid value for neofs_rest_gw_api_{metric}_duration_bucket"

        assert after_metrics_rest_gw[f"neofs_rest_gw_api_{metric}_duration_sum"][0]["value"] > 0, (
            f"invalid value for neofs_rest_gw_api_{metric}_duration_sum"
        )
        assert after_metrics_rest_gw[f"neofs_rest_gw_api_{metric}_duration_count"][0]["value"] > 0, (
            f"invalid value for neofs_rest_gw_api_{metric}_duration_count"
        )

    for metric in (
        "container_get",
        "container_put",
        "network_info",
        "object_get_init",
        "object_put_init",
        "object_put_stream",
        "session_create",
    ):
        assert (
            len(
                [
                    m
                    for _, m in enumerate(after_metrics_rest_gw[f"neofs_rest_gw_pool_{metric}_bucket"])
                    if m["value"] >= 1
                ]
            )
            >= 1
        ), f"invalid value for neofs_rest_gw_pool_{metric}_bucket"

        assert after_metrics_rest_gw[f"neofs_rest_gw_pool_{metric}_sum"][0]["value"] > 0, (
            f"neofs_rest_gw_pool_{metric}_sum"
        )
        assert after_metrics_rest_gw[f"neofs_rest_gw_pool_{metric}_count"][0]["value"] > 0, (
            f"neofs_rest_gw_pool_{metric}_count"
        )

    session_token, signature, pub_key = generate_credentials(
        gw_endpoint, default_wallet, verb="DELETE", wallet_connect=True
    )
    delete_container_rest_gw(gw_endpoint, cid, session_token, signature, pub_key, wallet_connect=True)
