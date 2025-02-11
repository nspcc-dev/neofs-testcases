import json
import logging
import sys
import threading
import time
from importlib.resources import files

import allure
import neofs_env.neofs_epoch as neofs_epoch
import pytest
import yaml
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
from neofs_testlib.env.env import NeoFSEnv, NodeWallet, StorageNode

logger = logging.getLogger("NeoLogger")


@pytest.fixture(scope="module")
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
    time.sleep(30)
    yield neofs_env
    neofs_env.kill()


def test_sn_metrics(single_noded_env: NeoFSEnv, default_wallet: NodeWallet):
    simple_object_size = 1000
    sn = single_noded_env.storage_nodes[0]

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

    with allure.step("Get metrics"):
        after_metrics = get_metrics(sn)
        allure.attach(json.dumps(dict(after_metrics)), "sn metrics", allure.attachment_type.JSON)

    size_metrics_for_container = next(
        (c for c in after_metrics["neofs_node_engine_container_size"] if c["params"]["cid"] == cid), None
    )
    assert size_metrics_for_container, "no metrics for the created container"
    assert size_metrics_for_container["value"] == simple_object_size, (
        "invalid value for neofs_node_engine_container_size"
    )
    assert after_metrics["neofs_node_object_get_payload"][0]["value"] == simple_object_size, (
        "invalid value for neofs_node_object_get_payload"
    )
    assert after_metrics["neofs_node_object_put_payload"][0]["value"] == simple_object_size, (
        "invalid value for neofs_node_object_put_payload"
    )
    assert after_metrics["neofs_node_state_health"][0]["value"] == 2.0, "invalid value for neofs_node_state_health"

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
        assert after_metrics[metric][0]["value"] == 1, f"invalid value for {metric}"

    assert after_metrics["neofs_node_engine_range_time_count"][0]["value"] == 2, (
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
        assert len([m for _, m in enumerate(after_metrics[metric]) if m["value"] >= 1]) >= 1, (
            f"invalid value for {metric}"
        )

    node_version = single_noded_env.get_binary_version(single_noded_env.neofs_node_path)
    assert after_metrics["neofs_node_version"][0]["params"]["version"] == node_version, (
        "invalid value for neofs_node_version"
    )

    fs_size = 0
    if sys.platform == "darwin":
        fs_size = single_noded_env.shell.exec("df -k . | awk 'NR==2 {print $2 * 1024}'").stdout.strip()
    else:
        fs_size = single_noded_env.shell.exec("df --block-size=1 . | awk 'NR==2 {print $2}'").stdout.strip()

    assert after_metrics["neofs_node_engine_capacity"][0]["value"] == float(fs_size), (
        "invalid value for neofs_node_engine_capacity"
    )

    fresh_epoch = neofs_epoch.ensure_fresh_epoch(single_noded_env)
    wait_for_metric_to_arrive(single_noded_env.storage_nodes[0], "neofs_node_state_epoch", float(fresh_epoch))

    delete_object(
        default_wallet.path,
        cid,
        oid,
        shell=single_noded_env.shell,
        endpoint=single_noded_env.sn_rpc,
    )

    with allure.step("Get metrics after object deletion"):
        after_metrics = get_metrics(sn)
        allure.attach(json.dumps(dict(after_metrics)), "sn metrics object delete", allure.attachment_type.JSON)

    for metric in ("neofs_node_object_delete_req_count", "neofs_node_object_delete_req_count_success"):
        assert after_metrics[metric][0]["value"] == 1, f"invalid value for {metric}"

    for metric in (
        "neofs_node_object_rpc_delete_time_bucket",
        "neofs_node_engine_inhume_time_bucket",
    ):
        assert len([m for _, m in enumerate(after_metrics[metric]) if m["value"] >= 1]) >= 1, (
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

    drop_object(single_noded_env.storage_nodes[0], cid, oid)

    with allure.step("Get metrics after object drop"):
        after_metrics = get_metrics(sn)
        allure.attach(json.dumps(dict(after_metrics)), "sn metrics object drop", allure.attachment_type.JSON)

    for metric in ("neofs_node_engine_delete_time_bucket",):
        assert len([m for _, m in enumerate(after_metrics[metric]) if m["value"] >= 1]) >= 1, (
            f"invalid value for {metric}"
        )

    delete_container(
        default_wallet.path, cid, shell=single_noded_env.shell, endpoint=single_noded_env.sn_rpc, await_mode=True
    )
