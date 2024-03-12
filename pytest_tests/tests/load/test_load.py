import allure
import pytest
from helpers.k6 import (
    CONTAINERS_COUNT,
    DELETERS,
    LOAD_NODE_SSH_PRIVATE_KEY_PATH,
    LOAD_NODE_SSH_USER,
    LOAD_NODES,
    LOAD_NODES_COUNT,
    LOAD_TIME,
    LOAD_TYPE,
    OBJ_COUNT,
    OBJ_SIZE,
    OUT_FILE,
    READERS,
    WRITERS,
    LoadParams,
    multi_node_k6_run,
    prepare_k6_instances,
)
from neofs_env.neofs_env_test_base import NeofsEnvTestBase


@pytest.mark.load
@pytest.mark.skip(reason="Need to clarify its purpose")
@pytest.mark.nspcc_dev__neofs_testcases__issue_544
class TestLoad(NeofsEnvTestBase):
    @pytest.mark.parametrize("obj_size, out_file", list(zip(OBJ_SIZE, OUT_FILE)))
    @pytest.mark.parametrize("writers, readers, deleters", list(zip(WRITERS, READERS, DELETERS)))
    @pytest.mark.parametrize("load_time", LOAD_TIME)
    @pytest.mark.parametrize("containers_count", CONTAINERS_COUNT)
    @pytest.mark.parametrize("load_type", LOAD_TYPE)
    @pytest.mark.parametrize("obj_count", OBJ_COUNT)
    @pytest.mark.parametrize("load_nodes_count", LOAD_NODES_COUNT)
    @pytest.mark.benchmark
    @pytest.mark.grpc
    @pytest.mark.nspcc_dev__neofs_testcases__issue_544
    def test_custom_load(
        self,
        obj_size,
        out_file,
        writers,
        readers,
        deleters,
        load_time,
        obj_count,
        load_type,
        load_nodes_count,
        containers_count,
    ):
        allure.dynamic.title(
            f"Load test "
            f"writers = {writers} readers = {readers}, "
            f"deleters = {deleters}, obj_size = {obj_size}, "
            f"load_time = {load_time}"
        )
        with allure.step("Get endpoints"):
            endpoints = ",".join(
                [node.control_grpc_endpoint for node in self.neofs_env.storage_nodes]
            )
        load_params = LoadParams(
            endpoint=endpoints,
            obj_size=obj_size,
            containers_count=containers_count,
            out_file=out_file,
            obj_count=obj_count,
            writers=writers,
            readers=readers,
            deleters=deleters,
            load_time=load_time,
            load_type=load_type,
        )
        load_nodes_list = LOAD_NODES[:load_nodes_count]
        k6_load_instances = prepare_k6_instances(
            load_nodes=load_nodes_list,
            login=LOAD_NODE_SSH_USER,
            pkey=LOAD_NODE_SSH_PRIVATE_KEY_PATH,
            load_params=load_params,
            ssh_port=2222,
        )
        with allure.step("Run load"):
            multi_node_k6_run(k6_load_instances)
