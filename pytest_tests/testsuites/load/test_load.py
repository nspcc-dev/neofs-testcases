import allure
import pytest
from common import LOAD_NODE_SSH_PRIVATE_KEY_PATH, LOAD_NODE_SSH_USER, LOAD_NODES
from neofs_testlib.hosting import Hosting

from pytest_tests.helpers.k6 import LoadParams
from pytest_tests.steps.load import (
    clear_cache_and_data,
    get_storage_host_endpoints,
    multi_node_k6_run,
    prepare_k6_instances,
)

CONTAINERS_COUNT = 1
OBJ_COUNT = 3


class TestLoad:
    @pytest.fixture(autouse=True)
    def clear_cache_and_data(self, hosting: Hosting):
        clear_cache_and_data(hosting=hosting)

    @pytest.mark.parametrize("obj_size, out_file", [(1000, "1mb_200.json")])
    @pytest.mark.parametrize("writers, readers, deleters", [(140, 60, 0), (200, 0, 0)])
    @pytest.mark.parametrize("load_time", [200, 900])
    @pytest.mark.parametrize("node_count", [4])
    def test_grpc_benchmark(
        self,
        obj_size,
        out_file,
        writers,
        readers,
        deleters,
        load_time,
        node_count,
        hosting: Hosting,
    ):
        allure.dynamic.title(
            f"Benchmark test - node_count = {node_count}, "
            f"writers = {writers} readers = {readers}, "
            f"deleters = {deleters}, obj_size = {obj_size}, "
            f"load_time = {load_time}"
        )
        with allure.step("Get endpoints"):
            endpoints_list = get_storage_host_endpoints(hosting=hosting)
            endpoints = ",".join(endpoints_list[:node_count])
        load_params = LoadParams(
            endpoint=endpoints,
            obj_size=obj_size,
            containers_count=CONTAINERS_COUNT,
            out_file=out_file,
            obj_count=OBJ_COUNT,
            writers=writers,
            readers=readers,
            deleters=deleters,
            load_time=load_time,
            load_type="grpc",
        )
        k6_load_instances = prepare_k6_instances(
            load_nodes=LOAD_NODES.split(','),
            login=LOAD_NODE_SSH_USER,
            pkey=LOAD_NODE_SSH_PRIVATE_KEY_PATH,
            load_params=load_params,
        )
        with allure.step("Run load"):
            multi_node_k6_run(k6_load_instances)
