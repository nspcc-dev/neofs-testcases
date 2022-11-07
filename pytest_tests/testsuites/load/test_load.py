from enum import Enum

import allure
import pytest
from common import (
    HTTP_GATE_SERVICE_NAME_REGEX,
    LOAD_NODE_SSH_PRIVATE_KEY_PATH,
    LOAD_NODE_SSH_USER,
    LOAD_NODES,
    STORAGE_NODE_SERVICE_NAME_REGEX,
)
from k6 import LoadParams
from load import (
    clear_cache_and_data,
    get_services_endpoints,
    multi_node_k6_run,
    prepare_k6_instances,
)
from neofs_testlib.hosting import Hosting


class LoadTime(Enum):
    EXPECTED_MAXIMUM = 200
    PMI_EXPECTATION = 900


CONTAINERS_COUNT = 1
OBJ_COUNT = 3


@pytest.mark.load
class TestLoad:
    @pytest.fixture(autouse=True)
    def clear_cache_and_data(self, hosting: Hosting):
        clear_cache_and_data(hosting=hosting)

    @pytest.mark.parametrize("obj_size, out_file", [(1000, "1mb_200.json")])
    @pytest.mark.parametrize("writers, readers, deleters", [(140, 60, 0), (200, 0, 0)])
    @pytest.mark.parametrize(
        "load_time", [LoadTime.EXPECTED_MAXIMUM.value, LoadTime.PMI_EXPECTATION.value]
    )
    @pytest.mark.parametrize("node_count", [4])
    @pytest.mark.benchmark
    @pytest.mark.grpc
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
            endpoints_list = get_services_endpoints(
                hosting=hosting,
                service_name_regex=STORAGE_NODE_SERVICE_NAME_REGEX,
                endpoint_attribute="rpc_endpoint",
            )
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
            load_nodes=LOAD_NODES,
            login=LOAD_NODE_SSH_USER,
            pkey=LOAD_NODE_SSH_PRIVATE_KEY_PATH,
            load_params=load_params,
        )
        with allure.step("Run load"):
            multi_node_k6_run(k6_load_instances)

    @pytest.mark.parametrize(
        "obj_size, out_file, writers",
        [
            (4, "4kb_300.json", 300),
            (16, "16kb_250.json", 250),
            (64, "64kb_250.json", 250),
            (128, "128kb_250.json", 250),
            (512, "512kb_200.json", 200),
            (1000, "1mb_200.json", 200),
            (8000, "8mb_150.json", 150),
            (32000, "32mb_150.json", 150),
            (128000, "128mb_100.json", 100),
            (512000, "512mb_50.json", 50),
        ],
    )
    @pytest.mark.parametrize(
        "load_time", [LoadTime.EXPECTED_MAXIMUM.value, LoadTime.PMI_EXPECTATION.value]
    )
    @pytest.mark.benchmark
    @pytest.mark.grpc
    def test_grpc_benchmark_write(
        self,
        obj_size,
        out_file,
        writers,
        load_time,
        hosting: Hosting,
    ):
        allure.dynamic.title(
            f"Single gate benchmark write test - "
            f"writers = {writers}, "
            f"obj_size = {obj_size}, "
            f"load_time = {load_time}"
        )
        with allure.step("Get endpoints"):
            endpoints_list = get_services_endpoints(
                hosting=hosting,
                service_name_regex=STORAGE_NODE_SERVICE_NAME_REGEX,
                endpoint_attribute="rpc_endpoint",
            )
            endpoints = ",".join(endpoints_list[:1])
        load_params = LoadParams(
            endpoint=endpoints,
            obj_size=obj_size,
            containers_count=CONTAINERS_COUNT,
            out_file=out_file,
            obj_count=OBJ_COUNT,
            writers=writers,
            readers=0,
            deleters=0,
            load_time=load_time,
            load_type="grpc",
        )
        k6_load_instances = prepare_k6_instances(
            load_nodes=LOAD_NODES,
            login=LOAD_NODE_SSH_USER,
            pkey=LOAD_NODE_SSH_PRIVATE_KEY_PATH,
            load_params=load_params,
        )
        with allure.step("Run load"):
            multi_node_k6_run(k6_load_instances)

    @pytest.mark.parametrize(
        "obj_size, out_file, writers, readers",
        [
            (8000, "8mb_350.json", 245, 105),
            (32000, "32mb_300.json", 210, 90),
            (128000, "128mb_100.json", 70, 30),
            (512000, "512mb_70.json", 49, 21),
        ],
    )
    @pytest.mark.parametrize(
        "load_time", [LoadTime.EXPECTED_MAXIMUM.value, LoadTime.PMI_EXPECTATION.value]
    )
    @pytest.mark.benchmark
    @pytest.mark.grpc
    def test_grpc_benchmark_write_read_70_30(
        self,
        obj_size,
        out_file,
        writers,
        readers,
        load_time,
        hosting: Hosting,
    ):
        allure.dynamic.title(
            f"Single gate benchmark write + read (70%/30%) test - "
            f"writers = {writers}, "
            f"readers = {readers}, "
            f"obj_size = {obj_size}, "
            f"load_time = {load_time}"
        )
        with allure.step("Get endpoints"):
            endpoints_list = get_services_endpoints(
                hosting=hosting,
                service_name_regex=STORAGE_NODE_SERVICE_NAME_REGEX,
                endpoint_attribute="rpc_endpoint",
            )
            endpoints = ",".join(endpoints_list[:1])
        load_params = LoadParams(
            endpoint=endpoints,
            obj_size=obj_size,
            containers_count=CONTAINERS_COUNT,
            out_file=out_file,
            obj_count=500,
            writers=writers,
            readers=readers,
            deleters=0,
            load_time=load_time,
            load_type="grpc",
        )
        k6_load_instances = prepare_k6_instances(
            load_nodes=LOAD_NODES,
            login=LOAD_NODE_SSH_USER,
            pkey=LOAD_NODE_SSH_PRIVATE_KEY_PATH,
            load_params=load_params,
        )
        with allure.step("Run load"):
            multi_node_k6_run(k6_load_instances)

    @pytest.mark.parametrize(
        "obj_size, out_file, readers",
        [
            (4, "4kb_300.json", 300),
            (16, "16kb_300.json", 300),
            (64, "64kb_300.json", 300),
            (128, "128kb_250.json", 250),
            (512, "512kb_150.json", 150),
            (1000, "1mb_150.json", 150),
            (8000, "8mb_150.json", 150),
            (32000, "32mb_100.json", 100),
            (128000, "128mb_25.json", 25),
            (512000, "512mb_25.json", 25),
        ],
    )
    @pytest.mark.parametrize(
        "load_time", [LoadTime.EXPECTED_MAXIMUM.value, LoadTime.PMI_EXPECTATION.value]
    )
    @pytest.mark.benchmark
    @pytest.mark.grpc
    def test_grpc_benchmark_read(
        self,
        obj_size,
        out_file,
        readers,
        load_time,
        hosting: Hosting,
    ):
        allure.dynamic.title(
            f"Single gate benchmark read test - "
            f"readers = {readers}, "
            f"obj_size = {obj_size}, "
            f"load_time = {load_time}"
        )
        with allure.step("Get endpoints"):
            endpoints_list = get_services_endpoints(
                hosting=hosting,
                service_name_regex=STORAGE_NODE_SERVICE_NAME_REGEX,
                endpoint_attribute="rpc_endpoint",
            )
            endpoints = ",".join(endpoints_list[:1])
        load_params = LoadParams(
            endpoint=endpoints,
            obj_size=obj_size,
            containers_count=1,
            out_file=out_file,
            obj_count=500,
            writers=0,
            readers=readers,
            deleters=0,
            load_time=load_time,
            load_type="grpc",
        )
        k6_load_instances = prepare_k6_instances(
            load_nodes=LOAD_NODES,
            login=LOAD_NODE_SSH_USER,
            pkey=LOAD_NODE_SSH_PRIVATE_KEY_PATH,
            load_params=load_params,
        )
        with allure.step("Run load"):
            multi_node_k6_run(k6_load_instances)

    @pytest.mark.parametrize(
        "obj_size, out_file, writers",
        [
            (4, "4kb_300.json", 300),
            (16, "16kb_250.json", 250),
            (64, "64kb_250.json", 250),
            (128, "128kb_250.json", 250),
            (512, "512kb_200.json", 200),
            (1000, "1mb_200.json", 200),
            (8000, "8mb_150.json", 150),
            (32000, "32mb_150.json", 150),
            (128000, "128mb_100.json", 100),
            (512000, "512mb_50.json", 50),
        ],
    )
    @pytest.mark.parametrize(
        "load_time", [LoadTime.EXPECTED_MAXIMUM.value, LoadTime.PMI_EXPECTATION.value]
    )
    @pytest.mark.benchmark
    @pytest.mark.http
    def test_http_benchmark_write(
        self,
        obj_size,
        out_file,
        writers,
        load_time,
        hosting: Hosting,
    ):
        allure.dynamic.title(
            f"Single gate benchmark write test - "
            f"writers = {writers}, "
            f"obj_size = {obj_size}, "
            f"load_time = {load_time}"
        )
        with allure.step("Get endpoints"):
            endpoints_list = get_services_endpoints(
                hosting=hosting,
                service_name_regex=HTTP_GATE_SERVICE_NAME_REGEX,
                endpoint_attribute="endpoint",
            )
            endpoints = ",".join(endpoints_list[:1])
        load_params = LoadParams(
            endpoint=endpoints,
            obj_size=obj_size,
            containers_count=CONTAINERS_COUNT,
            out_file=out_file,
            obj_count=OBJ_COUNT,
            writers=writers,
            readers=0,
            deleters=0,
            load_time=load_time,
            load_type="http",
        )
        k6_load_instances = prepare_k6_instances(
            load_nodes=LOAD_NODES,
            login=LOAD_NODE_SSH_USER,
            pkey=LOAD_NODE_SSH_PRIVATE_KEY_PATH,
            load_params=load_params,
        )
        with allure.step("Run load"):
            multi_node_k6_run(k6_load_instances)

    @pytest.mark.parametrize(
        "obj_size, out_file, writers, readers",
        [
            (8000, "8mb_350.json", 245, 105),
            (32000, "32mb_300.json", 210, 90),
            (128000, "128mb_100.json", 70, 30),
            (512000, "512mb_70.json", 49, 21),
        ],
    )
    @pytest.mark.parametrize(
        "load_time", [LoadTime.EXPECTED_MAXIMUM.value, LoadTime.PMI_EXPECTATION.value]
    )
    @pytest.mark.benchmark
    @pytest.mark.http
    def test_http_benchmark_write_read_70_30(
        self,
        obj_size,
        out_file,
        writers,
        readers,
        load_time,
        hosting: Hosting,
    ):
        allure.dynamic.title(
            f"Single gate benchmark write + read (70%/30%) test - "
            f"writers = {writers}, "
            f"readers = {readers}, "
            f"obj_size = {obj_size}, "
            f"load_time = {load_time}"
        )
        with allure.step("Get endpoints"):
            endpoints_list = get_services_endpoints(
                hosting=hosting,
                service_name_regex=HTTP_GATE_SERVICE_NAME_REGEX,
                endpoint_attribute="endpoint",
            )
            endpoints = ",".join(endpoints_list[:1])
        load_params = LoadParams(
            endpoint=endpoints,
            obj_size=obj_size,
            containers_count=CONTAINERS_COUNT,
            out_file=out_file,
            obj_count=500,
            writers=writers,
            readers=readers,
            deleters=0,
            load_time=load_time,
            load_type="http",
        )
        k6_load_instances = prepare_k6_instances(
            load_nodes=LOAD_NODES,
            login=LOAD_NODE_SSH_USER,
            pkey=LOAD_NODE_SSH_PRIVATE_KEY_PATH,
            load_params=load_params,
        )
        with allure.step("Run load"):
            multi_node_k6_run(k6_load_instances)

    @pytest.mark.parametrize(
        "obj_size, out_file, readers",
        [
            (4, "4kb_300.json", 300),
            (16, "16kb_300.json", 300),
            (64, "64kb_300.json", 300),
            (128, "128kb_250.json", 250),
            (512, "512kb_150.json", 150),
            (1000, "1mb_150.json", 150),
            (8000, "8mb_150.json", 150),
            (32000, "32mb_100.json", 100),
            (128000, "128mb_25.json", 25),
            (512000, "512mb_25.json", 25),
        ],
    )
    @pytest.mark.parametrize(
        "load_time", [LoadTime.EXPECTED_MAXIMUM.value, LoadTime.PMI_EXPECTATION.value]
    )
    @pytest.mark.benchmark
    @pytest.mark.http
    def test_http_benchmark_read(
        self,
        obj_size,
        out_file,
        readers,
        load_time,
        hosting: Hosting,
    ):
        allure.dynamic.title(
            f"Single gate benchmark read test - "
            f"readers = {readers}, "
            f"obj_size = {obj_size}, "
            f"load_time = {load_time}"
        )
        with allure.step("Get endpoints"):
            endpoints_list = get_services_endpoints(
                hosting=hosting,
                service_name_regex=HTTP_GATE_SERVICE_NAME_REGEX,
                endpoint_attribute="endpoint",
            )
            endpoints = ",".join(endpoints_list[:1])
        load_params = LoadParams(
            endpoint=endpoints,
            obj_size=obj_size,
            containers_count=1,
            out_file=out_file,
            obj_count=500,
            writers=0,
            readers=readers,
            deleters=0,
            load_time=load_time,
            load_type="http",
        )
        k6_load_instances = prepare_k6_instances(
            load_nodes=LOAD_NODES,
            login=LOAD_NODE_SSH_USER,
            pkey=LOAD_NODE_SSH_PRIVATE_KEY_PATH,
            load_params=load_params,
        )
        with allure.step("Run load"):
            multi_node_k6_run(k6_load_instances)
