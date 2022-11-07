import concurrent.futures
from dataclasses import asdict

import allure
from common import STORAGE_NODE_SERVICE_NAME_REGEX
from k6 import K6, LoadParams, LoadResults
from neofs_testlib.hosting import Hosting
from neofs_testlib.shell import SSHShell


@allure.title("Get services endpoints")
def get_services_endpoints(
    hosting: Hosting, service_name_regex: str, endpoint_attribute: str
) -> list[str]:
    service_configs = hosting.find_service_configs(service_name_regex)
    return [service_config.attributes[endpoint_attribute] for service_config in service_configs]


@allure.title("Clear cache and data from storage nodes")
def clear_cache_and_data(hosting: Hosting):
    service_configs = hosting.find_service_configs(STORAGE_NODE_SERVICE_NAME_REGEX)
    for service_config in service_configs:
        host = hosting.get_host_by_service(service_config.name)
        host.stop_service(service_config.name)
        host.delete_storage_node_data(service_config.name)
        host.start_service(service_config.name)


@allure.title("Prepare objects")
def prepare_objects(k6_instance: K6):
    k6_instance.prepare()


@allure.title("Prepare K6 instances and objects")
def prepare_k6_instances(load_nodes: list, login: str, pkey: str, load_params: LoadParams) -> list:
    k6_load_objects = []
    for load_node in load_nodes:
        ssh_client = SSHShell(host=load_node, login=login, private_key_path=pkey)
        k6_load_object = K6(load_params, ssh_client)
        k6_load_objects.append(k6_load_object)
    for k6_load_object in k6_load_objects:
        with allure.step("Prepare objects"):
            prepare_objects(k6_load_object)
    return k6_load_objects


@allure.title("Run K6")
def run_k6_load(k6_instance: K6) -> LoadResults:
    with allure.step("Executing load"):
        k6_instance.start()
        k6_instance.wait_until_finished(k6_instance.load_params.load_time * 2)
    with allure.step("Printing results"):
        k6_instance.get_k6_results()
        return k6_instance.parsing_results()


@allure.title("MultiNode K6 Run")
def multi_node_k6_run(k6_instances: list) -> dict:
    results = []
    avg_results = {}
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for k6_instance in k6_instances:
            futures.append(executor.submit(run_k6_load, k6_instance))
        for future in concurrent.futures.as_completed(futures):
            results.append(asdict(future.result()))
    for k6_result in results:
        for key in k6_result:
            try:
                avg_results[key] += k6_result[key] / len(results)
            except KeyError:
                avg_results[key] = k6_result[key] / len(results)
    return avg_results


@allure.title("Compare results")
def compare_load_results(result: dict, result_new: dict):
    for key in result:
        if result[key] != 0 and result_new[key] != 0:
            if (abs(result[key] - result_new[key]) / min(result[key], result_new[key])) < 0.25:
                continue
            else:
                raise AssertionError(f"Difference in {key} values more than 25%")
        elif result[key] == 0 and result_new[key] == 0:
            continue
        else:
            raise AssertionError(f"Unexpected zero value in {key}")
