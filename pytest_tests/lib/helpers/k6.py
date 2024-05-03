import concurrent.futures
import os
import re
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from time import sleep
from typing import Optional

import allure
from helpers.common import STORAGE_NODE_SERVICE_NAME_REGEX
from helpers.remote_process import RemoteProcess
from neofs_testlib.cli.neofs_authmate import NeofsAuthmate
from neofs_testlib.cli.neogo import NeoGo
from neofs_testlib.hosting import Hosting
from neofs_testlib.shell import CommandOptions, Shell, SSHShell
from neofs_testlib.shell.interfaces import InteractiveInput

NEOFS_AUTHMATE_PATH = "neofs-s3-authmate"
STOPPED_HOSTS = []

EXIT_RESULT_CODE = 0
LOAD_RESULTS_PATTERNS = {
    "grpc": {
        "write_ops": r"neofs_obj_put_total\W*\d*\W*(?P<write_ops>\d*\.\d*)",
        "read_ops": r"neofs_obj_get_total\W*\d*\W*(?P<read_ops>\d*\.\d*)",
    },
    "s3": {
        "write_ops": r"aws_obj_put_total\W*\d*\W*(?P<write_ops>\d*\.\d*)",
        "read_ops": r"aws_obj_get_total\W*\d*\W*(?P<write_ops>\d*\.\d*)",
    },
    "http": {"total_ops": r"http_reqs\W*\d*\W*(?P<total_ops>\d*\.\d*)"},
}


# Load node parameters
LOAD_NODES = os.getenv("LOAD_NODES", "127.0.0.1").split(",")
LOAD_NODE_SSH_USER = os.getenv("LOAD_NODE_SSH_USER", "k6")
LOAD_NODE_SSH_PRIVATE_KEY_PATH = os.getenv(
    "LOAD_NODE_SSH_PRIVATE_KEY_PATH", "../neofs-dev-env/services/k6_node/id_ed25519"
)
BACKGROUND_WRITERS_COUNT = os.getenv("BACKGROUND_WRITERS_COUNT", 10)
BACKGROUND_READERS_COUNT = os.getenv("BACKGROUND_READERS_COUNT", 10)
BACKGROUND_OBJ_SIZE = os.getenv("BACKGROUND_OBJ_SIZE", 1024)
BACKGROUND_LOAD_MAX_TIME = os.getenv("BACKGROUND_LOAD_MAX_TIME", 600)

# Load run parameters

OBJ_SIZE = [int(o) for o in os.getenv("OBJ_SIZE", "1000").split(",")]
CONTAINERS_COUNT = [int(c) for c in os.getenv("CONTAINERS_COUNT", "1").split(",")]
OUT_FILE = os.getenv("OUT_FILE", "1mb_200.json").split(",")
OBJ_COUNT = [int(o) for o in os.getenv("OBJ_COUNT", "4").split(",")]
WRITERS = [int(w) for w in os.getenv("WRITERS", "200").split(",")]
READERS = [int(r) for r in os.getenv("READER", "0").split(",")]
DELETERS = [int(d) for d in os.getenv("DELETERS", "0").split(",")]
LOAD_TIME = [int(ld) for ld in os.getenv("LOAD_TIME", "200").split(",")]
LOAD_TYPE = os.getenv("LOAD_TYPE", "grpc").split(",")
LOAD_NODES_COUNT = [int(ldc) for ldc in os.getenv("LOAD_NODES_COUNT", "1").split(",")]
STORAGE_NODE_COUNT = [int(s) for s in os.getenv("STORAGE_NODE_COUNT", "4").split(",")]
CONTAINER_PLACEMENT_POLICY = os.getenv("CONTAINER_PLACEMENT_POLICY", "REP 1 IN X CBF 1 SELECT 1  FROM * AS X")


@dataclass
class LoadParams:
    load_type: str
    endpoint: str
    writers: Optional[int] = None
    readers: Optional[int] = None
    deleters: Optional[int] = None
    clients: Optional[int] = None
    containers_count: Optional[int] = None
    out_file: Optional[str] = None
    load_time: Optional[int] = None
    obj_count: Optional[int] = None
    obj_size: Optional[int] = None
    registry_file: Optional[str] = None


@dataclass
class LoadResults:
    data_sent: float = 0.0
    data_received: float = 0.0
    read_ops: float = 0.0
    write_ops: float = 0.0
    total_ops: float = 0.0


class K6:
    def __init__(self, load_params: LoadParams, shell: Shell):
        self.load_params = load_params
        self.shell = shell
        self.k6_dir = "/xk6-neofs"

        self._k6_result = None

        self._k6_process = None
        self._k6_stop_attempts = 5
        self._k6_stop_timeout = 15

    @property
    def process_dir(self) -> str:
        return self._k6_process.process_dir

    @allure.step("Prepare containers and objects")
    def prepare(self) -> str:
        if self.load_params.load_type == "http" or self.load_params.load_type == "grpc":
            command = (
                f"{self.k6_dir}/scenarios/preset/preset_grpc.py "
                f"--size {self.load_params.obj_size}  "
                f"--containers {self.load_params.containers_count} "
                f"--out {self.k6_dir}/{self.load_params.load_type}_{self.load_params.out_file} "
                f"--endpoint {self.load_params.endpoint.split(',')[0]} "
                f"--preload_obj {self.load_params.obj_count} "
            )
            terminal = self.shell.exec(command)
            return terminal.stdout.strip("\n")
        elif self.load_params.load_type == "s3":
            command = (
                f"{self.k6_dir}/scenarios/preset/preset_s3.py --size {self.load_params.obj_size} "
                f"--buckets {self.load_params.containers_count} "
                f"--out {self.k6_dir}/{self.load_params.load_type}_{self.load_params.out_file} "
                f"--endpoint {self.load_params.endpoint.split(',')[0]} "
                f"--preload_obj {self.load_params.obj_count} "
                f"--location load-1-1"
            )
            terminal = self.shell.exec(command)
            return terminal.stdout.strip("\n")
        else:
            raise AssertionError("Wrong K6 load type")

    @allure.step("Generate K6 command")
    def _generate_env_variables(self, load_params: LoadParams, k6_dir: str) -> str:
        env_vars = {
            "DURATION": load_params.load_time or None,
            "WRITE_OBJ_SIZE": load_params.obj_size or None,
            "WRITERS": load_params.writers or 0,
            "READERS": load_params.readers or 0,
            "DELETERS": load_params.deleters or 0,
            "REGISTRY_FILE": load_params.registry_file or None,
            "CLIENTS": load_params.clients or None,
            f"{self.load_params.load_type.upper()}_ENDPOINTS": self.load_params.endpoint,
            "PREGEN_JSON": (
                f"{self.k6_dir}/{self.load_params.load_type}_{self.load_params.out_file}"
                if load_params.out_file
                else None
            ),
        }
        allure.attach(
            "\n".join(f"{param}: {value}" for param, value in env_vars.items()),
            "K6 ENV variables",
            allure.attachment_type.TEXT,
        )
        return " ".join([f"-e {param}={value}" for param, value in env_vars.items() if value is not None])

    @allure.step("Start K6 on initiator")
    def start(self) -> None:
        command = (
            f"{self.k6_dir}/k6 run {self._generate_env_variables(self.load_params, self.k6_dir)} "
            f"{self.k6_dir}/scenarios/{self.load_params.load_type}.js"
        )
        self._k6_process = RemoteProcess.create(command, self.shell)

    @allure.step("Wait until K6 is finished")
    def wait_until_finished(self, timeout: int = 0, k6_should_be_running: bool = False) -> None:
        if self._k6_process is None:
            assert "No k6 instances were executed"
        if k6_should_be_running:
            assert self._k6_process.running(), "k6 should be running."
        for __attempt in reversed(range(5)) if timeout else [0]:
            if not self._k6_process.running():
                return
            if __attempt:  # no sleep in last iteration
                sleep(int(timeout / 5))
        self._stop_k6()
        raise TimeoutError(f"Expected K6 finished in {timeout} sec.")

    @contextmanager
    def start_context(self, warm_up_time: int = 0, expected_finish: bool = False, expected_fail: bool = False) -> None:
        self.start()
        sleep(warm_up_time)
        try:
            yield self
        except Exception:
            if self._k6_process.running():
                self._kill_k6()
            raise

        if expected_fail:
            self._kill_k6()
        elif expected_finish:
            if self._k6_process.running():
                self._kill_k6()
                raise AssertionError("K6 has not finished in expected time")
            else:
                self._k6_should_be_finished()
        else:
            self._stop_k6()

    @allure.step("Get K6 results")
    def get_k6_results(self) -> None:
        self.__log_k6_output()

    @allure.step("Assert K6 should be finished")
    def _k6_should_be_finished(self) -> None:
        k6_rc = self._k6_process.rc()
        assert k6_rc == 0, f"K6 unexpectedly finished with RC {k6_rc}"

    @allure.step("Terminate K6 on initiator")
    def stop(self) -> None:
        if not self._k6_process.running():
            raise AssertionError("K6 unexpectedly finished")

        self._stop_k6()

        k6_rc = self._k6_process.rc()
        assert k6_rc == EXIT_RESULT_CODE, f"Return code of K6 job should be 0, but {k6_rc}"

    def check_k6_is_running(self) -> bool:
        if self._k6_process:
            return self._k6_process.running()
        return False

    @property
    def is_finished(self) -> bool:
        return not self._k6_process.running()

    def parsing_results(self) -> LoadResults:
        output = self._k6_process.stdout(full=True).replace("\n", "")
        metric_regex_map = {
            "data_received": r"data_received\W*\d*.\d*.\w*\W*(?P<data_received>\d*)",
            "data_sent": r"data_sent\W*\d*.\d*.\w*\W*(?P<data_sent>\d*)",
        }
        metric_regex_map.update(LOAD_RESULTS_PATTERNS[self.load_params.load_type])
        metric_values = {}
        for metric_name, metric_regex in metric_regex_map.items():
            match = re.search(metric_regex, output)
            if match:
                metric_values[metric_name] = float(match.group(metric_name))
                continue
            metric_values[metric_name] = 0.0
        load_result = LoadResults(**metric_values)
        return load_result

    @allure.step("Try to stop K6 with SIGTERM")
    def _stop_k6(self) -> None:
        for __attempt in range(self._k6_stop_attempts):
            if not self._k6_process.running():
                break

            self._k6_process.stop()
            sleep(self._k6_stop_timeout)
        else:
            raise AssertionError("Can not stop K6 process within timeout")

    def _kill_k6(self) -> None:
        self._k6_process.kill()

    @allure.step("Log K6 output")
    def __log_k6_output(self) -> None:
        allure.attach(self._k6_process.stdout(full=True), "K6 output", allure.attachment_type.TEXT)


@allure.title("Get services endpoints")
def get_services_endpoints(hosting: Hosting, service_name_regex: str, endpoint_attribute: str) -> list[str]:
    service_configs = hosting.find_service_configs(service_name_regex)
    return [service_config.attributes[endpoint_attribute] for service_config in service_configs]


@allure.title("Stop nodes")
def stop_unused_nodes(storage_nodes: list, used_nodes_count: int):
    for node in storage_nodes[used_nodes_count:]:
        host = node.host
        STOPPED_HOSTS.append(host)
        host.stop_host("hard")


@allure.title("Start nodes")
def start_stopped_nodes():
    for host in STOPPED_HOSTS:
        host.start_host()
        STOPPED_HOSTS.remove(host)


@allure.title("Init s3 client")
def init_s3_client(
    load_nodes: list,
    login: str,
    pkey: str,
    container_placement_policy: str,
    hosting: Hosting,
    ssh_port: int,
):
    service_configs = hosting.find_service_configs(STORAGE_NODE_SERVICE_NAME_REGEX)
    host = hosting.get_host_by_service(service_configs[0].name)
    wallet_path = service_configs[0].attributes["wallet_path"]
    neogo_cli_config = host.get_cli_config("neo-go")
    neogo_wallet = NeoGo(shell=host.get_shell(), neo_go_exec_path=neogo_cli_config.exec_path).wallet
    dump_keys_output = neogo_wallet.dump_keys(wallet=wallet_path, wallet_config=None).stdout
    public_key = str(re.search(r":\n(?P<public_key>.*)", dump_keys_output).group("public_key"))
    node_endpoint = service_configs[0].attributes["rpc_endpoint"]
    # prompt_pattern doesn't work at the moment
    for load_node in load_nodes:
        ssh_client = SSHShell(host=load_node, port=ssh_port, login=login, private_key_path=pkey)
        path = ssh_client.exec(r"sudo find . -name 'k6' -exec dirname {} \; -quit").stdout.strip("\n")
        neofs_authmate_exec = NeofsAuthmate(ssh_client, NEOFS_AUTHMATE_PATH)
        issue_secret_output = neofs_authmate_exec.secret.issue(
            wallet=f"{path}/scenarios/files/wallet.json",
            peer=node_endpoint,
            bearer_rules=f"{path}/scenarios/files/rules.json",
            gate_public_key=public_key,
            container_placement_policy=container_placement_policy,
            container_policy=f"{path}/scenarios/files/policy.json",
            wallet_password="",
        ).stdout
        aws_access_key_id = str(
            re.search(r"access_key_id.*:\s.(?P<aws_access_key_id>\w*)", issue_secret_output).group("aws_access_key_id")
        )
        aws_secret_access_key = str(
            re.search(r"secret_access_key.*:\s.(?P<aws_secret_access_key>\w*)", issue_secret_output).group(
                "aws_secret_access_key"
            )
        )
        # prompt_pattern doesn't work at the moment
        configure_input = [
            InteractiveInput(prompt_pattern=r"AWS Access Key ID.*", input=aws_access_key_id),
            InteractiveInput(prompt_pattern=r"AWS Secret Access Key.*", input=aws_secret_access_key),
            InteractiveInput(prompt_pattern=r".*", input=""),
            InteractiveInput(prompt_pattern=r".*", input=""),
        ]
        ssh_client.exec("aws configure", CommandOptions(interactive_inputs=configure_input))


@allure.title("Prepare objects")
def prepare_objects(k6_instance: K6):
    k6_instance.prepare()


@allure.title("Prepare K6 instances and objects")
def prepare_k6_instances(
    load_nodes: list,
    login: str,
    pkey: str,
    load_params: LoadParams,
    ssh_port: int,
    prepare: bool = True,
) -> list[K6]:
    k6_load_objects = []
    for load_node in load_nodes:
        ssh_client = SSHShell(port=ssh_port, host=load_node, login=login, private_key_path=pkey)
        k6_load_object = K6(load_params, ssh_client)
        k6_load_objects.append(k6_load_object)
    for k6_load_object in k6_load_objects:
        if prepare:
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
