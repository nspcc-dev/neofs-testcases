from contextlib import contextmanager
from dataclasses import dataclass
from time import sleep

import allure

from pytest_tests.helpers.remote_process import RemoteProcess
from pytest_tests.helpers.ssh_helper import HostClient

EXIT_RESULT_CODE = 0


@dataclass
class LoadParams:
    obj_size: int
    containers_count: int
    out_file: str
    obj_count: int
    writers_percent: int
    load_time: int
    clients_count: int
    load_type: str
    endpoint: str


class K6:
    def __init__(self, load_params: LoadParams, host_client: HostClient):

        self.load_params = load_params
        self.host_client = host_client

        self._k6_dir = None
        self._k6_result = None

        self._k6_process = None
        self._k6_stop_attempts = 5
        self._k6_stop_timeout = 15

    @property
    def process_dir(self) -> str:
        return self._k6_process.process_dir

    @property
    def k6_dir(self) -> str:
        if not self._k6_dir:
            self._k6_dir = self.host_client.exec("locate -l 1 'k6'").stdout.strip("\n")
        return self._k6_dir

    @allure.step("Prepare containers and objects")
    def prepare(self) -> str:
        self._k6_dir = self.k6_dir
        if self.load_params.load_type == "http" or self.load_params.load_type == "grpc":
            command = (
                f"{self.k6_dir}/scenarios/preset/preset_grpc.py "
                f"--size {self.load_params.obj_size}  "
                f"--containers {self.load_params.containers_count} "
                f"--out {self.load_params.load_type}_{self.load_params.out_file} "
                f"--endpoint {self.load_params.endpoint} "
                f"--preload_obj {self.load_params.obj_count} "
            )
            terminal = self.host_client.exec(command)
            return terminal.stdout.strip("\n")
        elif self.load_params.load_type == "s3":
            command = (
                f"{self.k6_dir}/scenarios/preset/preset_s3.py --size {self.load_params.obj_size} "
                f"--buckets {self.load_params.containers_count} "
                f"--out {self.load_params.load_type}_{self.load_params.out_file} "
                f"--endpoint {self.load_params.endpoint} "
                f"--preload_obj {self.load_params.obj_count} "
                f"--location load-1-1"
            )
            terminal = self.host_client.exec(command)
            return terminal.stdout.strip("\n")
        raise AssertionError("Wrong K6 load type")

    @allure.step("Start K6 on initiator")
    def start(self) -> None:

        self._k6_dir = self.k6_dir
        command = (
            f"{self.k6_dir}/k6 run -e "
            f"PROFILE={self.load_params.writers_percent}:{self.load_params.load_time} "
            f"-e WRITE_OBJ_SIZE={self.load_params.obj_size} "
            f"-e CLIENTS={self.load_params.clients_count} -e NODES={self.load_params.endpoint} "
            f"-e PREGEN_JSON={self.k6_dir}/{self.load_params.load_type}_{self.load_params.out_file} "
            f"{self.k6_dir}/scenarios/{self.load_params.load_type}.js"
        )
        self._k6_process = RemoteProcess.create(command, self.host_client)

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
    def start_context(
        self, warm_up_time: int = 0, expected_finish: bool = False, expected_fail: bool = False
    ) -> None:
        self.start()
        sleep(warm_up_time)
        try:
            yield self
        except Exception as err:
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
