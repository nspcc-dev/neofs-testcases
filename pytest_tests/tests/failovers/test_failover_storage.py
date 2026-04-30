import logging
import queue
import random
import re
import threading
import time
from typing import Any, Callable

import allure
import pytest
from helpers.complex_object_actions import (
    get_nodes_with_object,
    get_nodes_without_object,
    wait_object_replication,
)
from helpers.container import create_container
from helpers.file_helper import generate_file, get_file_hash
from helpers.neofs_verbs import (
    delete_object,
    get_netmap_netinfo,
    get_object,
    put_object,
    put_object_to_random_node,
    search_object,
)
from helpers.node_management import storage_node_healthcheck, wait_all_storage_nodes_returned
from helpers.wellknown_acl import PUBLIC_ACL
from neofs_testlib.env.env import NeoFSEnv, StorageNode
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

logger = logging.getLogger("NeoLogger")


class TestFailoverStorage:
    INCOMPLETE_STATUS_RETRY_DELAY_SEC = 2
    INCOMPLETE_STATUS_RETRY_ATTEMPTS = 45
    INCOMPLETE_STATUS_PATTERNS = (
        "incomplete status",
        "incomplete object",
        "object partially",
        "result may be incomplete",
    )
    RETRYABLE_OPERATION_ERROR_PATTERNS = (
        "context cancelled",
        "endpoints failed",
        "i/o timeout",
        "code = unavailable",
        "connection refused",
        "can't create api client",
        "gRPC dial",
    )

    def _has_incomplete_status(self, text: str) -> bool:
        text_lower = text.lower()
        return any(pattern in text_lower for pattern in self.INCOMPLETE_STATUS_PATTERNS)

    def _has_retryable_operation_error(self, text: str) -> bool:
        text_lower = text.lower()
        return any(pattern in text_lower for pattern in self.RETRYABLE_OPERATION_ERROR_PATTERNS)

    class RetryableIncompleteStatusWaitError(AssertionError):
        """Signals transient state while waiting for incomplete status."""

    @retry(
        wait=wait_fixed(INCOMPLETE_STATUS_RETRY_DELAY_SEC),
        stop=stop_after_attempt(INCOMPLETE_STATUS_RETRY_ATTEMPTS),
        retry=retry_if_exception_type(RetryableIncompleteStatusWaitError),
        reraise=True,
    )
    def _assert_operation_eventually_incomplete(
        self,
        operation_name: str,
        operation: Callable[[], Any],
        accept_success_result: Callable[[Any], bool] | None = None,
    ) -> str:
        try:
            result = operation()
        except Exception as exc:
            error_message = str(exc)
            if self._has_incomplete_status(error_message):
                logger.info(f"{operation_name} returned incomplete status as expected: {error_message}")
                return error_message
            if self._has_retryable_operation_error(error_message):
                logger.info(
                    f"{operation_name} returned transient error before incomplete status, will retry: {error_message}"
                )
                raise self.RetryableIncompleteStatusWaitError(error_message) from exc
            if accept_success_result is not None:
                oids_in_message = re.findall(r"(\w{43,44})", error_message)
                if accept_success_result(oids_in_message):
                    logger.info(f"{operation_name} outcome acceptable (expected OID(s) in CLI output): {error_message}")
                    return error_message
            raise

        if accept_success_result is not None and accept_success_result(result):
            logger.info(f"{operation_name} succeeded with acceptable outcome: {result!r}")
            return str(result)

        raise self.RetryableIncompleteStatusWaitError(
            f"{operation_name} unexpectedly succeeded before reporting incomplete status"
        )

    def _assert_operation_eventually_incomplete_or_fail(
        self,
        operation_name: str,
        operation: Callable[[], Any],
        accept_success_result: Callable[[Any], bool] | None = None,
    ) -> str:
        try:
            return self._assert_operation_eventually_incomplete(
                operation_name,
                operation,
                accept_success_result=accept_success_result,
            )
        except self.RetryableIncompleteStatusWaitError as exc:
            suffix = " or an acceptable successful result" if accept_success_result is not None else ""
            raise AssertionError(
                f"{operation_name} did not return incomplete status{suffix} in "
                f"{self.INCOMPLETE_STATUS_RETRY_ATTEMPTS * self.INCOMPLETE_STATUS_RETRY_DELAY_SEC}s; last error: {exc}"
            ) from exc

    @pytest.fixture
    def after_run_return_all_stopped_storage_nodes(self, neofs_env_function_scope: NeoFSEnv):
        yield
        unavailable_nodes = []
        for node in neofs_env_function_scope.storage_nodes:
            try:
                storage_node_healthcheck(node)
            except Exception:
                unavailable_nodes.append(node)
        self.return_stopped_storage_nodes(neofs_env_function_scope, unavailable_nodes)

    @allure.step("Return all stopped hosts")
    def return_stopped_storage_nodes(self, neofs_env: NeoFSEnv, stopped_nodes: list[StorageNode]) -> None:
        for node in stopped_nodes:
            with allure.step(f"Start {node}"):
                try:
                    node.start(fresh=False)
                except RuntimeError as exc:
                    if "already been started" not in str(exc):
                        raise

        wait_all_storage_nodes_returned(neofs_env)

    @allure.title("Lose and return storage node's process")
    @pytest.mark.parametrize("hard_restart", [True, False])
    @pytest.mark.simple
    def test_storage_node_failover(
        self,
        default_wallet,
        neofs_env_function_scope: NeoFSEnv,
        after_run_return_all_stopped_storage_nodes,
        hard_restart,
    ):
        self.neofs_env = neofs_env_function_scope
        self.shell = self.neofs_env.shell

        wallet = default_wallet
        placement_rule = "REP 2 IN X CBF 2 SELECT 2 FROM * AS X"
        source_file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        cid = create_container(
            wallet.path,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            rule=placement_rule,
            basic_acl=PUBLIC_ACL,
        )
        oid = put_object_to_random_node(wallet.path, source_file_path, cid, shell=self.shell, neofs_env=self.neofs_env)
        nodes_with_object = wait_object_replication(
            cid, oid, 2, shell=self.shell, nodes=self.neofs_env.storage_nodes, neofs_env=self.neofs_env
        )

        for node_to_stop in nodes_with_object:
            if hard_restart:
                node_to_stop.kill()
            else:
                node_to_stop.stop()

            object_nodes_after_stop = wait_object_replication(
                cid,
                oid,
                2,
                shell=self.shell,
                nodes=[sn for sn in self.neofs_env.storage_nodes if sn != node_to_stop],
                neofs_env=self.neofs_env,
            )
            assert node_to_stop not in object_nodes_after_stop

            with allure.step("Check object data is not corrupted"):
                for node in self.neofs_env.storage_nodes:
                    if node != node_to_stop:
                        got_file_path = get_object(wallet.path, cid, oid, shell=self.shell, endpoint=node.endpoint)
                        assert get_file_hash(source_file_path) == get_file_hash(got_file_path)

            with allure.step("Return stopped storage nodes"):
                self.return_stopped_storage_nodes(self.neofs_env, [node_to_stop])

            with allure.step("Check object data is not corrupted"):
                wait_object_replication(
                    cid, oid, 2, shell=self.shell, nodes=self.neofs_env.storage_nodes, neofs_env=self.neofs_env
                )
                for node in self.neofs_env.storage_nodes:
                    got_file_path = get_object(wallet.path, cid, oid, shell=self.shell, endpoint=node.endpoint)
                    assert get_file_hash(source_file_path) == get_file_hash(got_file_path)

    @pytest.mark.simple
    def test_put_get_without_storage_node(
        self,
        default_wallet,
        neofs_env_function_scope: NeoFSEnv,
        after_run_return_all_stopped_storage_nodes,
    ):
        self.neofs_env = neofs_env_function_scope
        self.shell = self.neofs_env.shell

        with allure.step("Kill one storage node"):
            dead_node = self.neofs_env.storage_nodes[0]
            alive_nodes = self.neofs_env.storage_nodes[1:]

            dead_node.kill()

        with allure.step("Create container"):
            wallet = default_wallet
            placement_rule = "REP 3"
            cid = create_container(
                wallet.path,
                shell=self.shell,
                endpoint=alive_nodes[0].endpoint,
                rule=placement_rule,
                basic_acl=PUBLIC_ACL,
            )

        with allure.step("Put objects"):
            for _ in range(10):
                source_file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
                oid = put_object(
                    wallet.path,
                    source_file_path,
                    cid,
                    shell=self.shell,
                    endpoint=random.choice(alive_nodes).endpoint,
                )
                wait_object_replication(cid, oid, 3, shell=self.shell, nodes=alive_nodes, neofs_env=self.neofs_env)

        with allure.step("Get last object"):
            got_file_path = get_object(wallet.path, cid, oid, shell=self.shell, endpoint=alive_nodes[0].endpoint)
            assert get_file_hash(source_file_path) == get_file_hash(got_file_path)

        with allure.step("Return stopped storage node"):
            self.return_stopped_storage_nodes(self.neofs_env, [dead_node])

        with allure.step("Get last object from previously dead node"):
            got_file_path = get_object(wallet.path, cid, oid, shell=self.shell, endpoint=dead_node.endpoint)
            assert get_file_hash(source_file_path) == get_file_hash(got_file_path)

    @pytest.mark.simple
    def test_put_get_without_storage_nodes(
        self,
        default_wallet,
        neofs_env_function_scope: NeoFSEnv,
        after_run_return_all_stopped_storage_nodes,
    ):
        self.neofs_env = neofs_env_function_scope
        self.shell = self.neofs_env.shell

        with allure.step("Kill two storage nodes"):
            dead_nodes = self.neofs_env.storage_nodes[:2]
            alive_nodes = self.neofs_env.storage_nodes[2:]

            for dead_node in dead_nodes:
                dead_node.kill()

        with allure.step("Create container"):
            wallet = default_wallet
            placement_rule = "REP 3"
            cid = create_container(
                wallet.path,
                shell=self.shell,
                endpoint=alive_nodes[0].endpoint,
                rule=placement_rule,
                basic_acl=PUBLIC_ACL,
            )

        with allure.step("Try to put object and expect error"):
            source_file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            with pytest.raises(
                Exception, match=r"(incomplete object PUT by placement|Object partially \(incomplete status\) stored)"
            ):
                put_object(
                    wallet.path,
                    source_file_path,
                    cid,
                    shell=self.shell,
                    endpoint=alive_nodes[0].endpoint,
                )

        with allure.step("Return stopped storage node"):
            self.return_stopped_storage_nodes(self.neofs_env, dead_nodes)

    def test_get_forwarding(
        self,
        default_wallet,
        neofs_env_function_scope: NeoFSEnv,
        after_run_return_all_stopped_storage_nodes,
    ):
        self.neofs_env = neofs_env_function_scope
        self.shell = self.neofs_env.shell

        with allure.step("Create container"):
            wallet = default_wallet
            placement_rule = "REP 2 IN X CBF 1 SELECT 2 FROM * AS X"
            cid = create_container(
                wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.storage_nodes[0].endpoint,
                rule=placement_rule,
                basic_acl=PUBLIC_ACL,
            )

        with allure.step("Put large object"):
            storage_node = self.neofs_env.storage_nodes[0]
            net_info = get_netmap_netinfo(
                wallet=storage_node.wallet.path,
                wallet_config=storage_node.cli_config,
                endpoint=storage_node.endpoint,
                shell=self.shell,
            )
            large_file_size = net_info["maximum_object_size"]
            source_file_path = generate_file(large_file_size)
            oid = put_object(
                wallet.path,
                source_file_path,
                cid,
                shell=self.shell,
                endpoint=self.neofs_env.storage_nodes[0].endpoint,
            )

        with allure.step("Get info about nodes with object"):
            nodes_with_object = get_nodes_with_object(
                cid,
                oid,
                shell=self.shell,
                nodes=self.neofs_env.storage_nodes,
                neofs_env=self.neofs_env,
            )

            nodes_without_object = get_nodes_without_object(
                wallet.path,
                cid,
                oid,
                shell=self.shell,
                nodes=self.neofs_env.storage_nodes,
            )

        with allure.step("Send get request to a random node without object and turn off a node with object"):
            exception_queue = queue.Queue()

            def get_object_from_node():
                for _ in range(5):
                    try:
                        logger.info(f"{nodes_without_object=}")
                        got_file_path = get_object(
                            wallet.path,
                            cid,
                            oid,
                            shell=self.shell,
                            endpoint=random.choice(nodes_without_object).endpoint,
                        )
                        assert get_file_hash(source_file_path) == get_file_hash(got_file_path), "Invalid file cache"
                    except Exception as e:
                        exception_queue.put(e)
                    time.sleep(2)

            get_thread = threading.Thread(target=get_object_from_node)
            get_thread.start()

            for _ in range(2):
                node_to_kill = random.choice(nodes_with_object)
                node_to_kill.kill()
                node_to_kill.start(fresh=False)
                wait_all_storage_nodes_returned(self.neofs_env)

            get_thread.join()
            if not exception_queue.empty():
                exc = exception_queue.get()
                raise AssertionError(f"Exception in thread: {exc}")

    @allure.title("Incomplete status on PUT/DELETE/SEARCH with one node down")
    def test_incomplete_status_single_node_down_rep4(
        self,
        default_wallet,
        neofs_env_function_scope: NeoFSEnv,
        after_run_return_all_stopped_storage_nodes,
    ):
        self.neofs_env = neofs_env_function_scope
        self.shell = self.neofs_env.shell

        wallet = default_wallet
        alive_nodes = self.neofs_env.storage_nodes[1:]
        dead_node = self.neofs_env.storage_nodes[0]
        operation_endpoint = alive_nodes[0].endpoint

        with allure.step("Create REP 4 container"):
            cid = create_container(
                wallet.path,
                shell=self.shell,
                endpoint=alive_nodes[0].endpoint,
                rule="REP 4",
                basic_acl=PUBLIC_ACL,
            )

        with allure.step("Put baseline object to use in DELETE/SEARCH checks"):
            source_file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oid = put_object(
                wallet.path,
                source_file_path,
                cid,
                shell=self.shell,
                endpoint=alive_nodes[0].endpoint,
            )
            wait_object_replication(
                cid, oid, 4, shell=self.shell, nodes=self.neofs_env.storage_nodes, neofs_env=self.neofs_env
            )

        with allure.step("Kill one storage node"):
            dead_node.kill()

        with allure.step("Wait baseline object replicated to alive nodes"):
            wait_object_replication(cid, oid, 3, shell=self.shell, nodes=alive_nodes, neofs_env=self.neofs_env)

        with allure.step("PUT eventually returns incomplete status"):
            put_source_file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            put_incomplete_error = self._assert_operation_eventually_incomplete_or_fail(
                operation_name="PUT",
                operation=lambda: put_object(
                    wallet.path,
                    put_source_file_path,
                    cid,
                    shell=self.shell,
                    endpoint=operation_endpoint,
                ),
            )
            match = re.search(r"OID:\s*([A-Za-z0-9]{43,44})", put_incomplete_error)
            assert match, f"PUT incomplete response does not contain object id: {put_incomplete_error}"
            incomplete_put_oid = match.group(1)

        with allure.step("SEARCH incomplete PUT object: incomplete status or success listing that object"):
            incomplete_put_filter_expr = [f"FileName EQ {put_source_file_path.split('/')[-1]}"]
            self._assert_operation_eventually_incomplete_or_fail(
                operation_name="SEARCH",
                accept_success_result=lambda found: incomplete_put_oid in found,
                operation=lambda: search_object(
                    wallet.path,
                    cid,
                    shell=self.shell,
                    endpoint=operation_endpoint,
                    filters=incomplete_put_filter_expr,
                ),
            )

        with allure.step("SEARCH baseline object: incomplete status or success listing that object"):
            filter_expr = [f"FileName EQ {source_file_path.split('/')[-1]}"]
            self._assert_operation_eventually_incomplete_or_fail(
                operation_name="SEARCH",
                accept_success_result=lambda found: oid in found,
                operation=lambda: search_object(
                    wallet.path,
                    cid,
                    shell=self.shell,
                    endpoint=operation_endpoint,
                    filters=filter_expr,
                ),
            )

        with allure.step("DELETE incomplete PUT object eventually returns incomplete status"):
            self._assert_operation_eventually_incomplete_or_fail(
                operation_name="DELETE",
                operation=lambda: delete_object(
                    wallet.path,
                    cid,
                    incomplete_put_oid,
                    shell=self.shell,
                    endpoint=operation_endpoint,
                ),
            )

        with allure.step("DELETE baseline object eventually returns incomplete status"):
            self._assert_operation_eventually_incomplete_or_fail(
                operation_name="DELETE",
                operation=lambda: delete_object(
                    wallet.path,
                    cid,
                    oid,
                    shell=self.shell,
                    endpoint=operation_endpoint,
                ),
            )
