import logging
import time
from contextlib import contextmanager

import allure
import pytest
from helpers.container import (
    create_container,
    delete_container,
    get_container,
    list_containers,
    set_container_attributes,
    wait_for_container_deletion,
)
from neofs_env.neofs_env_test_base import TestNeofsBase

logger = logging.getLogger("NeoLogger")

BASIC_LOCK_TIME = 10


@contextmanager
def container_lock_timer(lock_duration: int):
    start_time = time.time()
    lock_time = int(start_time) + lock_duration

    yield lock_time

    elapsed = time.time() - start_time
    remaining_time = lock_duration - elapsed + 2
    if remaining_time > 0:
        time.sleep(remaining_time)


class TestContainerLocks(TestNeofsBase):
    @pytest.mark.sanity
    def test_container_lock_sanity(self, default_wallet):
        with container_lock_timer(BASIC_LOCK_TIME) as lock_time:
            with allure.step("Create a container"):
                cid = create_container(
                    default_wallet.path,
                    rule="REP 1",
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    attributes={"__NEOFS__LOCK_UNTIL": lock_time},
                )

            with allure.step("Verify the created container has correct attributes"):
                containers = list_containers(default_wallet.path, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
                assert cid in containers, f"Expected container {cid} in containers: {containers}"

                container_info: str = get_container(
                    default_wallet.path,
                    cid,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                )

                assert container_info["attributes"]["__NEOFS__LOCK_UNTIL"] == str(lock_time), (
                    "Invalid __NEOFS__LOCK_UNTIL value"
                )

            with allure.step("Try to delete the locked container"):
                with pytest.raises(Exception, match="container is locked"):
                    delete_container(default_wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

        with allure.step("Delete container after lock expires"):
            delete_container(default_wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
            wait_for_container_deletion(default_wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

    def test_add_lock_to_container_without_attributes(self, default_wallet):
        with allure.step("Create a container without any attributes"):
            cid = create_container(
                default_wallet.path,
                rule="REP 1",
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Verify container is created without lock attribute"):
            container_info = get_container(
                default_wallet.path,
                cid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            assert "__NEOFS__LOCK_UNTIL" not in container_info["attributes"]

        with container_lock_timer(BASIC_LOCK_TIME) as lock_time:
            with allure.step("Add lock attribute to the container"):
                set_container_attributes(
                    default_wallet,
                    cid,
                    self.neofs_env,
                    attributes={"__NEOFS__LOCK_UNTIL": lock_time},
                )

            with allure.step("Verify container cannot be deleted while locked"):
                with pytest.raises(Exception, match="container is locked"):
                    delete_container(default_wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

        with allure.step("Delete container after lock expires"):
            delete_container(default_wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
            wait_for_container_deletion(default_wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

    def test_container_sequential_locks(self, default_wallet):
        with container_lock_timer(BASIC_LOCK_TIME) as lock_time:
            with allure.step("Create a container with first lock"):
                cid = create_container(
                    default_wallet.path,
                    rule="REP 1",
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    attributes={"__NEOFS__LOCK_UNTIL": lock_time},
                )

            with allure.step("Verify first lock is active"):
                container_info = get_container(
                    default_wallet.path,
                    cid,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                )
                assert container_info["attributes"]["__NEOFS__LOCK_UNTIL"] == str(lock_time)

            with allure.step("Verify container cannot be deleted while first lock is active"):
                with pytest.raises(Exception, match="container is locked"):
                    delete_container(default_wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

        with container_lock_timer(BASIC_LOCK_TIME) as lock_time:
            with allure.step("Set second lock on the same container"):
                set_container_attributes(
                    default_wallet,
                    cid,
                    self.neofs_env,
                    attributes={"__NEOFS__LOCK_UNTIL": lock_time},
                )

            with allure.step("Verify container cannot be deleted while second lock is active"):
                with pytest.raises(Exception, match="container is locked"):
                    delete_container(default_wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

        with allure.step("Delete container after second lock expires"):
            delete_container(default_wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
            wait_for_container_deletion(default_wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

    def test_multiple_containers_with_locks(self, default_wallet):
        containers = []
        LOCK_TIME = 30

        with container_lock_timer(LOCK_TIME) as lock_time:
            with allure.step("Create multiple containers with locks"):
                for _ in range(3):
                    cid = create_container(
                        default_wallet.path,
                        rule="REP 1",
                        shell=self.shell,
                        endpoint=self.neofs_env.sn_rpc,
                        attributes={"__NEOFS__LOCK_UNTIL": lock_time},
                    )
                    containers.append(cid)

            with allure.step("Verify all containers are created and locked"):
                all_containers = list_containers(default_wallet.path, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
                for cid in containers:
                    assert cid in all_containers, f"Container {cid} not found in list"

                    container_info = get_container(
                        default_wallet.path,
                        cid,
                        shell=self.shell,
                        endpoint=self.neofs_env.sn_rpc,
                    )
                    assert container_info["attributes"]["__NEOFS__LOCK_UNTIL"] == str(lock_time)

            with allure.step("Verify all containers cannot be deleted while locked"):
                for cid in containers:
                    with pytest.raises(Exception, match="container is locked"):
                        delete_container(default_wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

        with allure.step("Delete all containers after locks expire"):
            for cid in containers:
                delete_container(default_wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

            for cid in containers:
                wait_for_container_deletion(default_wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

    def test_container_lock_update(self, default_wallet):
        with container_lock_timer(BASIC_LOCK_TIME) as lock_time:
            with allure.step("Create a container with initial lock"):
                cid = create_container(
                    default_wallet.path,
                    rule="REP 1",
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    attributes={"__NEOFS__LOCK_UNTIL": lock_time},
                )

            with allure.step("Verify initial lock"):
                container_info = get_container(
                    default_wallet.path,
                    cid,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                )
                assert container_info["attributes"]["__NEOFS__LOCK_UNTIL"] == str(lock_time)

            with allure.step("Wait 1 sec for the unix epoch tick"):
                time.sleep(1)

        with container_lock_timer(BASIC_LOCK_TIME) as lock_time:
            with allure.step("Update lock to extend expiration time"):
                set_container_attributes(
                    default_wallet,
                    cid,
                    self.neofs_env,
                    attributes={"__NEOFS__LOCK_UNTIL": lock_time},
                )

            with allure.step("Verify container still cannot be deleted with updated lock"):
                with pytest.raises(Exception, match="container is locked"):
                    delete_container(default_wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

        with allure.step("Delete container after updated lock expires"):
            delete_container(default_wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
            wait_for_container_deletion(default_wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

    def test_cannot_create_container_with_lock_past_timestamp(self, default_wallet):
        with allure.step("Create a container with past lock timestamp"):
            with pytest.raises(Exception):
                create_container(
                    default_wallet.path,
                    rule="REP 1",
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    attributes={"__NEOFS__LOCK_UNTIL": int(time.time()) - BASIC_LOCK_TIME},
                )

    def test_lock_from_past(self, default_wallet):
        with allure.step("Create a container without lock"):
            cid = create_container(
                default_wallet.path,
                rule="REP 1",
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Try to set __NEOFS__LOCK_UNTIL to past epoch value"):
            with pytest.raises(Exception, match=r".*lock expiration time .* is not later than current .*"):
                set_container_attributes(
                    default_wallet,
                    cid,
                    self.neofs_env,
                    attributes={"__NEOFS__LOCK_UNTIL": int(time.time()) - 10},
                )

        with allure.step("Verify container can still be deleted"):
            delete_container(default_wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
            wait_for_container_deletion(default_wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

    def test_multiple_containers_different_lock_times(self, default_wallet):
        FIRST_LOCK_OFFSET = 10
        SECOND_LOCK_OFFSET = 15
        THIRD_LOCK_OFFSET = 20

        start_time = time.time()
        current_time = int(start_time)
        lock_times = [
            current_time + FIRST_LOCK_OFFSET,
            current_time + SECOND_LOCK_OFFSET,
            current_time + THIRD_LOCK_OFFSET,
        ]
        created_containers = []

        with allure.step("Create containers with different lock times"):
            for lock_time in lock_times:
                cid = create_container(
                    default_wallet.path,
                    rule="REP 1",
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    attributes={"__NEOFS__LOCK_UNTIL": lock_time},
                )
                created_containers.append((cid, lock_time))

        with allure.step("Verify all containers are locked"):
            for cid, lock_time in created_containers:
                container_info = get_container(
                    default_wallet.path,
                    cid,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                )
                assert container_info["attributes"]["__NEOFS__LOCK_UNTIL"] == str(lock_time)

        elapsed = time.time() - start_time
        assert elapsed < FIRST_LOCK_OFFSET - 2, (
            f"Container setup took too long ({elapsed:.2f}s). "
            f"Expected less than {FIRST_LOCK_OFFSET - 2}s to safely test first lock expiration."
        )

        with allure.step(f"Wait for first lock to expire (~{FIRST_LOCK_OFFSET}s) and delete first container"):
            wait_time = FIRST_LOCK_OFFSET - elapsed + 1
            time.sleep(wait_time)
            delete_container(
                default_wallet.path,
                created_containers[0][0],
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Verify other containers still locked"):
            elapsed = time.time() - start_time
            assert elapsed < SECOND_LOCK_OFFSET - 1, (
                f"Test execution took too long ({elapsed:.2f}s). "
                f"Second container may have already unlocked (lock time: {SECOND_LOCK_OFFSET}s)."
            )

            for cid, _ in created_containers[1:]:
                with pytest.raises(Exception, match="container is locked"):
                    delete_container(default_wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

        with allure.step(f"Wait for second lock to expire (~{SECOND_LOCK_OFFSET}s) and delete second container"):
            current_elapsed = time.time() - start_time
            wait_time = SECOND_LOCK_OFFSET - current_elapsed + 1
            time.sleep(wait_time)
            delete_container(
                default_wallet.path,
                created_containers[1][0],
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Verify third container still locked"):
            elapsed = time.time() - start_time
            assert elapsed < THIRD_LOCK_OFFSET - 1, (
                f"Test execution took too long ({elapsed:.2f}s). "
                f"Third container may have already unlocked (lock time: {THIRD_LOCK_OFFSET}s)."
            )

            with pytest.raises(Exception, match="container is locked"):
                delete_container(
                    default_wallet.path,
                    created_containers[2][0],
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                )

        with allure.step(f"Wait for third lock to expire (~{THIRD_LOCK_OFFSET}s) and delete third container"):
            current_elapsed = time.time() - start_time
            wait_time = THIRD_LOCK_OFFSET - current_elapsed + 1
            time.sleep(wait_time)
            delete_container(
                default_wallet.path,
                created_containers[2][0],
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Verify all containers are deleted"):
            for cid, _ in created_containers:
                wait_for_container_deletion(default_wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

    def test_cannot_remove_active_lock_attribute(self, default_wallet):
        with container_lock_timer(BASIC_LOCK_TIME) as lock_time:
            with allure.step("Create a container with lock set to future epoch"):
                cid = create_container(
                    default_wallet.path,
                    rule="REP 1",
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    attributes={"__NEOFS__LOCK_UNTIL": lock_time},
                )

            with allure.step("Verify lock is active"):
                container_info = get_container(
                    default_wallet.path,
                    cid,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                )
                assert container_info["attributes"]["__NEOFS__LOCK_UNTIL"] == str(lock_time)

            with allure.step("Try to remove __NEOFS__LOCK_UNTIL while lock is active"):
                with pytest.raises(Exception):
                    set_container_attributes(
                        default_wallet, cid, self.neofs_env, remove_attributes=["__NEOFS__LOCK_UNTIL"]
                    )

            with allure.step("Verify lock attribute is still present"):
                container_info = get_container(
                    default_wallet.path,
                    cid,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                )
                assert container_info["attributes"]["__NEOFS__LOCK_UNTIL"] == str(lock_time)

        with allure.step("Verify lock has expired and container can be deleted"):
            delete_container(default_wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
            wait_for_container_deletion(default_wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

    def test_can_remove_expired_lock_attribute(self, default_wallet):
        with container_lock_timer(5) as lock_time:
            with allure.step("Create a container with lock set to near future"):
                cid = create_container(
                    default_wallet.path,
                    rule="REP 1",
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    attributes={"__NEOFS__LOCK_UNTIL": lock_time},
                )

        with allure.step("Remove expired __NEOFS__LOCK_UNTIL attribute"):
            set_container_attributes(
                default_wallet,
                cid,
                self.neofs_env,
                remove_attributes=["__NEOFS__LOCK_UNTIL"],
            )

        with allure.step("Verify container can be deleted"):
            delete_container(default_wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
            wait_for_container_deletion(default_wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
