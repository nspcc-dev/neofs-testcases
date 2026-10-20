import logging
from functools import wraps

import allure
import pytest
from helpers.chain_time import fs_chain_unix_time, rfc3339_utc, wait_until_chain_time_after
from helpers.complex_object_actions import get_object_holder_endpoint
from helpers.container import DEFAULT_PLACEMENT_RULE, EC_3_1_PLACEMENT_RULE, create_container
from helpers.file_helper import generate_file, get_file_hash
from helpers.grpc_responses import OBJECT_NOT_FOUND
from helpers.neofs_verbs import (
    get_object,
    get_object_from_random_node,
    head_object,
    put_object,
    put_object_to_random_node,
)
from helpers.utility import parse_version, wait_for_gc_pass_on_storage_nodes
from neofs_env.neofs_env_test_base import TestNeofsBase
from neofs_testlib.env.env import NodeWallet
from pytest import FixtureRequest

logger = logging.getLogger("NeoLogger")


def skip_without_expiration_time(test):
    @wraps(test)
    def wrapper(self, *args, **kwargs):
        node_version = self.neofs_env.get_binary_version(self.neofs_env.neofs_node_path)
        if parse_version(node_version) <= parse_version("0.57.0"):
            pytest.skip(f"time-based object expiration requires neofs-node newer than v0.57.0, got {node_version}")
        return test(self, *args, **kwargs)

    return wrapper


class TestObjectApiLifetime(TestNeofsBase):
    @allure.title("Test object life time")
    @pytest.mark.parametrize(
        "object_size,expiration_flag",
        [
            pytest.param("simple_object_size", "lifetime", id="simple object, lifetime", marks=pytest.mark.simple),
            pytest.param("complex_object_size", "expire_at", id="complex object, expire_at", marks=pytest.mark.complex),
        ],
    )
    @pytest.mark.parametrize(
        "container_policy",
        [
            pytest.param(DEFAULT_PLACEMENT_RULE, id="simple object", marks=pytest.mark.simple),
            pytest.param(EC_3_1_PLACEMENT_RULE, id="complex object", marks=pytest.mark.complex),
        ],
    )
    def test_object_api_lifetime(
        self,
        default_wallet: NodeWallet,
        request: FixtureRequest,
        object_size: int,
        expiration_flag: str,
        container_policy: str,
    ):
        """
        Test object deleted after expiration epoch.
        """

        allure.dynamic.title(f"Test object life time for {request.node.callspec.id}")

        wallet = default_wallet
        endpoint = self.neofs_env.sn_rpc
        cid = create_container(wallet.path, self.shell, endpoint, rule=container_policy)

        file_path = generate_file(self.neofs_env.get_object_size(object_size))
        file_hash = get_file_hash(file_path)
        epoch = self.ensure_fresh_epoch()

        oid = put_object_to_random_node(
            wallet.path,
            file_path,
            cid,
            self.shell,
            neofs_env=self.neofs_env,
            expire_at=epoch + 1 if expiration_flag == "expire_at" else None,
            lifetime=1 if expiration_flag == "lifetime" else None,
        )
        got_file = get_object_from_random_node(wallet.path, cid, oid, self.shell, neofs_env=self.neofs_env)
        assert get_file_hash(got_file) == file_hash

        with allure.step("Tick two epochs"):
            self.tick_epochs_and_wait(2)

        # Wait for GC, because object with expiration is counted as alive until GC removes it
        wait_for_gc_pass_on_storage_nodes()

        with allure.step("Check object deleted because it expires-on epoch"):
            with pytest.raises(Exception, match=OBJECT_NOT_FOUND):
                get_object_from_random_node(wallet.path, cid, oid, self.shell, neofs_env=self.neofs_env)

    @pytest.mark.parametrize(
        "container_policy",
        [
            pytest.param(DEFAULT_PLACEMENT_RULE, id="simple object", marks=pytest.mark.simple),
            pytest.param(EC_3_1_PLACEMENT_RULE, id="complex object", marks=pytest.mark.complex),
        ],
    )
    def test_put_of_already_expired_object(self, default_wallet: NodeWallet, container_policy: str):
        """
        Test put of an object with expired epoch.
        """

        wallet = default_wallet
        endpoint = self.neofs_env.sn_rpc
        cid = create_container(wallet.path, self.shell, endpoint, rule=container_policy)

        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        current_epoch = self.ensure_fresh_epoch()

        with pytest.raises(Exception, match=".*object has expired.*"):
            put_object_to_random_node(
                wallet.path,
                file_path,
                cid,
                self.shell,
                neofs_env=self.neofs_env,
                expire_at=current_epoch - 1,
            )

    @allure.title("Test object life time by expiration time")
    @pytest.mark.parametrize(
        "object_size,container_policy",
        [
            pytest.param(
                "simple_object_size",
                DEFAULT_PLACEMENT_RULE,
                id="rep-simple object",
                marks=pytest.mark.simple,
            ),
            pytest.param(
                "complex_object_size",
                DEFAULT_PLACEMENT_RULE,
                id="rep-complex object",
                marks=pytest.mark.complex,
            ),
            pytest.param(
                "complex_object_size",
                EC_3_1_PLACEMENT_RULE,
                id="ec-complex object",
                marks=pytest.mark.complex,
            ),
        ],
    )
    @skip_without_expiration_time
    def test_object_api_lifetime_by_time(
        self,
        default_wallet: NodeWallet,
        request: FixtureRequest,
        object_size: str,
        container_policy: str,
    ):
        allure.dynamic.title(f"Test object life time by expiration time for {request.node.callspec.id}")

        wallet = default_wallet
        endpoint = self.neofs_env.storage_nodes[1].rpc_endpoint
        cid = create_container(wallet.path, self.shell, endpoint, rule=container_policy)

        file_path = generate_file(self.neofs_env.get_object_size(object_size))
        file_hash = get_file_hash(file_path)
        _EXPIRATION_LEAD_SEC = {
            "simple_object_size": 25,
            "complex_object_size": 40,
        }
        expiration = fs_chain_unix_time(self.neofs_env) + _EXPIRATION_LEAD_SEC[object_size]
        expire_at_time = rfc3339_utc(expiration)

        oid = put_object(
            wallet.path,
            file_path,
            cid,
            self.shell,
            endpoint,
            expire_at_time=expire_at_time,
        )
        holder = get_object_holder_endpoint(self.neofs_env, self.shell, cid, oid)
        got_file = get_object(wallet.path, cid, oid, self.shell, holder)
        assert get_file_hash(got_file) == file_hash

        with allure.step("Check header reports expiration time"):
            head = head_object(
                wallet.path,
                cid,
                oid,
                self.shell,
                holder,
                json_output=False,
            )
            assert f"ExpirationTime: {expire_at_time} ({expiration})" in head.stdout

        with allure.step(f"Wait until FS chain time passes {expire_at_time}"):
            wait_until_chain_time_after(self.neofs_env, expiration)

        wait_for_gc_pass_on_storage_nodes()

        with allure.step("Check object deleted because its expiration time has passed"):
            with pytest.raises(Exception, match=OBJECT_NOT_FOUND):
                get_object(wallet.path, cid, oid, self.shell, holder)

    @allure.title("Time-based expiration is not an epoch expiration")
    @pytest.mark.parametrize(
        "container_policy",
        [
            pytest.param(DEFAULT_PLACEMENT_RULE, id="rep", marks=pytest.mark.complex),
            pytest.param(EC_3_1_PLACEMENT_RULE, id="ec", marks=pytest.mark.complex),
        ],
    )
    @skip_without_expiration_time
    def test_time_expiration_is_independent_of_epoch(self, default_wallet: NodeWallet, container_policy: str):
        wallet = default_wallet
        endpoint = self.neofs_env.storage_nodes[1].rpc_endpoint
        cid = create_container(wallet.path, self.shell, endpoint, rule=container_policy)

        file_path = generate_file(self.neofs_env.get_object_size("complex_object_size"))
        file_hash = get_file_hash(file_path)
        expiration = fs_chain_unix_time(self.neofs_env) + 3600

        oid = put_object(
            wallet.path,
            file_path,
            cid,
            self.shell,
            endpoint,
            expire_at_time=rfc3339_utc(expiration),
        )
        holder = get_object_holder_endpoint(self.neofs_env, self.shell, cid, oid)
        got_file = get_object(wallet.path, cid, oid, self.shell, holder)
        assert get_file_hash(got_file) == file_hash

        with allure.step("Tick two epochs"):
            self.tick_epochs_and_wait(2)
            wait_for_gc_pass_on_storage_nodes()

        got_file = get_object(wallet.path, cid, oid, self.shell, holder)
        assert get_file_hash(got_file) == file_hash

    @allure.title("Small EC object with expiration time is readable")
    @pytest.mark.complex
    @skip_without_expiration_time
    def test_small_ec_object_with_expiration_time_is_readable(self, default_wallet: NodeWallet):
        wallet = default_wallet
        endpoint = self.neofs_env.storage_nodes[1].rpc_endpoint
        cid = create_container(wallet.path, self.shell, endpoint, rule=EC_3_1_PLACEMENT_RULE)

        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        file_hash = get_file_hash(file_path)
        expiration = fs_chain_unix_time(self.neofs_env) + 3600

        oid = put_object(
            wallet.path,
            file_path,
            cid,
            self.shell,
            endpoint,
            expire_at_time=rfc3339_utc(expiration),
        )
        got_file = get_object(wallet.path, cid, oid, self.shell, endpoint)
        assert get_file_hash(got_file) == file_hash

    @allure.title("Small replicated object stays readable across epochs")
    @pytest.mark.simple
    @skip_without_expiration_time
    def test_small_replicated_object_stays_readable_across_epochs(self, default_wallet: NodeWallet):
        wallet = default_wallet
        endpoint = self.neofs_env.storage_nodes[1].rpc_endpoint
        cid = create_container(wallet.path, self.shell, endpoint, rule=DEFAULT_PLACEMENT_RULE)

        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        file_hash = get_file_hash(file_path)
        expiration = fs_chain_unix_time(self.neofs_env) + 3600

        oid = put_object(
            wallet.path,
            file_path,
            cid,
            self.shell,
            endpoint,
            expire_at_time=rfc3339_utc(expiration),
        )
        holder = get_object_holder_endpoint(self.neofs_env, self.shell, cid, oid)
        got_file = get_object(wallet.path, cid, oid, self.shell, holder)
        assert get_file_hash(got_file) == file_hash

        with allure.step("Tick two epochs"):
            self.tick_epochs_and_wait(2)
            wait_for_gc_pass_on_storage_nodes()

        chain_now = fs_chain_unix_time(self.neofs_env)
        assert chain_now < expiration, f"FS chain time {chain_now} passed expiration {expiration}"

        for node in self.neofs_env.storage_nodes:
            got_file = get_object(wallet.path, cid, oid, self.shell, node.endpoint)
            assert get_file_hash(got_file) == file_hash

    @pytest.mark.parametrize(
        "container_policy",
        [
            pytest.param(DEFAULT_PLACEMENT_RULE, id="rep", marks=pytest.mark.simple),
            pytest.param(EC_3_1_PLACEMENT_RULE, id="ec", marks=pytest.mark.complex),
        ],
    )
    @skip_without_expiration_time
    def test_put_of_already_expired_by_time_object(self, default_wallet: NodeWallet, container_policy: str):
        wallet = default_wallet
        endpoint = self.neofs_env.sn_rpc
        cid = create_container(wallet.path, self.shell, endpoint, rule=container_policy)

        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        expired_at = fs_chain_unix_time(self.neofs_env) - 60

        with pytest.raises(Exception, match=".*object has expired.*"):
            put_object_to_random_node(
                wallet.path,
                file_path,
                cid,
                self.shell,
                neofs_env=self.neofs_env,
                expire_at_time=rfc3339_utc(expired_at),
            )

    @allure.title("Put rejects invalid expiration time")
    @pytest.mark.simple
    @skip_without_expiration_time
    def test_put_rejects_invalid_expiration_time(self, default_wallet: NodeWallet):
        wallet = default_wallet
        endpoint = self.neofs_env.sn_rpc
        cid = create_container(wallet.path, self.shell, endpoint)
        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        future = rfc3339_utc(fs_chain_unix_time(self.neofs_env) + 3600)

        cases = [
            (
                "expire-at together with expire-at-time",
                {"expire_at": 10, "expire_at_time": future},
                "none of the others can be",
            ),
            (
                "lifetime together with expire-at-time",
                {"lifetime": 1, "expire_at_time": future},
                "none of the others can be",
            ),
            ("not RFC3339", {"expire_at_time": "not-a-time"}, "invalid --expire-at-time"),
            (
                "non-positive unix time",
                {"expire_at_time": "1970-01-01T00:00:00Z"},
                "positive Unix timestamp",
            ),
            (
                "expiration epoch attribute together with expire-at-time",
                {
                    "expire_at_time": future,
                    "attributes": {"__NEOFS__EXPIRATION_EPOCH": "100"},
                },
                "expiration epoch attribute cannot be used with --expire-at-time",
            ),
        ]
        for title, kwargs, error in cases:
            with allure.step(title):
                with pytest.raises(Exception, match=error):
                    put_object_to_random_node(
                        wallet.path,
                        file_path,
                        cid,
                        self.shell,
                        neofs_env=self.neofs_env,
                        **kwargs,
                    )

    @allure.title("Expiration time user attribute does not expire an object")
    @pytest.mark.parametrize(
        "container_policy",
        [
            pytest.param(DEFAULT_PLACEMENT_RULE, id="rep", marks=pytest.mark.simple),
            pytest.param(EC_3_1_PLACEMENT_RULE, id="ec", marks=pytest.mark.complex),
        ],
    )
    @skip_without_expiration_time
    def test_expiration_time_attribute_does_not_expire_object(self, default_wallet: NodeWallet, container_policy: str):
        wallet = default_wallet
        endpoint = self.neofs_env.sn_rpc
        cid = create_container(wallet.path, self.shell, endpoint, rule=container_policy)

        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        file_hash = get_file_hash(file_path)

        oid = put_object_to_random_node(
            wallet.path,
            file_path,
            cid,
            self.shell,
            neofs_env=self.neofs_env,
            attributes={"$Object:expirationTime": "1"},
        )
        got_file = get_object_from_random_node(wallet.path, cid, oid, self.shell, neofs_env=self.neofs_env)
        assert get_file_hash(got_file) == file_hash

        head = head_object(wallet.path, cid, oid, self.shell, endpoint, json_output=False)
        assert "ExpirationTime:" not in head.stdout
