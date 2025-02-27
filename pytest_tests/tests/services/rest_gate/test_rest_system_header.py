import calendar
import logging
from datetime import datetime, timedelta
from typing import Optional

import allure
import neofs_env.neofs_epoch as neofs_epoch
import pytest
from helpers.container import create_container
from helpers.file_helper import generate_file
from helpers.grpc_responses import OBJECT_NOT_FOUND
from helpers.neofs_verbs import get_netmap_netinfo, get_object_from_random_node, head_object
from helpers.rest_gate import (
    attr_into_str_header,
    get_epoch_duration_via_rest_gate,
    try_to_get_object_and_expect_error,
    upload_via_rest_gate,
)
from helpers.wellknown_acl import PUBLIC_ACL
from neofs_env.neofs_env_test_base import NeofsEnvTestBase
from pytest_lazy_fixtures import lf
from rest_gw.rest_utils import get_object_and_verify_hashes

logger = logging.getLogger("NeoLogger")
EXPIRATION_TIMESTAMP_HEADER = "__NEOFS__EXPIRATION_TIMESTAMP"
EXPIRATION_EPOCH_HEADER = "__NEOFS__EXPIRATION_EPOCH"
EXPIRATION_DURATION_HEADER = "__NEOFS__EXPIRATION_DURATION"
EXPIRATION_EXPIRATION_RFC = "__NEOFS__EXPIRATION_RFC3339"
NEOFS_EXPIRATION_EPOCH = "Neofs-Expiration-Epoch"
NEOFS_EXPIRATION_DURATION = "Neofs-Expiration-Duration"
NEOFS_EXPIRATION_TIMESTAMP = "Neofs-Expiration-Timestamp"
NEOFS_EXIPRATION_RFC3339 = "Neofs-Expiration-RFC3339"


class Test_rest_system_header(NeofsEnvTestBase):
    PLACEMENT_RULE = "REP 2 IN X CBF 1 SELECT 2 FROM * AS X"

    @pytest.fixture(scope="class", autouse=True)
    @allure.title("[Class/Autouse]: Prepare wallet and deposit")
    def prepare_wallet(self, default_wallet):
        Test_rest_system_header.wallet = default_wallet

    @pytest.fixture(scope="class")
    @allure.title("Create container")
    def user_container(self):
        return create_container(
            wallet=self.wallet.path,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            rule=self.PLACEMENT_RULE,
            basic_acl=PUBLIC_ACL,
        )

    @pytest.fixture(scope="class")
    @allure.title("epoch_duration in seconds")
    def epoch_duration(self) -> int:
        net_info = get_netmap_netinfo(
            wallet=self.wallet.path,
            endpoint=self.neofs_env.sn_rpc,
            shell=self.shell,
        )
        epoch_duration_in_blocks = net_info["epoch_duration"]
        time_per_block = net_info["time_per_block"]
        return int(epoch_duration_in_blocks * time_per_block)

    @allure.title("Return N-epoch count in minutes")
    def epoch_count_into_mins(self, epoch_duration: int, epoch: int) -> str:
        mins = epoch_duration * epoch / 60
        return f"{mins}m"

    @allure.title("Return future timestamp after N epochs are passed")
    def epoch_count_into_timestamp(self, epoch_duration: int, epoch: int, rfc3339: Optional[bool] = False) -> str:
        current_datetime = datetime.now()
        epoch_count_in_seconds = epoch_duration * epoch
        future_datetime = current_datetime + timedelta(seconds=epoch_count_in_seconds)
        if rfc3339:
            return future_datetime.isoformat("T") + "Z"
        else:
            return str(calendar.timegm(future_datetime.timetuple()))

    @allure.title("Check is  (header_output) Key=Value exists and equal in passed (header_to_find)")
    def check_key_value_presented_header(self, header_output: dict, header_to_find: dict) -> bool:
        header_att = header_output["header"]["attributes"]
        for key_to_check, val_to_check in header_to_find.items():
            if key_to_check not in header_att or val_to_check != header_att[key_to_check]:
                logger.info(f"Unable to find {key_to_check}: '{val_to_check}' in {header_att}")
                return False
        return True

    @allure.title(f"Validate that only {EXPIRATION_EPOCH_HEADER} exists in header and other headers are abesent")
    def validation_for_http_header_attr(self, head_info: dict, expected_epoch: int) -> None:
        # check that __NEOFS__EXPIRATION_EPOCH attribute has corresponding epoch
        assert self.check_key_value_presented_header(head_info, {EXPIRATION_EPOCH_HEADER: str(expected_epoch)}), (
            f"Expected to find {EXPIRATION_EPOCH_HEADER}: {expected_epoch} in: {head_info['header']['attributes']}"
        )
        # check that {EXPIRATION_EPOCH_HEADER} absents in header output
        assert not (self.check_key_value_presented_header(head_info, {EXPIRATION_DURATION_HEADER: ""})), (
            f"Only {EXPIRATION_EPOCH_HEADER} can be displayed in header attributes"
        )
        # check that {EXPIRATION_TIMESTAMP_HEADER} absents in header output
        assert not (self.check_key_value_presented_header(head_info, {EXPIRATION_TIMESTAMP_HEADER: ""})), (
            f"Only {EXPIRATION_TIMESTAMP_HEADER} can be displayed in header attributes"
        )
        # check that {EXPIRATION_EXPIRATION_RFC} absents in header output
        assert not (self.check_key_value_presented_header(head_info, {EXPIRATION_EXPIRATION_RFC: ""})), (
            f"Only {EXPIRATION_EXPIRATION_RFC} can be displayed in header attributes"
        )

    @allure.title("Put / get / verify object and return head command result to invoker")
    def oid_header_info_for_object(self, file_path: str, attributes: dict, user_container: str, gw_endpoint: str):
        oid = upload_via_rest_gate(
            cid=user_container,
            path=file_path,
            endpoint=gw_endpoint,
            headers=attr_into_str_header(attributes),
        )
        get_object_and_verify_hashes(
            oid=oid,
            file_name=file_path,
            wallet=self.wallet.path,
            cid=user_container,
            shell=self.shell,
            nodes=self.neofs_env.storage_nodes,
            endpoint=gw_endpoint,
        )
        head = head_object(
            wallet=self.wallet.path,
            cid=user_container,
            oid=oid,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )
        return oid, head

    @allure.title("[negative] attempt to put object with expired epoch")
    def test_unable_put_expired_epoch(self, user_container: str, simple_object_size: int, gw_endpoint):
        headers = attr_into_str_header({"Neofs-Expiration-Epoch": str(neofs_epoch.get_epoch(self.neofs_env) - 1)})
        file_path = generate_file(simple_object_size)
        with allure.step("Put object using HTTP with attribute Expiration-Epoch where epoch is expired"):
            upload_via_rest_gate(
                cid=user_container,
                path=file_path,
                endpoint=gw_endpoint,
                headers=headers,
                error_pattern="object has expired",
            )

    @allure.title("[negative] attempt to put object with negative Neofs-Expiration-Duration")
    def test_unable_put_negative_duration(self, user_container: str, simple_object_size: int, gw_endpoint):
        headers = attr_into_str_header({"Neofs-Expiration-Duration": "-1h"})
        file_path = generate_file(simple_object_size)
        with allure.step("Put object using HTTP with attribute Neofs-Expiration-Duration where duration is negative"):
            upload_via_rest_gate(
                cid=user_container,
                path=file_path,
                endpoint=gw_endpoint,
                headers=headers,
                error_pattern=f"{EXPIRATION_DURATION_HEADER} must be positive",
            )

    @allure.title("[negative] attempt to put object with Neofs-Expiration-Timestamp value in the past")
    def test_unable_put_expired_timestamp(self, user_container: str, simple_object_size: int, gw_endpoint):
        headers = attr_into_str_header({"Neofs-Expiration-Timestamp": "1635075727"})
        file_path = generate_file(simple_object_size)
        with allure.step(
            "Put object using HTTP with attribute Neofs-Expiration-Timestamp where duration is in the past"
        ):
            upload_via_rest_gate(
                cid=user_container,
                path=file_path,
                endpoint=gw_endpoint,
                headers=headers,
                error_pattern=f"{EXPIRATION_TIMESTAMP_HEADER} must be in the future",
            )

    @allure.title(
        "[negative] Put object using HTTP with attribute Neofs-Expiration-RFC3339 where duration is in the past"
    )
    def test_unable_put_expired_rfc(self, user_container: str, simple_object_size: int, gw_endpoint):
        headers = attr_into_str_header({"Neofs-Expiration-RFC3339": "2021-11-22T09:55:49Z"})
        file_path = generate_file(simple_object_size)
        upload_via_rest_gate(
            cid=user_container,
            path=file_path,
            endpoint=gw_endpoint,
            headers=headers,
            error_pattern=f"{EXPIRATION_EXPIRATION_RFC} must be in the future",
        )

    @pytest.mark.sanity
    @allure.title("priority of attributes epoch>duration")
    @pytest.mark.parametrize(
        "object_size",
        [lf("simple_object_size"), lf("complex_object_size")],
        ids=["simple object", "complex object"],
    )
    def test_http_attr_priority_epoch_duration(
        self, user_container: str, object_size: int, epoch_duration: int, gw_endpoint
    ):
        self.tick_epochs_and_wait(1)
        epoch_count = 1
        expected_epoch = neofs_epoch.get_epoch(self.neofs_env) + epoch_count
        logger.info(
            f"epoch duration={epoch_duration}, current_epoch= {neofs_epoch.get_epoch(self.neofs_env)} expected_epoch {expected_epoch}"
        )
        attributes = {NEOFS_EXPIRATION_EPOCH: expected_epoch, NEOFS_EXPIRATION_DURATION: "1m"}
        file_path = generate_file(object_size)
        with allure.step(
            f"Put objects using HTTP with attributes and head command should display {EXPIRATION_EPOCH_HEADER}: {expected_epoch} attr"
        ):
            oid, head_info = self.oid_header_info_for_object(
                file_path=file_path,
                attributes=attributes,
                user_container=user_container,
                gw_endpoint=gw_endpoint,
            )
            self.validation_for_http_header_attr(head_info=head_info, expected_epoch=expected_epoch)
        with allure.step("Check that object becomes unavailable when epoch is expired"):
            self.tick_epochs_and_wait(epoch_count + 1)
            assert neofs_epoch.get_epoch(self.neofs_env) == expected_epoch + 1, (
                f"Epochs should be equal: {neofs_epoch.get_epoch(self.neofs_env)} != {expected_epoch + 1}"
            )

            with allure.step("Check object deleted because it expires-on epoch"):
                neofs_epoch.wait_for_epochs_align(self.neofs_env)
                try_to_get_object_and_expect_error(
                    cid=user_container,
                    oid=oid,
                    error_pattern="404 Not Found",
                    endpoint=gw_endpoint,
                )
                # check that object is not available via grpc
                with pytest.raises(Exception, match=OBJECT_NOT_FOUND):
                    get_object_from_random_node(
                        self.wallet.path,
                        user_container,
                        oid,
                        self.shell,
                        neofs_env=self.neofs_env,
                    )

    @allure.title(
        f"priority of attributes duration>timestamp, duration time has higher priority and should be converted {EXPIRATION_EPOCH_HEADER}"
    )
    @pytest.mark.parametrize(
        "object_size",
        [lf("simple_object_size"), lf("complex_object_size")],
        ids=["simple object", "complex object"],
    )
    def test_http_attr_priority_dur_timestamp(
        self, user_container: str, object_size: int, epoch_duration: int, gw_endpoint
    ):
        self.tick_epochs_and_wait(1)
        epoch_count = 2
        get_epoch_duration_via_rest_gate(gw_endpoint)
        expected_epoch = neofs_epoch.get_epoch(self.neofs_env) + epoch_count
        logger.info(
            f"epoch duration={epoch_duration}, current_epoch= {neofs_epoch.get_epoch(self.neofs_env)} expected_epoch {expected_epoch}"
        )
        attributes = {
            NEOFS_EXPIRATION_DURATION: self.epoch_count_into_mins(epoch_duration=epoch_duration, epoch=2),
            NEOFS_EXPIRATION_TIMESTAMP: self.epoch_count_into_timestamp(epoch_duration=epoch_duration, epoch=1),
        }
        file_path = generate_file(object_size)
        with allure.step(
            f"Put objects using HTTP with attributes and head command should display {EXPIRATION_EPOCH_HEADER}: {expected_epoch} attr"
        ):
            oid, head_info = self.oid_header_info_for_object(
                file_path=file_path,
                attributes=attributes,
                user_container=user_container,
                gw_endpoint=gw_endpoint,
            )
            self.validation_for_http_header_attr(head_info=head_info, expected_epoch=expected_epoch)
        with allure.step("Check that object becomes unavailable when epoch is expired"):
            self.tick_epochs_and_wait(epoch_count + 1)
            assert neofs_epoch.get_epoch(self.neofs_env) == expected_epoch + 1, (
                f"Epochs should be equal: {neofs_epoch.get_epoch(self.neofs_env)} != {expected_epoch + 1}"
            )

            with allure.step("Check object deleted because it expires-on epoch"):
                neofs_epoch.wait_for_epochs_align(self.neofs_env)
                try_to_get_object_and_expect_error(
                    cid=user_container,
                    oid=oid,
                    error_pattern="404 Not Found",
                    endpoint=gw_endpoint,
                )
                # check that object is not available via grpc
                with pytest.raises(Exception, match=OBJECT_NOT_FOUND):
                    get_object_from_random_node(
                        self.wallet.path,
                        user_container,
                        oid,
                        self.shell,
                        neofs_env=self.neofs_env,
                    )

    @allure.title(
        f"priority of attributes timestamp>Expiration-RFC, timestamp has higher priority and should be converted {EXPIRATION_EPOCH_HEADER}"
    )
    @pytest.mark.parametrize(
        "object_size",
        [lf("simple_object_size"), lf("complex_object_size")],
        ids=["simple object", "complex object"],
    )
    def test_http_attr_priority_timestamp_rfc(
        self, user_container: str, object_size: int, epoch_duration: int, gw_endpoint
    ):
        self.tick_epochs_and_wait(1)
        epoch_count = 2
        get_epoch_duration_via_rest_gate(gw_endpoint)
        expected_epoch = neofs_epoch.get_epoch(self.neofs_env) + epoch_count
        logger.info(
            f"epoch duration={epoch_duration}, current_epoch= {neofs_epoch.get_epoch(self.neofs_env)} expected_epoch {expected_epoch}"
        )
        attributes = {
            NEOFS_EXPIRATION_TIMESTAMP: self.epoch_count_into_timestamp(epoch_duration=epoch_duration, epoch=2),
            NEOFS_EXIPRATION_RFC3339: self.epoch_count_into_timestamp(
                epoch_duration=epoch_duration, epoch=1, rfc3339=True
            ),
        }
        file_path = generate_file(object_size)
        with allure.step(
            f"Put objects using HTTP with attributes and head command should display {EXPIRATION_EPOCH_HEADER}: {expected_epoch} attr"
        ):
            oid, head_info = self.oid_header_info_for_object(
                file_path=file_path,
                attributes=attributes,
                user_container=user_container,
                gw_endpoint=gw_endpoint,
            )
            self.validation_for_http_header_attr(head_info=head_info, expected_epoch=expected_epoch)
        with allure.step("Check that object becomes unavailable when epoch is expired"):
            self.tick_epochs_and_wait(epoch_count + 1)
            assert neofs_epoch.get_epoch(self.neofs_env) == expected_epoch + 1, (
                f"Epochs should be equal: {neofs_epoch.get_epoch(self.neofs_env)} != {expected_epoch + 1}"
            )

            with allure.step("Check object deleted because it expires-on epoch"):
                neofs_epoch.wait_for_epochs_align(self.neofs_env)
                try_to_get_object_and_expect_error(
                    cid=user_container,
                    oid=oid,
                    error_pattern="404 Not Found",
                    endpoint=gw_endpoint,
                )
                # check that object is not available via grpc
                with pytest.raises(Exception, match=OBJECT_NOT_FOUND):
                    get_object_from_random_node(
                        self.wallet.path,
                        user_container,
                        oid,
                        self.shell,
                        neofs_env=self.neofs_env,
                    )

    @allure.title("Test that object is automatically delete when expiration passed")
    @pytest.mark.parametrize(
        "object_size",
        [lf("simple_object_size"), lf("complex_object_size")],
        ids=["simple object", "complex object"],
    )
    def test_http_rfc_object_unavailable_after_expir(
        self, user_container: str, object_size: int, epoch_duration: int, gw_endpoint
    ):
        self.tick_epochs_and_wait(1)
        epoch_count = 2
        get_epoch_duration_via_rest_gate(gw_endpoint)
        expected_epoch = neofs_epoch.get_epoch(self.neofs_env) + epoch_count
        logger.info(
            f"epoch duration={epoch_duration}, current_epoch= {neofs_epoch.get_epoch(self.neofs_env)} expected_epoch {expected_epoch}"
        )
        attributes = {
            NEOFS_EXIPRATION_RFC3339: self.epoch_count_into_timestamp(
                epoch_duration=epoch_duration, epoch=2, rfc3339=True
            )
        }
        file_path = generate_file(object_size)
        with allure.step(
            f"Put objects using HTTP with attributes and head command should display {EXPIRATION_EPOCH_HEADER}: {expected_epoch} attr"
        ):
            oid, head_info = self.oid_header_info_for_object(
                file_path=file_path,
                attributes=attributes,
                user_container=user_container,
                gw_endpoint=gw_endpoint,
            )
            self.validation_for_http_header_attr(head_info=head_info, expected_epoch=expected_epoch)
        with allure.step("Check that object becomes unavailable when epoch is expired"):
            self.tick_epochs_and_wait(epoch_count + 1)
            # check that {EXPIRATION_EXPIRATION_RFC} absents in header output
            assert neofs_epoch.get_epoch(self.neofs_env) == expected_epoch + 1, (
                f"Epochs should be equal: {neofs_epoch.get_epoch(self.neofs_env)} != {expected_epoch + 1}"
            )

            with allure.step("Check object deleted because it expires-on epoch"):
                neofs_epoch.wait_for_epochs_align(self.neofs_env)
                try_to_get_object_and_expect_error(
                    cid=user_container,
                    oid=oid,
                    error_pattern="404 Not Found",
                    endpoint=gw_endpoint,
                )
                # check that object is not available via grpc
                with pytest.raises(Exception, match=OBJECT_NOT_FOUND):
                    get_object_from_random_node(
                        self.wallet.path,
                        user_container,
                        oid,
                        self.shell,
                        neofs_env=self.neofs_env,
                    )
