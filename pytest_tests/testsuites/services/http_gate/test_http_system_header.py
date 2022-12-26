import calendar
import datetime
import logging
import os
import time

import allure
import pytest
from container import create_container, delete_container
from epoch import get_epoch, tick_epoch
from file_helper import generate_file
from http_gate import (
    attr_into_str_header_curl,
    get_object_and_verify_hashes,
    get_object_by_attr_and_verify_hashes,
    upload_via_http_gate_curl,
)
from pytest import FixtureRequest
from python_keywords.neofs_verbs import get_netmap_netinfo, head_object
from wellknown_acl import PUBLIC_ACL

from helpers.storage_object_info import StorageObjectInfo
from steps.cluster_test_base import ClusterTestBase

logger = logging.getLogger("NeoLogger")


@pytest.mark.sanity
@pytest.mark.http_gate
class Test_http_system_header(ClusterTestBase):
    PLACEMENT_RULE = "REP 2 IN X CBF 1 SELECT 4 FROM * AS X"

    @pytest.fixture(scope="class", autouse=True)
    @allure.title("[Class/Autouse]: Prepare wallet and deposit")
    def prepare_wallet(self, default_wallet):
        Test_http_system_header.wallet = default_wallet

    @pytest.fixture(scope="class")
    @allure.title("Create container")
    def create_container(self):
        return create_container(
            wallet=self.wallet,
            shell=self.shell,
            endpoint=self.cluster.default_rpc_endpoint,
            rule=self.PLACEMENT_RULE,
            basic_acl=PUBLIC_ACL,
        )

    @pytest.fixture(scope="class")
    @allure.title("return_epoch_duration in seconds")
    def return_epoch_duration(self) -> int:
        net_info = get_netmap_netinfo(
            wallet=self.wallet,
            endpoint=self.cluster.default_rpc_endpoint,
            shell=self.shell,
        )
        return net_info["epoch_duration"]

    @allure.title("Return 2 epoch time in format: 5m where m=minutes")
    def return_2epoch_in_mins(self, return_epoch_duration):
        mins: int
        mins = return_epoch_duration * 2 / 60
        return f"{(mins)}m"

    @allure.title("Return timestamp after 2 epoch")
    def return_2epoch_timestamp(return_epoch_duration: int):
        current_datetime = datetime.datetime.utcnow()
        future_datetime = current_datetime + datetime.timedelta(seconds=return_epoch_duration)
        future_timetuple = future_datetime.timetuple()
        future_timestamp = calendar.timegm(future_timetuple)
        return future_timestamp

    def return_curr_epoch(self) -> int:
        epoch = get_epoch(self.shell, self.cluster)
        return epoch

    @pytest.fixture()
    def return_next_epoch(self) -> int:
        epoch = get_epoch(self.shell, self.cluster) + 1
        return epoch

    def check_key_value_presented_header(self, header_output: dict, header_to_find: dict) -> bool:
        header_att = header_output["header"]["attributes"]
        for key_to_check, val_to_check in header_to_find.items():
            if key_to_check not in header_att or val_to_check != header_att[key_to_check]:
                logger.info(f"Unable to find {key_to_check}: '{val_to_check}' in {header_att}")
                return False
        return True

    @allure.title("[negative] attempt to put object with expired epoch")
    @pytest.mark.parametrize(
        "object_size",
        [pytest.lazy_fixture("simple_object_size")],
        ids=["simple object"],
    )
    def test_unable_put_expired_epoch(self, create_container: str, object_size: int):
        headers = attr_into_str_header_curl(
            {"Neofs-Expiration-Epoch": str(self.return_curr_epoch - 1)}
        )
        file_path = generate_file(object_size)
        with allure.step(
            "Put object using HTTP with attribute Expiration-Epoch where epoch is expired"
        ):
            upload_via_http_gate_curl(
                cid=create_container,
                filepath=file_path,
                endpoint=self.cluster.default_http_gate_endpoint,
                headers=headers,
                error_pattern="object has expired",
            )

    @allure.title("[negative] attempt to put object with negative Neofs-Expiration-Duration")
    @pytest.mark.parametrize(
        "object_size",
        [pytest.lazy_fixture("simple_object_size")],
        ids=["simple object"],
    )
    def test_unable_put_negative_duration(self, create_container: str, object_size: int):
        headers = attr_into_str_header_curl({"Neofs-Expiration-Duration": "-1h"})
        file_path = generate_file(object_size)
        with allure.step(
            "Put object using HTTP with attribute Neofs-Expiration-Duration where duration is negative"
        ):
            upload_via_http_gate_curl(
                cid=create_container,
                filepath=file_path,
                endpoint=self.cluster.default_http_gate_endpoint,
                headers=headers,
                error_pattern="__NEOFS__EXPIRATION_DURATION must be positive",
            )

    @allure.title(
        "[negative] attempt to put object with Neofs-Expiration-Timestamp value in the past"
    )
    @pytest.mark.parametrize(
        "object_size",
        [pytest.lazy_fixture("simple_object_size")],
        ids=["simple object"],
    )
    def test_unable_put_expired_timestamp(self, create_container: str, object_size: int):
        headers = attr_into_str_header_curl({"Neofs-Expiration-Timestamp": "1635075727"})
        file_path = generate_file(object_size)
        with allure.step(
            "Put object using HTTP with attribute Neofs-Expiration-Timestamp where duration is in the past"
        ):
            upload_via_http_gate_curl(
                cid=create_container,
                filepath=file_path,
                endpoint=self.cluster.default_http_gate_endpoint,
                headers=headers,
                error_pattern="__NEOFS__EXPIRATION_TIMESTAMP must be in the future",
            )

    @allure.title(
        "Put object using HTTP with attribute Neofs-Expiration-RFC3339 where duration is in the past"
    )
    @pytest.mark.parametrize(
        "object_size",
        [pytest.lazy_fixture("simple_object_size")],
        ids=["simple object"],
    )
    def test_unable_put_expired_rfc(self, create_container: str, object_size: int):
        headers = attr_into_str_header_curl({"Neofs-Expiration-RFC3339": "2021-11-22T09:55:49Z"})
        file_path = generate_file(object_size)
        with allure.step(
            "[negative] Put object using HTTP with attribute Neofs-Expiration-RFC3339 where duration is in the past"
        ):
            upload_via_http_gate_curl(
                cid=create_container,
                filepath=file_path,
                endpoint=self.cluster.default_http_gate_endpoint,
                headers=headers,
                error_pattern="__NEOFS__EXPIRATION_RFC3339 must be in the future",
            )

    @allure.title("priority of attributes")
    @pytest.mark.parametrize(
        "object_size",
        [pytest.lazy_fixture("simple_object_size")],
        ids=["simple object"],
    )
    @pytest.mark.parametrize(
        "attribute_key_1, attribute_val_1, attribute_key_2, attribute_val_2, expected_epoch",
        [
            (
                "Neofs-Expiration-Epoch",
                pytest.lazy_fixture("return_next_epoch"),
                "Neofs-Expiration-Duration",
                "1h",
                None,
            ),
            (
                "Neofs-Expiration-Duration",
                "return_2epoch_in_mins()",
                "Neofs-Expiration-Timestamp",
                'return_2epoch_timestamp(pytest.lazy_fixture("return_epoch_duration"))',
                "return_next_epoch",
            ),
            (
                "Neofs-Expiration-Timestamp",
                "return_2epoch_timestamp()",
                "Neofs-Expiration-RFC3339",
                "",
                None,
            ),
        ],
        ids=["epoch>duration", "duration>timestamp", "timestamp>rfc"],
    )
    def test_http_attributes_priority(
        self,
        create_container: str,
        object_size: int,
        return_epoch_duration: int,
        attribute_key_1: str,
        attribute_val_1,
        return_next_epoch: int,
        attribute_key_2: str,
        attribute_val_2,
        expected_epoch: int,
    ):

        exp_epoch_attr_header = "__NEOFS__EXPIRATION_EPOCH"
        tick_epoch(self.shell, self.cluster)
        if expected_epoch is None:
            expected_epoch = attribute_val_1
        else:
            expected_epoch += 2

        logger.info(
            f"epoch duration={return_epoch_duration}, current_epoch= {self.return_curr_epoch()} next {self.return_next_epoch()}, fixt: {return_next_epoch}"
        )

        attributes = {attribute_key_1: str(attribute_val_1), attribute_key_2: str(attribute_val_2)}
        file_path = generate_file(object_size)
        with allure.step(
            f"Put objects using HTTP with attributes and head command should display only {exp_epoch_attr_header}:{attribute_val_1} attr"
        ):
            oid = upload_via_http_gate_curl(
                cid=create_container,
                filepath=file_path,
                endpoint=self.cluster.default_http_gate_endpoint,
                headers=attr_into_str_header_curl(attributes),
            )
            head_info = head_object(
                wallet=self.wallet,
                cid=create_container,
                oid=oid,
                shell=self.shell,
                endpoint=self.cluster.default_rpc_endpoint,
            )
            get_object_and_verify_hashes(
                oid=oid,
                file_name=file_path,
                wallet=self.wallet,
                cid=create_container,
                shell=self.shell,
                nodes=self.cluster.storage_nodes,
                endpoint=self.cluster.default_http_gate_endpoint,
            )
            # check that __NEOFS__EXPIRATION_EPOCH attribute has corresponding epoch
            assert (
                self.check_key_value_presented_header(
                    head_info, {exp_epoch_attr_header: str(expected_epoch)}
                )
                == True
            ), f'Expected to find {exp_epoch_attr_header}: {expected_epoch} in: {head_info["header"]["attributes"]}'
            # check that 2nd attribute absents in header
            # assert (self.check_key_value_presented_header(head_info, {attr_header_name2: attributes[attribute_key_2]}) == False), f"Only {attr_header_name1} should be in header attributes"

    @allure.title("priority of attributes")
    @pytest.mark.parametrize(
        "object_size",
        [pytest.lazy_fixture("simple_object_size")],
        ids=["simple object"],
    )
    def test_http_attributes_priority_new(
        self, create_container: str, object_size: int, return_epoch_duration: int
    ):
        @pytest.mark.parametrize(
            "attribute_key_1, attribute_val_1, attribute_key_2, attribute_val_2, expected_epoch",
            [
                (
                    "Neofs-Expiration-Epoch",
                    self.return_next_epoch(),
                    "Neofs-Expiration-Duration",
                    "1h",
                    None,
                ),
            ],
            ids=["epoch>duration"],
        )
        def test_http_attributes_priority_2(
            self,
            create_container: str,
            object_size: int,
            return_epoch_duration: int,
            attribute_key_1: str,
            attribute_val_1,
            attribute_key_2: str,
            attribute_val_2,
            expected_epoch: int,
        ):
            logger.info(
                f"epoch duration={return_epoch_duration}, current_epoch= {self.return_curr_epoch()} next {self.return_next_epoch()}"
            )
