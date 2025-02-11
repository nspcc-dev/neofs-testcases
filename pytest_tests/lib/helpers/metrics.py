import json
import logging
import re
from collections import defaultdict
from typing import Union

import allure
import requests
from helpers.test_control import wait_for_success
from neofs_testlib.env.env import S3_GW, InnerRing, StorageNode

logger = logging.getLogger("NeoLogger")


def parse_prometheus_metrics(metrics_lines: str) -> dict:
    parsed_metrics = defaultdict(list)
    for line in metrics_lines.strip().splitlines():
        if line.startswith("#"):
            continue
        match = re.match(r"^(?P<name>[^\{]+)(\{(?P<params>[^\}]+)\})?\s+(?P<value>\S+)$", line)
        if match:
            name = match.group("name")
            params = match.group("params")
            value = float(match.group("value"))

            params_dict = {}
            if params:
                params_pairs = params.split(",")
                for pair in params_pairs:
                    key, val = pair.split("=")
                    params_dict[key.strip()] = val.strip().strip('"')

            parsed_metrics[name].append({"value": value, "params": params_dict})
    return parsed_metrics


def get_metrics(node: Union[StorageNode | InnerRing | S3_GW]) -> dict:
    resp = requests.get(f"http://{node.prometheus_address}")
    if resp.status_code != 200:
        raise AssertionError(f"Invalid status code from metrics url: {resp.status_code}; {resp.reason}; {resp.text};")
    return parse_prometheus_metrics(resp.text)


@allure.step("Wait for correct metric value")
@wait_for_success(120, 1)
def wait_for_metric_to_arrive(node: Union[StorageNode | InnerRing | S3_GW], metric_name: str, expected_value: float):
    metrics = get_metrics(node)
    allure.attach(
        json.dumps(dict(metrics)),
        "metrics",
        allure.attachment_type.JSON,
    )
    actual_value = metrics[metric_name][0]["value"]
    logger.info(f"Current value of {metric_name} = {actual_value}")
    assert actual_value == expected_value, (
        f"invalid value for {metric_name}, expected: {expected_value}, got: {actual_value}"
    )
