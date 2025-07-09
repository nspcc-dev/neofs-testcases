import itertools
import logging
import math
import operator
from datetime import datetime

import allure
import base58
import neofs_env.neofs_epoch as neofs_epoch
import pytest
from helpers.common import SIMPLE_OBJECT_SIZE
from helpers.complex_object_actions import get_object_chunks
from helpers.container import create_container, delete_container
from helpers.file_helper import generate_file
from helpers.neofs_verbs import (
    delete_object,
    generate_filter_json_file,
    head_object,
    put_object,
    put_object_to_random_node,
    search_object,
    search_objectv2,
)
from helpers.test_control import wait_for_success
from neofs_testlib.env.env import NeoFSEnv, NodeWallet

logger = logging.getLogger("NeoLogger")


def timestamp_to_epoch(timestamp_str: str) -> int:
    dt_part = timestamp_str[:-10].strip()
    dt_naive = datetime.strptime(dt_part, "%Y-%m-%d %H:%M:%S")
    return int(dt_naive.timestamp())


def get_attribute_value_from_found_object(found_object: dict, attr_name: str) -> str:
    value = next((attr[attr_name] for attr in found_object["attrs"] if attr_name in attr), None)
    assert value, f"no {attr_name} found in {found_object}"
    return value


@wait_for_success(90, 10)
def expected_number_of_object_found(
    default_wallet: NodeWallet, cid: str, neofs_env: NeoFSEnv, expected_number_of_objects: int
):
    found_objects, _ = search_objectv2(
        rpc_endpoint=neofs_env.sn_rpc,
        wallet=default_wallet.path,
        cid=cid,
        shell=neofs_env.shell,
    )

    assert len(found_objects) == expected_number_of_objects, "invalid number of found objects"


@pytest.fixture
def container(default_wallet: NodeWallet, neofs_env: NeoFSEnv) -> str:
    cid = create_container(
        default_wallet.path,
        shell=neofs_env.shell,
        endpoint=neofs_env.sn_rpc,
        rule="REP 3 CBF 3",
        attributes={"__NEOFS__METAINFO_CONSISTENCY": "strict"}
        if neofs_env.inner_ring_nodes[0].chain_meta_data
        else None,
    )
    yield cid
    delete_container(default_wallet.path, cid, shell=neofs_env.shell, endpoint=neofs_env.sn_rpc)


@pytest.mark.sanity
@pytest.mark.parametrize(
    "neofs_env",
    [
        {"chain_meta_data": False},
        {"chain_meta_data": True},
    ],
    ids=["chain_meta_data=False", "chain_meta_data=True"],
    indirect=True,
)
@pytest.mark.simple
def test_search_sanity(default_wallet: NodeWallet, container: str, neofs_env: NeoFSEnv):
    cid = container
    created_objects = []
    for _ in range(2):
        created_objects.append(
            put_object_to_random_node(
                default_wallet.path,
                generate_file(neofs_env.get_object_size("simple_object_size")),
                cid,
                shell=neofs_env.shell,
                neofs_env=neofs_env,
            )
        )
    found_objects, _ = search_objectv2(
        rpc_endpoint=neofs_env.sn_rpc, wallet=default_wallet.path, cid=cid, shell=neofs_env.shell
    )
    assert len(found_objects) == len(created_objects), "invalid number of objects"
    for created_obj_id in created_objects:
        assert any(found_obj["id"] == created_obj_id for found_obj in found_objects), (
            f"created object {created_obj_id} not found in search output"
        )


@pytest.mark.parametrize(
    "neofs_env",
    [
        {"chain_meta_data": False},
        {"chain_meta_data": True},
    ],
    ids=["chain_meta_data=False", "chain_meta_data=True"],
    indirect=True,
)
@pytest.mark.simple
def test_search_single_filter_by_custom_int_attributes(default_wallet: NodeWallet, container: str, neofs_env: NeoFSEnv):
    cid = container
    created_objects = []
    int_attributes_values = [-(2**255) - 1, -1, 0, 1, 10, 2**255 + 1]
    int_attribute_name = "int_attribute"

    for int_value in int_attributes_values:
        file_path = generate_file(neofs_env.get_object_size("simple_object_size"))

        created_objects.append(
            {
                int_attribute_name: int_value,
                "id": put_object_to_random_node(
                    default_wallet.path,
                    file_path,
                    container,
                    shell=neofs_env.shell,
                    neofs_env=neofs_env,
                    attributes={int_attribute_name: int_value},
                ),
            }
        )

    operators = {"GT": operator.gt, "GE": operator.ge, "LT": operator.lt, "LE": operator.le}

    for operator_str, comparator in operators.items():
        for int_value in int_attributes_values:
            found_objects, _ = search_objectv2(
                rpc_endpoint=neofs_env.sn_rpc,
                wallet=default_wallet.path,
                cid=cid,
                shell=neofs_env.shell,
                filters=[f"{int_attribute_name} {operator_str} {int_value}"],
                attributes=[int_attribute_name],
            )

            if found_objects:
                assert len(found_objects[0]["attrs"]) == 1, f"invalid number of attributes for {found_objects[0]}"
                min_int_value = int(found_objects[0]["attrs"][0][int_attribute_name])

                for found_obj in found_objects[1:]:
                    assert len(found_obj["attrs"]) == 1, f"invalid number of attributes for {found_obj}"
                    assert int(found_obj["attrs"][0][int_attribute_name]) > min_int_value, (
                        "invalid ordering in search output"
                    )
                    min_int_value = int(found_obj["attrs"][0][int_attribute_name])

            for created_obj in created_objects:
                condition_met = comparator(created_obj[int_attribute_name], int_value)

                if condition_met:
                    assert any(found_obj["id"] == created_obj["id"] for found_obj in found_objects), (
                        f"created object {created_obj['id']} not found in search output"
                    )
                else:
                    assert not any(found_obj["id"] == created_obj["id"] for found_obj in found_objects), (
                        f"created object {created_obj['id']} found in search output, while shouldn't"
                    )

    too_large_int_value = 2**257 + 1
    put_object_to_random_node(
        default_wallet.path,
        file_path,
        container,
        shell=neofs_env.shell,
        neofs_env=neofs_env,
        attributes={int_attribute_name: too_large_int_value},
    )
    for operator_str, comparator in operators.items():
        found_objects, _ = search_objectv2(
            rpc_endpoint=neofs_env.sn_rpc,
            wallet=default_wallet.path,
            cid=cid,
            shell=neofs_env.shell,
            filters=[f"{int_attribute_name} {operator_str} {too_large_int_value}"],
            attributes=[int_attribute_name],
        )
        assert len(found_objects) == 0, "invalid number of objects"


@pytest.mark.parametrize(
    "neofs_env",
    [
        {"chain_meta_data": False},
        {"chain_meta_data": True},
    ],
    ids=["chain_meta_data=False", "chain_meta_data=True"],
    indirect=True,
)
@pytest.mark.simple
def test_search_single_filter_by_custom_str_attributes(
    default_wallet: NodeWallet,
    container: str,
    neofs_env: NeoFSEnv,
):
    cid = container
    created_objects = []
    str_attributes_values = ["Aaa", "Aaabcd", "Aaabcd", "1Aaabcd2", "A11a//b_c.//", "!@#$%ˆ&*()", "#FFFFFF"]
    str_attribute_name = "str_attribute"

    for str_value in str_attributes_values:
        file_path = generate_file(neofs_env.get_object_size("simple_object_size"))

        created_objects.append(
            {
                str_attribute_name: str_value,
                "id": put_object_to_random_node(
                    default_wallet.path,
                    file_path,
                    container,
                    shell=neofs_env.shell,
                    neofs_env=neofs_env,
                    attributes={str_attribute_name: str_value},
                ),
            }
        )

    operators = {
        "EQ": lambda obj_val, filter_val: obj_val == filter_val,
        "NE": lambda obj_val, filter_val: obj_val != filter_val,
        "COMMON_PREFIX": lambda obj_val, filter_val: obj_val.startswith(filter_val),
        "GT": lambda obj_val, filter_val: False,
        "GE": lambda obj_val, filter_val: False,
        "LT": lambda obj_val, filter_val: False,
        "LE": lambda obj_val, filter_val: False,
    }

    for operator_str, comparator in operators.items():
        for str_value in str_attributes_values:
            found_objects, _ = search_objectv2(
                rpc_endpoint=neofs_env.sn_rpc,
                wallet=default_wallet.path,
                cid=cid,
                shell=neofs_env.shell,
                filters=[f"{str_attribute_name} {operator_str} {str_value}"],
                attributes=[str_attribute_name],
            )

            for created_obj in created_objects:
                obj_value = created_obj[str_attribute_name]
                condition_met = comparator(obj_value, str_value)

                if condition_met:
                    assert any(
                        found_obj["id"] == created_obj["id"]
                        and comparator(found_obj["attrs"][0][str_attribute_name], str_value)
                        for found_obj in found_objects
                    ), f"Created object {created_obj['id']} not found in search output"
                else:
                    assert not any(found_obj["id"] == created_obj["id"] for found_obj in found_objects), (
                        f"Created object {created_obj['id']} found in search output, but shouldn't"
                    )


@pytest.mark.parametrize(
    "neofs_env",
    [
        {"chain_meta_data": False},
        {"chain_meta_data": True},
    ],
    ids=["chain_meta_data=False", "chain_meta_data=True"],
    indirect=True,
)
@pytest.mark.simple
def test_search_multiple_filters_same_attribute(default_wallet: NodeWallet, container: str, neofs_env: NeoFSEnv):
    cid = container
    file_path = generate_file(neofs_env.get_object_size("simple_object_size"))
    put_object_to_random_node(
        default_wallet.path,
        file_path,
        cid,
        shell=neofs_env.shell,
        neofs_env=neofs_env,
        attributes={"int_attr": 100, "str_attr": "abcd"},
    )
    testcases = [
        {
            "filters": ["int_attr GT 100", "int_attr LE 100"],
            "expected_result": 0,
        },
        {
            "filters": ["int_attr GE 100", "int_attr GT 100"],
            "expected_result": 0,
        },
        {
            "filters": ["int_attr LT 101", "int_attr GE 100"],
            "expected_result": 1,
        },
        {
            "filters": ["int_attr COMMON_PREFIX 10", "int_attr GE 100"],
            "expected_result": 1,
        },
        {
            "filters": ["str_attr EQ abcd", "str_attr NE dcba"],
            "expected_result": 1,
        },
        {
            "filters": ["str_attr EQ abcd", "str_attr EQ dcba"],
            "expected_result": 0,
        },
        {
            "filters": ["str_attr NE 5555", "str_attr COMMON_PREFIX abcde"],
            "expected_result": 0,
        },
    ]
    for testcase in testcases:
        found_objects, _ = search_objectv2(
            rpc_endpoint=neofs_env.sn_rpc,
            wallet=default_wallet.path,
            cid=cid,
            shell=neofs_env.shell,
            filters=testcase["filters"],
        )

        assert len(found_objects) == testcase["expected_result"], "invalid number of objects"


@pytest.mark.parametrize(
    "neofs_env",
    [
        {"chain_meta_data": False},
        {"chain_meta_data": True},
    ],
    ids=["chain_meta_data=False", "chain_meta_data=True"],
    indirect=True,
)
@pytest.mark.simple
def test_search_empty_attrs_in_filters(default_wallet: NodeWallet, container: str, neofs_env: NeoFSEnv):
    cid = container
    file_path = generate_file(neofs_env.get_object_size("simple_object_size"))
    put_object_to_random_node(
        default_wallet.path,
        file_path,
        cid,
        shell=neofs_env.shell,
        neofs_env=neofs_env,
        attributes={"attr1": 0, "attr2": "b"},
    )

    testcases = [
        {
            "filters": [generate_filter_json_file(['attr1 COMMON_PREFIX ""', 'attr2 COMMON_PREFIX ""'])],
            "expected_result": 1,
        },
        {
            "filters": [generate_filter_json_file(["attr1 COMMON_PREFIX 0", 'attr2 COMMON_PREFIX ""'])],
            "expected_result": 1,
        },
        {
            "filters": [generate_filter_json_file(['attr1 COMMON_PREFIX ""', "attr2 COMMON_PREFIX b"])],
            "expected_result": 1,
        },
        {
            "filters": [generate_filter_json_file(['attr1 COMMON_PREFIX ""', "attr2 COMMON_PREFIX 2"])],
            "expected_result": 0,
        },
        {
            "filters": [generate_filter_json_file(["attr1 COMMON_PREFIX 2", 'attr2 COMMON_PREFIX ""'])],
            "expected_result": 0,
        },
    ]

    for testcase in testcases:
        found_objects, _ = search_objectv2(
            rpc_endpoint=neofs_env.sn_rpc,
            wallet=default_wallet.path,
            cid=cid,
            shell=neofs_env.shell,
            filters=testcase["filters"],
            attributes=["attr1", "attr2"],
        )

        assert len(found_objects) == testcase["expected_result"], "invalid number of found objects"


@pytest.mark.parametrize(
    "neofs_env",
    [
        {"chain_meta_data": False},
        {"chain_meta_data": True},
    ],
    ids=["chain_meta_data=False", "chain_meta_data=True"],
    indirect=True,
)
@pytest.mark.simple
def test_search_multiple_filters_by_custom_int_attributes(
    default_wallet: NodeWallet,
    container: str,
    neofs_env: NeoFSEnv,
):
    cid = container
    created_objects = []

    int_attributes_values = [0, 1, -1]

    for attr0, attr1, attr2 in list(itertools.permutations(int_attributes_values, 3)):
        file_path = generate_file(neofs_env.get_object_size("simple_object_size"))
        attrs = {"int_attr0": attr0, "int_attr1": attr1, "int_attr2": attr2}
        created_objects.append(
            {
                "attrs": attrs,
                "id": put_object_to_random_node(
                    default_wallet.path,
                    file_path,
                    container,
                    shell=neofs_env.shell,
                    neofs_env=neofs_env,
                    attributes=attrs,
                ),
            }
        )

    operators = {"GT": operator.gt, "GE": operator.ge, "LT": operator.lt, "LE": operator.le}

    filters = [("GT", "GE", "LT"), ("GT", "GE", "LE"), ("GT", "LT", "GE"), ("GT", "LT", "LE"), ("GT", "LE", "GE")]

    for op0, op1, op2 in filters:
        for int_value in int_attributes_values:
            found_objects, _ = search_objectv2(
                rpc_endpoint=neofs_env.sn_rpc,
                wallet=default_wallet.path,
                cid=cid,
                shell=neofs_env.shell,
                filters=[
                    f"int_attr0 {op0} {int_value}",
                    f"int_attr1 {op1} {int_value}",
                    f"int_attr2 {op2} {int_value}",
                ],
                attributes=["int_attr0", "int_attr1", "int_attr2"],
            )

            for found_obj in found_objects:
                for i, op in enumerate([op0, op1, op2]):
                    attr_name = f"int_attr{i}"
                    assert operators[op](int(get_attribute_value_from_found_object(found_obj, attr_name)), int_value), (
                        f"Invalid object returned from searchv2: {found_obj}"
                    )

            for created_obj in created_objects:
                attrs = created_obj["attrs"]
                matches_all = all(
                    operators[op](attrs[f"int_attr{i}"], int_value) for i, op in enumerate([op0, op1, op2])
                )
                if matches_all:
                    assert any(found_obj["id"] == created_obj["id"] for found_obj in found_objects), (
                        f"created object {created_obj['id']} not found in search output"
                    )
                else:
                    assert not any(found_obj["id"] == created_obj["id"] for found_obj in found_objects), (
                        f"created object {created_obj['id']} found in search output, while shouldn't"
                    )


@pytest.mark.parametrize(
    "neofs_env",
    [
        {"chain_meta_data": False},
        {"chain_meta_data": True},
    ],
    ids=["chain_meta_data=False", "chain_meta_data=True"],
    indirect=True,
)
@pytest.mark.simple
def test_search_multiple_filters_by_custom_str_attributes(
    default_wallet: NodeWallet,
    container: str,
    neofs_env: NeoFSEnv,
):
    cid = container
    created_objects = []
    str_attributes_values = ["Aaa", "Aaabcd", "123", "1234"]

    for attr0, attr1, attr2 in list(itertools.permutations(str_attributes_values, 3)):
        file_path = generate_file(neofs_env.get_object_size("simple_object_size"))
        attrs = {"str_attr0": attr0, "str_attr1": attr1, "str_attr2": attr2}
        created_objects.append(
            {
                "attrs": attrs,
                "id": put_object_to_random_node(
                    default_wallet.path,
                    file_path,
                    container,
                    shell=neofs_env.shell,
                    neofs_env=neofs_env,
                    attributes=attrs,
                ),
            }
        )

    operators = {
        "EQ": lambda obj_val, filter_val: obj_val == filter_val,
        "NE": lambda obj_val, filter_val: obj_val != filter_val,
        "COMMON_PREFIX": lambda obj_val, filter_val: obj_val.startswith(filter_val),
    }

    filters = [
        ("EQ", "NE", "COMMON_PREFIX"),
        ("EQ", "COMMON_PREFIX", "NE"),
        ("NE", "EQ", "COMMON_PREFIX"),
        ("NE", "COMMON_PREFIX", "EQ"),
    ]

    for op0, op1, op2 in filters:
        for str_value in str_attributes_values:
            found_objects, _ = search_objectv2(
                rpc_endpoint=neofs_env.sn_rpc,
                wallet=default_wallet.path,
                cid=cid,
                shell=neofs_env.shell,
                filters=[
                    f"str_attr0 {op0} {str_value}",
                    f"str_attr1 {op1} {str_value}",
                    f"str_attr2 {op2} {str_value}",
                ],
                attributes=["str_attr0", "str_attr1", "str_attr2"],
            )

            for found_obj in found_objects:
                for i, op in enumerate([op0, op1, op2]):
                    attr_name = f"str_attr{i}"
                    assert operators[op](get_attribute_value_from_found_object(found_obj, attr_name), str_value), (
                        f"Invalid object returned from searchv2: {found_obj}"
                    )

            for created_obj in created_objects:
                attrs = created_obj["attrs"]
                matches_all = all(
                    operators[op](attrs[f"str_attr{i}"], str_value) for i, op in enumerate([op0, op1, op2])
                )
                if matches_all:
                    assert any(found_obj["id"] == created_obj["id"] for found_obj in found_objects), (
                        f"created object {created_obj['id']} not found in search output"
                    )
                else:
                    assert not any(found_obj["id"] == created_obj["id"] for found_obj in found_objects), (
                        f"created object {created_obj['id']} found in search output, while shouldn't"
                    )


@pytest.mark.parametrize(
    "neofs_env",
    [
        {"chain_meta_data": False},
        {"chain_meta_data": True},
    ],
    ids=["chain_meta_data=False", "chain_meta_data=True"],
    indirect=True,
)
@pytest.mark.simple
def test_search_by_mixed_attributes_contents(
    default_wallet: NodeWallet,
    container: str,
    neofs_env: NeoFSEnv,
):
    cid = container
    file_path = generate_file(neofs_env.get_object_size("simple_object_size"))
    oid1 = put_object_to_random_node(
        default_wallet.path,
        file_path,
        cid,
        shell=neofs_env.shell,
        neofs_env=neofs_env,
        attributes={"str_attr": "345", "int_attr": "abcd"},
    )
    oid2 = put_object_to_random_node(
        default_wallet.path,
        file_path,
        cid,
        shell=neofs_env.shell,
        neofs_env=neofs_env,
        attributes={"str_attr": "abcd", "int_attr": "345"},
    )
    found_objects, _ = search_objectv2(
        rpc_endpoint=neofs_env.sn_rpc,
        wallet=default_wallet.path,
        cid=cid,
        shell=neofs_env.shell,
        filters=[
            "str_attr GT 0",
            "int_attr COMMON_PREFIX abcd",
        ],
        attributes=["str_attr", "int_attr"],
    )

    assert len(found_objects) == 1, "invalid number of objects"
    assert found_objects[0]["id"] == oid1, "invalid object returned from search"

    found_objects, _ = search_objectv2(
        rpc_endpoint=neofs_env.sn_rpc,
        wallet=default_wallet.path,
        cid=cid,
        shell=neofs_env.shell,
        filters=[
            "str_attr COMMON_PREFIX abcd",
            "int_attr LE 345",
        ],
        attributes=["str_attr", "int_attr"],
    )

    assert len(found_objects) == 1, "invalid number of objects"
    assert found_objects[0]["id"] == oid2, "invalid object returned from search"


@pytest.mark.parametrize(
    "neofs_env",
    [
        {"chain_meta_data": False},
        {"chain_meta_data": True},
    ],
    ids=["chain_meta_data=False", "chain_meta_data=True"],
    indirect=True,
)
@pytest.mark.simple
def test_search_multiple_filters_by_custom_mixed_attributes(
    default_wallet: NodeWallet,
    container: str,
    neofs_env: NeoFSEnv,
):
    cid = container
    created_objects = []
    str_attributes_values = ["Aaa", "Aaabcd", "Aaabcd"]
    int_attributes_values = [0, 1, -1]

    attr_values = [
        l1 + l2
        for l1, l2 in zip(
            list(itertools.permutations(str_attributes_values, 3)),
            list(itertools.permutations(int_attributes_values, 3)),
        )
    ]

    for str_attr0, str_attr1, str_attr2, int_attr0, int_attr1, int_attr2 in attr_values:
        file_path = generate_file(neofs_env.get_object_size("simple_object_size"))
        attrs = {
            "str_attr0": str_attr0,
            "str_attr1": str_attr1,
            "str_attr2": str_attr2,
            "int_attr0": int_attr0,
            "int_attr1": int_attr1,
            "int_attr2": int_attr2,
        }
        created_objects.append(
            {
                "attrs": attrs,
                "id": put_object_to_random_node(
                    default_wallet.path,
                    file_path,
                    cid,
                    shell=neofs_env.shell,
                    neofs_env=neofs_env,
                    attributes=attrs,
                ),
            }
        )

    str_operators = {
        "EQ": lambda obj_val, filter_val: obj_val == filter_val,
        "NE": lambda obj_val, filter_val: obj_val != filter_val,
        "COMMON_PREFIX": lambda obj_val, filter_val: obj_val.startswith(filter_val),
    }

    int_operators = {"GT": operator.gt, "GE": operator.ge, "LT": operator.lt, "LE": operator.le}

    filters = [
        ("EQ", "NE", "LE"),
        ("EQ", "COMMON_PREFIX", "GT"),
        ("EQ", "GT", "GE"),
        ("NE", "COMMON_PREFIX", "GT"),
        ("NE", "GT", "LT"),
        ("COMMON_PREFIX", "GT", "GE"),
        ("GT", "EQ", "COMMON_PREFIX"),
        ("LT", "GE", "EQ"),
        ("LT", "NE", "GE"),
    ]

    for op0, op1, op2 in filters:
        for str_value in str_attributes_values:
            for int_value in int_attributes_values:
                search_filters = []

                attributes = []

                if op0 in str_operators:
                    search_filters.append(f"str_attr0 {op0} {str_value}")
                    attributes.append("str_attr0")
                else:
                    search_filters.append(f"int_attr0 {op0} {int_value}")
                    attributes.append("int_attr0")
                if op1 in str_operators:
                    search_filters.append(f"str_attr1 {op1} {str_value}")
                    attributes.append("str_attr1")
                else:
                    search_filters.append(f"int_attr1 {op1} {int_value}")
                    attributes.append("int_attr1")
                if op2 in str_operators:
                    search_filters.append(f"str_attr2 {op2} {str_value}")
                    attributes.append("str_attr2")
                else:
                    search_filters.append(f"int_attr2 {op2} {int_value}")
                    attributes.append("int_attr2")

                found_objects, _ = search_objectv2(
                    rpc_endpoint=neofs_env.sn_rpc,
                    wallet=default_wallet.path,
                    cid=cid,
                    shell=neofs_env.shell,
                    filters=search_filters,
                    attributes=attributes,
                )

                for found_obj in found_objects:
                    if op0 in str_operators:
                        assert str_operators[op0](
                            get_attribute_value_from_found_object(found_obj, "str_attr0"), str_value
                        ), f"Invalid object returned from searchv2: {found_obj}"
                    else:
                        assert int_operators[op0](
                            int(get_attribute_value_from_found_object(found_obj, "int_attr0")), int_value
                        ), f"Invalid object returned from searchv2: {found_obj}"
                    if op1 in str_operators:
                        assert str_operators[op1](
                            get_attribute_value_from_found_object(found_obj, "str_attr1"), str_value
                        ), f"Invalid object returned from searchv2: {found_obj}"
                    else:
                        assert int_operators[op1](
                            int(get_attribute_value_from_found_object(found_obj, "int_attr1")), int_value
                        ), f"Invalid object returned from searchv2: {found_obj}"
                    if op2 in str_operators:
                        assert str_operators[op2](
                            get_attribute_value_from_found_object(found_obj, "str_attr2"), str_value
                        ), f"Invalid object returned from searchv2: {found_obj}"
                    else:
                        assert int_operators[op2](
                            int(get_attribute_value_from_found_object(found_obj, "int_attr2")), int_value
                        ), f"Invalid object returned from searchv2: {found_obj}"

                for created_obj in created_objects:
                    attrs = created_obj["attrs"]
                    matches = []
                    for i, op in enumerate([op0, op1, op2]):
                        if op in str_operators:
                            matches.append(str_operators[op](attrs[f"str_attr{i}"], str_value))
                        else:
                            matches.append(int_operators[op](attrs[f"int_attr{i}"], int_value))
                    if all(matches):
                        assert any(found_obj["id"] == created_obj["id"] for found_obj in found_objects), (
                            f"created object {created_obj['id']} not found in search output"
                        )
                    else:
                        assert not any(found_obj["id"] == created_obj["id"] for found_obj in found_objects), (
                            f"created object {created_obj['id']} found in search output, while shouldn't"
                        )


@pytest.mark.parametrize(
    "neofs_env",
    [
        {"chain_meta_data": False},
        {"chain_meta_data": True},
    ],
    ids=["chain_meta_data=False", "chain_meta_data=True"],
    indirect=True,
)
@pytest.mark.simple
def test_search_by_system_attributes(
    default_wallet: NodeWallet,
    container: str,
    neofs_env: NeoFSEnv,
):
    cid = container
    created_objects = []
    for idx in range(4):
        file_path = generate_file(neofs_env.get_object_size("simple_object_size") + (idx * 100))
        oid = put_object_to_random_node(
            default_wallet.path,
            file_path,
            cid,
            shell=neofs_env.shell,
            neofs_env=neofs_env,
        )
        head_info = head_object(
            default_wallet.path,
            cid,
            oid,
            shell=neofs_env.shell,
            endpoint=neofs_env.sn_rpc,
        )
        logger.info(f"{head_info=}")
        system_attributes = {
            "FileName": head_info["header"]["attributes"]["FileName"],
            "Timestamp": head_info["header"]["attributes"]["Timestamp"],
            "$Object:creationEpoch": head_info["header"]["creationEpoch"],
            "$Object:payloadLength": head_info["header"]["payloadLength"],
            "$Object:objectType": head_info["header"]["objectType"],
            "$Object:version": head_info["header"]["version"],
            "$Object:payloadHash": base58.b58decode(head_info["header"]["payloadHash"]).hex(),
            "$Object:homomorphicHash": base58.b58decode(head_info["header"]["homomorphicHash"]).hex(),
            "$Object:ownerID": head_info["header"]["ownerID"],
        }
        created_objects.append({"id": oid, "attrs": system_attributes})

    for system_attr in system_attributes.keys():
        for created_obj in created_objects:
            created_obj_attr_value = created_obj["attrs"][system_attr]
            found_objects, _ = search_objectv2(
                rpc_endpoint=neofs_env.sn_rpc,
                wallet=default_wallet.path,
                cid=cid,
                shell=neofs_env.shell,
                filters=[f"{system_attr} EQ {created_obj_attr_value}"],
                attributes=[system_attr],
            )
            logger.info(f"{found_objects}")
            for found_obj in found_objects:
                if system_attr == "Timestamp":
                    epoch_from_timestamp = timestamp_to_epoch(
                        get_attribute_value_from_found_object(found_obj, system_attr)
                    )
                    assert epoch_from_timestamp == int(created_obj_attr_value), (
                        f"Invalid object returned from searchv2: {found_obj}"
                    )
                else:
                    assert get_attribute_value_from_found_object(found_obj, system_attr) == created_obj_attr_value, (
                        f"Invalid object returned from searchv2: {found_obj}"
                    )
            assert any(found_obj["id"] == created_obj["id"] for found_obj in found_objects), (
                f"created object {created_obj['id']} not found in search output"
            )


@pytest.mark.parametrize(
    "neofs_env",
    [
        {"chain_meta_data": False},
        {"chain_meta_data": True},
    ],
    ids=["chain_meta_data=False", "chain_meta_data=True"],
    indirect=True,
)
@pytest.mark.complex
def test_search_by_split_attributes(
    default_wallet: NodeWallet,
    container: str,
    neofs_env: NeoFSEnv,
):
    cid = container
    created_objects = []
    for idx in range(3):
        file_path = generate_file(neofs_env.get_object_size("complex_object_size") * (idx + 1))
        oid = put_object_to_random_node(
            default_wallet.path,
            file_path,
            cid,
            shell=neofs_env.shell,
            neofs_env=neofs_env,
        )
        head_info = head_object(
            default_wallet.path, cid, oid, shell=neofs_env.shell, endpoint=neofs_env.sn_rpc, is_raw=True
        )
        head_info = head_object(
            default_wallet.path,
            cid,
            head_info["link"],
            shell=neofs_env.shell,
            endpoint=neofs_env.sn_rpc,
        )
        system_attributes = {
            "$Object:split.parent": head_info["header"]["split"]["parent"],
            "$Object:split.first": head_info["header"]["split"]["first"],
        }
        created_objects.append({"id": oid, "attrs": system_attributes})

    for system_attr in system_attributes.keys():
        for created_obj in created_objects:
            created_obj_attr_value = created_obj["attrs"][system_attr]
            found_objects, _ = search_objectv2(
                rpc_endpoint=neofs_env.sn_rpc,
                wallet=default_wallet.path,
                cid=cid,
                shell=neofs_env.shell,
                filters=[f"{system_attr} EQ {created_obj_attr_value}"],
                attributes=[system_attr],
            )
            assert len(found_objects) > 0, "no objects found"
            for found_obj in found_objects:
                assert get_attribute_value_from_found_object(found_obj, system_attr) == created_obj_attr_value, (
                    f"Invalid object returned from searchv2: {found_obj}"
                )


@pytest.mark.parametrize(
    "neofs_env",
    [
        {"chain_meta_data": False},
        {"chain_meta_data": True},
    ],
    ids=["chain_meta_data=False", "chain_meta_data=True"],
    indirect=True,
)
@pytest.mark.simple
@pytest.mark.parametrize("with_attributes", [True, False])
def test_search_by_non_existing_attributes(
    default_wallet: NodeWallet, container: str, neofs_env: NeoFSEnv, with_attributes: bool
):
    cid = container
    created_objects = []
    for _ in range(3):
        file_path = generate_file(neofs_env.get_object_size("simple_object_size"))
        created_objects.append(
            put_object_to_random_node(
                default_wallet.path,
                file_path,
                cid,
                shell=neofs_env.shell,
                neofs_env=neofs_env,
            )
        )

    for op in ["GT", "GE", "LT", "LE", "EQ", "NE", "COMMON_PREFIX"]:
        found_objects, _ = search_objectv2(
            rpc_endpoint=neofs_env.sn_rpc,
            wallet=default_wallet.path,
            cid=cid,
            shell=neofs_env.shell,
            filters=[f"nonExistentAttr {op} 1234"],
            attributes=["nonExistentAttr"] if with_attributes else None,
        )
        assert len(found_objects) == 0, "invalid number of found objects"


@pytest.mark.parametrize(
    "neofs_env",
    [
        {"chain_meta_data": False},
        {"chain_meta_data": True},
    ],
    ids=["chain_meta_data=False", "chain_meta_data=True"],
    indirect=True,
)
@pytest.mark.complex
def test_search_of_complex_object(
    default_wallet: NodeWallet,
    container: str,
    neofs_env: NeoFSEnv,
):
    cid = container
    file_path = generate_file(neofs_env.get_object_size("complex_object_size"))
    oid = put_object_to_random_node(
        default_wallet.path,
        file_path,
        cid,
        shell=neofs_env.shell,
        neofs_env=neofs_env,
        attributes={"complex_attr": "489-0"},
    )

    parts = get_object_chunks(default_wallet.path, container, oid, neofs_env.shell, neofs_env)

    found_complex_objects, _ = search_objectv2(
        rpc_endpoint=neofs_env.sn_rpc,
        wallet=default_wallet.path,
        cid=cid,
        shell=neofs_env.shell,
        filters=["str_attr1 NOPRESENT", "$Object:objectType EQ LINK"],
        attributes=["str_attr1", "$Object:objectType"],
    )

    assert len(found_complex_objects) == 1, "there is an unexpected number of LINK objects"

    found_complex_objects, _ = search_objectv2(
        rpc_endpoint=neofs_env.sn_rpc,
        wallet=default_wallet.path,
        cid=cid,
        shell=neofs_env.shell,
        filters=["str_attr1 NOPRESENT", "$Object:objectType EQ REGULAR"],
        attributes=["str_attr1", "$Object:objectType"],
    )

    assert len(found_complex_objects) == len(parts) + 1, "tthere is an unexpected number of REGULAR objects"


@pytest.mark.parametrize(
    "neofs_env",
    [
        {"chain_meta_data": False},
        {"chain_meta_data": True},
    ],
    ids=["chain_meta_data=False", "chain_meta_data=True"],
    indirect=True,
)
@pytest.mark.simple
@pytest.mark.parametrize("with_attributes", [True, False])
def test_search_by_various_attributes(
    default_wallet: NodeWallet, container: str, neofs_env: NeoFSEnv, with_attributes: bool
):
    cid = container
    file_path = generate_file(neofs_env.get_object_size("simple_object_size"))
    oid1 = put_object_to_random_node(
        default_wallet.path,
        file_path,
        cid,
        shell=neofs_env.shell,
        neofs_env=neofs_env,
        attributes={"str_attr0": "interesting.value_for_some*reason", "int_attr0": 54321},
    )
    head_info1 = head_object(
        default_wallet.path,
        cid,
        oid1,
        shell=neofs_env.shell,
        endpoint=neofs_env.sn_rpc,
    )
    neofs_epoch.ensure_fresh_epoch(neofs_env)
    file_path = generate_file(neofs_env.get_object_size("simple_object_size"))
    oid2 = put_object_to_random_node(
        default_wallet.path,
        file_path,
        cid,
        shell=neofs_env.shell,
        neofs_env=neofs_env,
        attributes={"str_attr0": "interesting.value_for_some*reason", "str_attr1": "oops", "int_attr0": 54321},
    )

    found_objects, _ = search_objectv2(
        rpc_endpoint=neofs_env.sn_rpc,
        wallet=default_wallet.path,
        cid=cid,
        shell=neofs_env.shell,
        filters=[
            f"$Object:payloadLength EQ {neofs_env.get_object_size('simple_object_size')}",
            "str_attr1 NOPRESENT",
            "int_attr0 GE 10",
        ],
        attributes=["$Object:payloadLength", "str_attr1", "int_attr0"] if with_attributes else None,
    )
    assert len(found_objects) == 1, "invalid number of found objects"
    assert found_objects[0]["id"] == oid1, "invalid object returned from search"

    found_objects, _ = search_objectv2(
        rpc_endpoint=neofs_env.sn_rpc,
        wallet=default_wallet.path,
        cid=cid,
        shell=neofs_env.shell,
        filters=[
            "int_attr0 GT 1000",
            f"$Object:creationEpoch NE {head_info1['header']['creationEpoch']}",
            "str_attr1 NOPRESENT",
        ],
        attributes=["int_attr0", "$Object:creationEpoch", "str_attr1"] if with_attributes else None,
    )
    assert len(found_objects) == 0, "invalid number of found objects"

    found_objects, _ = search_objectv2(
        rpc_endpoint=neofs_env.sn_rpc,
        wallet=default_wallet.path,
        cid=cid,
        shell=neofs_env.shell,
        filters=[
            "non_existentATTR NOPRESENT",
            "$Object:creationEpoch GT 1",
            f"FileName NE {head_info1['header']['attributes']['FileName']}",
        ],
        attributes=["non_existentATTR", "$Object:creationEpoch", "FileName"] if with_attributes else None,
    )
    assert len(found_objects) == 1, "invalid number of found objects"
    assert found_objects[0]["id"] == oid2, "invalid object returned from search"


@pytest.mark.parametrize(
    "neofs_env",
    [
        {"chain_meta_data": False},
        {"chain_meta_data": True},
    ],
    ids=["chain_meta_data=False", "chain_meta_data=True"],
    indirect=True,
)
@pytest.mark.simple
def test_search_attrs_ordering(
    default_wallet: NodeWallet,
    container: str,
    neofs_env: NeoFSEnv,
):
    cid = container
    created_objects = []
    for idx in range(5):
        file_path = generate_file(neofs_env.get_object_size("simple_object_size") + (idx * 100))
        attrs = {"str_attr": "ab" * (idx + 1)}
        created_objects.append(
            {
                "id": put_object_to_random_node(
                    default_wallet.path,
                    file_path,
                    cid,
                    shell=neofs_env.shell,
                    neofs_env=neofs_env,
                    attributes={"str_attr": "ab" * (6 - idx)},
                ),
                "attrs": attrs,
            }
        )

    with allure.step("Verify unfiltered ordering"):
        found_objects, _ = search_objectv2(
            rpc_endpoint=neofs_env.sn_rpc,
            wallet=default_wallet.path,
            cid=cid,
            shell=neofs_env.shell,
        )

        max_oid = base58.b58decode(found_objects[0]["id"])
        for found_obj in found_objects[1:]:
            current_oid = base58.b58decode(found_obj["id"])
            assert current_oid > max_oid, "invalid ordering in search output"
            max_oid = current_oid

    with allure.step("Verify numeric ordering"):
        found_objects, _ = search_objectv2(
            rpc_endpoint=neofs_env.sn_rpc,
            wallet=default_wallet.path,
            cid=cid,
            shell=neofs_env.shell,
            filters=[
                f"$Object:payloadLength GE {neofs_env.get_object_size('simple_object_size')}",
                "str_attr COMMON_PREFIX ab",
            ],
            attributes=["$Object:payloadLength", "str_attr"],
        )

        max_length = int(get_attribute_value_from_found_object(found_objects[0], "$Object:payloadLength"))
        for found_obj in found_objects[1:]:
            current_length = int(get_attribute_value_from_found_object(found_obj, "$Object:payloadLength"))
            assert current_length > max_length, "invalid ordering in search output"
            max_length = current_length

    with allure.step("Verify lexicographical ordering"):
        found_objects, _ = search_objectv2(
            rpc_endpoint=neofs_env.sn_rpc,
            wallet=default_wallet.path,
            cid=cid,
            shell=neofs_env.shell,
            filters=[
                "str_attr COMMON_PREFIX ab",
                f"$Object:payloadLength GE {neofs_env.get_object_size('simple_object_size')}",
            ],
            attributes=["str_attr", "$Object:payloadLength"],
        )

        max_str = get_attribute_value_from_found_object(found_objects[0], "str_attr")
        for found_obj in found_objects[1:]:
            current_str = get_attribute_value_from_found_object(found_obj, "str_attr")
            assert current_str > max_str, "invalid ordering in search output"
            max_str = current_str


@pytest.mark.parametrize(
    "neofs_env",
    [
        {"chain_meta_data": False},
        {"chain_meta_data": True},
    ],
    ids=["chain_meta_data=False", "chain_meta_data=True"],
    indirect=True,
)
@pytest.mark.simple
def test_search_attrs_ordering_with_cursor(
    default_wallet: NodeWallet,
    container: str,
    neofs_env: NeoFSEnv,
):
    cid = container
    created_objects = []
    for idx in range(5):
        file_path = generate_file(neofs_env.get_object_size("simple_object_size") + (idx * 100))
        attrs = {"str_attr": "ab" * (idx + 1)}
        created_objects.append(
            {
                "id": put_object_to_random_node(
                    default_wallet.path,
                    file_path,
                    cid,
                    shell=neofs_env.shell,
                    neofs_env=neofs_env,
                    attributes={"str_attr": "ab" * (6 - idx)},
                ),
                "attrs": attrs,
            }
        )

    with allure.step("Verify unfiltered ordering without cursor"):
        found_objects, _ = search_objectv2(
            rpc_endpoint=neofs_env.sn_rpc,
            wallet=default_wallet.path,
            cid=cid,
            shell=neofs_env.shell,
        )

        max_oid = base58.b58decode(found_objects[0]["id"])
        for found_obj in found_objects[1:]:
            current_oid = base58.b58decode(found_obj["id"])
            assert current_oid > max_oid, "invalid ordering in search output without cursor"
            max_oid = current_oid

    with allure.step("Verify unfiltered ordering with cursor"):
        cursor = None
        found_objects = []
        for _ in range(len(created_objects)):
            new_found_objects, cursor = search_objectv2(
                rpc_endpoint=neofs_env.sn_rpc,
                wallet=default_wallet.path,
                cid=cid,
                shell=neofs_env.shell,
                cursor=cursor,
                count=1,
            )
            found_objects.extend(new_found_objects)

        assert cursor is None, "cursor is not None after all objects were found"

        max_oid = base58.b58decode(found_objects[0]["id"])
        for found_obj in found_objects[1:]:
            current_oid = base58.b58decode(found_obj["id"])
            assert current_oid > max_oid, "invalid ordering in search output with cursor"
            max_oid = current_oid

    with allure.step("Verify numeric ordering"):
        cursor = None
        found_objects = []
        for _ in range(len(created_objects)):
            new_found_objects, cursor = search_objectv2(
                rpc_endpoint=neofs_env.sn_rpc,
                wallet=default_wallet.path,
                cid=cid,
                shell=neofs_env.shell,
                filters=[
                    f"$Object:payloadLength GE {neofs_env.get_object_size('simple_object_size')}",
                    "str_attr COMMON_PREFIX ab",
                ],
                attributes=["$Object:payloadLength", "str_attr"],
                cursor=cursor,
                count=1,
            )
            found_objects.extend(new_found_objects)

        assert cursor is None, "cursor is not None after all objects were found"

        max_length = int(get_attribute_value_from_found_object(found_objects[0], "$Object:payloadLength"))
        for found_obj in found_objects[1:]:
            current_length = int(get_attribute_value_from_found_object(found_obj, "$Object:payloadLength"))
            assert current_length > max_length, "invalid ordering in search output"
            max_length = current_length

    with allure.step("Verify lexicographical ordering"):
        cursor = None
        found_objects = []
        for _ in range(len(created_objects)):
            new_found_objects, cursor = search_objectv2(
                rpc_endpoint=neofs_env.sn_rpc,
                wallet=default_wallet.path,
                cid=cid,
                shell=neofs_env.shell,
                filters=[
                    "str_attr COMMON_PREFIX ab",
                    f"$Object:payloadLength GE {neofs_env.get_object_size('simple_object_size')}",
                ],
                attributes=["str_attr", "$Object:payloadLength"],
                cursor=cursor,
                count=1,
            )
            found_objects.extend(new_found_objects)

        assert cursor is None, "cursor is not None after all objects were found"

        max_str = get_attribute_value_from_found_object(found_objects[0], "str_attr")
        for found_obj in found_objects[1:]:
            current_str = get_attribute_value_from_found_object(found_obj, "str_attr")
            assert current_str > max_str, "invalid ordering in search output"
            max_str = current_str


@pytest.mark.parametrize(
    "neofs_env",
    [
        {"chain_meta_data": False},
        {"chain_meta_data": True},
    ],
    ids=["chain_meta_data=False", "chain_meta_data=True"],
    indirect=True,
)
@pytest.mark.simple
def test_search_with_cursor_empty_filters_and_attributes(
    default_wallet: NodeWallet,
    container: str,
    neofs_env: NeoFSEnv,
):
    cid = container
    created_objects = []
    for idx in range(5):
        file_path = generate_file(neofs_env.get_object_size("simple_object_size") + (idx * 100))
        created_objects.append(
            {
                "id": put_object_to_random_node(
                    default_wallet.path,
                    file_path,
                    cid,
                    shell=neofs_env.shell,
                    neofs_env=neofs_env,
                ),
            }
        )

    for count in [1, 2, 3]:
        with allure.step(f"Search with {count=}"):
            cursor = None
            found_objects = []
            for _ in range(math.ceil(len(created_objects) / count)):
                new_found_objects, cursor = search_objectv2(
                    rpc_endpoint=neofs_env.sn_rpc,
                    wallet=default_wallet.path,
                    cid=cid,
                    shell=neofs_env.shell,
                    cursor=cursor,
                    count=count,
                )
                found_objects.extend(new_found_objects)

            assert cursor is None, "cursor is not empty after all objects were found"
            assert len(found_objects) == len(created_objects), "invalid number of found objects"

            max_oid = base58.b58decode(found_objects[0]["id"])
            for found_obj in found_objects[1:]:
                current_oid = base58.b58decode(found_obj["id"])
                assert current_oid > max_oid, "invalid ordering in search output"
                max_oid = current_oid


@pytest.mark.parametrize(
    "neofs_env",
    [
        {"chain_meta_data": False},
        {"chain_meta_data": True},
    ],
    ids=["chain_meta_data=False", "chain_meta_data=True"],
    indirect=True,
)
@pytest.mark.simple
def test_search_count_and_cursor(
    default_wallet: NodeWallet,
    container: str,
    neofs_env: NeoFSEnv,
):
    cid = container
    created_objects = []
    for _ in range(5):
        file_path = generate_file(neofs_env.get_object_size("simple_object_size"))
        created_objects.append(
            {
                "id": put_object_to_random_node(
                    default_wallet.path,
                    file_path,
                    cid,
                    shell=neofs_env.shell,
                    neofs_env=neofs_env,
                ),
            }
        )

    for count_value in [len(created_objects), len(created_objects) + 1]:
        found_objects, cursor = search_objectv2(
            rpc_endpoint=neofs_env.sn_rpc,
            wallet=default_wallet.path,
            cid=cid,
            shell=neofs_env.shell,
            filters=["$Object:creationEpoch GE 0"],
            attributes=["$Object:creationEpoch"],
            count=count_value,
        )

        assert not cursor, "there should be no cursor object in search output"
        assert len(found_objects) == len(created_objects), "invalid objects count after search"

    found_objects = []

    first_found_objects, first_cursor = search_objectv2(
        rpc_endpoint=neofs_env.sn_rpc,
        wallet=default_wallet.path,
        cid=cid,
        shell=neofs_env.shell,
        filters=["$Object:creationEpoch GE 0"],
        attributes=["$Object:creationEpoch"],
        count=2,
    )

    assert len(first_found_objects) == 2, "invalid objects count after search"

    found_objects.extend(first_found_objects)

    second_found_objects, cursor = search_objectv2(
        rpc_endpoint=neofs_env.sn_rpc,
        wallet=default_wallet.path,
        cid=cid,
        shell=neofs_env.shell,
        filters=["$Object:creationEpoch GE 0"],
        attributes=["$Object:creationEpoch"],
        cursor=first_cursor,
        count=2,
    )

    assert len(second_found_objects) == 2, "invalid objects count after search"

    found_objects.extend(second_found_objects)

    last_found_objects, last_cursor = search_objectv2(
        rpc_endpoint=neofs_env.sn_rpc,
        wallet=default_wallet.path,
        cid=cid,
        shell=neofs_env.shell,
        filters=["$Object:creationEpoch GE 0"],
        attributes=["$Object:creationEpoch"],
        cursor=cursor,
        count=2,
    )

    assert not last_cursor, "there should be no last cursor object in search output"
    assert len(last_found_objects) == 1, "invalid objects count after search"

    found_objects.extend(last_found_objects)

    for created_obj in created_objects:
        assert any(found_obj["id"] == created_obj["id"] for found_obj in found_objects), (
            f"created object {created_obj['id']} not found in search output"
        )

    last_found_objects, last_cursor = search_objectv2(
        rpc_endpoint=neofs_env.sn_rpc,
        wallet=default_wallet.path,
        cid=cid,
        shell=neofs_env.shell,
        filters=["$Object:creationEpoch GE 0"],
        attributes=["$Object:creationEpoch"],
        cursor=cursor,
        count=2,
    )

    assert not last_cursor, "there should be no last cursor object in search output"
    assert len(last_found_objects) == 1, "invalid objects count after search"

    # invalid cursor
    with pytest.raises(Exception):
        last_found_objects, cursor = search_objectv2(
            rpc_endpoint=neofs_env.sn_rpc,
            wallet=default_wallet.path,
            cid=cid,
            shell=neofs_env.shell,
            filters=["$Object:creationEpoch GE 0"],
            attributes=["$Object:creationEpoch"],
            cursor="0123_???!##",
            count=2,
        )

    for created_obj in created_objects:
        delete_object(
            default_wallet.path,
            cid,
            created_obj["id"],
            neofs_env.shell,
            neofs_env.sn_rpc,
        )

    neofs_epoch.tick_epoch_and_wait(neofs_env)
    neofs_epoch.tick_epoch_and_wait(neofs_env)

    tombstone_objects, _ = search_objectv2(
        rpc_endpoint=neofs_env.sn_rpc,
        wallet=default_wallet.path,
        cid=cid,
        shell=neofs_env.shell,
        filters=["$Object:creationEpoch GE 0"],
        attributes=["$Object:creationEpoch", "$Object:objectType"],
        cursor=first_cursor,
    )
    assert len(tombstone_objects) > 0, "invalid objects count after search"

    with allure.step("Verify --root doesn't return tombstones"):
        found_objects, _ = search_objectv2(
            rpc_endpoint=neofs_env.sn_rpc, wallet=default_wallet.path, cid=cid, shell=neofs_env.shell, root=True
        )
        assert len(found_objects) == 0, "invalid number of found objects with --root filter"

    with allure.step("Verify --phy returns tombstones"):
        tombstone_objects, _ = search_objectv2(
            rpc_endpoint=neofs_env.sn_rpc, wallet=default_wallet.path, cid=cid, shell=neofs_env.shell, phy=True
        )

        assert len(tombstone_objects) > 0, "invalid objects count after search"

    with pytest.raises(Exception, match=".*wrong primary attribute.*"):
        not_tombstone_objects, _ = search_objectv2(
            rpc_endpoint=neofs_env.sn_rpc,
            wallet=default_wallet.path,
            cid=cid,
            shell=neofs_env.shell,
            filters=["$Object:objectType NE TOMBSTONE"],
            attributes=["$Object:objectType"],
            cursor=first_cursor,
        )


@pytest.mark.parametrize(
    "neofs_env",
    [
        {"chain_meta_data": False},
        {"chain_meta_data": True},
    ],
    ids=["chain_meta_data=False", "chain_meta_data=True"],
    indirect=True,
)
@pytest.mark.simple
def test_search_invalid_filters(
    default_wallet: NodeWallet,
    container: str,
    neofs_env: NeoFSEnv,
):
    cid = container
    created_objects = []
    for _ in range(2):
        file_path = generate_file(neofs_env.get_object_size("simple_object_size"))
        created_objects.append(
            {
                "id": put_object_to_random_node(
                    default_wallet.path,
                    file_path,
                    cid,
                    shell=neofs_env.shell,
                    neofs_env=neofs_env,
                ),
            }
        )
    for op in ["GE", "GT", "LT", "LE", "EQ", "COMMON_PREFIX"]:
        found_objects, _ = search_objectv2(
            rpc_endpoint=neofs_env.sn_rpc,
            wallet=default_wallet.path,
            cid=cid,
            shell=neofs_env.shell,
            filters=[f"$Object:creationEpoch {op} abc"],
            attributes=["$Object:creationEpoch"],
        )
        assert len(found_objects) == 0, "invalid number of found objects"
        found_objects, _ = search_objectv2(
            rpc_endpoint=neofs_env.sn_rpc,
            wallet=default_wallet.path,
            cid=cid,
            shell=neofs_env.shell,
            filters=[f"$Object:creationEpoch {op} 0_0"],
            attributes=["$Object:creationEpoch"],
        )
        assert len(found_objects) == 0, "invalid number of found objects"

    with pytest.raises(Exception, match=r".*unsupported operation.*"):
        search_objectv2(
            rpc_endpoint=neofs_env.sn_rpc,
            wallet=default_wallet.path,
            cid=cid,
            shell=neofs_env.shell,
            filters=["$Object:creationEpoch ?? 0"],
            attributes=["$Object:creationEpoch"],
        )

    with pytest.raises(Exception, match=r".*unsupported operation.*"):
        search_objectv2(
            rpc_endpoint=neofs_env.sn_rpc,
            wallet=default_wallet.path,
            cid=cid,
            shell=neofs_env.shell,
            filters=["$Object:creationEpoch GT 0, ab ?? 0"],
            attributes=["$Object:creationEpoch"],
        )

    for op in ["GE", "GT", "LT", "LE", "EQ", "NE", "COMMON_PREFIX"]:
        with pytest.raises(Exception, match=r".*unsupported operation.*"):
            search_objectv2(
                rpc_endpoint=neofs_env.sn_rpc,
                wallet=default_wallet.path,
                cid=cid,
                shell=neofs_env.shell,
                filters=[f"$Object:creationEpoch {op}"],
                attributes=["$Object:creationEpoch"],
            )

    with pytest.raises(Exception, match=r".*unsupported operation.*"):
        search_objectv2(
            rpc_endpoint=neofs_env.sn_rpc,
            wallet=default_wallet.path,
            cid=cid,
            shell=neofs_env.shell,
            filters=["$Object:creationEpoch NOPRESENT 123"],
            attributes=["$Object:creationEpoch"],
        )


@pytest.mark.parametrize(
    "neofs_env",
    [
        {"chain_meta_data": False},
        {"chain_meta_data": True},
    ],
    ids=["chain_meta_data=False", "chain_meta_data=True"],
    indirect=True,
)
@pytest.mark.simple
def test_search_conflicting_filters(
    default_wallet: NodeWallet,
    container: str,
    neofs_env: NeoFSEnv,
):
    cid = container
    file_path = generate_file(neofs_env.get_object_size("simple_object_size"))
    (
        put_object_to_random_node(
            default_wallet.path,
            file_path,
            cid,
            shell=neofs_env.shell,
            neofs_env=neofs_env,
            attributes={"int_attr": 1000},
        ),
    )

    found_objects, _ = search_objectv2(
        rpc_endpoint=neofs_env.sn_rpc,
        wallet=default_wallet.path,
        cid=cid,
        shell=neofs_env.shell,
        filters=["int_attr GE 1000", "int_attr LT 1000"],
        attributes=["int_attr"],
    )

    assert len(found_objects) == 0, "invalid number of found objects"


@pytest.mark.parametrize(
    "neofs_env",
    [
        {"chain_meta_data": False},
        {"chain_meta_data": True},
    ],
    ids=["chain_meta_data=False", "chain_meta_data=True"],
    indirect=True,
)
@pytest.mark.simple
def test_search_filters_attributes_limits(
    default_wallet: NodeWallet,
    container: str,
    neofs_env: NeoFSEnv,
):
    cid = container
    file_path = generate_file(neofs_env.get_object_size("simple_object_size"))
    put_object_to_random_node(
        default_wallet.path,
        file_path,
        cid,
        shell=neofs_env.shell,
        neofs_env=neofs_env,
        attributes={f"int_attr{x}": 1000 + x for x in range(10)},
    )
    found_objects, _ = search_objectv2(
        rpc_endpoint=neofs_env.sn_rpc,
        wallet=default_wallet.path,
        cid=cid,
        shell=neofs_env.shell,
        filters=[f"int_attr{x} GE 1000" for x in range(8)],
        attributes=[f"int_attr{x}" for x in range(8)],
    )

    assert len(found_objects) == 1, "invalid number of found objects"
    assert len(found_objects[0]["attrs"]) == 8, "invalid number of attributes in found object"

    with pytest.raises(Exception, match=r".*more than 8 filters.*"):
        found_objects, _ = search_objectv2(
            rpc_endpoint=neofs_env.sn_rpc,
            wallet=default_wallet.path,
            cid=cid,
            shell=neofs_env.shell,
            filters=[f"int_attr{x} GE 1000" for x in range(9)],
            attributes=[f"int_attr{x}" for x in range(8)],
        )

    with pytest.raises(Exception, match=r".*more than 8 attributes.*"):
        found_objects, _ = search_objectv2(
            rpc_endpoint=neofs_env.sn_rpc,
            wallet=default_wallet.path,
            cid=cid,
            shell=neofs_env.shell,
            filters=[f"int_attr{x} GE 1000" for x in range(8)],
            attributes=[f"int_attr{x}" for x in range(9)],
        )


@pytest.mark.parametrize("meta_info_consistency", ["strict", "optimistic"])
def test_searchv2_meta_enabled_containers(
    neofs_env_chain_meta_data: NeoFSEnv, default_wallet: NodeWallet, meta_info_consistency: str
):
    neofs_env = neofs_env_chain_meta_data
    with allure.step(f"Create container with __NEOFS__METAINFO_CONSISTENCY={meta_info_consistency}"):
        cid = create_container(
            default_wallet.path,
            shell=neofs_env.shell,
            endpoint=neofs_env.sn_rpc,
            rule="REP 1 CBF 4",
            attributes={"__NEOFS__METAINFO_CONSISTENCY": meta_info_consistency},
        )

    with allure.step("Put objects to different nodes"):
        for sn in neofs_env.storage_nodes:
            for _ in range(2):
                file_path = generate_file(int(SIMPLE_OBJECT_SIZE))
                put_object(
                    default_wallet.path,
                    file_path,
                    cid,
                    neofs_env.shell,
                    sn.endpoint,
                )

    with allure.step("Search from one node should return all created objects"):
        expected_number_of_object_found(
            default_wallet, cid, neofs_env, expected_number_of_objects=len(neofs_env.storage_nodes) * 2
        )

    with allure.step("Kill all nodes except one"):
        alive_node = neofs_env.storage_nodes[0]
        for sn in neofs_env.storage_nodes[1:]:
            sn.kill()

    with allure.step("Search from alive node should return all created objects"):
        found_objects, _ = search_objectv2(
            rpc_endpoint=alive_node.endpoint,
            wallet=default_wallet.path,
            cid=cid,
            shell=neofs_env.shell,
        )

        assert len(found_objects) == len(neofs_env.storage_nodes) * 2, "invalid number of found objects"


@allure.title("Test ROOT object search for big objects")
@pytest.mark.complex
def test_root_search_for_big_object(default_wallet: NodeWallet, container: str, neofs_env: NeoFSEnv):
    cid = container
    oid = put_object_to_random_node(
        default_wallet.path,
        generate_file(neofs_env.get_object_size("complex_object_size") * 3),
        cid,
        shell=neofs_env.shell,
        neofs_env=neofs_env,
    )

    found_objects = search_object(
        default_wallet.path,
        cid,
        shell=neofs_env.shell,
        endpoint=neofs_env.sn_rpc,
        expected_objects_list=[oid],
        root=True,
        fail_on_assert=True,
    )

    found_objects, _ = search_objectv2(
        rpc_endpoint=neofs_env.sn_rpc,
        wallet=default_wallet.path,
        cid=cid,
        shell=neofs_env.shell,
        root=True,
    )
    assert len(found_objects) == 1, "invalid number of objects in searchv2 outut"
    assert any(found_obj["id"] == oid for found_obj in found_objects), (
        f"created object {oid} not found in searchv2 output"
    )
