import random
from typing import Optional

import allure
from cluster import Cluster
from file_helper import get_file_hash
from grpc_responses import OBJECT_ACCESS_DENIED, error_matches_status
from neofs_testlib.env.env import NeoFSEnv
from neofs_testlib.shell import Shell
from python_keywords.neofs_verbs import (
    delete_object,
    get_object_from_random_node,
    get_range,
    get_range_hash,
    head_object,
    put_object_to_random_node,
    search_object,
)

OPERATION_ERROR_TYPE = RuntimeError


def can_get_object(
    wallet: str,
    cid: str,
    oid: str,
    file_name: str,
    shell: Shell,
    cluster: Optional[Cluster] = None,
    neofs_env: Optional[NeoFSEnv] = None,
    bearer: Optional[str] = None,
    wallet_config: Optional[str] = None,
    xhdr: Optional[dict] = None,
) -> bool:
    with allure.step("Try get object from container"):
        try:
            got_file_path = get_object_from_random_node(
                wallet,
                cid,
                oid,
                bearer=bearer,
                wallet_config=wallet_config,
                xhdr=xhdr,
                shell=shell,
                cluster=cluster,
                neofs_env=neofs_env,
            )
        except OPERATION_ERROR_TYPE as err:
            assert error_matches_status(
                err, OBJECT_ACCESS_DENIED
            ), f"Expected {err} to match {OBJECT_ACCESS_DENIED}"
            return False
        assert get_file_hash(file_name) == get_file_hash(got_file_path)
    return True


def can_put_object(
    wallet: str,
    cid: str,
    file_name: str,
    shell: Shell,
    cluster: Optional[Cluster] = None,
    neofs_env: Optional[NeoFSEnv] = None,
    bearer: Optional[str] = None,
    wallet_config: Optional[str] = None,
    xhdr: Optional[dict] = None,
    attributes: Optional[dict] = None,
) -> bool:
    with allure.step("Try put object to container"):
        try:
            put_object_to_random_node(
                wallet,
                file_name,
                cid,
                bearer=bearer,
                wallet_config=wallet_config,
                xhdr=xhdr,
                attributes=attributes,
                shell=shell,
                cluster=cluster,
                neofs_env=neofs_env,
            )
        except OPERATION_ERROR_TYPE as err:
            assert error_matches_status(
                err, OBJECT_ACCESS_DENIED
            ), f"Expected {err} to match {OBJECT_ACCESS_DENIED}"
            return False
    return True


def can_delete_object(
    wallet: str,
    cid: str,
    oid: str,
    shell: Shell,
    endpoint: str,
    bearer: Optional[str] = None,
    wallet_config: Optional[str] = None,
    xhdr: Optional[dict] = None,
) -> bool:
    with allure.step("Try delete object from container"):
        try:
            delete_object(
                wallet,
                cid,
                oid,
                bearer=bearer,
                wallet_config=wallet_config,
                xhdr=xhdr,
                shell=shell,
                endpoint=endpoint,
            )
        except OPERATION_ERROR_TYPE as err:
            assert error_matches_status(
                err, OBJECT_ACCESS_DENIED
            ), f"Expected {err} to match {OBJECT_ACCESS_DENIED}"
            return False
    return True


def can_get_head_object(
    wallet: str,
    cid: str,
    oid: str,
    shell: Shell,
    endpoint: str,
    bearer: Optional[str] = None,
    wallet_config: Optional[str] = None,
    xhdr: Optional[dict] = None,
) -> bool:
    with allure.step("Try get head of object"):
        try:
            head_object(
                wallet,
                cid,
                oid,
                bearer=bearer,
                wallet_config=wallet_config,
                xhdr=xhdr,
                shell=shell,
                endpoint=endpoint,
            )
        except OPERATION_ERROR_TYPE as err:
            assert error_matches_status(
                err, OBJECT_ACCESS_DENIED
            ), f"Expected {err} to match {OBJECT_ACCESS_DENIED}"
            return False
    return True


def _generate_random_range_cut(offset: int = 0, length: int = 10):
    # [X:0] requests are not allowed
    offset = random.randint(offset, length - 1)
    length = length - random.randint(offset, length - 1)
    return f"{offset}:{length}"


def can_get_range_of_object(
    wallet: str,
    cid: str,
    oid: str,
    shell: Shell,
    endpoint: str,
    bearer: Optional[str] = None,
    wallet_config: Optional[str] = None,
    xhdr: Optional[dict] = None,
) -> bool:
    with allure.step("Try get range of object"):
        try:
            get_range(
                wallet,
                cid,
                oid,
                bearer=bearer,
                range_cut=_generate_random_range_cut(),
                wallet_config=wallet_config,
                xhdr=xhdr,
                shell=shell,
                endpoint=endpoint,
            )
        except OPERATION_ERROR_TYPE as err:
            assert error_matches_status(
                err, OBJECT_ACCESS_DENIED
            ), f"Expected {err} to match {OBJECT_ACCESS_DENIED}"
            return False
    return True


def can_get_range_hash_of_object(
    wallet: str,
    cid: str,
    oid: str,
    shell: Shell,
    endpoint: str,
    bearer: Optional[str] = None,
    wallet_config: Optional[str] = None,
    xhdr: Optional[dict] = None,
) -> bool:
    with allure.step("Try get range hash of object"):
        try:
            get_range_hash(
                wallet,
                cid,
                oid,
                bearer=bearer,
                range_cut=_generate_random_range_cut(),
                wallet_config=wallet_config,
                xhdr=xhdr,
                shell=shell,
                endpoint=endpoint,
            )
        except OPERATION_ERROR_TYPE as err:
            assert error_matches_status(
                err, OBJECT_ACCESS_DENIED
            ), f"Expected {err} to match {OBJECT_ACCESS_DENIED}"
            return False
    return True


def can_search_object(
    wallet: str,
    cid: str,
    shell: Shell,
    endpoint: str,
    oid: Optional[str] = None,
    bearer: Optional[str] = None,
    wallet_config: Optional[str] = None,
    xhdr: Optional[dict] = None,
) -> bool:
    with allure.step("Try search object in container"):
        try:
            oids = search_object(
                wallet,
                cid,
                bearer=bearer,
                wallet_config=wallet_config,
                xhdr=xhdr,
                shell=shell,
                endpoint=endpoint,
            )
        except OPERATION_ERROR_TYPE as err:
            assert error_matches_status(
                err, OBJECT_ACCESS_DENIED
            ), f"Expected {err} to match {OBJECT_ACCESS_DENIED}"
            return False
        if oid:
            return oid in oids
    return True
