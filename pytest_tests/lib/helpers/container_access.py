import random
from typing import List, Optional

from helpers.acl import EACLOperation
from helpers.object_access import (
    can_delete_object,
    can_get_head_object,
    can_get_object,
    can_get_range_hash_of_object,
    can_get_range_of_object,
    can_put_object,
    can_search_object,
)
from neofs_testlib.env.env import NeoFSEnv
from neofs_testlib.shell import Shell


def check_full_access_to_container(
    wallet: str,
    cid: str,
    oid: str,
    file_name: str,
    shell: Shell,
    neofs_env: Optional[NeoFSEnv] = None,
    bearer: Optional[str] = None,
    wallet_config: Optional[str] = None,
    xhdr: Optional[dict] = None,
):
    if neofs_env:
        endpoint = random.choice(neofs_env.storage_nodes).endpoint
    assert can_put_object(
        wallet=wallet,
        cid=cid,
        file_name=file_name,
        shell=shell,
        neofs_env=neofs_env,
        bearer=bearer,
        wallet_config=wallet_config,
        xhdr=xhdr,
    )
    assert can_get_head_object(
        wallet=wallet,
        cid=cid,
        oid=oid,
        shell=shell,
        endpoint=endpoint,
        bearer=bearer,
        wallet_config=wallet_config,
        xhdr=xhdr,
    )
    assert can_get_range_of_object(
        wallet=wallet,
        cid=cid,
        oid=oid,
        shell=shell,
        endpoint=endpoint,
        bearer=bearer,
        wallet_config=wallet_config,
        xhdr=xhdr,
    )
    assert can_get_range_hash_of_object(
        wallet=wallet,
        cid=cid,
        oid=oid,
        shell=shell,
        endpoint=endpoint,
        bearer=bearer,
        wallet_config=wallet_config,
        xhdr=xhdr,
    )
    assert can_search_object(
        wallet=wallet,
        cid=cid,
        oid=oid,
        shell=shell,
        endpoint=endpoint,
        bearer=bearer,
        wallet_config=wallet_config,
        xhdr=xhdr,
    )
    assert can_get_object(
        wallet=wallet,
        cid=cid,
        oid=oid,
        file_name=file_name,
        shell=shell,
        neofs_env=neofs_env,
        bearer=bearer,
        wallet_config=wallet_config,
        xhdr=xhdr,
    )
    assert can_delete_object(
        wallet=wallet,
        cid=cid,
        oid=oid,
        shell=shell,
        endpoint=endpoint,
        bearer=bearer,
        wallet_config=wallet_config,
        xhdr=xhdr,
    )


def check_no_access_to_container(
    wallet: str,
    cid: str,
    oid: str,
    file_name: str,
    shell: Shell,
    neofs_env: Optional[NeoFSEnv] = None,
    bearer: Optional[str] = None,
    wallet_config: Optional[str] = None,
    xhdr: Optional[dict] = None,
):
    if neofs_env:
        endpoint = random.choice(neofs_env.storage_nodes).endpoint
    assert not can_put_object(
        wallet=wallet,
        cid=cid,
        file_name=file_name,
        shell=shell,
        neofs_env=neofs_env,
        bearer=bearer,
        wallet_config=wallet_config,
        xhdr=xhdr,
    )
    assert not can_get_head_object(
        wallet=wallet,
        cid=cid,
        oid=oid,
        shell=shell,
        endpoint=endpoint,
        bearer=bearer,
        wallet_config=wallet_config,
        xhdr=xhdr,
    )
    assert not can_get_range_of_object(
        wallet=wallet,
        cid=cid,
        oid=oid,
        shell=shell,
        endpoint=endpoint,
        bearer=bearer,
        wallet_config=wallet_config,
        xhdr=xhdr,
    )
    assert not can_get_range_hash_of_object(
        wallet=wallet,
        cid=cid,
        oid=oid,
        shell=shell,
        endpoint=endpoint,
        bearer=bearer,
        wallet_config=wallet_config,
        xhdr=xhdr,
    )
    assert not can_search_object(
        wallet=wallet,
        cid=cid,
        oid=oid,
        shell=shell,
        endpoint=endpoint,
        bearer=bearer,
        wallet_config=wallet_config,
        xhdr=xhdr,
    )
    assert not can_get_object(
        wallet=wallet,
        cid=cid,
        oid=oid,
        file_name=file_name,
        shell=shell,
        neofs_env=neofs_env,
        bearer=bearer,
        wallet_config=wallet_config,
        xhdr=xhdr,
    )
    assert not can_delete_object(
        wallet=wallet,
        cid=cid,
        oid=oid,
        shell=shell,
        endpoint=endpoint,
        bearer=bearer,
        wallet_config=wallet_config,
        xhdr=xhdr,
    )


def check_custom_access_to_container(
    wallet: str,
    cid: str,
    oid: str,
    file_name: str,
    shell: Shell,
    neofs_env: Optional[NeoFSEnv] = None,
    deny_operations: Optional[List[EACLOperation]] = None,
    ignore_operations: Optional[List[EACLOperation]] = None,
    bearer: Optional[str] = None,
    wallet_config: Optional[str] = None,
    xhdr: Optional[dict] = None,
):
    if neofs_env:
        endpoint = random.choice(neofs_env.storage_nodes).endpoint
    deny_operations = [op.value for op in deny_operations or []]
    ignore_operations = [op.value for op in ignore_operations or []]
    checks: dict = {}
    if EACLOperation.PUT.value not in ignore_operations:
        checks[EACLOperation.PUT.value] = can_put_object(
            wallet=wallet,
            cid=cid,
            file_name=file_name,
            shell=shell,
            neofs_env=neofs_env,
            bearer=bearer,
            wallet_config=wallet_config,
            xhdr=xhdr,
        )
    if EACLOperation.HEAD.value not in ignore_operations:
        checks[EACLOperation.HEAD.value] = can_get_head_object(
            wallet, cid, oid, shell, endpoint, bearer, wallet_config, xhdr
        )
    if EACLOperation.GET_RANGE.value not in ignore_operations:
        checks[EACLOperation.GET_RANGE.value] = can_get_range_of_object(
            wallet, cid, oid, shell, endpoint, bearer, wallet_config, xhdr
        )
    if EACLOperation.GET_RANGE_HASH.value not in ignore_operations:
        checks[EACLOperation.GET_RANGE_HASH.value] = can_get_range_hash_of_object(
            wallet, cid, oid, shell, endpoint, bearer, wallet_config, xhdr
        )
    if EACLOperation.SEARCH.value not in ignore_operations:
        checks[EACLOperation.SEARCH.value] = can_search_object(
            wallet, cid, shell, endpoint, oid, bearer, wallet_config, xhdr
        )
    if EACLOperation.GET.value not in ignore_operations:
        checks[EACLOperation.GET.value] = can_get_object(
            wallet=wallet,
            cid=cid,
            oid=oid,
            file_name=file_name,
            shell=shell,
            neofs_env=neofs_env,
            bearer=bearer,
            wallet_config=wallet_config,
            xhdr=xhdr,
        )
    if EACLOperation.DELETE.value not in ignore_operations:
        checks[EACLOperation.DELETE.value] = can_delete_object(
            wallet, cid, oid, shell, endpoint, bearer, wallet_config, xhdr
        )

    failed_checks = [
        f"allowed {action} failed"
        for action, success in checks.items()
        if not success and action not in deny_operations
    ] + [
        f"denied {action} succeeded"
        for action, success in checks.items()
        if success and action in deny_operations
    ]

    assert not failed_checks, ", ".join(failed_checks)


def check_read_only_container(
    wallet: str,
    cid: str,
    oid: str,
    file_name: str,
    shell: Shell,
    neofs_env: Optional[NeoFSEnv] = None,
    bearer: Optional[str] = None,
    wallet_config: Optional[str] = None,
    xhdr: Optional[dict] = None,
):
    return check_custom_access_to_container(
        wallet,
        cid,
        oid,
        file_name,
        deny_operations=[EACLOperation.PUT, EACLOperation.DELETE],
        bearer=bearer,
        wallet_config=wallet_config,
        xhdr=xhdr,
        shell=shell,
        neofs_env=neofs_env,
    )
