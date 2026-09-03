import logging

import allure
import pytest
from helpers.complex_object_actions import get_nodes_with_object
from helpers.container import create_container, delete_container
from helpers.file_helper import generate_file
from helpers.neofs_verbs import delete_object, head_object, put_object, search_object
from helpers.node_management import drop_object
from helpers.test_control import wait_for_success
from neofs_testlib.env.env import NeoFSEnv, NodeWallet, StorageNode

logger = logging.getLogger("NeoLogger")

SHARED_ATTR = "ec_search_marker"
UNIQUE_ATTR = "ec_search_oid_idx"
SEARCH_REPEATS = 3
OBJECTS_COUNT = 12
EC_PART_IDX_ATTR = "__NEOFS__EC_PART_IDX"
KEEP_NODES_PER_OBJECT = 2


DEGRADED_ENV = {
    "replication_cooldown": "1h",
    "chain_meta_data": False,
    "disable_post_initial_queue": True,
    "sn_with_tls_index": None,
}
HEALTHY_ENV = {
    "chain_meta_data": False,
    "sn_with_tls_index": None,
}

EC_SEARCH_SETUPS = [
    pytest.param(DEGRADED_ENV, "EC 2/1 CBF 2", True, id="EC_2/1_CBF_2_degraded"),
    pytest.param(HEALTHY_ENV, "EC 1/1 CBF 2", False, id="EC_1/1_CBF_2"),
]


def _object_attributes(head_info: dict) -> dict:
    attrs = head_info["header"].get("attributes")
    if isinstance(attrs, list):
        return {attr["key"]: attr["value"] for attr in attrs}
    return attrs or {}


def put_objects_with_search_attrs(
    wallet: NodeWallet,
    cid: str,
    neofs_env: NeoFSEnv,
    count: int,
    object_size: str,
    extra_attrs: dict | None = None,
) -> list[dict]:
    created = []
    file_size = neofs_env.get_object_size(object_size)
    for idx in range(count):
        endpoint = neofs_env.storage_nodes[idx % len(neofs_env.storage_nodes)].endpoint
        attrs = {SHARED_ATTR: "shared", UNIQUE_ATTR: str(idx)}
        if extra_attrs:
            attrs.update(extra_attrs)
        created.append(
            {
                "id": put_object(
                    wallet.path,
                    generate_file(file_size),
                    cid,
                    neofs_env.shell,
                    endpoint,
                    attributes=attrs,
                ),
                UNIQUE_ATTR: idx,
                **(extra_attrs or {}),
            }
        )
    return created


def found_ids(found_objects: list[dict]) -> set[str]:
    return {found_obj["id"] for found_obj in found_objects}


def assert_all_expected_found(found_objects: list[dict], expected_oids: set[str], context: str):
    actual = found_ids(found_objects)
    missing = expected_oids - actual
    assert not missing, f"{context}: search missed objects {missing}; found {actual}"


def leave_parts_on_two_nodes(wallet: NodeWallet, cid: str, parent_oids: set[str], neofs_env: NeoFSEnv):
    found_objects, _ = search_object(
        rpc_endpoint=neofs_env.sn_rpc,
        wallet=wallet.path,
        cid=cid,
        shell=neofs_env.shell,
        phy=True,
    )

    copies: dict[str, list[tuple[str, StorageNode]]] = {oid: [] for oid in parent_oids}

    for obj in found_objects:
        oid = obj["id"]
        head_info = head_object(
            wallet.path,
            cid,
            oid,
            shell=neofs_env.shell,
            endpoint=neofs_env.sn_rpc,
        )
        attrs = _object_attributes(head_info)
        if EC_PART_IDX_ATTR not in attrs:
            continue
        parent_oid = (head_info["header"].get("split") or {}).get("parent")
        if parent_oid not in parent_oids:
            continue
        for host in get_nodes_with_object(
            cid,
            oid,
            shell=neofs_env.shell,
            nodes=neofs_env.storage_nodes,
            neofs_env=neofs_env,
        ):
            copies[parent_oid].append((oid, host))

    keep_hosts: dict[str, set[str]] = {}
    keep_keys: set[tuple[str, str]] = set()
    extra_parts: list[tuple[StorageNode, str]] = []

    for parent_oid, part_hosts in copies.items():
        hosts_by_endpoint: dict[str, StorageNode] = {}
        for part_oid, host in part_hosts:
            if host.endpoint not in hosts_by_endpoint and len(hosts_by_endpoint) < KEEP_NODES_PER_OBJECT:
                hosts_by_endpoint[host.endpoint] = host
                keep_keys.add((host.endpoint, part_oid))
            elif (host.endpoint, part_oid) not in keep_keys:
                extra_parts.append((host, part_oid))
        assert len(hosts_by_endpoint) == KEEP_NODES_PER_OBJECT, (
            f"{parent_oid}: need parts on {KEEP_NODES_PER_OBJECT} SNs, found {list(hosts_by_endpoint)}"
        )
        keep_hosts[parent_oid] = set(hosts_by_endpoint)

    dropped: set[tuple[str, str]] = set()
    for host, oid in extra_parts:
        key = (host.endpoint, oid)
        if key in keep_keys or key in dropped:
            continue
        drop_object(host, cid, oid)
        dropped.add(key)

    for parent_oid, kept in keep_hosts.items():
        for host in neofs_env.storage_nodes:
            key = (host.endpoint, parent_oid)
            if host.endpoint in kept or key in dropped:
                continue
            drop_object(host, cid, parent_oid)
            dropped.add(key)

    for parent_oid, kept in keep_hosts.items():
        for host in neofs_env.storage_nodes:
            if host.endpoint not in kept:
                continue
            found_objects, _ = search_object(
                rpc_endpoint=host.endpoint,
                wallet=wallet.path,
                cid=cid,
                shell=neofs_env.shell,
                filters=[f"{SHARED_ATTR} EQ shared"],
                attributes=[SHARED_ATTR],
                root=True,
                ttl=1,
            )
            assert parent_oid in found_ids(found_objects), (
                f"kept part on {host.endpoint} is not locally searchable for {parent_oid}"
            )


def assert_filtered_search_is_complete_and_stable(
    wallet: NodeWallet,
    cid: str,
    neofs_env: NeoFSEnv,
    expected_oids: set[str],
    filters: list[str],
    attributes: list[str] | None = None,
):
    for sn in neofs_env.storage_nodes:
        previous = None
        for attempt in range(SEARCH_REPEATS):
            found_objects, _ = search_object(
                rpc_endpoint=sn.endpoint,
                wallet=wallet.path,
                cid=cid,
                shell=neofs_env.shell,
                filters=filters,
                attributes=attributes,
                root=True,
            )
            context = f"node {sn.endpoint}, attempt {attempt + 1}/{SEARCH_REPEATS}"
            assert_all_expected_found(found_objects, expected_oids, context)
            current = found_ids(found_objects) & expected_oids
            if previous is not None:
                assert current == previous, f"{context}: search result changed between attempts"
            previous = current


def _prepare_searchable_objects(
    wallet: NodeWallet,
    cid: str,
    neofs_env: NeoFSEnv,
    degrade: bool,
    count: int = OBJECTS_COUNT,
) -> list[dict]:
    created = put_objects_with_search_attrs(wallet, cid, neofs_env, count, "simple_object_size")
    if degrade:
        leave_parts_on_two_nodes(wallet, cid, {obj["id"] for obj in created}, neofs_env)
    return created


@pytest.mark.parametrize("neofs_env, policy, degrade", EC_SEARCH_SETUPS, indirect=["neofs_env"])
@pytest.mark.sanity
@pytest.mark.simple
def test_ec_search_filtered_complete_with_cbf(
    default_wallet: NodeWallet, neofs_env: NeoFSEnv, policy: str, degrade: bool
):
    wallet = default_wallet
    assert len(neofs_env.storage_nodes) == 4, "regression requires 4 storage nodes"

    with allure.step(f"Create {policy} container"):
        cid = create_container(
            wallet.path,
            rule=policy,
            name="ec-search",
            shell=neofs_env.shell,
            endpoint=neofs_env.sn_rpc,
        )

    try:
        with allure.step(f"Put {OBJECTS_COUNT} objects with user attributes"):
            created = _prepare_searchable_objects(wallet, cid, neofs_env, degrade)
            expected_oids = {obj["id"] for obj in created}

        with allure.step("Filtered SEARCH from every SN returns every user object"):
            assert_filtered_search_is_complete_and_stable(
                wallet,
                cid,
                neofs_env,
                expected_oids,
                filters=[f"{SHARED_ATTR} EQ shared"],
                attributes=[SHARED_ATTR],
            )
    finally:
        delete_container(wallet.path, cid, shell=neofs_env.shell, endpoint=neofs_env.sn_rpc)


@pytest.mark.parametrize("neofs_env, policy, degrade", EC_SEARCH_SETUPS, indirect=["neofs_env"])
@pytest.mark.simple
def test_ec_search_single_object_by_unique_attribute(
    default_wallet: NodeWallet, neofs_env: NeoFSEnv, policy: str, degrade: bool
):
    wallet = default_wallet

    with allure.step(f"Create {policy} container"):
        cid = create_container(
            wallet.path,
            rule=policy,
            name="ec-search",
            shell=neofs_env.shell,
            endpoint=neofs_env.sn_rpc,
        )

    try:
        created = _prepare_searchable_objects(wallet, cid, neofs_env, degrade)

        with allure.step("Each unique attribute filter finds the matching object from every SN"):
            for obj in created:
                expected = {obj["id"]}
                for sn in neofs_env.storage_nodes:
                    for attempt in range(SEARCH_REPEATS):
                        found_objects, _ = search_object(
                            rpc_endpoint=sn.endpoint,
                            wallet=wallet.path,
                            cid=cid,
                            shell=neofs_env.shell,
                            filters=[f"{UNIQUE_ATTR} EQ {obj[UNIQUE_ATTR]}"],
                            attributes=[UNIQUE_ATTR],
                            root=True,
                        )
                        assert_all_expected_found(
                            found_objects,
                            expected,
                            f"oid {obj['id']} node {sn.endpoint} attempt {attempt + 1}",
                        )
    finally:
        delete_container(wallet.path, cid, shell=neofs_env.shell, endpoint=neofs_env.sn_rpc)


@pytest.mark.parametrize("neofs_env, policy, degrade", EC_SEARCH_SETUPS, indirect=["neofs_env"])
@pytest.mark.simple
def test_ec_search_after_delete(default_wallet: NodeWallet, neofs_env: NeoFSEnv, policy: str, degrade: bool):
    wallet = default_wallet

    with allure.step(f"Create {policy} container"):
        cid = create_container(
            wallet.path,
            rule=policy,
            name="ec-search",
            shell=neofs_env.shell,
            endpoint=neofs_env.sn_rpc,
        )

    try:
        created = _prepare_searchable_objects(wallet, cid, neofs_env, degrade)
        deleted = created[0]
        remaining = {obj["id"] for obj in created[1:]}

        with allure.step("Delete one object and search for the rest"):
            delete_object(wallet.path, cid, deleted["id"], neofs_env.shell, neofs_env.sn_rpc)

            @wait_for_success(60, 5)
            def deleted_object_is_gone():
                found_objects, _ = search_object(
                    rpc_endpoint=neofs_env.sn_rpc,
                    wallet=wallet.path,
                    cid=cid,
                    shell=neofs_env.shell,
                    filters=[f"{UNIQUE_ATTR} EQ {deleted[UNIQUE_ATTR]}"],
                    attributes=[UNIQUE_ATTR],
                    root=True,
                )
                assert deleted["id"] not in found_ids(found_objects), (
                    f"deleted object {deleted['id']} is still present in search"
                )

            deleted_object_is_gone()
            assert_filtered_search_is_complete_and_stable(
                wallet,
                cid,
                neofs_env,
                remaining,
                filters=[f"{SHARED_ATTR} EQ shared"],
                attributes=[SHARED_ATTR],
            )
    finally:
        delete_container(wallet.path, cid, shell=neofs_env.shell, endpoint=neofs_env.sn_rpc)
