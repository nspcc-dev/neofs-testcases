import json
import os
import time
import uuid
from collections.abc import Callable
from typing import NamedTuple

import allure
import base58
import grpc
import pytest
from helpers.acl import (
    EACLAccess,
    EACLOperation,
    EACLRole,
    EACLRule,
    create_eacl,
    get_eacl,
    set_eacl,
)
from helpers.container import create_container, delete_container, get_container, list_containers
from helpers.grpc_responses import (
    MISSING_REQUEST_API_VERSION,
    OUTDATED_REQUEST_API_VERSION,
    OUTDATED_STRUCTURE_API_VERSION,
)
from helpers.grpc_utils import create_verification_header, get_wallet_keys
from helpers.wellknown_acl import PUBLIC_ACL
from neofs_testlib.env.env import NeoFSEnv, NodeWallet
from neofs_testlib.protobuf.generated.accounting import service_pb2 as accounting_service_pb2
from neofs_testlib.protobuf.generated.container import service_pb2 as container_service_pb2
from neofs_testlib.protobuf.generated.netmap import service_pb2 as netmap_service_pb2
from neofs_testlib.protobuf.generated.object import service_pb2 as object_service_pb2
from neofs_testlib.protobuf.generated.reputation import service_pb2 as reputation_service_pb2
from neofs_testlib.protobuf.generated.session import service_pb2 as session_service_pb2

_BAD_REQUEST = 1028
_MIN_API_VERSION = (2, 22)
_RPC_TIMEOUT_SEC = 10


class _Rpc(NamedTuple):
    name: str
    method: str
    request: type
    response: type
    kind: str = "unary"


_RPCS = (
    _Rpc(
        "Accounting.Balance",
        "/neo.fs.v2.accounting.AccountingService/Balance",
        accounting_service_pb2.BalanceRequest,
        accounting_service_pb2.BalanceResponse,
    ),
    _Rpc(
        "Container.Put",
        "/neo.fs.v2.container.ContainerService/Put",
        container_service_pb2.PutRequest,
        container_service_pb2.PutResponse,
    ),
    _Rpc(
        "Container.Delete",
        "/neo.fs.v2.container.ContainerService/Delete",
        container_service_pb2.DeleteRequest,
        container_service_pb2.DeleteResponse,
    ),
    _Rpc(
        "Container.Get",
        "/neo.fs.v2.container.ContainerService/Get",
        container_service_pb2.GetRequest,
        container_service_pb2.GetResponse,
    ),
    _Rpc(
        "Container.List",
        "/neo.fs.v2.container.ContainerService/List",
        container_service_pb2.ListRequest,
        container_service_pb2.ListResponse,
    ),
    _Rpc(
        "Container.SetExtendedACL",
        "/neo.fs.v2.container.ContainerService/SetExtendedACL",
        container_service_pb2.SetExtendedACLRequest,
        container_service_pb2.SetExtendedACLResponse,
    ),
    _Rpc(
        "Container.GetExtendedACL",
        "/neo.fs.v2.container.ContainerService/GetExtendedACL",
        container_service_pb2.GetExtendedACLRequest,
        container_service_pb2.GetExtendedACLResponse,
    ),
    _Rpc(
        "Netmap.LocalNodeInfo",
        "/neo.fs.v2.netmap.NetmapService/LocalNodeInfo",
        netmap_service_pb2.LocalNodeInfoRequest,
        netmap_service_pb2.LocalNodeInfoResponse,
    ),
    _Rpc(
        "Netmap.NetworkInfo",
        "/neo.fs.v2.netmap.NetmapService/NetworkInfo",
        netmap_service_pb2.NetworkInfoRequest,
        netmap_service_pb2.NetworkInfoResponse,
    ),
    _Rpc(
        "Netmap.NetmapSnapshot",
        "/neo.fs.v2.netmap.NetmapService/NetmapSnapshot",
        netmap_service_pb2.NetmapSnapshotRequest,
        netmap_service_pb2.NetmapSnapshotResponse,
    ),
    _Rpc(
        "Object.Get",
        "/neo.fs.v2.object.ObjectService/Get",
        object_service_pb2.GetRequest,
        object_service_pb2.GetResponse,
        "server-stream",
    ),
    _Rpc(
        "Object.Put",
        "/neo.fs.v2.object.ObjectService/Put",
        object_service_pb2.PutRequest,
        object_service_pb2.PutResponse,
        "client-stream",
    ),
    _Rpc(
        "Object.Delete",
        "/neo.fs.v2.object.ObjectService/Delete",
        object_service_pb2.DeleteRequest,
        object_service_pb2.DeleteResponse,
    ),
    _Rpc(
        "Object.Head",
        "/neo.fs.v2.object.ObjectService/Head",
        object_service_pb2.HeadRequest,
        object_service_pb2.HeadResponse,
    ),
    _Rpc(
        "Object.SearchV2",
        "/neo.fs.v2.object.ObjectService/SearchV2",
        object_service_pb2.SearchV2Request,
        object_service_pb2.SearchV2Response,
    ),
    _Rpc(
        "Reputation.AnnounceLocalTrust",
        "/neo.fs.v2.reputation.ReputationService/AnnounceLocalTrust",
        reputation_service_pb2.AnnounceLocalTrustRequest,
        reputation_service_pb2.AnnounceLocalTrustResponse,
    ),
    _Rpc(
        "Reputation.AnnounceIntermediateResult",
        "/neo.fs.v2.reputation.ReputationService/AnnounceIntermediateResult",
        reputation_service_pb2.AnnounceIntermediateResultRequest,
        reputation_service_pb2.AnnounceIntermediateResultResponse,
    ),
    _Rpc(
        "Session.Create",
        "/neo.fs.v2.session.SessionService/Create",
        session_service_pb2.CreateRequest,
        session_service_pb2.CreateResponse,
    ),
)


def _request_with_version(request_cls: type, version: tuple[int, int] | None):
    request = request_cls()
    if version is None:
        return request
    major, minor = version
    request.meta_header.version.major = major
    request.meta_header.version.minor = minor
    return request


def _call_rpc(endpoint: str, rpc: _Rpc, request, timeout: int = _RPC_TIMEOUT_SEC):
    channel = grpc.insecure_channel(endpoint)
    try:
        options = {
            "request_serializer": rpc.request.SerializeToString,
            "response_deserializer": rpc.response.FromString,
        }
        if rpc.kind == "unary":
            return channel.unary_unary(rpc.method, **options)(request, timeout=timeout)
        if rpc.kind == "client-stream":
            return channel.stream_unary(rpc.method, **options)(iter([request]), timeout=timeout)
        stream = channel.unary_stream(rpc.method, **options)(request, timeout=timeout)
        return next(stream)
    finally:
        channel.close()


def _ir_log_offsets(neofs_env: NeoFSEnv) -> list[int]:
    offsets = []
    for ir_node in neofs_env.inner_ring_nodes:
        offsets.append(os.path.getsize(ir_node.stderr) if os.path.isfile(ir_node.stderr) else 0)
    return offsets


def _ir_log_since(neofs_env: NeoFSEnv, offsets: list[int]) -> str:
    chunks = []
    for ir_node, offset in zip(neofs_env.inner_ring_nodes, offsets):
        with open(ir_node.stderr) as log_file:
            log_file.seek(offset)
            chunks.append(log_file.read())
    return "".join(chunks)


def _wait_for_ir_message(neofs_env: NeoFSEnv, offsets: list[int], message: str, context: str) -> None:
    deadline = time.time() + 20
    log = ""
    while time.time() < deadline:
        log = _ir_log_since(neofs_env, offsets)
        if message in log:
            return
        time.sleep(0.5)

    relevant = "\n".join(
        line
        for line in log.splitlines()
        if "container" in line or "eACL" in line or "API version" in line or "notary event" in line
    )
    raise AssertionError(f"Inner ring did not log {message!r}.\n{context}\nIR log:\n{relevant[-3000:]}")


def _assert_ir_rejects(neofs_env: NeoFSEnv, send: Callable[[], None], message: str) -> None:
    offsets = _ir_log_offsets(neofs_env)
    with pytest.raises(RuntimeError) as err:
        send()
    _wait_for_ir_message(neofs_env, offsets, message, f"CLI error: {err.value}")


def _eacl_with_version(table_path: str, major: int, minor: int) -> str:
    with open(table_path) as table_file:
        table = json.load(table_file)
    table["version"] = {"major": major, "minor": minor}

    versioned_path = f"{table_path}.v{major}.{minor}.json"
    with open(versioned_path, "w") as versioned_file:
        json.dump(table, versioned_file)
    return versioned_path


def _put_container_request(wallet: NodeWallet, major: int, minor: int) -> container_service_pb2.PutRequest:
    request = container_service_pb2.PutRequest()
    container = request.body.container
    container.version.major = major
    container.version.minor = minor
    container.owner_id.value = base58.b58decode(wallet.address)
    container.nonce = uuid.uuid4().bytes
    container.basic_acl = 0x1C8C8CCC
    container.placement_policy.container_backup_factor = 1
    container.placement_policy.replicas.add().count = 1
    attribute = container.attributes.add()
    attribute.key = "Name"
    attribute.value = "old-api"
    request.body.signature.key = b"\x01"
    request.body.signature.sign = b"\x02"

    request.meta_header.version.major, request.meta_header.version.minor = _MIN_API_VERSION
    request.meta_header.epoch = 1
    request.meta_header.ttl = 2
    public_key, private_key = get_wallet_keys(wallet)
    request.verify_header.CopyFrom(
        create_verification_header(
            request.body.SerializeToString(deterministic=True),
            request.meta_header.SerializeToString(deterministic=True),
            public_key,
            private_key,
        )
    )
    return request


@allure.title("Storage node rejects API requests older than v2.22")
def test_requests_older_than_minimum_api_version_are_rejected(neofs_env: NeoFSEnv):
    endpoint = neofs_env.sn_rpc
    cases = (
        ("missing", None, MISSING_REQUEST_API_VERSION),
        ("v0.0", (0, 0), OUTDATED_REQUEST_API_VERSION.format(major=0, minor=0)),
        ("v1.22", (1, 22), OUTDATED_REQUEST_API_VERSION.format(major=1, minor=22)),
        ("v2.21", (2, 21), OUTDATED_REQUEST_API_VERSION.format(major=2, minor=21)),
    )
    failures = []

    for rpc in _RPCS:
        for version_name, version, message in cases:
            request = _request_with_version(rpc.request, version)
            try:
                response = _call_rpc(endpoint, rpc, request)
            except (grpc.RpcError, StopIteration) as exc:
                failures.append(f"{rpc.name} {version_name}: transport error {exc}")
                continue

            status = response.meta_header.status
            if status.code != _BAD_REQUEST or status.message != message:
                failures.append(
                    f"{rpc.name} {version_name}: code={status.code} message={status.message!r}, "
                    f"expected code={_BAD_REQUEST} message={message!r}"
                )

    assert not failures, "Old API requests were not rejected:\n" + "\n".join(failures)


@allure.title("Storage node accepts an API v2.22 request")
def test_request_at_minimum_api_version_is_accepted(neofs_env: NeoFSEnv, default_wallet: NodeWallet):
    request = netmap_service_pb2.NetworkInfoRequest()
    request.body.SetInParent()
    request.meta_header.version.major, request.meta_header.version.minor = _MIN_API_VERSION
    request.meta_header.epoch = 1
    request.meta_header.ttl = 2

    public_key, private_key = get_wallet_keys(default_wallet)
    request.verify_header.CopyFrom(
        create_verification_header(
            request.body.SerializeToString(deterministic=True),
            request.meta_header.SerializeToString(deterministic=True),
            public_key,
            private_key,
        )
    )

    response = _call_rpc(
        neofs_env.sn_rpc,
        next(rpc for rpc in _RPCS if rpc.name == "Netmap.NetworkInfo"),
        request,
    )
    status = response.meta_header.status
    assert status.code == 0, f"API v2.22 NetworkInfo failed: code={status.code} message={status.message!r}"
    assert response.body.HasField("network_info"), "API v2.22 NetworkInfo returned an empty body"


@allure.title("Inner ring rejects containers older than API v2.22")
@pytest.mark.parametrize("version", [(2, 21), (2, 0), (1, 22)], ids=["v2.21", "v2.0", "v1.22"])
def test_container_older_than_minimum_api_version_is_rejected(
    neofs_env: NeoFSEnv, default_wallet: NodeWallet, version: tuple[int, int]
):
    major, minor = version
    message = OUTDATED_STRUCTURE_API_VERSION.format(major=major, minor=minor)
    containers_before = set(list_containers(default_wallet.path, neofs_env.shell, neofs_env.sn_rpc))
    offsets = _ir_log_offsets(neofs_env)
    put = next(rpc for rpc in _RPCS if rpc.name == "Container.Put")
    response = _call_rpc(
        neofs_env.sn_rpc,
        put,
        _put_container_request(default_wallet, major, minor),
        timeout=25,
    )
    status = response.meta_header.status
    _wait_for_ir_message(
        neofs_env,
        offsets,
        message,
        f"Put response: code={status.code} message={status.message!r}",
    )

    containers_after = set(list_containers(default_wallet.path, neofs_env.shell, neofs_env.sn_rpc))
    assert containers_after == containers_before, (
        f"Container v{major}.{minor} was persisted: {containers_after - containers_before}"
    )


@allure.title("Inner ring rejects eACL tables older than API v2.22")
@pytest.mark.parametrize("version", [(2, 21), (2, 7)], ids=["v2.21", "v2.7"])
def test_eacl_older_than_minimum_api_version_is_rejected(
    neofs_env: NeoFSEnv, default_wallet: NodeWallet, version: tuple[int, int]
):
    major, minor = version
    cid = create_container(
        default_wallet.path,
        shell=neofs_env.shell,
        endpoint=neofs_env.sn_rpc,
        rule="REP 1",
        basic_acl=PUBLIC_ACL,
    )
    table_path = create_eacl(
        cid,
        [EACLRule(access=EACLAccess.DENY, role=EACLRole.OTHERS, operation=EACLOperation.GET)],
        neofs_env.shell,
    )
    versioned_table = _eacl_with_version(table_path, major, minor)
    message = OUTDATED_STRUCTURE_API_VERSION.format(major=major, minor=minor)

    _assert_ir_rejects(
        neofs_env,
        lambda: set_eacl(default_wallet.path, cid, versioned_table, neofs_env.shell, neofs_env.sn_rpc),
        message,
    )
    assert get_eacl(default_wallet.path, cid, neofs_env.shell, neofs_env.sn_rpc) is None, (
        f"eACL v{major}.{minor} was persisted"
    )


@allure.title("Container and eACL at API v2.22 are accepted")
def test_container_and_eacl_at_minimum_api_version_are_accepted(neofs_env: NeoFSEnv, default_wallet: NodeWallet):
    cid = create_container(
        default_wallet.path,
        shell=neofs_env.shell,
        endpoint=neofs_env.sn_rpc,
        rule="REP 1",
        basic_acl=PUBLIC_ACL,
    )
    container_info = get_container(default_wallet.path, cid, shell=neofs_env.shell, endpoint=neofs_env.sn_rpc)
    container_version = (container_info["version"]["major"], container_info["version"]["minor"])
    assert container_version >= _MIN_API_VERSION, f"Container version {container_version} is below v2.22"

    major, minor = _MIN_API_VERSION
    table_path = create_eacl(
        cid,
        [EACLRule(access=EACLAccess.DENY, role=EACLRole.OTHERS, operation=EACLOperation.GET)],
        neofs_env.shell,
    )
    set_eacl(
        default_wallet.path,
        cid,
        _eacl_with_version(table_path, major, minor),
        neofs_env.shell,
        neofs_env.sn_rpc,
    )
    eacl_raw = get_eacl(default_wallet.path, cid, neofs_env.shell, neofs_env.sn_rpc)
    assert eacl_raw, "eACL v2.22 was not stored"
    eacl = json.loads(eacl_raw[eacl_raw.find("{") :])
    assert (
        eacl["version"]["major"],
        eacl["version"]["minor"],
    ) == _MIN_API_VERSION, f"Stored eACL version is not v2.22:\n{eacl}"
    assert eacl["records"], f"eACL v2.22 was stored without records:\n{eacl}"

    delete_container(default_wallet.path, cid, shell=neofs_env.shell, endpoint=neofs_env.sn_rpc)
