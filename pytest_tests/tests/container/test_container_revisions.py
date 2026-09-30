"""End-to-end checks for container revisions.

A container starts at revision 1. The counter increases when a mutable attribute
changes (CORS) or when NEP-11 ownership is transferred. Put, search and delete
pin that value with ``--container-revision``. Storage nodes reject the call when
the pin does not match. Omitting the flag on an object call sends no revision.
Set-eACL without the flag lets the CLI send the latest revision it knows.
"""

import json
import time

import allure
import pytest
from helpers.acl import (
    EACLAccess,
    EACLOperation,
    EACLRole,
    EACLRule,
    create_eacl,
    set_eacl,
)
from helpers.common import NEOFS_CLI_EXEC, WALLET_CONFIG
from helpers.container import (
    create_container,
    create_multi_account_wallet,
    get_container,
    perform_ownership_transfer,
    refill_gas,
    set_container_attributes,
    validate_nep11_attributes,
)
from helpers.file_helper import generate_file, get_file_hash
from helpers.grpc_responses import (
    CONTAINER_REVISION_MISMATCH,
    OBJECT_ACCESS_DENIED,
    OBJECT_ALREADY_REMOVED,
    OBJECT_NOT_FOUND,
    error_matches_status,
)
from helpers.neofs_verbs import delete_object, get_object, put_object, search_object
from helpers.object_access import can_get_object
from helpers.utility import parse_version
from helpers.wellknown_acl import PUBLIC_ACL
from neo3.wallet import account as neo3_account
from neofs_env.neofs_env_test_base import TestNeofsBase
from neofs_testlib.cli import NeofsCli
from neofs_testlib.env.env import NeoFSEnv, NodeWallet

REVISION_PROPAGATION_TIMEOUT = 40
ACCESS_PROPAGATION_TIMEOUT = 30


def _cors_payload(methods: list[str], origin: str) -> str:
    rule = {
        "AllowedMethods": methods,
        "AllowedOrigins": [origin],
        "AllowedHeaders": ["*"],
        "ExposeHeaders": [],
    }
    return json.dumps([rule], separators=(",", ":"))


def _revision_mismatch(requested: int, server: int) -> str:
    return rf"{CONTAINER_REVISION_MISMATCH}.*requested: {requested}, server's: {server}"


class TestContainerRevisions(TestNeofsBase):
    @pytest.fixture(scope="class", autouse=True)
    def skip_if_container_revisions_unsupported(self, neofs_env: NeoFSEnv) -> None:
        node_version = neofs_env.get_binary_version(neofs_env.neofs_node_path)
        if parse_version(node_version) <= parse_version("0.56.0"):
            pytest.skip(f"container revisions are not supported by neofs-node {node_version} (<= 0.56.0)")

    def _endpoints(self) -> list[str]:
        return [node.rpc_endpoint for node in self.neofs_env.storage_nodes]

    def _read_revision(self, wallet: str, cid: str, endpoint: str) -> int:
        info = get_container(wallet, cid, shell=self.shell, endpoint=endpoint)
        if "revision" not in info:
            return 0
        return int(info["revision"])

    def _wait_for_revision(self, wallet: str, cid: str, expected: int) -> None:
        deadline = time.time() + REVISION_PROPAGATION_TIMEOUT
        last = {}
        while time.time() < deadline:
            last = {}
            aligned = True
            for endpoint in self._endpoints():
                try:
                    last[endpoint] = self._read_revision(wallet, cid, endpoint)
                except Exception as err:
                    last[endpoint] = str(err)
                    aligned = False
                    continue
                if last[endpoint] != expected:
                    aligned = False
            if aligned:
                return
            time.sleep(1)
        raise AssertionError(
            f"container {cid} did not reach revision {expected} on every storage node, last reads: {last}"
        )

    def _assert_revision_holds(self, wallet: str, cid: str, expected: int, seconds: int = 5) -> None:
        deadline = time.time() + seconds
        while time.time() < deadline:
            for endpoint in self._endpoints():
                actual = self._read_revision(wallet, cid, endpoint)
                assert actual == expected, f"{endpoint} reports revision {actual}, expected {expected}"
            time.sleep(1)

    def _root_object_ids(self, wallet: str, cid: str, endpoint: str, revision: int | None = None) -> set[str]:
        found, _ = search_object(
            rpc_endpoint=endpoint,
            wallet=wallet,
            cid=cid,
            shell=self.shell,
            root=True,
            container_revision=revision,
        )
        return {obj["id"] for obj in found}

    def _assert_payload(self, wallet: str, cid: str, oid: str, source: str, endpoint: str, xhdr: dict = None) -> None:
        downloaded = get_object(wallet, cid, oid, self.shell, endpoint, xhdr=xhdr)
        assert get_file_hash(downloaded) == get_file_hash(source), f"payload of {oid} differs from {source}"

    def _reject_pinned_ops(self, wallet: str, cid: str, source: str, requested: int, server: int) -> None:
        pattern = _revision_mismatch(requested, server)
        for endpoint in self._endpoints():
            with allure.step(f"Revision {requested} is rejected by {endpoint} (server has {server})"):
                with pytest.raises(RuntimeError, match=pattern):
                    put_object(wallet, source, cid, self.shell, endpoint, container_revision=requested)
                with pytest.raises(RuntimeError, match=pattern):
                    self._root_object_ids(wallet, cid, endpoint, revision=requested)

    def _assert_cors(self, wallet: str, cid: str, endpoint: str, payload: str | None) -> None:
        info = get_container(wallet, cid, shell=self.shell, endpoint=endpoint)
        stored = info["attributes"].get("CORS")
        assert stored == payload, f"CORS attribute is {stored}, expected {payload}"

    def _set_eacl(self, wallet: str, cid: str, rules: list[EACLRule], revision: int, endpoint: str) -> None:
        table = create_eacl(cid, rules, shell=self.shell)
        set_eacl(
            wallet,
            cid,
            table,
            shell=self.shell,
            endpoint=endpoint,
            container_revision=revision,
        )

    def _wait_until_read(self, wallet: str, cid: str, oid: str, source: str, allowed: bool) -> None:
        deadline = time.time() + ACCESS_PROPAGATION_TIMEOUT
        last = None
        while time.time() < deadline:
            last = can_get_object(
                wallet,
                cid,
                oid,
                source,
                shell=self.shell,
                neofs_env=self.neofs_env,
                expected_error=OBJECT_ACCESS_DENIED,
            )
            if last is allowed:
                return
            time.sleep(1)
        raise AssertionError(f"expected other-user GET allowed={allowed} for {cid}/{oid}, last result was {last}")

    def _wait_until_removed(self, wallet: str, cid: str, oid: str, endpoint: str) -> None:
        deadline = time.time() + ACCESS_PROPAGATION_TIMEOUT
        last_error = None
        while time.time() < deadline:
            try:
                get_object(wallet, cid, oid, self.shell, endpoint)
            except RuntimeError as err:
                if error_matches_status(err, OBJECT_ALREADY_REMOVED) or error_matches_status(err, OBJECT_NOT_FOUND):
                    return
                last_error = err
            time.sleep(1)
        raise AssertionError(f"object {cid}/{oid} is still readable, last error: {last_error}")

    def test_revision_pins_object_and_eacl_flow(self, default_wallet: NodeWallet, not_owner_wallet: NodeWallet):
        """A client that pinned revision 1 keeps writing until CORS changes.

        After the attribute update every storage node must reject the pinned
        revision for put and search, leave already stored objects
        readable, and accept the same calls once the client refreshes the
        revision. Replacing eACL with a stale revision must keep the previous
        access policy in force for another user.
        """
        owner = default_wallet.path
        other = not_owner_wallet.path
        endpoint = self.neofs_env.sn_rpc
        other_endpoint = self._endpoints()[-1]
        file_size = self.neofs_env.get_object_size("simple_object_size")
        cors_v1 = _cors_payload(["GET"], "*")
        cors_v2 = _cors_payload(["GET", "PUT"], "https://example.com")
        deny_others_get = [EACLRule(access=EACLAccess.DENY, role=EACLRole.OTHERS, operation=EACLOperation.GET)]
        allow_others_get = [EACLRule(access=EACLAccess.ALLOW, role=EACLRole.OTHERS, operation=EACLOperation.GET)]

        with allure.step("Create a public container at revision 1"):
            cid = create_container(
                owner,
                shell=self.shell,
                endpoint=endpoint,
                basic_acl=PUBLIC_ACL,
            )
            self._wait_for_revision(owner, cid, 1)

        with allure.step("Store objects with the pinned revision and without one"):
            source_a = generate_file(file_size)
            oid_a = put_object(owner, source_a, cid, self.shell, endpoint, container_revision=1)
            assert oid_a in self._root_object_ids(owner, cid, endpoint, revision=1)
            self._assert_payload(owner, cid, oid_a, source_a, endpoint)

            source_b = generate_file(file_size)
            oid_b = put_object(owner, source_b, cid, self.shell, endpoint)
            stored = self._root_object_ids(owner, cid, endpoint)
            assert {oid_a, oid_b} <= stored

        with allure.step("Another user can read the public container"):
            self._wait_until_read(other, cid, oid_a, source_a, allowed=True)

        with allure.step("Deny GET for others at revision 1; revision stays 1"):
            self._set_eacl(owner, cid, deny_others_get, revision=1, endpoint=endpoint)
            self._assert_revision_holds(owner, cid, 1)
            self._wait_until_read(other, cid, oid_a, source_a, allowed=False)

        with allure.step("The same revision still accepts new objects after the eACL update"):
            source_c = generate_file(file_size)
            oid_c = put_object(owner, source_c, cid, self.shell, endpoint, container_revision=1)
            stored = self._root_object_ids(owner, cid, endpoint, revision=1)
            assert {oid_a, oid_b, oid_c} <= stored

        with allure.step("Publish CORS and wait until every node reports revision 2"):
            set_container_attributes(default_wallet, cid, self.neofs_env, attributes={"CORS": cors_v1})
            self._wait_for_revision(owner, cid, 2)
            self._assert_cors(owner, cid, other_endpoint, cors_v1)

        with allure.step("Stale revision 1 cannot put or search on any node"):
            self._reject_pinned_ops(owner, cid, source_a, requested=1, server=2)
            assert self._root_object_ids(owner, cid, endpoint) == stored
            self._assert_payload(owner, cid, oid_a, source_a, other_endpoint)
            self._assert_payload(owner, cid, oid_a, source_a, endpoint)

        with allure.step("A non-numeric container revision is rejected and stores nothing"):
            cli = NeofsCli(self.shell, NEOFS_CLI_EXEC, WALLET_CONFIG)
            with pytest.raises(RuntimeError, match='invalid argument "nope" for "--container-revision"'):
                cli.object.put(
                    rpc_endpoint=endpoint,
                    wallet=owner,
                    file=generate_file(file_size),
                    cid=cid,
                    container_revision="nope",
                    no_progress=True,
                )
            assert self._root_object_ids(owner, cid, endpoint) == stored

        with allure.step("Revision 2 and an unpinned client can extend the container"):
            source_d = generate_file(file_size)
            oid_d = put_object(owner, source_d, cid, self.shell, other_endpoint, container_revision=2)
            source_e = generate_file(file_size)
            oid_e = put_object(owner, source_e, cid, self.shell, endpoint)
            stored = self._root_object_ids(owner, cid, endpoint, revision=2)
            assert {oid_a, oid_b, oid_c, oid_d, oid_e} <= stored
            self._assert_payload(owner, cid, oid_d, source_d, endpoint)

        with allure.step("Stale eACL replacement keeps the deny policy"):
            with pytest.raises(RuntimeError, match=_revision_mismatch(1, 2)):
                self._set_eacl(owner, cid, allow_others_get, revision=1, endpoint=other_endpoint)
            for _ in range(3):
                assert (
                    can_get_object(
                        other,
                        cid,
                        oid_a,
                        source_a,
                        shell=self.shell,
                        neofs_env=self.neofs_env,
                        expected_error=OBJECT_ACCESS_DENIED,
                    )
                    is False
                )
                time.sleep(1)

        with allure.step("eACL replacement at revision 2 restores access and does not bump the revision"):
            self._set_eacl(owner, cid, allow_others_get, revision=2, endpoint=endpoint)
            self._assert_revision_holds(owner, cid, 2)
            self._wait_until_read(other, cid, oid_a, source_a, allowed=True)
            put_object(owner, generate_file(file_size), cid, self.shell, endpoint, container_revision=2)

        with allure.step("Replacing CORS moves the revision to 3 and invalidates revision 2"):
            set_container_attributes(default_wallet, cid, self.neofs_env, attributes={"CORS": cors_v2})
            self._wait_for_revision(owner, cid, 3)
            self._assert_cors(owner, cid, endpoint, cors_v2)
            self._reject_pinned_ops(owner, cid, source_d, requested=2, server=3)
            source_f = generate_file(file_size)
            oid_f = put_object(owner, source_f, cid, self.shell, endpoint, container_revision=3)
            self._assert_payload(owner, cid, oid_f, source_f, other_endpoint)
            self._assert_payload(owner, cid, oid_a, source_a, endpoint)

        with allure.step("Removing CORS moves the revision to 4; current pin deletes, stale pin does not"):
            # object delete has no --container-revision flag, so these pins never reach the node.
            set_container_attributes(default_wallet, cid, self.neofs_env, remove_attributes=["CORS"])
            self._wait_for_revision(owner, cid, 4)
            self._assert_cors(owner, cid, other_endpoint, None)
            with pytest.raises(RuntimeError, match=_revision_mismatch(3, 4)):
                delete_object(owner, cid, oid_a, self.shell, endpoint, container_revision=3)
            self._assert_payload(owner, cid, oid_a, source_a, endpoint)

            delete_object(owner, cid, oid_d, self.shell, other_endpoint, container_revision=4)
            self._wait_until_removed(owner, cid, oid_d, endpoint)
            assert oid_d not in self._root_object_ids(owner, cid, endpoint, revision=4)
            self._assert_payload(owner, cid, oid_a, source_a, other_endpoint)
            put_object(owner, generate_file(file_size), cid, self.shell, endpoint, container_revision=4)

    def test_ownership_transfer_bumps_revision(self):
        """Transferring a container increments its revision and changes who may use the new value.

        The previous owner is rejected with a revision mismatch while still
        holding the old number, and with an access error once the number
        matches. The new owner continues the object flow only at the current
        revision, including a later CORS update.
        """
        with allure.step("Prepare owner wallets"):
            current_owner = neo3_account.Account.create_new(self.neofs_env.default_password)
            new_owner = neo3_account.Account.create_new(self.neofs_env.default_password)
            current_owner_wallet = create_multi_account_wallet(self.neofs_env, [current_owner], "revision-owner")
            new_owner_wallet = create_multi_account_wallet(self.neofs_env, [new_owner], "revision-next-owner")
            multi_wallet = create_multi_account_wallet(
                self.neofs_env, [current_owner, new_owner], "revision-both-owners"
            )
            new_owner_node_wallet = NodeWallet(
                path=new_owner_wallet,
                address=new_owner.address,
                password=self.neofs_env.default_password,
            )

        endpoint = self.neofs_env.sn_rpc
        other_endpoint = self._endpoints()[-1]
        file_size = self.neofs_env.get_object_size("simple_object_size")

        with allure.step("Create a container and store an object at revision 1"):
            cid = create_container(current_owner_wallet, shell=self.shell, endpoint=endpoint)
            self._wait_for_revision(current_owner_wallet, cid, 1)
            source_a = generate_file(file_size)
            oid_a = put_object(current_owner_wallet, source_a, cid, self.shell, endpoint, container_revision=1)
            assert oid_a in self._root_object_ids(current_owner_wallet, cid, endpoint, revision=1)
            self._assert_payload(current_owner_wallet, cid, oid_a, source_a, endpoint)

        with allure.step("Fund both wallets and transfer the container"):
            refill_gas(self.neofs_env, current_owner_wallet, current_owner.address)
            refill_gas(self.neofs_env, new_owner_wallet, new_owner.address)
            validate_nep11_attributes(self.neofs_env, current_owner_wallet, current_owner.address, cid)
            perform_ownership_transfer(
                self.neofs_env,
                current_owner_wallet,
                current_owner.address,
                new_owner_wallet,
                new_owner.address,
                multi_wallet,
            )
            validate_nep11_attributes(self.neofs_env, new_owner_wallet, new_owner.address, cid)

        with allure.step("Every node observes revision 2 after the transfer"):
            self._wait_for_revision(new_owner_wallet, cid, 2)

        with allure.step("Old revision is a mismatch; the matching revision follows the new owner"):
            fresh = generate_file(file_size)
            pattern_stale = _revision_mismatch(1, 2)
            with pytest.raises(RuntimeError, match=pattern_stale):
                put_object(current_owner_wallet, fresh, cid, self.shell, endpoint, container_revision=1)
            with pytest.raises(RuntimeError, match=OBJECT_ACCESS_DENIED):
                put_object(current_owner_wallet, fresh, cid, self.shell, endpoint, container_revision=2)
            with pytest.raises(RuntimeError, match=pattern_stale):
                put_object(new_owner_wallet, fresh, cid, self.shell, other_endpoint, container_revision=1)

            source_b = generate_file(file_size)
            oid_b = put_object(new_owner_wallet, source_b, cid, self.shell, other_endpoint, container_revision=2)
            self._assert_payload(new_owner_wallet, cid, oid_b, source_b, endpoint)
            self._assert_payload(new_owner_wallet, cid, oid_a, source_a, other_endpoint)
            with pytest.raises(RuntimeError, match=OBJECT_ACCESS_DENIED):
                get_object(current_owner_wallet, cid, oid_a, self.shell, endpoint)

        with allure.step("New owner CORS update moves the revision to 3"):
            cors = _cors_payload(["PUT"], "https://new-owner.example")
            set_container_attributes(new_owner_node_wallet, cid, self.neofs_env, attributes={"CORS": cors})
            self._wait_for_revision(new_owner_wallet, cid, 3)
            self._assert_cors(new_owner_wallet, cid, endpoint, cors)
            with pytest.raises(RuntimeError, match=_revision_mismatch(2, 3)):
                put_object(
                    new_owner_wallet,
                    generate_file(file_size),
                    cid,
                    self.shell,
                    endpoint,
                    container_revision=2,
                )
            with pytest.raises(RuntimeError, match=_revision_mismatch(2, 3)):
                self._root_object_ids(new_owner_wallet, cid, other_endpoint, revision=2)
            source_c = generate_file(file_size)
            oid_c = put_object(new_owner_wallet, source_c, cid, self.shell, endpoint, container_revision=3)
            self._assert_payload(new_owner_wallet, cid, oid_c, source_c, other_endpoint)
            self._assert_payload(new_owner_wallet, cid, oid_b, source_b, endpoint)

        with allure.step("Delete follows the same revision and owner rules"):
            with pytest.raises(RuntimeError, match=OBJECT_ACCESS_DENIED):
                delete_object(
                    current_owner_wallet,
                    cid,
                    oid_a,
                    self.shell,
                    endpoint,
                    container_revision=3,
                )
            self._assert_payload(new_owner_wallet, cid, oid_a, source_a, endpoint)
            with pytest.raises(RuntimeError, match=_revision_mismatch(2, 3)):
                delete_object(new_owner_wallet, cid, oid_a, self.shell, endpoint, container_revision=2)
            self._assert_payload(new_owner_wallet, cid, oid_a, source_a, other_endpoint)

            delete_object(new_owner_wallet, cid, oid_a, self.shell, other_endpoint, container_revision=3)
            self._wait_until_removed(new_owner_wallet, cid, oid_a, endpoint)
            assert oid_b in self._root_object_ids(new_owner_wallet, cid, endpoint, revision=3)
