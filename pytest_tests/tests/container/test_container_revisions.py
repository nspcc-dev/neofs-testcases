"""End-to-end checks for container revisions.

A container starts at revision 0. The counter increases when a mutable attribute
changes (CORS) or when NEP-11 ownership is transferred. Storage nodes drop the
cached container on ``ContainerUpdated`` and reject put, search, delete and
set-eACL requests that still carry the previous revision.

Object put sends the revision in the ``__NEOFS__CONTAINER_REVISION`` header.
CLI x-header preparation replaces the header list, so put sets that header directly.
"""

import json
import os
import time
import uuid

import allure
import base58
import pytest
from helpers.acl import (
    EACLAccess,
    EACLOperation,
    EACLRole,
    EACLRule,
    create_eacl,
    set_eacl,
)
from helpers.common import get_assets_dir_path
from helpers.container import (
    create_container,
    get_container,
    set_container_attributes,
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
from neo3.wallet import wallet as neo3_wallet
from neofs_env.neofs_env_test_base import TestNeofsBase
from neofs_testlib.env.env import NeoFSEnv, NodeWallet

CONTAINER_REVISION_HEADER = "__NEOFS__CONTAINER_REVISION"
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


def _revision_header(revision: int) -> dict:
    return {CONTAINER_REVISION_HEADER: str(revision)}


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

    def _reject_pinned_ops(self, wallet: str, cid: str, oid: str, source: str, requested: int, server: int) -> None:
        pattern = _revision_mismatch(requested, server)
        for endpoint in self._endpoints():
            with allure.step(f"Revision {requested} is rejected by {endpoint} (server has {server})"):
                with pytest.raises(RuntimeError, match=pattern):
                    put_object(wallet, source, cid, self.shell, endpoint, xhdr=_revision_header(requested))
                with pytest.raises(RuntimeError, match=pattern):
                    self._root_object_ids(wallet, cid, endpoint, revision=requested)
                with pytest.raises(RuntimeError, match=pattern):
                    delete_object(
                        wallet,
                        cid,
                        oid,
                        self.shell,
                        endpoint,
                        xhdr=_revision_header(requested),
                    )

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

    def _create_multi_account_wallet(self, accounts: list[neo3_account.Account], prefix: str) -> str:
        wallet_path = os.path.join(get_assets_dir_path(), f"{prefix}-{uuid.uuid4()}.json")
        wallet = neo3_wallet.Wallet()
        for account in accounts:
            wallet.account_add(account)
        with open(wallet_path, "w") as out:
            json.dump(wallet.to_json(self.neofs_env.default_password), out)
        return wallet_path

    def _validate_nep11_owner(self, wallet_path: str, expected_owner_address: str, expected_cid: str) -> None:
        cid_in_hex = base58.b58decode(expected_cid).hex()
        balance = self.neofs_env.neo_go().nep11.balance(
            wallet=wallet_path,
            rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
        )
        assert balance["account_address"] == expected_owner_address
        neofs_adm = self.neofs_env.neofs_adm()
        contracts_hashes = neofs_adm.fschain.parse_dump_hashes(
            neofs_adm.fschain.dump_hashes(rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}").stdout
        )
        assert balance["contract_hash"] == contracts_hashes["container"]
        matching_token_id = None
        for token_id in balance["token_ids"]:
            props = self.neofs_env.neo_go().nep11.properties(
                token=balance["contract_hash"],
                id=token_id,
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
            )
            if props["name"] == expected_cid and token_id == cid_in_hex:
                matching_token_id = token_id
                break
        assert matching_token_id is not None, f"no NEP-11 token for container {expected_cid}"
        owner_address = self.neofs_env.neo_go().nep11.owner_of(
            token=balance["contract_hash"],
            id=matching_token_id,
            rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
        )
        assert owner_address == expected_owner_address

    def _transfer_container(
        self,
        from_wallet_path: str,
        from_address: str,
        to_wallet_path: str,
        to_address: str,
        multi_wallet_path: str,
    ) -> None:
        balance = self.neofs_env.neo_go().nep11.balance(
            wallet=from_wallet_path,
            rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
        )
        token_id = balance["token_ids"][0]
        transaction_file = self.neofs_env._generate_temp_file(self.neofs_env._env_dir, prefix="transfer_transaction")
        multi_wallet_config = self.neofs_env.generate_neo_go_config(
            NodeWallet(path=multi_wallet_path, address=from_address, password=self.neofs_env.default_password)
        )
        self.neofs_env.neo_go().nep11.transfer(
            wallet_config=multi_wallet_config,
            rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
            from_address=from_address,
            to_address=to_address,
            id=token_id,
            token=balance["contract_hash"],
            signer=to_address,
            out=transaction_file,
        )
        new_owner_wallet_config = self.neofs_env.generate_neo_go_config(
            NodeWallet(path=to_wallet_path, address=to_address, password=self.neofs_env.default_password)
        )
        self.neofs_env.neo_go().wallet.sign(
            input_file=transaction_file,
            address=to_address,
            rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
            wallet_config=new_owner_wallet_config,
            await_=True,
        )

    def test_revision_pins_object_and_eacl_flow(self, default_wallet: NodeWallet, not_owner_wallet: NodeWallet):
        """A client that pinned revision 0 keeps writing until CORS changes.

        After the attribute update every storage node must reject the pinned
        revision for put, search and delete, leave already stored objects
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

        with allure.step("Create a public container at revision 0"):
            cid = create_container(
                owner,
                shell=self.shell,
                endpoint=endpoint,
                basic_acl=PUBLIC_ACL,
            )
            self._wait_for_revision(owner, cid, 0)

        with allure.step("Store objects with the pinned revision and without one"):
            source_a = generate_file(file_size)
            oid_a = put_object(owner, source_a, cid, self.shell, endpoint, xhdr=_revision_header(0))
            assert oid_a in self._root_object_ids(owner, cid, endpoint, revision=0)
            self._assert_payload(owner, cid, oid_a, source_a, endpoint)

            source_b = generate_file(file_size)
            oid_b = put_object(owner, source_b, cid, self.shell, endpoint)
            stored = self._root_object_ids(owner, cid, endpoint)
            assert {oid_a, oid_b} <= stored

        with allure.step("Another user can read the public container"):
            self._wait_until_read(other, cid, oid_a, source_a, allowed=True)

        with allure.step("Deny GET for others at revision 0; revision stays 0"):
            self._set_eacl(owner, cid, deny_others_get, revision=0, endpoint=endpoint)
            self._assert_revision_holds(owner, cid, 0)
            self._wait_until_read(other, cid, oid_a, source_a, allowed=False)

        with allure.step("The same revision still accepts new objects after the eACL update"):
            source_c = generate_file(file_size)
            oid_c = put_object(owner, source_c, cid, self.shell, endpoint, xhdr=_revision_header(0))
            stored = self._root_object_ids(owner, cid, endpoint, revision=0)
            assert {oid_a, oid_b, oid_c} <= stored

        with allure.step("Publish CORS and wait until every node reports revision 1"):
            set_container_attributes(default_wallet, cid, self.neofs_env, attributes={"CORS": cors_v1})
            self._wait_for_revision(owner, cid, 1)
            self._assert_cors(owner, cid, other_endpoint, cors_v1)

        with allure.step("Stale revision 0 cannot put, search or delete on any node"):
            self._reject_pinned_ops(owner, cid, oid_a, source_a, requested=0, server=1)
            assert self._root_object_ids(owner, cid, endpoint) == stored
            self._assert_payload(owner, cid, oid_a, source_a, other_endpoint)
            self._assert_payload(owner, cid, oid_a, source_a, endpoint, xhdr=_revision_header(0))

        with allure.step("A malformed revision header is rejected and stores nothing"):
            with pytest.raises(RuntimeError, match="parsing container revision"):
                put_object(
                    owner,
                    generate_file(file_size),
                    cid,
                    self.shell,
                    endpoint,
                    xhdr={CONTAINER_REVISION_HEADER: "nope"},
                )
            assert self._root_object_ids(owner, cid, endpoint) == stored

        with allure.step("Revision 1 and an unpinned client can extend the container"):
            source_d = generate_file(file_size)
            oid_d = put_object(owner, source_d, cid, self.shell, other_endpoint, xhdr=_revision_header(1))
            source_e = generate_file(file_size)
            oid_e = put_object(owner, source_e, cid, self.shell, endpoint)
            stored = self._root_object_ids(owner, cid, endpoint, revision=1)
            assert {oid_a, oid_b, oid_c, oid_d, oid_e} <= stored
            self._assert_payload(owner, cid, oid_d, source_d, endpoint)

        with allure.step("Stale eACL replacement keeps the deny policy"):
            with pytest.raises(RuntimeError, match=_revision_mismatch(0, 1)):
                self._set_eacl(owner, cid, allow_others_get, revision=0, endpoint=other_endpoint)
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

        with allure.step("eACL replacement at revision 1 restores access and does not bump the revision"):
            self._set_eacl(owner, cid, allow_others_get, revision=1, endpoint=endpoint)
            self._assert_revision_holds(owner, cid, 1)
            self._wait_until_read(other, cid, oid_a, source_a, allowed=True)
            put_object(owner, generate_file(file_size), cid, self.shell, endpoint, xhdr=_revision_header(1))

        with allure.step("Replacing CORS moves the revision to 2 and invalidates revision 1"):
            set_container_attributes(default_wallet, cid, self.neofs_env, attributes={"CORS": cors_v2})
            self._wait_for_revision(owner, cid, 2)
            self._assert_cors(owner, cid, endpoint, cors_v2)
            self._reject_pinned_ops(owner, cid, oid_d, source_d, requested=1, server=2)
            source_f = generate_file(file_size)
            oid_f = put_object(owner, source_f, cid, self.shell, endpoint, xhdr=_revision_header(2))
            self._assert_payload(owner, cid, oid_f, source_f, other_endpoint)
            self._assert_payload(owner, cid, oid_a, source_a, endpoint)

        with allure.step("Removing CORS moves the revision to 3; current pin deletes, stale pin does not"):
            set_container_attributes(default_wallet, cid, self.neofs_env, remove_attributes=["CORS"])
            self._wait_for_revision(owner, cid, 3)
            self._assert_cors(owner, cid, other_endpoint, None)
            with pytest.raises(RuntimeError, match=_revision_mismatch(2, 3)):
                delete_object(owner, cid, oid_a, self.shell, endpoint, xhdr=_revision_header(2))
            self._assert_payload(owner, cid, oid_a, source_a, endpoint)

            delete_object(owner, cid, oid_d, self.shell, other_endpoint, xhdr=_revision_header(3))
            self._wait_until_removed(owner, cid, oid_d, endpoint)
            assert oid_d not in self._root_object_ids(owner, cid, endpoint, revision=3)
            self._assert_payload(owner, cid, oid_a, source_a, other_endpoint)
            put_object(owner, generate_file(file_size), cid, self.shell, endpoint, xhdr=_revision_header(3))

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
            current_owner_wallet = self._create_multi_account_wallet([current_owner], "revision-owner")
            new_owner_wallet = self._create_multi_account_wallet([new_owner], "revision-next-owner")
            multi_wallet = self._create_multi_account_wallet([current_owner, new_owner], "revision-both-owners")
            new_owner_node_wallet = NodeWallet(
                path=new_owner_wallet,
                address=new_owner.address,
                password=self.neofs_env.default_password,
            )

        endpoint = self.neofs_env.sn_rpc
        other_endpoint = self._endpoints()[-1]
        file_size = self.neofs_env.get_object_size("simple_object_size")

        with allure.step("Create a container and store an object at revision 0"):
            cid = create_container(current_owner_wallet, shell=self.shell, endpoint=endpoint)
            self._wait_for_revision(current_owner_wallet, cid, 0)
            source_a = generate_file(file_size)
            oid_a = put_object(current_owner_wallet, source_a, cid, self.shell, endpoint, xhdr=_revision_header(0))
            assert oid_a in self._root_object_ids(current_owner_wallet, cid, endpoint, revision=0)
            self._assert_payload(current_owner_wallet, cid, oid_a, source_a, endpoint)

        with allure.step("Fund both wallets and transfer the container"):
            self.neofs_env.neofs_adm().fschain.refill_gas(
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                alphabet_wallets=self.neofs_env.alphabet_wallets_dir,
                storage_wallet=current_owner_wallet,
                gas="200.0",
                wallet_address=current_owner.address,
            )
            self.neofs_env.neofs_adm().fschain.refill_gas(
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                alphabet_wallets=self.neofs_env.alphabet_wallets_dir,
                storage_wallet=new_owner_wallet,
                gas="200.0",
                wallet_address=new_owner.address,
            )
            self._validate_nep11_owner(current_owner_wallet, current_owner.address, cid)
            self._transfer_container(
                current_owner_wallet,
                current_owner.address,
                new_owner_wallet,
                new_owner.address,
                multi_wallet,
            )
            self._validate_nep11_owner(new_owner_wallet, new_owner.address, cid)

        with allure.step("Every node observes revision 1 after the transfer"):
            self._wait_for_revision(new_owner_wallet, cid, 1)

        with allure.step("Old revision is a mismatch; the matching revision follows the new owner"):
            fresh = generate_file(file_size)
            pattern_stale = _revision_mismatch(0, 1)
            with pytest.raises(RuntimeError, match=pattern_stale):
                put_object(current_owner_wallet, fresh, cid, self.shell, endpoint, xhdr=_revision_header(0))
            with pytest.raises(RuntimeError, match=OBJECT_ACCESS_DENIED):
                put_object(current_owner_wallet, fresh, cid, self.shell, endpoint, xhdr=_revision_header(1))
            with pytest.raises(RuntimeError, match=pattern_stale):
                put_object(new_owner_wallet, fresh, cid, self.shell, other_endpoint, xhdr=_revision_header(0))

            source_b = generate_file(file_size)
            oid_b = put_object(new_owner_wallet, source_b, cid, self.shell, other_endpoint, xhdr=_revision_header(1))
            self._assert_payload(new_owner_wallet, cid, oid_b, source_b, endpoint)
            self._assert_payload(new_owner_wallet, cid, oid_a, source_a, other_endpoint)
            with pytest.raises(RuntimeError, match=OBJECT_ACCESS_DENIED):
                get_object(current_owner_wallet, cid, oid_a, self.shell, endpoint)

        with allure.step("Delete follows the same revision and owner rules"):
            with pytest.raises(RuntimeError, match=OBJECT_ACCESS_DENIED):
                delete_object(
                    current_owner_wallet,
                    cid,
                    oid_a,
                    self.shell,
                    endpoint,
                    xhdr=_revision_header(1),
                )
            self._assert_payload(new_owner_wallet, cid, oid_a, source_a, endpoint)
            with pytest.raises(RuntimeError, match=_revision_mismatch(0, 1)):
                delete_object(new_owner_wallet, cid, oid_a, self.shell, endpoint, xhdr=_revision_header(0))
            self._assert_payload(new_owner_wallet, cid, oid_a, source_a, other_endpoint)

            delete_object(new_owner_wallet, cid, oid_a, self.shell, other_endpoint, xhdr=_revision_header(1))
            self._wait_until_removed(new_owner_wallet, cid, oid_a, endpoint)
            assert oid_b in self._root_object_ids(new_owner_wallet, cid, endpoint, revision=1)

        with allure.step("New owner CORS update moves the revision to 2"):
            cors = _cors_payload(["PUT"], "https://new-owner.example")
            set_container_attributes(new_owner_node_wallet, cid, self.neofs_env, attributes={"CORS": cors})
            self._wait_for_revision(new_owner_wallet, cid, 2)
            self._assert_cors(new_owner_wallet, cid, endpoint, cors)
            with pytest.raises(RuntimeError, match=_revision_mismatch(1, 2)):
                put_object(
                    new_owner_wallet, generate_file(file_size), cid, self.shell, endpoint, xhdr=_revision_header(1)
                )
            with pytest.raises(RuntimeError, match=_revision_mismatch(1, 2)):
                self._root_object_ids(new_owner_wallet, cid, other_endpoint, revision=1)
            source_c = generate_file(file_size)
            oid_c = put_object(new_owner_wallet, source_c, cid, self.shell, endpoint, xhdr=_revision_header(2))
            self._assert_payload(new_owner_wallet, cid, oid_c, source_c, other_endpoint)
            self._assert_payload(new_owner_wallet, cid, oid_b, source_b, endpoint)
