import json
import os
import uuid

import allure
import base58
import pytest
from helpers.common import (
    get_assets_dir_path,
)
from helpers.complex_object_actions import wait_object_replication
from helpers.container import (
    create_container,
    delete_container,
    get_container,
    list_containers,
    wait_for_container_creation,
    wait_for_container_deletion,
)
from helpers.file_helper import generate_file
from helpers.grpc_responses import CONTAINER_DELETION_TIMED_OUT, NOT_CONTAINER_OWNER
from helpers.neofs_verbs import get_object, put_object_to_random_node
from helpers.node_management import wait_all_storage_nodes_returned
from helpers.utility import parse_version, placement_policy_from_container
from helpers.wellknown_acl import PRIVATE_ACL_F, PUBLIC_ACL
from neo3.wallet import account as neo3_account
from neo3.wallet import wallet as neo3_wallet
from neofs_env.neofs_env_test_base import TestNeofsBase
from neofs_testlib.env.env import NeoFSEnv, NodeWallet, StorageNode


def object_should_be_gc_marked(neofs_env: NeoFSEnv, node: StorageNode, cid: str, oid: str):
    response = neofs_env.neofs_cli(node.cli_config).control.object_status(
        address=node.wallet.address,
        endpoint=node.control_endpoint,
        object=f"{cid}/{oid}",
        wallet=node.wallet.path,
    )
    assert "GC MARKED" in response.stdout, "Unexected output from control object status command"


class TestContainer(TestNeofsBase):
    def _create_multi_account_wallet(self, accounts: list[neo3_account.Account], prefix: str) -> str:
        wallet_path = os.path.join(get_assets_dir_path(), f"{prefix}-{str(uuid.uuid4())}.json")
        wallet = neo3_wallet.Wallet()
        for account in accounts:
            wallet.account_add(account)
        with open(wallet_path, "w") as out:
            json.dump(wallet.to_json(self.neofs_env.default_password), out)
        return wallet_path

    def _refill_gas(self, wallet_path: str, address: str, amount: str = "200.0"):
        self.neofs_env.neofs_adm().fschain.refill_gas(
            rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
            alphabet_wallets=self.neofs_env.alphabet_wallets_dir,
            storage_wallet=wallet_path,
            gas=amount,
            wallet_address=address,
        )

    def _validate_nep11_attributes(
        self,
        wallet_path: str,
        expected_owner_address: str,
        expected_cid: str,
    ):
        with allure.step("Validate NEP11 attributes"):
            cid_in_hex = base58.b58decode(expected_cid).hex()
            balance = self.neofs_env.neo_go().nep11.balance(
                wallet=wallet_path,
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
            )
            assert balance["account_address"] == expected_owner_address, "Unexpected account address in balance result"
            neofs_adm = self.neofs_env.neofs_adm()
            contracts_hashes = neofs_adm.fschain.parse_dump_hashes(
                neofs_adm.fschain.dump_hashes(
                    rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                ).stdout
            )
            assert balance["contract_hash"] == contracts_hashes["container"], "Unexpected container id"

            # Find the token ID that matches the expected container
            token_ids = balance["token_ids"]
            assert len(token_ids) > 0, f"No tokens found for {expected_owner_address}"

            matching_token_id = None
            for token_id in token_ids:
                props = self.neofs_env.neo_go().nep11.properties(
                    token=balance["contract_hash"],
                    id=token_id,
                    rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                )
                if props["name"] == expected_cid and token_id == cid_in_hex:
                    matching_token_id = token_id
                    break

            assert matching_token_id is not None, f"No token found with container ID {expected_cid} among {token_ids}"

            owner_address = self.neofs_env.neo_go().nep11.owner_of(
                token=balance["contract_hash"],
                id=matching_token_id,
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
            )
            assert owner_address == expected_owner_address, (
                f"Invalid owner of {balance['container_id']}/{matching_token_id}"
            )

            tokens_of_owner = self.neofs_env.neo_go().nep11.tokens_of(
                token=balance["contract_hash"],
                address=expected_owner_address,
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
            )
            assert matching_token_id == tokens_of_owner or matching_token_id in tokens_of_owner.split("\n"), (
                f"Token {matching_token_id} not found in tokens_of result for {expected_owner_address}"
            )

            all_tokens_str = self.neofs_env.neo_go().nep11.tokens(
                token=balance["contract_hash"],
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
            )
            all_tokens = all_tokens_str.split("\n")
            assert matching_token_id in all_tokens, f"Token {matching_token_id} not in {all_tokens}"

    def _perform_ownership_transfer(
        self,
        from_wallet_path: str,
        from_address: str,
        to_wallet_path: str,
        to_address: str,
        multi_wallet_path: str,
        token_id: str = None,
    ) -> dict:
        balance = self.neofs_env.neo_go().nep11.balance(
            wallet=from_wallet_path,
            rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
        )

        if not token_id:
            token_id = balance["token_ids"][0]

        transaction_file = self.neofs_env._generate_temp_file(self.neofs_env._env_dir, prefix="transfer_transaction")

        multi_wallet_config = self.neofs_env.generate_neo_go_config(
            NodeWallet(
                path=multi_wallet_path,
                address=from_address,
                password=self.neofs_env.default_password,
            )
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
            NodeWallet(
                path=to_wallet_path,
                address=to_address,
                password=self.neofs_env.default_password,
            )
        )

        self.neofs_env.neo_go().wallet.sign(
            input_file=transaction_file,
            address=to_address,
            rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
            wallet_config=new_owner_wallet_config,
            await_=True,
        )

        return self.neofs_env.neo_go().nep11.balance(
            wallet=to_wallet_path,
            rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
        )

    @pytest.mark.parametrize("name", ["", "test-container"], ids=["No name", "Set particular name"])
    @pytest.mark.sanity
    def test_container_creation(self, default_wallet, name):
        scenario_title = f"with name {name}" if name else "without name"
        allure.dynamic.title(f"User can create container {scenario_title}")

        wallet = default_wallet
        with open(wallet.path) as file:
            json_wallet = json.load(file)

        placement_rule = "REP 2 IN X CBF 1 SELECT 2 FROM * AS X"
        cid = create_container(
            wallet.path,
            rule=placement_rule,
            name=name,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )

        containers = list_containers(wallet.path, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
        assert cid in containers, f"Expected container {cid} in containers: {containers}"

        container_info: str = get_container(
            wallet.path,
            cid,
            json_mode=False,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )
        container_info = container_info.casefold()  # To ignore case when comparing with expected values

        info_to_check = {
            f"basic ACL: {PRIVATE_ACL_F} (private)",
            f"owner ID: {json_wallet.get('accounts')[0].get('address')}",
            f"container ID: {cid}",
        }
        if name:
            info_to_check.add(f"Name={name}")

        with allure.step("Check container has correct information"):
            expected_policy = placement_rule.casefold()
            actual_policy = placement_policy_from_container(container_info)
            assert actual_policy == expected_policy, (
                f"Expected policy\n{expected_policy} but got policy\n{actual_policy}"
            )

            for info in info_to_check:
                expected_info = info.casefold()
                assert expected_info in container_info, f"Expected {expected_info} in container info:\n{container_info}"

        with allure.step("Delete container and check it was deleted"):
            delete_container(wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
            self.tick_epochs_and_wait(1)
            wait_for_container_deletion(wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

    @allure.title("Not owner and not trusted party can NOT delete container")
    def test_only_owner_can_delete_container(self, not_owner_wallet: NodeWallet, default_wallet: str):
        cid = create_container(
            wallet=default_wallet.path,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )

        with allure.step("Try to delete container"):
            with pytest.raises(RuntimeError, match=NOT_CONTAINER_OWNER):
                delete_container(
                    wallet=not_owner_wallet.path,
                    cid=cid,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    await_mode=True,
                )

        with allure.step("Try to force delete container"):
            with pytest.raises(RuntimeError, match=CONTAINER_DELETION_TIMED_OUT):
                delete_container(
                    wallet=not_owner_wallet.path,
                    cid=cid,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    await_mode=True,
                    force=True,
                )

    @allure.title("Parallel container creation and deletion")
    def test_container_creation_deletion_parallel(self, default_wallet):
        containers_count = 3
        wallet = default_wallet
        placement_rule = "REP 2 IN X CBF 1 SELECT 2 FROM * AS X"

        cids: list[str] = []
        with allure.step(f"Create {containers_count} containers"):
            for _ in range(containers_count):
                cids.append(
                    create_container(
                        wallet.path,
                        rule=placement_rule,
                        await_mode=False,
                        shell=self.shell,
                        endpoint=self.neofs_env.sn_rpc,
                        wait_for_creation=False,
                    )
                )

        with allure.step("Wait for containers occur in container list"):
            for cid in cids:
                wait_for_container_creation(
                    wallet.path,
                    cid,
                    sleep_interval=containers_count,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                )

        with allure.step("Delete containers and check they were deleted"):
            for cid in cids:
                delete_container(wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
            self.tick_epochs_and_wait(1)
            wait_for_container_deletion(wallet.path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

    @allure.title("Container deletion while some storage nodes down")
    @pytest.mark.simple
    def test_container_deletion_while_sn_down(self, default_wallet):
        with allure.step("Create container"):
            wallet = default_wallet
            placement_rule = "REP 2 IN X CBF 2 SELECT 2 FROM * AS X"
            source_file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            cid = create_container(
                wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                rule=placement_rule,
                basic_acl=PUBLIC_ACL,
            )
        with allure.step("Put object"):
            oid = put_object_to_random_node(
                wallet.path, source_file_path, cid, shell=self.shell, neofs_env=self.neofs_env
            )
            nodes_with_object = wait_object_replication(
                cid, oid, 2, shell=self.shell, nodes=self.neofs_env.storage_nodes, neofs_env=self.neofs_env
            )

        stopped_nodes = []
        try:
            with allure.step("Down storage node that stores the object"):
                node_to_stop = nodes_with_object[0]
                alive_node_with_object = nodes_with_object[1]

                node_to_stop.stop()
                stopped_nodes.append(node_to_stop)

            with allure.step("Delete container"):
                delete_container(
                    wallet.path,
                    cid,
                    shell=self.shell,
                    endpoint=alive_node_with_object.endpoint,
                    await_mode=True,
                    force=True,
                )

            with allure.step("Object should become immediately unavailable"):
                with pytest.raises(Exception):
                    get_object(
                        default_wallet.path,
                        cid,
                        oid,
                        self.neofs_env.shell,
                        alive_node_with_object.endpoint,
                    )

            with allure.step("Start storage node"):
                node_to_stop.start(fresh=False)
                wait_all_storage_nodes_returned(self.neofs_env)
                stopped_nodes.remove(node_to_stop)

            with allure.step("Object should be unavailable from the restarted node"):
                with pytest.raises(Exception):
                    get_object(
                        default_wallet.path,
                        cid,
                        oid,
                        self.neofs_env.shell,
                        node_to_stop.endpoint,
                    )
        finally:
            for node in list(stopped_nodes):
                with allure.step(f"Start {node}"):
                    node.start(fresh=False)
                stopped_nodes.remove(node)

            wait_all_storage_nodes_returned(self.neofs_env)

    def test_container_global_name(self, default_wallet):
        with allure.step("Create container"):
            wallet = default_wallet
            placement_rule = "REP 2 IN X CBF 2 SELECT 2 FROM * AS X"
            create_container(
                wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                rule=placement_rule,
                name="foo",
                basic_acl=PUBLIC_ACL,
                global_name=True,
            )

        with allure.step("Get NNS names"):
            raw_dumped_names = (
                self.neofs_env.neofs_adm().fschain.dump_names(f"http://{self.neofs_env.fschain_rpc}").stdout
            )
            assert "foo.container" in raw_dumped_names, "Updated name not found"

        with allure.step("Try to create container with same name"):
            wallet = default_wallet
            placement_rule = "REP 2 IN X CBF 2 SELECT 2 FROM * AS X"
            with pytest.raises(RuntimeError, match=".*name is already taken.*"):
                create_container(
                    wallet.path,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    rule=placement_rule,
                    name="foo",
                    basic_acl=PUBLIC_ACL,
                    global_name=True,
                )

    @allure.title("Without owner and wallet flags all containers should be displayed")
    def test_show_all_containers_from_network(self, not_owner_wallet: NodeWallet, default_wallet: str):
        cids = []
        cids.append(
            create_container(
                wallet=default_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
        )
        cids.append(
            create_container(
                wallet=not_owner_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
        )

        containers = list_containers(None, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
        assert len(containers) >= len(cids), f"Less than {len(cids)} containers in the network"
        assert all(cid in containers for cid in cids), f"Expected containers {cids} in containers: {containers}"

    def test_container_ownership_transfer(self):
        if parse_version(self.neofs_env.get_binary_version(self.neofs_env.neofs_node_path)) <= parse_version("0.50.2"):
            pytest.skip("Transfer ownership tests require fresh neofs-contracts")
        with allure.step("Prepare wallets"):
            current_owner_account = neo3_account.Account.create_new(self.neofs_env.default_password)
            new_owner_account = neo3_account.Account.create_new(self.neofs_env.default_password)

            current_owner_wallet_path = self._create_multi_account_wallet(
                [current_owner_account], "current-owner-wallet"
            )
            new_owner_wallet_path = self._create_multi_account_wallet([new_owner_account], "new-owner-wallet")
            multi_acc_wallet_path = self._create_multi_account_wallet(
                [current_owner_account, new_owner_account], "multi-acc-wallet"
            )

        with allure.step("Create container and put an object"):
            cid = create_container(
                wallet=current_owner_wallet_path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            source_file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oid = put_object_to_random_node(
                current_owner_wallet_path, source_file_path, cid, shell=self.shell, neofs_env=self.neofs_env
            )

        with allure.step("Add some gas to make transfer command"):
            self._refill_gas(current_owner_wallet_path, current_owner_account.address)
            self._refill_gas(new_owner_wallet_path, new_owner_account.address)

        self._validate_nep11_attributes(current_owner_wallet_path, current_owner_account.address, cid)

        with allure.step("Perform ownership transfer"):
            balance = self._perform_ownership_transfer(
                from_wallet_path=current_owner_wallet_path,
                from_address=current_owner_account.address,
                to_wallet_path=new_owner_wallet_path,
                to_address=new_owner_account.address,
                multi_wallet_path=multi_acc_wallet_path,
            )

            assert balance["account_address"] == new_owner_account.address, (
                "Unexpected account address in balance result"
            )

        self._validate_nep11_attributes(new_owner_wallet_path, new_owner_account.address, cid)

        with allure.step("Verify old owner can't delete a container"):
            with pytest.raises(Exception):
                delete_container(
                    current_owner_wallet_path,
                    cid,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    await_mode=True,
                    force=True,
                )

        with allure.step("Verify old owner can't get an object"):
            with pytest.raises(Exception, match="operation denied"):
                get_object(
                    current_owner_wallet_path,
                    cid,
                    oid,
                    self.neofs_env.shell,
                    self.neofs_env.sn_rpc,
                )

        source_file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        with allure.step("Verify old owner can't put an object into a container"):
            with pytest.raises(Exception, match="operation denied"):
                put_object_to_random_node(
                    current_owner_wallet_path, source_file_path, cid, shell=self.shell, neofs_env=self.neofs_env
                )

        with allure.step("Verify new owner can get an object"):
            get_object(
                new_owner_wallet_path,
                cid,
                oid,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

        with allure.step("Verify new owner can put an object into a container"):
            put_object_to_random_node(
                new_owner_wallet_path, source_file_path, cid, shell=self.shell, neofs_env=self.neofs_env
            )

        with allure.step("Verify new owner can delete a container"):
            delete_container(
                new_owner_wallet_path,
                cid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                await_mode=True,
                force=True,
            )

    def test_container_ownership_transfer_chain(self):
        if parse_version(self.neofs_env.get_binary_version(self.neofs_env.neofs_node_path)) <= parse_version("0.50.2"):
            pytest.skip("Transfer ownership tests require fresh neofs-contracts")
        """Test multiple consecutive ownership transfers (A -> B -> C)"""
        with allure.step("Prepare wallets for three owners"):
            owner_a_account = neo3_account.Account.create_new(self.neofs_env.default_password)
            owner_b_account = neo3_account.Account.create_new(self.neofs_env.default_password)
            owner_c_account = neo3_account.Account.create_new(self.neofs_env.default_password)

            owner_a_wallet_path = self._create_multi_account_wallet([owner_a_account], "owner-a-wallet")
            owner_b_wallet_path = self._create_multi_account_wallet([owner_b_account], "owner-b-wallet")
            owner_c_wallet_path = self._create_multi_account_wallet([owner_c_account], "owner-c-wallet")

            multi_acc_wallet_ab_path = self._create_multi_account_wallet(
                [owner_a_account, owner_b_account], "multi-acc-wallet-ab"
            )
            multi_acc_wallet_bc_path = self._create_multi_account_wallet(
                [owner_b_account, owner_c_account], "multi-acc-wallet-bc"
            )

        with allure.step("Create container and put an object as owner A"):
            cid = create_container(
                wallet=owner_a_wallet_path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            source_file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oid = put_object_to_random_node(
                owner_a_wallet_path, source_file_path, cid, shell=self.shell, neofs_env=self.neofs_env
            )

        with allure.step("Refill gas for all owners"):
            self._refill_gas(owner_a_wallet_path, owner_a_account.address)
            self._refill_gas(owner_b_wallet_path, owner_b_account.address)
            self._refill_gas(owner_c_wallet_path, owner_c_account.address)

        self._validate_nep11_attributes(owner_a_wallet_path, owner_a_account.address, cid)

        with allure.step("Transfer ownership from A to B"):
            balance_b = self._perform_ownership_transfer(
                from_wallet_path=owner_a_wallet_path,
                from_address=owner_a_account.address,
                to_wallet_path=owner_b_wallet_path,
                to_address=owner_b_account.address,
                multi_wallet_path=multi_acc_wallet_ab_path,
            )
            assert balance_b["account_address"] == owner_b_account.address

        self._validate_nep11_attributes(owner_b_wallet_path, owner_b_account.address, cid)

        with allure.step("Verify owner B can access the object"):
            get_object(
                owner_b_wallet_path,
                cid,
                oid,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

        with allure.step("Transfer ownership from B to C"):
            balance_c = self._perform_ownership_transfer(
                from_wallet_path=owner_b_wallet_path,
                from_address=owner_b_account.address,
                to_wallet_path=owner_c_wallet_path,
                to_address=owner_c_account.address,
                multi_wallet_path=multi_acc_wallet_bc_path,
            )
            assert balance_c["account_address"] == owner_c_account.address

        self._validate_nep11_attributes(owner_c_wallet_path, owner_c_account.address, cid)

        with allure.step("Verify owner A can't access the object"):
            with pytest.raises(Exception, match="operation denied"):
                get_object(
                    owner_a_wallet_path,
                    cid,
                    oid,
                    self.neofs_env.shell,
                    self.neofs_env.sn_rpc,
                )

        with allure.step("Verify owner B can't access the object"):
            with pytest.raises(Exception, match="operation denied"):
                get_object(
                    owner_b_wallet_path,
                    cid,
                    oid,
                    self.neofs_env.shell,
                    self.neofs_env.sn_rpc,
                )

        with allure.step("Verify owner C can access and put objects"):
            get_object(
                owner_c_wallet_path,
                cid,
                oid,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )
            new_file = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            put_object_to_random_node(owner_c_wallet_path, new_file, cid, shell=self.shell, neofs_env=self.neofs_env)

        with allure.step("Verify owner C can delete the container"):
            delete_container(
                owner_c_wallet_path,
                cid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                await_mode=True,
                force=True,
            )

    def test_container_ownership_transfer_with_multiple_objects(self):
        if parse_version(self.neofs_env.get_binary_version(self.neofs_env.neofs_node_path)) <= parse_version("0.50.2"):
            pytest.skip("Transfer ownership tests require fresh neofs-contracts")
        """Test ownership transfer with container containing multiple objects"""
        with allure.step("Prepare wallets"):
            current_owner_account = neo3_account.Account.create_new(self.neofs_env.default_password)
            new_owner_account = neo3_account.Account.create_new(self.neofs_env.default_password)

            current_owner_wallet_path = self._create_multi_account_wallet(
                [current_owner_account], "current-owner-wallet"
            )
            new_owner_wallet_path = self._create_multi_account_wallet([new_owner_account], "new-owner-wallet")
            multi_acc_wallet_path = self._create_multi_account_wallet(
                [current_owner_account, new_owner_account], "multi-acc-wallet"
            )

        with allure.step("Create container and put multiple objects"):
            cid = create_container(
                wallet=current_owner_wallet_path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

            object_ids = []
            for _ in range(5):
                source_file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
                oid = put_object_to_random_node(
                    current_owner_wallet_path, source_file_path, cid, shell=self.shell, neofs_env=self.neofs_env
                )
                object_ids.append(oid)

        with allure.step("Refill gas for wallets"):
            self._refill_gas(current_owner_wallet_path, current_owner_account.address)
            self._refill_gas(new_owner_wallet_path, new_owner_account.address)

        self._validate_nep11_attributes(current_owner_wallet_path, current_owner_account.address, cid)

        with allure.step("Verify current owner can access all objects before transfer"):
            for oid in object_ids:
                get_object(
                    current_owner_wallet_path,
                    cid,
                    oid,
                    self.neofs_env.shell,
                    self.neofs_env.sn_rpc,
                )

        with allure.step("Perform ownership transfer"):
            self._perform_ownership_transfer(
                from_wallet_path=current_owner_wallet_path,
                from_address=current_owner_account.address,
                to_wallet_path=new_owner_wallet_path,
                to_address=new_owner_account.address,
                multi_wallet_path=multi_acc_wallet_path,
            )

        self._validate_nep11_attributes(new_owner_wallet_path, new_owner_account.address, cid)

        with allure.step("Verify old owner can't access any objects"):
            for oid in object_ids:
                with pytest.raises(Exception, match="operation denied"):
                    get_object(
                        current_owner_wallet_path,
                        cid,
                        oid,
                        self.neofs_env.shell,
                        self.neofs_env.sn_rpc,
                    )

        with allure.step("Verify new owner can access all objects"):
            for oid in object_ids:
                get_object(
                    new_owner_wallet_path,
                    cid,
                    oid,
                    self.neofs_env.shell,
                    self.neofs_env.sn_rpc,
                )

        with allure.step("Verify new owner can put new objects"):
            new_file = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            new_oid = put_object_to_random_node(
                new_owner_wallet_path, new_file, cid, shell=self.shell, neofs_env=self.neofs_env
            )
            get_object(
                new_owner_wallet_path,
                cid,
                new_oid,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

        with allure.step("Verify old owner can't put objects"):
            source_file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            with pytest.raises(Exception, match="operation denied"):
                put_object_to_random_node(
                    current_owner_wallet_path, source_file_path, cid, shell=self.shell, neofs_env=self.neofs_env
                )

        with allure.step("Cleanup - delete container"):
            delete_container(
                new_owner_wallet_path,
                cid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                await_mode=True,
                force=True,
            )

    def test_container_ownership_transfer_roundtrip(self):
        if parse_version(self.neofs_env.get_binary_version(self.neofs_env.neofs_node_path)) <= parse_version("0.50.2"):
            pytest.skip("Transfer ownership tests require fresh neofs-contracts")
        """Test ownership transfer back to original owner (A -> B -> A)"""
        with allure.step("Prepare wallets"):
            owner_a_account = neo3_account.Account.create_new(self.neofs_env.default_password)
            owner_b_account = neo3_account.Account.create_new(self.neofs_env.default_password)

            owner_a_wallet_path = self._create_multi_account_wallet([owner_a_account], "owner-a-wallet")
            owner_b_wallet_path = self._create_multi_account_wallet([owner_b_account], "owner-b-wallet")

            multi_acc_wallet_path = self._create_multi_account_wallet(
                [owner_a_account, owner_b_account], "multi-acc-wallet"
            )

        with allure.step("Create container and put an object as owner A"):
            cid = create_container(
                wallet=owner_a_wallet_path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            source_file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oid = put_object_to_random_node(
                owner_a_wallet_path, source_file_path, cid, shell=self.shell, neofs_env=self.neofs_env
            )

        with allure.step("Refill gas for both owners"):
            self._refill_gas(owner_a_wallet_path, owner_a_account.address)
            self._refill_gas(owner_b_wallet_path, owner_b_account.address)

        self._validate_nep11_attributes(owner_a_wallet_path, owner_a_account.address, cid)

        with allure.step("Transfer ownership from A to B"):
            balance_b = self._perform_ownership_transfer(
                from_wallet_path=owner_a_wallet_path,
                from_address=owner_a_account.address,
                to_wallet_path=owner_b_wallet_path,
                to_address=owner_b_account.address,
                multi_wallet_path=multi_acc_wallet_path,
            )
            assert balance_b["account_address"] == owner_b_account.address

        self._validate_nep11_attributes(owner_b_wallet_path, owner_b_account.address, cid)

        with allure.step("Verify owner B can access the object"):
            get_object(
                owner_b_wallet_path,
                cid,
                oid,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

        with allure.step("Verify owner A can't access objects"):
            with pytest.raises(Exception, match="operation denied"):
                get_object(
                    owner_a_wallet_path,
                    cid,
                    oid,
                    self.neofs_env.shell,
                    self.neofs_env.sn_rpc,
                )

        with allure.step("Transfer ownership back from B to A"):
            balance_a_final = self._perform_ownership_transfer(
                from_wallet_path=owner_b_wallet_path,
                from_address=owner_b_account.address,
                to_wallet_path=owner_a_wallet_path,
                to_address=owner_a_account.address,
                multi_wallet_path=multi_acc_wallet_path,
            )
            assert balance_a_final["account_address"] == owner_a_account.address

        self._validate_nep11_attributes(owner_a_wallet_path, owner_a_account.address, cid)

        with allure.step("Verify owner A can access the original object"):
            get_object(
                owner_a_wallet_path,
                cid,
                oid,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

        with allure.step("Verify owner A can put new objects"):
            new_file = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            put_object_to_random_node(owner_a_wallet_path, new_file, cid, shell=self.shell, neofs_env=self.neofs_env)

        with allure.step("Verify owner B can't access objects anymore"):
            with pytest.raises(Exception, match="operation denied"):
                get_object(
                    owner_b_wallet_path,
                    cid,
                    oid,
                    self.neofs_env.shell,
                    self.neofs_env.sn_rpc,
                )

        with allure.step("Verify owner A can delete the container"):
            delete_container(
                owner_a_wallet_path,
                cid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                await_mode=True,
                force=True,
            )

    def test_container_ownership_swap(self):
        if parse_version(self.neofs_env.get_binary_version(self.neofs_env.neofs_node_path)) <= parse_version("0.50.2"):
            pytest.skip("Transfer ownership tests require fresh neofs-contracts")
        """Test swapping ownership of two containers between two owners (A->containerA, B->containerB => A->containerB, B->containerA)"""
        with allure.step("Prepare wallets for two owners"):
            owner_a_account = neo3_account.Account.create_new(self.neofs_env.default_password)
            owner_b_account = neo3_account.Account.create_new(self.neofs_env.default_password)

            owner_a_wallet_path = self._create_multi_account_wallet([owner_a_account], "owner-a-wallet")
            owner_b_wallet_path = self._create_multi_account_wallet([owner_b_account], "owner-b-wallet")

            multi_acc_wallet_path = self._create_multi_account_wallet(
                [owner_a_account, owner_b_account], "multi-acc-wallet"
            )

        with allure.step("Create container A owned by owner A"):
            container_a_id = create_container(
                wallet=owner_a_wallet_path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            source_file_a = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            object_a_id = put_object_to_random_node(
                owner_a_wallet_path, source_file_a, container_a_id, shell=self.shell, neofs_env=self.neofs_env
            )

        with allure.step("Create container B owned by owner B"):
            container_b_id = create_container(
                wallet=owner_b_wallet_path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            source_file_b = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            object_b_id = put_object_to_random_node(
                owner_b_wallet_path, source_file_b, container_b_id, shell=self.shell, neofs_env=self.neofs_env
            )

        with allure.step("Refill gas for both owners"):
            self._refill_gas(owner_a_wallet_path, owner_a_account.address)
            self._refill_gas(owner_b_wallet_path, owner_b_account.address)

        with allure.step("Validate initial ownership state"):
            self._validate_nep11_attributes(owner_a_wallet_path, owner_a_account.address, container_a_id)
            owner_a_initial_balance = self.neofs_env.neo_go().nep11.balance(
                wallet=owner_a_wallet_path,
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
            )
            self._validate_nep11_attributes(owner_b_wallet_path, owner_b_account.address, container_b_id)
            owner_b_initial_balance = self.neofs_env.neo_go().nep11.balance(
                wallet=owner_b_wallet_path,
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
            )

        with allure.step("Verify owner A can access container A but not container B"):
            get_object(
                owner_a_wallet_path,
                container_a_id,
                object_a_id,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )
            with pytest.raises(Exception, match="operation denied"):
                get_object(
                    owner_a_wallet_path,
                    container_b_id,
                    object_b_id,
                    self.neofs_env.shell,
                    self.neofs_env.sn_rpc,
                )

        with allure.step("Verify owner B can access container B but not container A"):
            get_object(
                owner_b_wallet_path,
                container_b_id,
                object_b_id,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )
            with pytest.raises(Exception, match="operation denied"):
                get_object(
                    owner_b_wallet_path,
                    container_a_id,
                    object_a_id,
                    self.neofs_env.shell,
                    self.neofs_env.sn_rpc,
                )

        with allure.step("Transfer container A from owner A to owner B"):
            balance_b_container_a = self._perform_ownership_transfer(
                from_wallet_path=owner_a_wallet_path,
                from_address=owner_a_account.address,
                to_wallet_path=owner_b_wallet_path,
                to_address=owner_b_account.address,
                multi_wallet_path=multi_acc_wallet_path,
                token_id=owner_a_initial_balance["token_ids"][0],
            )
            assert balance_b_container_a["account_address"] == owner_b_account.address

        self._validate_nep11_attributes(owner_b_wallet_path, owner_b_account.address, container_a_id)

        with allure.step("Transfer container B from owner B to owner A"):
            balance_a_container_b = self._perform_ownership_transfer(
                from_wallet_path=owner_b_wallet_path,
                from_address=owner_b_account.address,
                to_wallet_path=owner_a_wallet_path,
                to_address=owner_a_account.address,
                multi_wallet_path=multi_acc_wallet_path,
                token_id=owner_b_initial_balance["token_ids"][0],
            )
            assert balance_a_container_b["account_address"] == owner_a_account.address

        self._validate_nep11_attributes(owner_a_wallet_path, owner_a_account.address, container_b_id)

        with allure.step("Verify owner A can now access container B but not container A"):
            get_object(
                owner_a_wallet_path,
                container_b_id,
                object_b_id,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )
            with pytest.raises(Exception, match="operation denied"):
                get_object(
                    owner_a_wallet_path,
                    container_a_id,
                    object_a_id,
                    self.neofs_env.shell,
                    self.neofs_env.sn_rpc,
                )

        with allure.step("Verify owner B can now access container A but not container B"):
            get_object(
                owner_b_wallet_path,
                container_a_id,
                object_a_id,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )
            with pytest.raises(Exception, match="operation denied"):
                get_object(
                    owner_b_wallet_path,
                    container_b_id,
                    object_b_id,
                    self.neofs_env.shell,
                    self.neofs_env.sn_rpc,
                )

        with allure.step("Verify owner A can put new objects into container B"):
            new_file_a = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            put_object_to_random_node(
                owner_a_wallet_path, new_file_a, container_b_id, shell=self.shell, neofs_env=self.neofs_env
            )

        with allure.step("Verify owner B can put new objects into container A"):
            new_file_b = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            put_object_to_random_node(
                owner_b_wallet_path, new_file_b, container_a_id, shell=self.shell, neofs_env=self.neofs_env
            )

        with allure.step("Cleanup - delete both containers"):
            delete_container(
                owner_a_wallet_path,
                container_b_id,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                await_mode=True,
                force=True,
            )
            delete_container(
                owner_b_wallet_path,
                container_a_id,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                await_mode=True,
                force=True,
            )
