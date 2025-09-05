import logging
import os

import allure
import neofs_env.neofs_epoch as neofs_epoch
import pytest
from helpers.complex_object_actions import get_nodes_with_object
from helpers.container import create_container
from helpers.file_helper import generate_file
from helpers.neofs_verbs import put_object
from helpers.node_management import restart_storage_nodes
from helpers.utility import parse_version, verify_container_estimations
from helpers.wallet_helpers import create_wallet_with_money, get_neofs_balance
from neofs_testlib.env.env import NeoFSEnv, NodeWallet

logger = logging.getLogger("NeoLogger")


@pytest.fixture
def wallet_with_money(neofs_env_with_mainchain: NeoFSEnv) -> NodeWallet:
    return create_wallet_with_money(neofs_env_with_mainchain)


class TestContainerPayments:
    @pytest.fixture
    def _cleanup_files(self):
        yield
        for f in self.files:
            os.remove(f)

    @pytest.mark.parametrize(
        "replicas_number, objects_count_multiplier",
        [
            (1, 3),
            (2, 2),
            (3, 1),
        ],
    )
    def test_container_payments(
        self,
        neofs_env_with_mainchain: NeoFSEnv,
        wallet_with_money: NodeWallet,
        replicas_number: int,
        objects_count_multiplier: int,
        _cleanup_files,
    ):
        neofs_env = neofs_env_with_mainchain
        GAS = 10**12
        GB = 10**9
        MAX_OBJECT_SIZE = 10**8
        EPOCH_DURATION = 20
        CONTAINER_FEE = GAS
        STORAGE_FEE = GAS

        with allure.step("Set more convenient network config values"):
            neofs_env.neofs_adm().fschain.set_config(
                rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
                alphabet_wallets=neofs_env.alphabet_wallets_dir,
                post_data=f"MaxObjectSize={MAX_OBJECT_SIZE} ContainerFee={CONTAINER_FEE} BasicIncomeRate={STORAGE_FEE} EpochDuration={EPOCH_DURATION}",
            )

            # Temporary workaround for a problem with propagading MaxObjectSize between storage nodes
            restart_storage_nodes(neofs_env.storage_nodes)

            neofs_epoch.tick_epoch_and_wait(neofs_env=neofs_env)

            objects_count = int(GB / MAX_OBJECT_SIZE) * objects_count_multiplier

        with allure.step("Create container and validate that a user was charged with the required amount of GAS"):
            user_wallet_balance_before_container_creation = get_neofs_balance(
                neofs_env, neofs_env.neofs_cli(wallet_with_money.cli_config), wallet_with_money
            )
            cid = create_container(
                wallet_with_money.path,
                rule=f"REP {replicas_number} IN X CBF 1 SELECT {replicas_number} FROM * AS X",
                shell=neofs_env.shell,
                endpoint=neofs_env.sn_rpc,
            )

            user_wallet_balance_after_container_creation = get_neofs_balance(
                neofs_env, neofs_env.neofs_cli(wallet_with_money.cli_config), wallet_with_money
            )

            assert user_wallet_balance_after_container_creation == user_wallet_balance_before_container_creation - (
                CONTAINER_FEE / GAS
            ), "Incorrect user wallet balance after a container creation"

        with allure.step(f"Create {objects_count} objects"):
            storage_nodes_info = {}

            for sn in neofs_env.storage_nodes:
                storage_nodes_info[sn] = {
                    "balance": get_neofs_balance(neofs_env, neofs_env.neofs_cli(sn.cli_config), sn.wallet),
                    "objects_count": 0,
                }

            created_objects = []
            self.files = [generate_file(MAX_OBJECT_SIZE) for _ in range(objects_count)]

            for f in self.files:
                created_objects.append(
                    put_object(
                        wallet_with_money.path,
                        f,
                        cid,
                        neofs_env.shell,
                        neofs_env.sn_rpc,
                    )
                )

            for oid in created_objects:
                for sn in get_nodes_with_object(
                    cid,
                    oid,
                    shell=neofs_env.shell,
                    nodes=neofs_env.storage_nodes,
                    neofs_env=neofs_env,
                ):
                    storage_nodes_info[sn]["objects_count"] += 1

        with allure.step("Wait for a couple of epochs to arrive"):
            new_epoch = neofs_epoch.wait_until_new_epoch(neofs_env, neofs_epoch.get_epoch(neofs_env))
            new_epoch = neofs_epoch.wait_until_new_epoch(neofs_env, new_epoch)

            if parse_version(neofs_env.get_binary_version(neofs_env.neofs_node_path)) > parse_version("0.48.3"):
                verify_container_estimations(
                    neofs_env.neofs_adm()
                    .fschain.estimations(rpc_endpoint=f"http://{neofs_env.fschain_rpc}", cid=cid)
                    .stdout.strip(),
                    cid,
                    container_size=objects_count * MAX_OBJECT_SIZE,
                    number_of_objects=objects_count,
                )

        with allure.step("Ensure the user wallet balance is charged only once per epoch"):
            deltas = []
            last_balance = get_neofs_balance(
                neofs_env, neofs_env.neofs_cli(wallet_with_money.cli_config), wallet_with_money
            )
            while neofs_epoch.get_epoch(neofs_env, neofs_env.storage_nodes[0]) == new_epoch:
                current_balance = get_neofs_balance(
                    neofs_env, neofs_env.neofs_cli(wallet_with_money.cli_config), wallet_with_money
                )
                if current_balance < last_balance:
                    deltas.append(last_balance - current_balance)
                    last_balance = current_balance
            assert len(deltas) == 1, "invalid number of withdrawals from the user wallet per epoch"
            single_node_gain_per_epoch = int((objects_count * MAX_OBJECT_SIZE) / GB)
            assert abs(deltas[0] - (single_node_gain_per_epoch * replicas_number)) <= 1, "Invalid user wallet balance"

        with allure.step("Wait for a couple of epochs to arrive"):
            new_epoch = neofs_epoch.wait_until_new_epoch(neofs_env, neofs_epoch.get_epoch(neofs_env))
            new_epoch = neofs_epoch.wait_until_new_epoch(neofs_env, new_epoch)

        with allure.step("Ensure the storage node balance is debited only once per epoch"):
            sn, _ = next(((sn, sn_info) for sn, sn_info in storage_nodes_info.items() if sn_info["objects_count"] > 0))
            deltas = []
            last_balance = get_neofs_balance(neofs_env, neofs_env.neofs_cli(sn.cli_config), sn.wallet)
            while neofs_epoch.get_epoch(neofs_env, sn) == new_epoch:
                current_balance = get_neofs_balance(neofs_env, neofs_env.neofs_cli(sn.cli_config), sn.wallet)
                if current_balance > last_balance:
                    deltas.append(current_balance - last_balance)
                    last_balance = current_balance
            assert len(deltas) == 1, "invalid number of debits to the storage node wallet per epoch"
            single_node_gain_per_epoch = int((objects_count * MAX_OBJECT_SIZE) / GB)
            assert abs(deltas[0] - single_node_gain_per_epoch) <= 1, "Invalid storage node wallet balance"
