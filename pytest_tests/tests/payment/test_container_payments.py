import logging
import os
import time

import allure
import neofs_env.neofs_epoch as neofs_epoch
import pytest
from helpers.complex_object_actions import get_nodes_with_object
from helpers.container import create_container
from helpers.file_helper import generate_file
from helpers.neofs_verbs import put_object
from helpers.wallet_helpers import get_neofs_balance, get_wallet_balance
from neofs_testlib.env.env import NeoFSEnv, NodeWallet
from neofs_testlib.utils import wallet as wallet_utils

logger = logging.getLogger("NeoLogger")


@pytest.fixture
def wallet_with_money(neofs_env_with_mainchain: NeoFSEnv) -> NodeWallet:
    neofs_env = neofs_env_with_mainchain

    with allure.step("Create wallet for deposit"):
        wallet = NodeWallet(
            path=neofs_env_with_mainchain._generate_temp_file(prefix="deposit_withdrawal_test_wallet"),
            address="",
            password=neofs_env.default_password,
        )
        wallet_utils.init_wallet(wallet.path, wallet.password)
        wallet.address = wallet_utils.get_last_address_from_wallet(wallet.path, wallet.password)
        wallet.neo_go_config = neofs_env.generate_neo_go_config(wallet)
        wallet.cli_config = neofs_env.generate_cli_config(wallet)

    with allure.step("Transfer some money to created wallet"):
        neo_go = neofs_env.neo_go()
        neo_go.nep17.transfer(
            "GAS",
            wallet.address,
            f"http://{neofs_env.main_chain.rpc_address}",
            from_address=neofs_env.inner_ring_nodes[-1].alphabet_wallet.address,
            amount=1000,
            force=True,
            wallet_config=neofs_env.main_chain.neo_go_config,
        )
        time.sleep(10)
        assert (
            get_wallet_balance(neofs_env, neo_go, wallet, wallet.neo_go_config) == 1000.0
        ), "Money transfer from alphabet to test wallet didn't succeed"

    with allure.step("Deposit money to neofs contract"):
        neo_go.nep17.transfer(
            "GAS",
            neofs_env.main_chain.neofs_contract_address,
            f"http://{neofs_env.main_chain.rpc_address}",
            from_address=wallet.address,
            amount=100,
            force=True,
            wallet_config=wallet.neo_go_config,
        )
        time.sleep(10)
        assert (
            get_wallet_balance(neofs_env, neo_go, wallet, wallet.neo_go_config) <= 900
        ), "Wallet balance is not correct after deposit"
        neofs_cli = neofs_env.neofs_cli(wallet.cli_config)
        assert (
            get_neofs_balance(neofs_env, neofs_cli, wallet) == 100
        ), "Wallet balance in neofs is not correct after deposit"

    return wallet


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
            neofs_env.neofs_adm().morph.set_config(
                rpc_endpoint=f"http://{neofs_env.morph_rpc}",
                alphabet_wallets=neofs_env.alphabet_wallets_dir,
                post_data=f"MaxObjectSize={MAX_OBJECT_SIZE} ContainerFee={CONTAINER_FEE} BasicIncomeRate={STORAGE_FEE} EpochDuration={EPOCH_DURATION}",
            )

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

        with allure.step("Wait until a new epoch arrives"):
            new_epoch = neofs_epoch.wait_until_new_epoch(neofs_env, neofs_epoch.get_epoch(neofs_env))

        with allure.step("Get balances in the beginning of a new epoch"):
            user_wallet_balance_before_new_epoch = get_neofs_balance(
                neofs_env, neofs_env.neofs_cli(wallet_with_money.cli_config), wallet_with_money
            )

            for sn, sn_info in storage_nodes_info.items():
                sn_info["node_balance"] = get_neofs_balance(neofs_env, neofs_env.neofs_cli(sn.cli_config), sn.wallet)

        with allure.step("Wait until another epoch arrives"):
            neofs_epoch.wait_until_new_epoch(neofs_env, new_epoch)

        with allure.step("Validate balances of storage nodes and a user wallet"):
            single_node_gain_per_epoch = int((objects_count * MAX_OBJECT_SIZE) / GB)

            for sn, sn_info in storage_nodes_info.items():
                new_balance = get_neofs_balance(neofs_env, neofs_env.neofs_cli(sn.cli_config), sn.wallet)
                if sn_info["objects_count"] > 0:
                    assert (
                        abs(new_balance - sn_info["node_balance"] - single_node_gain_per_epoch) <= 1
                    ), "Invalid storage node balance"
                else:
                    assert new_balance == sn_info["node_balance"], "Invalid storage node balance"

            user_wallet_balance_after_new_epoch = get_neofs_balance(
                neofs_env, neofs_env.neofs_cli(wallet_with_money.cli_config), wallet_with_money
            )
            assert (
                abs(
                    user_wallet_balance_before_new_epoch
                    - user_wallet_balance_after_new_epoch
                    - (single_node_gain_per_epoch * replicas_number)
                )
                <= 1
            ), "Invalid user wallet balance"
