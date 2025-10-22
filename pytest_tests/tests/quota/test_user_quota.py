import time

import allure
import pytest
from helpers.container import create_container
from helpers.file_helper import generate_file
from helpers.neofs_verbs import delete_object, get_object, put_object
from helpers.quota import TestQuotaBase
from helpers.utility import wait_for_gc_pass_on_storage_nodes
from helpers.wallet_helpers import create_wallet
from neofs_testlib.cli import NeofsAdm
from neofs_testlib.env.env import NodeWallet


class TestUserQuota(TestQuotaBase):
    @pytest.mark.parametrize("quota_value,quota_type", [(100, "hard"), (100, "soft")])
    @pytest.mark.sanity
    def test_user_quota(self, unique_wallet: NodeWallet, quota_value: int, quota_type: str):
        placement_rule = "EC 2/2"
        cid = create_container(
            unique_wallet.path,
            rule=placement_rule,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )
        neofs_adm: NeofsAdm = self.neofs_env.neofs_adm()
        neofs_adm.fschain.refill_gas(
            rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
            alphabet_wallets=self.neofs_env.alphabet_wallets_dir,
            storage_wallet=unique_wallet.path,
            gas="100.0",
        )
        with allure.step(f"Set {quota_type} quota on user {unique_wallet.address=}; {quota_value=}"):
            neofs_adm.fschain.user_quota(
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                account=unique_wallet.address,
                wallet=unique_wallet.path,
                wallet_password=unique_wallet.password,
                post_data=str(quota_value),
                soft=quota_type == "soft",
            )

        self.tick_epochs_and_wait(2)

        if quota_type == "hard":
            with allure.step("Try to put object bigger than quota size"):
                file_path = generate_file(quota_value + 1)
                with pytest.raises(Exception, match=r".*size quota limits are exceeded.*"):
                    put_object(unique_wallet.path, file_path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

        with allure.step("Try to put object of the less than quota size"):
            if quota_type == "soft":
                initial_line_count_before_small = self._get_log_line_count()

            file_path = generate_file(quota_value // 2)
            oid = put_object(unique_wallet.path, file_path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
            get_object(
                unique_wallet.path,
                cid,
                oid,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

            if quota_type == "soft":
                self._check_soft_quota_warning_in_logs(
                    cid, start_line=initial_line_count_before_small, expect_warning=False
                )

        self.tick_epochs_and_wait(2)

        with allure.step("Verify quota enforcement across multiple containers for the same user"):
            cid2 = create_container(
                unique_wallet.path,
                rule=placement_rule,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

            if quota_type == "soft":
                initial_line_count_multi = self._get_log_line_count()

            remaining_quota = quota_value - (quota_value // 2)

            if quota_type == "hard":
                step_text = "Put object in second container that uses remaining quota space"
                file_size = remaining_quota - 10
            else:
                step_text = "Put object in second container that makes total usage exceed soft quota"
                file_size = remaining_quota + 10

            with allure.step(step_text):
                file_path_remaining = generate_file(file_size)
                oid2 = put_object(
                    unique_wallet.path, file_path_remaining, cid2, shell=self.shell, endpoint=self.neofs_env.sn_rpc
                )
                get_object(
                    unique_wallet.path,
                    cid2,
                    oid2,
                    self.neofs_env.shell,
                    self.neofs_env.sn_rpc,
                )

                if quota_type == "soft":
                    self._check_soft_quota_warning_in_logs(
                        cid2, start_line=initial_line_count_multi, expect_warning=True
                    )

            self.tick_epochs_and_wait(2)

            if quota_type == "hard":
                with allure.step("Try to put any object in any container - should fail as global quota is exceeded"):
                    file_path_small = generate_file(20)
                    with pytest.raises(Exception, match=r".*size quota limits are exceeded.*"):
                        put_object(
                            unique_wallet.path, file_path_small, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc
                        )
                    with pytest.raises(Exception, match=r".*size quota limits are exceeded.*"):
                        put_object(
                            unique_wallet.path, file_path_small, cid2, shell=self.shell, endpoint=self.neofs_env.sn_rpc
                        )
            else:
                with allure.step("Put small object that would further exceed quota - should still generate warning"):
                    initial_line_count_further = self._get_log_line_count()

                    file_path_small = generate_file(10)
                    oid3 = put_object(
                        unique_wallet.path, file_path_small, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc
                    )
                    get_object(
                        unique_wallet.path,
                        cid,
                        oid3,
                        self.neofs_env.shell,
                        self.neofs_env.sn_rpc,
                    )
                    self._check_soft_quota_warning_in_logs(
                        cid, start_line=initial_line_count_further, expect_warning=True
                    )

    @pytest.mark.parametrize("quota_value,quota_type", [(100, "hard"), (100, "soft")])
    def test_multiple_users_quota_independence(self, unique_wallet: NodeWallet, quota_value: int, quota_type: str):
        placement_rule = "EC 2/2"

        user1_wallet = unique_wallet
        user2_wallet = create_wallet()

        cid1 = create_container(
            user1_wallet.path,
            rule=placement_rule,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )
        cid2 = create_container(
            user2_wallet.path,
            rule=placement_rule,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )

        neofs_adm: NeofsAdm = self.neofs_env.neofs_adm()

        with allure.step("Refill gas for both wallets"):
            neofs_adm.fschain.refill_gas(
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                alphabet_wallets=self.neofs_env.alphabet_wallets_dir,
                storage_wallet=user1_wallet.path,
                gas="100.0",
            )
            neofs_adm.fschain.refill_gas(
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                alphabet_wallets=self.neofs_env.alphabet_wallets_dir,
                storage_wallet=user2_wallet.path,
                gas="100.0",
            )

        with allure.step(f"Set {quota_type} quota for user1"):
            neofs_adm.fschain.user_quota(
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                account=user1_wallet.address,
                wallet=user1_wallet.path,
                wallet_password=user1_wallet.password,
                post_data=str(quota_value),
                soft=quota_type == "soft",
            )

        other_quota_type = "soft" if quota_type == "hard" else "hard"
        with allure.step(f"Set {other_quota_type} quota for user2"):
            neofs_adm.fschain.user_quota(
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                account=user2_wallet.address,
                wallet=user2_wallet.path,
                wallet_password=user2_wallet.password,
                post_data=str(quota_value),
                soft=other_quota_type == "soft",
            )

        self.tick_epochs_and_wait(2)

        with allure.step("User1: Fill quota with object smaller than limit"):
            file_path_user1_small = generate_file(quota_value // 2)
            oid1 = put_object(
                user1_wallet.path, file_path_user1_small, cid1, shell=self.shell, endpoint=self.neofs_env.sn_rpc
            )
            get_object(
                user1_wallet.path,
                cid1,
                oid1,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

        with allure.step(f"User2: Put object larger than {other_quota_type} quota"):
            if other_quota_type == "soft":
                initial_line_count = self._get_log_line_count()

            file_path_user2_large = generate_file(quota_value * 2)

            if other_quota_type == "hard":
                with pytest.raises(Exception, match=r".*size quota limits are exceeded.*"):
                    put_object(
                        user2_wallet.path, file_path_user2_large, cid2, shell=self.shell, endpoint=self.neofs_env.sn_rpc
                    )
            else:
                oid2 = put_object(
                    user2_wallet.path, file_path_user2_large, cid2, shell=self.shell, endpoint=self.neofs_env.sn_rpc
                )
                get_object(
                    user2_wallet.path,
                    cid2,
                    oid2,
                    self.neofs_env.shell,
                    self.neofs_env.sn_rpc,
                )
                self._check_soft_quota_warning_in_logs(cid2, start_line=initial_line_count, expect_warning=True)

        self.tick_epochs_and_wait(2)

        with allure.step(f"User1: Try to exceed {quota_type} quota"):
            remaining_quota = quota_value - (quota_value // 2)
            file_path_user1_exceed = generate_file(remaining_quota + 10)

            if quota_type == "hard":
                with pytest.raises(Exception, match=r".*size quota limits are exceeded.*"):
                    put_object(
                        user1_wallet.path,
                        file_path_user1_exceed,
                        cid1,
                        shell=self.shell,
                        endpoint=self.neofs_env.sn_rpc,
                    )
            else:
                initial_line_count_user1 = self._get_log_line_count()
                oid1_exceed = put_object(
                    user1_wallet.path, file_path_user1_exceed, cid1, shell=self.shell, endpoint=self.neofs_env.sn_rpc
                )
                get_object(
                    user1_wallet.path,
                    cid1,
                    oid1_exceed,
                    self.neofs_env.shell,
                    self.neofs_env.sn_rpc,
                )
                self._check_soft_quota_warning_in_logs(cid1, start_line=initial_line_count_user1, expect_warning=True)

        if other_quota_type == "soft":
            with allure.step("User2: Can still put more objects despite user1's quota status"):
                file_path_user2_more = generate_file(quota_value)
                oid3 = put_object(
                    user2_wallet.path, file_path_user2_more, cid2, shell=self.shell, endpoint=self.neofs_env.sn_rpc
                )
                get_object(
                    user2_wallet.path,
                    cid2,
                    oid3,
                    self.neofs_env.shell,
                    self.neofs_env.sn_rpc,
                )

        self.tick_epochs_and_wait(2)

        if quota_type == "hard":
            with allure.step("User1: Can still put object within remaining quota space"):
                file_path_user1_within = generate_file(remaining_quota - 10)
                oid4 = put_object(
                    user1_wallet.path, file_path_user1_within, cid1, shell=self.shell, endpoint=self.neofs_env.sn_rpc
                )
                get_object(
                    user1_wallet.path,
                    cid1,
                    oid4,
                    self.neofs_env.shell,
                    self.neofs_env.sn_rpc,
                )

    @pytest.mark.parametrize("invalid_quota", [-1, -100, "abcd"])
    def test_negative_quota_error_handling(self, unique_wallet: NodeWallet, invalid_quota: int):
        neofs_adm: NeofsAdm = self.neofs_env.neofs_adm()
        neofs_adm.fschain.refill_gas(
            rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
            alphabet_wallets=self.neofs_env.alphabet_wallets_dir,
            storage_wallet=unique_wallet.path,
            gas="100.0",
        )

        with allure.step(f"Try to set negative quota {invalid_quota} - should fail"):
            with pytest.raises(Exception):
                neofs_adm.fschain.user_quota(
                    rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                    account=unique_wallet.address,
                    wallet=unique_wallet.path,
                    wallet_password=unique_wallet.password,
                    post_data=str(invalid_quota),
                )

    @pytest.mark.parametrize(
        "initial_quota,updated_quota,quota_type",
        [(100, 200, "hard"), (200, 50, "hard"), (100, 200, "soft"), (200, 50, "soft")],
    )
    def test_quota_update(self, unique_wallet: NodeWallet, initial_quota: int, updated_quota: int, quota_type: str):
        placement_rule = "EC 2/2"
        cid = create_container(
            unique_wallet.path,
            rule=placement_rule,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )
        neofs_adm: NeofsAdm = self.neofs_env.neofs_adm()
        neofs_adm.fschain.refill_gas(
            rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
            alphabet_wallets=self.neofs_env.alphabet_wallets_dir,
            storage_wallet=unique_wallet.path,
            gas="100.0",
        )

        with allure.step(f"Set initial {quota_type} quota {initial_quota}"):
            neofs_adm.fschain.user_quota(
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                account=unique_wallet.address,
                wallet=unique_wallet.path,
                wallet_password=unique_wallet.password,
                post_data=str(initial_quota),
                soft=quota_type == "soft",
            )

        self.tick_epochs_and_wait(2)

        with allure.step("Put object within initial quota"):
            if quota_type == "soft":
                initial_line_count = self._get_log_line_count()

            file_path = generate_file(initial_quota // 2)
            oid = put_object(unique_wallet.path, file_path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
            get_object(
                unique_wallet.path,
                cid,
                oid,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

            if quota_type == "soft":
                self._check_soft_quota_warning_in_logs(cid, start_line=initial_line_count, expect_warning=False)

        with allure.step(f"Update {quota_type} quota to {updated_quota}"):
            neofs_adm.fschain.user_quota(
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                account=unique_wallet.address,
                wallet=unique_wallet.path,
                wallet_password=unique_wallet.password,
                post_data=str(updated_quota),
                soft=quota_type == "soft",
            )

        self.tick_epochs_and_wait(2)

        if updated_quota > initial_quota:
            with allure.step("Increased quota should allow larger objects"):
                if quota_type == "soft":
                    update_line_count = self._get_log_line_count()

                file_path_large = generate_file(initial_quota)
                oid_large = put_object(
                    unique_wallet.path, file_path_large, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc
                )
                get_object(
                    unique_wallet.path,
                    cid,
                    oid_large,
                    self.neofs_env.shell,
                    self.neofs_env.sn_rpc,
                )

                if quota_type == "soft":
                    self._check_soft_quota_warning_in_logs(cid, start_line=update_line_count, expect_warning=False)
        else:
            # Current usage is initial_quota // 2, new limit is updated_quota
            remaining_quota = updated_quota - (initial_quota // 2)
            if remaining_quota > 0:
                with allure.step(
                    f"Decreased {quota_type} quota should affect behavior for objects exceeding new limit"
                ):
                    file_path_exceed = generate_file(remaining_quota + 10)

                    if quota_type == "hard":
                        with pytest.raises(Exception, match=r".*size quota limits are exceeded.*"):
                            put_object(
                                unique_wallet.path,
                                file_path_exceed,
                                cid,
                                shell=self.shell,
                                endpoint=self.neofs_env.sn_rpc,
                            )
                    else:
                        decrease_line_count = self._get_log_line_count()
                        oid_exceed = put_object(
                            unique_wallet.path, file_path_exceed, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc
                        )
                        get_object(
                            unique_wallet.path,
                            cid,
                            oid_exceed,
                            self.neofs_env.shell,
                            self.neofs_env.sn_rpc,
                        )
                        self._check_soft_quota_warning_in_logs(cid, start_line=decrease_line_count, expect_warning=True)

    @pytest.mark.parametrize("quota_value,quota_type", [(200, "hard"), (200, "soft")])
    def test_quota_inheritance_new_containers(self, unique_wallet: NodeWallet, quota_value: int, quota_type: str):
        placement_rule = "EC 2/2"
        neofs_adm: NeofsAdm = self.neofs_env.neofs_adm()
        neofs_adm.fschain.refill_gas(
            rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
            alphabet_wallets=self.neofs_env.alphabet_wallets_dir,
            storage_wallet=unique_wallet.path,
            gas="100.0",
        )

        with allure.step(f"Set user {quota_type} quota {quota_value}"):
            neofs_adm.fschain.user_quota(
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                account=unique_wallet.address,
                wallet=unique_wallet.path,
                wallet_password=unique_wallet.password,
                post_data=str(quota_value),
                soft=quota_type == "soft",
            )

        self.tick_epochs_and_wait(2)

        with allure.step("Create container after setting quota"):
            cid1 = create_container(
                unique_wallet.path,
                rule=placement_rule,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step(f"Verify {quota_type} quota applies to new container"):
            if quota_type == "soft":
                initial_line_count = self._get_log_line_count()

            file_path_within = generate_file(quota_value // 2)
            oid1 = put_object(
                unique_wallet.path, file_path_within, cid1, shell=self.shell, endpoint=self.neofs_env.sn_rpc
            )
            get_object(
                unique_wallet.path,
                cid1,
                oid1,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

            if quota_type == "soft":
                self._check_soft_quota_warning_in_logs(cid1, start_line=initial_line_count, expect_warning=False)

            self.tick_epochs_and_wait(2)

            if quota_type == "soft":
                exceed_line_count = self._get_log_line_count()

            file_path_exceed = generate_file(quota_value)

            if quota_type == "hard":
                with pytest.raises(Exception, match=r".*size quota limits are exceeded.*"):
                    put_object(
                        unique_wallet.path, file_path_exceed, cid1, shell=self.shell, endpoint=self.neofs_env.sn_rpc
                    )
            else:
                oid_exceed = put_object(
                    unique_wallet.path, file_path_exceed, cid1, shell=self.shell, endpoint=self.neofs_env.sn_rpc
                )
                get_object(
                    unique_wallet.path,
                    cid1,
                    oid_exceed,
                    self.neofs_env.shell,
                    self.neofs_env.sn_rpc,
                )
                self._check_soft_quota_warning_in_logs(cid1, start_line=exceed_line_count, expect_warning=True)

        if quota_type == "hard":
            with allure.step("Create another container and verify global quota enforcement"):
                cid2 = create_container(
                    unique_wallet.path,
                    rule=placement_rule,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                )

                remaining_quota = quota_value - (quota_value // 2)
                file_path_remaining = generate_file(remaining_quota - 10)
                oid2 = put_object(
                    unique_wallet.path, file_path_remaining, cid2, shell=self.shell, endpoint=self.neofs_env.sn_rpc
                )
                get_object(
                    unique_wallet.path,
                    cid2,
                    oid2,
                    self.neofs_env.shell,
                    self.neofs_env.sn_rpc,
                )

                self.tick_epochs_and_wait(2)

                file_path_fail = generate_file(20)
                with pytest.raises(Exception, match=r".*size quota limits are exceeded.*"):
                    put_object(
                        unique_wallet.path, file_path_fail, cid1, shell=self.shell, endpoint=self.neofs_env.sn_rpc
                    )
                with pytest.raises(Exception, match=r".*size quota limits are exceeded.*"):
                    put_object(
                        unique_wallet.path, file_path_fail, cid2, shell=self.shell, endpoint=self.neofs_env.sn_rpc
                    )

    @pytest.mark.parametrize("quota_value,quota_type", [(200, "hard"), (200, "soft")])
    def test_object_delete_and_quota_reclaim(self, unique_wallet: NodeWallet, quota_value: int, quota_type: str):
        placement_rule = "EC 2/2"
        cid = create_container(
            unique_wallet.path,
            rule=placement_rule,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )
        neofs_adm: NeofsAdm = self.neofs_env.neofs_adm()
        neofs_adm.fschain.refill_gas(
            rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
            alphabet_wallets=self.neofs_env.alphabet_wallets_dir,
            storage_wallet=unique_wallet.path,
            gas="100.0",
        )

        with allure.step(f"Set user {quota_type} quota {quota_value}"):
            neofs_adm.fschain.user_quota(
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                account=unique_wallet.address,
                wallet=unique_wallet.path,
                wallet_password=unique_wallet.password,
                post_data=str(quota_value),
                soft=quota_type == "soft",
            )

        self.tick_epochs_and_wait(2)

        with allure.step("Put objects that nearly fill the quota"):
            file_path1 = generate_file(quota_value // 2)
            oid1 = put_object(unique_wallet.path, file_path1, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
            get_object(
                unique_wallet.path,
                cid,
                oid1,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

            file_path2 = generate_file(quota_value // 2 - 20)
            oid2 = put_object(unique_wallet.path, file_path2, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
            get_object(
                unique_wallet.path,
                cid,
                oid2,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

        self.tick_epochs_and_wait(2)

        with allure.step("Verify quota is full"):
            file_path_exceed = generate_file(50)

            if quota_type == "hard":
                with pytest.raises(Exception, match=r".*size quota limits are exceeded.*"):
                    put_object(
                        unique_wallet.path, file_path_exceed, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc
                    )
            else:
                exceed_line_count = self._get_log_line_count()
                oid_exceed = put_object(
                    unique_wallet.path, file_path_exceed, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc
                )
                get_object(
                    unique_wallet.path,
                    cid,
                    oid_exceed,
                    self.neofs_env.shell,
                    self.neofs_env.sn_rpc,
                )
                self._check_soft_quota_warning_in_logs(cid, start_line=exceed_line_count, expect_warning=True)

        with allure.step("Delete first object to reclaim quota"):
            delete_object(
                unique_wallet.path,
                cid,
                oid1,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

        self.tick_epochs_and_wait(2)

        with allure.step("Verify quota is reclaimed - should be able to put object again"):
            file_path_after_delete = generate_file(quota_value // 2 - 10)
            oid3 = put_object(
                unique_wallet.path, file_path_after_delete, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc
            )
            get_object(
                unique_wallet.path,
                cid,
                oid3,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

        with allure.step("Delete second object as well"):
            delete_object(
                unique_wallet.path,
                cid,
                oid2,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

        self.tick_epochs_and_wait(2)

        with allure.step("Verify significant quota space is available after deleting both objects"):
            file_path_large = generate_file(quota_value // 2 - 20)
            oid4 = put_object(
                unique_wallet.path, file_path_large, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc
            )
            get_object(
                unique_wallet.path,
                cid,
                oid4,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

    @pytest.mark.parametrize("quota_value,quota_type", [(200, "hard"), (200, "soft")])
    def test_user_object_lifetime_and_quota_reclaim(self, unique_wallet: NodeWallet, quota_value: int, quota_type: str):
        placement_rule = "EC 2/2"
        cid = create_container(
            unique_wallet.path,
            rule=placement_rule,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )
        neofs_adm: NeofsAdm = self.neofs_env.neofs_adm()
        neofs_adm.fschain.refill_gas(
            rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
            alphabet_wallets=self.neofs_env.alphabet_wallets_dir,
            storage_wallet=unique_wallet.path,
            gas="100.0",
        )

        with allure.step(f"Set user {quota_type} quota {quota_value}"):
            neofs_adm.fschain.user_quota(
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                account=unique_wallet.address,
                wallet=unique_wallet.path,
                wallet_password=unique_wallet.password,
                post_data=str(quota_value),
                soft=quota_type == "soft",
            )

        self.tick_epochs_and_wait(2)

        with allure.step("Put objects with short lifetime that nearly fill the user quota"):
            file_path1 = generate_file(quota_value // 2)
            oid1 = put_object(
                unique_wallet.path, file_path1, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc, lifetime=1
            )
            get_object(
                unique_wallet.path,
                cid,
                oid1,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

            file_path2 = generate_file(quota_value // 2 - 20)
            oid2 = put_object(
                unique_wallet.path, file_path2, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc, lifetime=1
            )
            get_object(
                unique_wallet.path,
                cid,
                oid2,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

        self.tick_epochs_and_wait(2)
        wait_for_gc_pass_on_storage_nodes()
        self.tick_epochs_and_wait(1)
        time.sleep(5)

        with allure.step("Verify quota is reclaimed after auto-deletion - should be able to put objects again"):
            file_path_after_auto_delete = generate_file(quota_value // 2 - 10)
            if quota_type == "soft":
                reclaim_line_count = self._get_log_line_count()

            oid3 = put_object(
                unique_wallet.path, file_path_after_auto_delete, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc
            )
            get_object(
                unique_wallet.path,
                cid,
                oid3,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

            if quota_type == "soft":
                self._check_soft_quota_warning_in_logs(cid, start_line=reclaim_line_count, expect_warning=False)

        with allure.step("Verify quota space is available after auto-deletion"):
            file_path_large = generate_file(quota_value // 2 - 20)
            if quota_type == "soft":
                large_file_line_count = self._get_log_line_count()

            oid4 = put_object(
                unique_wallet.path, file_path_large, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc
            )
            get_object(
                unique_wallet.path,
                cid,
                oid4,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

            if quota_type == "soft":
                self._check_soft_quota_warning_in_logs(cid, start_line=large_file_line_count, expect_warning=False)

    @pytest.mark.parametrize("quota_type,quota_value", [("hard", 100), ("soft", 100)])
    @pytest.mark.sanity
    def test_user_quota_rep2_placement(self, unique_wallet: NodeWallet, quota_type: str, quota_value: int):
        placement_rule = "REP 2"
        cid = create_container(
            unique_wallet.path,
            rule=placement_rule,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )
        neofs_adm: NeofsAdm = self.neofs_env.neofs_adm()
        neofs_adm.fschain.refill_gas(
            rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
            alphabet_wallets=self.neofs_env.alphabet_wallets_dir,
            storage_wallet=unique_wallet.path,
            gas="100.0",
        )

        with allure.step(f"Set user {quota_type} quota {quota_value}"):
            neofs_adm.fschain.user_quota(
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                account=unique_wallet.address,
                wallet=unique_wallet.path,
                wallet_password=unique_wallet.password,
                post_data=str(quota_value),
                soft=quota_type == "soft",
            )

        self.tick_epochs_and_wait(2)

        with allure.step("Put object of half quota size - should succeed with REP 2"):
            file_path = generate_file(quota_value // 2)
            oid = put_object(unique_wallet.path, file_path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
            get_object(
                unique_wallet.path,
                cid,
                oid,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

        if quota_type == "soft":
            initial_line_count = self._get_log_line_count()

        self.tick_epochs_and_wait(2)

        with allure.step("Try to put another small object"):
            file_path = generate_file(10)
            if quota_type == "soft":
                oid2 = put_object(unique_wallet.path, file_path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
                get_object(
                    unique_wallet.path,
                    cid,
                    oid2,
                    self.neofs_env.shell,
                    self.neofs_env.sn_rpc,
                )
                self._check_soft_quota_warning_in_logs(cid, start_line=initial_line_count, expect_warning=True)
            else:
                with pytest.raises(Exception, match=r".*size quota limits are exceeded.*"):
                    put_object(unique_wallet.path, file_path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

    @pytest.mark.parametrize("quota_type,initial_quota", [("hard", 100), ("soft", 100)])
    def test_user_quota_removal(self, unique_wallet: NodeWallet, quota_type: str, initial_quota: int):
        placement_rule = "EC 2/2"
        cid = create_container(
            unique_wallet.path,
            rule=placement_rule,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )
        neofs_adm: NeofsAdm = self.neofs_env.neofs_adm()
        neofs_adm.fschain.refill_gas(
            rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
            alphabet_wallets=self.neofs_env.alphabet_wallets_dir,
            storage_wallet=unique_wallet.path,
            gas="100.0",
        )

        with allure.step(f"Set initial user {quota_type} quota to {initial_quota}"):
            neofs_adm.fschain.user_quota(
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                account=unique_wallet.address,
                wallet=unique_wallet.path,
                wallet_password=unique_wallet.password,
                post_data=str(initial_quota),
                soft=quota_type == "soft",
            )

        self.tick_epochs_and_wait(2)

        with allure.step("Verify quota enforcement with initial quota"):
            file_path = generate_file(initial_quota * 2)
            if quota_type == "soft":
                initial_line_count = self._get_log_line_count()
                oid = put_object(unique_wallet.path, file_path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
                get_object(
                    unique_wallet.path,
                    cid,
                    oid,
                    self.neofs_env.shell,
                    self.neofs_env.sn_rpc,
                )
                self._check_soft_quota_warning_in_logs(cid, start_line=initial_line_count, expect_warning=True)
            else:
                with pytest.raises(Exception, match=r".*size quota limits are exceeded.*"):
                    put_object(unique_wallet.path, file_path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

        with allure.step(f"Reset user {quota_type} quota to 0"):
            neofs_adm.fschain.user_quota(
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                account=unique_wallet.address,
                wallet=unique_wallet.path,
                wallet_password=unique_wallet.password,
                post_data="0",
                soft=quota_type == "soft",
            )

        self.tick_epochs_and_wait(2)

        with allure.step("Verify quota removal - should be able to put large objects now"):
            file_path = generate_file(initial_quota * 2)
            if quota_type == "soft":
                line_count_before_large = self._get_log_line_count()

            oid2 = put_object(unique_wallet.path, file_path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
            get_object(
                unique_wallet.path,
                cid,
                oid2,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

            if quota_type == "soft":
                self._check_soft_quota_warning_in_logs(cid, start_line=line_count_before_large, expect_warning=False)

    @pytest.mark.parametrize("soft_quota,hard_quota", [(50, 100), (100, 50), (75, 75)])
    def test_user_mixed_quotas(self, unique_wallet: NodeWallet, soft_quota: int, hard_quota: int):
        placement_rule = "EC 2/2"
        cid = create_container(
            unique_wallet.path,
            rule=placement_rule,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )
        neofs_adm: NeofsAdm = self.neofs_env.neofs_adm()
        neofs_adm.fschain.refill_gas(
            rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
            alphabet_wallets=self.neofs_env.alphabet_wallets_dir,
            storage_wallet=unique_wallet.path,
            gas="100.0",
        )

        with allure.step(f"Set user soft quota to {soft_quota}"):
            neofs_adm.fschain.user_quota(
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                account=unique_wallet.address,
                wallet=unique_wallet.path,
                wallet_password=unique_wallet.password,
                post_data=str(soft_quota),
                soft=True,
            )

        with allure.step(f"Set user hard quota to {hard_quota}"):
            neofs_adm.fschain.user_quota(
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                account=unique_wallet.address,
                wallet=unique_wallet.path,
                wallet_password=unique_wallet.password,
                post_data=str(hard_quota),
                soft=False,
            )

        self.tick_epochs_and_wait(2)

        effective_limit = min(soft_quota, hard_quota)

        with allure.step(f"Put object within both quotas (size: {effective_limit // 2})"):
            file_path_within = generate_file(effective_limit // 2)
            oid1 = put_object(
                unique_wallet.path, file_path_within, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc
            )
            get_object(
                unique_wallet.path,
                cid,
                oid1,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

        self.tick_epochs_and_wait(2)

        if soft_quota < hard_quota:
            with allure.step("Put object that exceeds soft quota but within hard quota"):
                initial_line_count = self._get_log_line_count()
                file_path_over_soft = generate_file(soft_quota - (effective_limit // 2) + 10)
                oid2 = put_object(
                    unique_wallet.path, file_path_over_soft, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc
                )
                get_object(
                    unique_wallet.path,
                    cid,
                    oid2,
                    self.neofs_env.shell,
                    self.neofs_env.sn_rpc,
                )
                self._check_soft_quota_warning_in_logs(cid, start_line=initial_line_count, expect_warning=True)

            self.tick_epochs_and_wait(2)

            with allure.step("Try to exceed hard quota - should fail"):
                remaining_hard_quota = hard_quota - (effective_limit // 2) - (soft_quota - (effective_limit // 2) + 10)
                if remaining_hard_quota > 0:
                    file_path_exceed_hard = generate_file(remaining_hard_quota + 10)
                    with pytest.raises(Exception, match=r".*size quota limits are exceeded.*"):
                        put_object(
                            unique_wallet.path,
                            file_path_exceed_hard,
                            cid,
                            shell=self.shell,
                            endpoint=self.neofs_env.sn_rpc,
                        )

        elif hard_quota < soft_quota:
            with allure.step("Try to exceed hard quota (which is lower) - should fail"):
                remaining_quota = hard_quota - (effective_limit // 2)
                if remaining_quota > 0:
                    file_path_exceed = generate_file(remaining_quota + 10)
                    with pytest.raises(Exception, match=r".*size quota limits are exceeded.*"):
                        put_object(
                            unique_wallet.path, file_path_exceed, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc
                        )

        else:
            with allure.step("Try to exceed both equal quotas - should fail"):
                initial_line_count = self._get_log_line_count()
                remaining_quota = effective_limit - (effective_limit // 2)
                if remaining_quota > 0:
                    file_path_exceed = generate_file(remaining_quota + 10)
                    with pytest.raises(Exception, match=r".*size quota limits are exceeded.*"):
                        put_object(
                            unique_wallet.path, file_path_exceed, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc
                        )
