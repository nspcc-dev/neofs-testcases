import time

import allure
import pytest
from helpers.container import create_container
from helpers.file_helper import generate_file
from helpers.neofs_verbs import delete_object, get_object, put_object
from helpers.quota import TestQuotaBase
from helpers.utility import wait_for_gc_pass_on_storage_nodes
from neofs_testlib.cli import NeofsAdm
from neofs_testlib.env.env import NodeWallet


class TestContainerQuota(TestQuotaBase):
    @pytest.mark.parametrize(
        "data_shards,parity_shards",
        [
            (3, 1),
            (2, 1),
            (1, 1),
            (1, 2),
            (2, 2),
            (1, 3),
        ],
        ids=[
            "EC_3/1",
            "EC_2/1",
            "EC_1/1",
            "EC_1/2",
            "EC_2/2",
            "EC_1/3",
        ],
    )
    @pytest.mark.parametrize("quota_type,quota_value", [("hard", 100)])
    @pytest.mark.sanity
    def test_ec_container_quota(
        self, default_wallet: NodeWallet, quota_type: str, quota_value: int, data_shards: int, parity_shards: int
    ):
        placement_rule = f"EC {data_shards}/{parity_shards} CBF 1"
        max_allowed_part_size = quota_value // (data_shards + parity_shards)
        max_object_size_for_the_current_quota = max_allowed_part_size * data_shards
        cid = create_container(
            default_wallet.path,
            rule=placement_rule,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )
        neofs_adm: NeofsAdm = self.neofs_env.neofs_adm()
        neofs_adm.fschain.refill_gas(
            rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
            alphabet_wallets=self.neofs_env.alphabet_wallets_dir,
            storage_wallet=default_wallet.path,
            gas="100.0",
        )

        is_soft = quota_type == "soft"
        with allure.step(f"Set container {quota_type} quota to {quota_value}"):
            neofs_adm.fschain.container_quota(
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                cid=cid,
                wallet=default_wallet.path,
                wallet_password=default_wallet.password,
                soft=is_soft,
                post_data=str(quota_value),
            )
            expected_soft = quota_value if is_soft else 0
            expected_hard = quota_value if not is_soft else 0
            self.get_and_verify_container_quota(
                default_wallet, cid, expected_soft=expected_soft, expected_hard=expected_hard
            )

        with allure.step("Try to put object bigger than quota size"):
            file_path = generate_file(max_object_size_for_the_current_quota + 1)
            if is_soft:
                oid = put_object(default_wallet.path, file_path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
                self._check_soft_quota_warning_in_logs(cid, expect_warning=True)
                initial_line_count = self._get_log_line_count()
            else:
                with pytest.raises(Exception, match=r".*size quota limits are exceeded.*"):
                    put_object(default_wallet.path, file_path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

        with allure.step("Try to put object of the equal to quota size"):
            file_path = generate_file(max_object_size_for_the_current_quota)
            oid = put_object(default_wallet.path, file_path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
            get_object(
                default_wallet.path,
                cid,
                oid,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )
            if is_soft:
                self._check_soft_quota_warning_in_logs(cid, start_line=initial_line_count, expect_warning=False)

    @pytest.mark.parametrize("quota_type,quota_value", [("hard", 200)])
    def test_ec_container_quota_multiple_objects(self, default_wallet: NodeWallet, quota_type: str, quota_value: int):
        data_shards = 3
        parity_shards = 1
        placement_rule = f"EC {data_shards}/{parity_shards} CBF 1"
        max_allowed_part_size = quota_value // (data_shards + parity_shards)
        max_object_size_for_the_current_quota = max_allowed_part_size * data_shards

        cid = create_container(
            default_wallet.path,
            rule=placement_rule,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )
        neofs_adm: NeofsAdm = self.neofs_env.neofs_adm()
        neofs_adm.fschain.refill_gas(
            rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
            alphabet_wallets=self.neofs_env.alphabet_wallets_dir,
            storage_wallet=default_wallet.path,
            gas="100.0",
        )

        is_soft = quota_type == "soft"
        with allure.step(f"Set container {quota_type} quota to {quota_value}"):
            neofs_adm.fschain.container_quota(
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                cid=cid,
                wallet=default_wallet.path,
                wallet_password=default_wallet.password,
                soft=is_soft,
                post_data=str(quota_value),
            )
            expected_soft = quota_value if is_soft else 0
            expected_hard = quota_value if not is_soft else 0
            self.get_and_verify_container_quota(
                default_wallet, cid, expected_soft=expected_soft, expected_hard=expected_hard
            )

        self.tick_epochs_and_wait(2)

        with allure.step("Put multiple small objects within quota"):
            small_size = max_object_size_for_the_current_quota // 5
            oids = []
            for _ in range(3):
                file_path = generate_file(small_size)
                oid = put_object(default_wallet.path, file_path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
                oids.append(oid)

        if is_soft:
            initial_line_count = self._get_log_line_count()

        self.tick_epochs_and_wait(1)
        prev_report = self.wait_until_quota_values_reported(cid, expected_objects=3 * (data_shards + parity_shards))

        with allure.step("Add object that exceeds quota"):
            large_size = max_object_size_for_the_current_quota // 2
            file_path = generate_file(large_size)
            if is_soft:
                oid = put_object(default_wallet.path, file_path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
                oids.append(oid)
                self._check_soft_quota_warning_in_logs(cid, start_line=initial_line_count, expect_warning=True)
            else:
                with pytest.raises(Exception, match=r".*size quota limits are exceeded.*"):
                    put_object(default_wallet.path, file_path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

        if is_soft:
            self.tick_epochs_and_wait(1)
            prev_report = self.wait_until_quota_values_reported(
                cid,
                prev_report,
                expected_objects=4 * (data_shards + parity_shards),
            )

            with allure.step("Continue adding more objects - should keep warning"):
                line_count_before_additional = self._get_log_line_count()
                additional_size = max_object_size_for_the_current_quota // 4
                file_path = generate_file(additional_size)
                oid = put_object(default_wallet.path, file_path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
                oids.append(oid)
                self._check_soft_quota_warning_in_logs(
                    cid, start_line=line_count_before_additional, expect_warning=True
                )

        with allure.step("Verify all objects are accessible"):
            for oid in oids:
                get_object(
                    default_wallet.path,
                    cid,
                    oid,
                    self.neofs_env.shell,
                    self.neofs_env.sn_rpc,
                )

    @pytest.mark.parametrize("quota_type1,quota_type2,quota_value", [("hard", "soft", 150)])
    def test_ec_multiple_containers_quota_isolation(
        self, default_wallet: NodeWallet, quota_type1: str, quota_type2: str, quota_value: int
    ):
        """Test that quotas are enforced per container and don't affect each other."""
        data_shards = 3
        parity_shards = 1
        placement_rule = f"EC {data_shards}/{parity_shards} CBF 1"
        max_allowed_part_size = quota_value // (data_shards + parity_shards)
        max_object_size_for_the_current_quota = max_allowed_part_size * data_shards

        cid1 = create_container(
            default_wallet.path,
            rule=placement_rule,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )
        cid2 = create_container(
            default_wallet.path,
            rule=placement_rule,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )

        neofs_adm: NeofsAdm = self.neofs_env.neofs_adm()
        neofs_adm.fschain.refill_gas(
            rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
            alphabet_wallets=self.neofs_env.alphabet_wallets_dir,
            storage_wallet=default_wallet.path,
            gas="100.0",
        )

        is_soft1 = quota_type1 == "soft"
        is_soft2 = quota_type2 == "soft"

        with allure.step(f"Set {quota_type1} quota on first container {quota_value}"):
            neofs_adm.fschain.container_quota(
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                cid=cid1,
                wallet=default_wallet.path,
                wallet_password=default_wallet.password,
                soft=is_soft1,
                post_data=str(quota_value),
            )
            expected_soft1 = quota_value if is_soft1 else 0
            expected_hard1 = quota_value if not is_soft1 else 0
            self.get_and_verify_container_quota(
                default_wallet, cid1, expected_soft=expected_soft1, expected_hard=expected_hard1
            )

        with allure.step(f"Set {quota_type2} quota on second container {quota_value}"):
            neofs_adm.fschain.container_quota(
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                cid=cid2,
                wallet=default_wallet.path,
                wallet_password=default_wallet.password,
                soft=is_soft2,
                post_data=str(quota_value),
            )
            expected_soft2 = quota_value if is_soft2 else 0
            expected_hard2 = quota_value if not is_soft2 else 0
            self.get_and_verify_container_quota(
                default_wallet, cid2, expected_soft=expected_soft2, expected_hard=expected_hard2
            )

        self.tick_epochs_and_wait(2)

        with allure.step("Fill first container to near quota limit"):
            file_path = generate_file(max_object_size_for_the_current_quota - 20)
            oid1 = put_object(default_wallet.path, file_path, cid1, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

        with allure.step("Fill second container beyond quota limit"):
            if is_soft2:
                initial_line_count = self._get_log_line_count()
            file_path = generate_file(max_object_size_for_the_current_quota + 50)
            if is_soft2:
                oid2 = put_object(
                    default_wallet.path, file_path, cid2, shell=self.shell, endpoint=self.neofs_env.sn_rpc
                )
                self._check_soft_quota_warning_in_logs(cid2, start_line=initial_line_count, expect_warning=True)
            else:
                with pytest.raises(Exception, match=r".*size quota limits are exceeded.*"):
                    put_object(default_wallet.path, file_path, cid2, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
                file_path_small = generate_file(max_object_size_for_the_current_quota - 30)
                oid2 = put_object(
                    default_wallet.path, file_path_small, cid2, shell=self.shell, endpoint=self.neofs_env.sn_rpc
                )

        self.tick_epochs_and_wait(1)
        self.wait_until_quota_values_reported(cid1, expected_objects=1 * (data_shards + parity_shards))
        self.wait_until_quota_values_reported(cid2, expected_objects=1 * (data_shards + parity_shards))

        with allure.step("Try to exceed quota in first container"):
            file_path = generate_file(30)
            if is_soft1:
                line_count_before_first = self._get_log_line_count()
                oid1_additional = put_object(
                    default_wallet.path, file_path, cid1, shell=self.shell, endpoint=self.neofs_env.sn_rpc
                )
                self._check_soft_quota_warning_in_logs(cid1, start_line=line_count_before_first, expect_warning=True)
            else:
                with pytest.raises(Exception, match=r".*size quota limits are exceeded.*"):
                    put_object(default_wallet.path, file_path, cid1, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

        with allure.step("Continue adding to second container"):
            file_path = generate_file(50)
            if is_soft2:
                line_count_before_second = self._get_log_line_count()
                oid2_additional = put_object(
                    default_wallet.path, file_path, cid2, shell=self.shell, endpoint=self.neofs_env.sn_rpc
                )
                self._check_soft_quota_warning_in_logs(cid2, start_line=line_count_before_second, expect_warning=True)
            else:
                with pytest.raises(Exception, match=r".*size quota limits are exceeded.*"):
                    put_object(default_wallet.path, file_path, cid2, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

        with allure.step("Verify objects in both containers are accessible"):
            get_object(default_wallet.path, cid1, oid1, self.neofs_env.shell, self.neofs_env.sn_rpc)
            get_object(default_wallet.path, cid2, oid2, self.neofs_env.shell, self.neofs_env.sn_rpc)
            if is_soft1:
                get_object(default_wallet.path, cid1, oid1_additional, self.neofs_env.shell, self.neofs_env.sn_rpc)
            if is_soft2:
                get_object(default_wallet.path, cid2, oid2_additional, self.neofs_env.shell, self.neofs_env.sn_rpc)

    @pytest.mark.parametrize("quota_type,initial_quota,updated_quota", [("hard", 100, 200)])
    def test_ec_container_quota_update(
        self, default_wallet: NodeWallet, quota_type: str, initial_quota: int, updated_quota: int
    ):
        data_shards = 3
        parity_shards = 1
        placement_rule = f"EC {data_shards}/{parity_shards} CBF 1"

        cid = create_container(
            default_wallet.path,
            rule=placement_rule,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )
        neofs_adm: NeofsAdm = self.neofs_env.neofs_adm()
        neofs_adm.fschain.refill_gas(
            rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
            alphabet_wallets=self.neofs_env.alphabet_wallets_dir,
            storage_wallet=default_wallet.path,
            gas="100.0",
        )

        is_soft = quota_type == "soft"
        with allure.step(f"Set initial container {quota_type} quota {initial_quota}"):
            neofs_adm.fschain.container_quota(
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                cid=cid,
                wallet=default_wallet.path,
                wallet_password=default_wallet.password,
                soft=is_soft,
                post_data=str(initial_quota),
            )
            expected_soft = initial_quota if is_soft else 0
            expected_hard = initial_quota if not is_soft else 0
            self.get_and_verify_container_quota(
                default_wallet, cid, expected_soft=expected_soft, expected_hard=expected_hard
            )

        self.tick_epochs_and_wait(2)

        max_allowed_part_size_initial = initial_quota // (data_shards + parity_shards)
        max_object_size_initial = max_allowed_part_size_initial * data_shards

        with allure.step("Put object within initial quota"):
            file_path = generate_file(max_object_size_initial // 2)
            oid = put_object(default_wallet.path, file_path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
            get_object(
                default_wallet.path,
                cid,
                oid,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

        with allure.step(f"Update container {quota_type} quota to {updated_quota}"):
            neofs_adm.fschain.container_quota(
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                cid=cid,
                wallet=default_wallet.path,
                wallet_password=default_wallet.password,
                soft=is_soft,
                post_data=str(updated_quota),
            )
            expected_soft = updated_quota if is_soft else 0
            expected_hard = updated_quota if not is_soft else 0
            self.get_and_verify_container_quota(
                default_wallet, cid, expected_soft=expected_soft, expected_hard=expected_hard
            )

        self.tick_epochs_and_wait(1)
        self.wait_until_quota_values_reported(cid, expected_objects=1 * (data_shards + parity_shards))

        max_allowed_part_size_updated = updated_quota // (data_shards + parity_shards)
        max_object_size_updated = max_allowed_part_size_updated * data_shards

        if updated_quota > initial_quota:
            with allure.step("Increased quota should allow larger objects"):
                file_path_large = generate_file(max_object_size_initial)
                oid_large = put_object(
                    default_wallet.path, file_path_large, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc
                )
                get_object(
                    default_wallet.path,
                    cid,
                    oid_large,
                    self.neofs_env.shell,
                    self.neofs_env.sn_rpc,
                )
        else:
            if is_soft:
                with allure.step("Decreased soft quota should still allow objects with warning"):
                    initial_line_count = self._get_log_line_count()
                    remaining_quota_object_size = max_object_size_updated - (max_object_size_initial // 2)
                    if remaining_quota_object_size > 0:
                        file_path_exceed = generate_file(remaining_quota_object_size + 10)
                        oid_exceed = put_object(
                            default_wallet.path, file_path_exceed, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc
                        )
                        get_object(
                            default_wallet.path,
                            cid,
                            oid_exceed,
                            self.neofs_env.shell,
                            self.neofs_env.sn_rpc,
                        )
                        self._check_soft_quota_warning_in_logs(cid, start_line=initial_line_count, expect_warning=True)
            else:
                with allure.step("Decreased hard quota should prevent objects that would exceed new limit"):
                    remaining_quota_object_size = max_object_size_updated - (max_object_size_initial // 2)
                    if remaining_quota_object_size > 0:
                        file_path_exceed = generate_file(remaining_quota_object_size + 10)
                        with pytest.raises(Exception, match=r".*size quota limits are exceeded.*"):
                            put_object(
                                default_wallet.path,
                                file_path_exceed,
                                cid,
                                shell=self.shell,
                                endpoint=self.neofs_env.sn_rpc,
                            )

    @pytest.mark.parametrize("quota_type,quota_value", [("soft", 200)])
    def test_ec_container_object_delete_and_quota_reclaim(
        self, default_wallet: NodeWallet, quota_type: str, quota_value: int
    ):
        data_shards = 3
        parity_shards = 1
        placement_rule = f"EC {data_shards}/{parity_shards} CBF 1"
        max_allowed_part_size = quota_value // (data_shards + parity_shards)
        max_object_size_for_the_current_quota = max_allowed_part_size * data_shards

        cid = create_container(
            default_wallet.path,
            rule=placement_rule,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )
        neofs_adm: NeofsAdm = self.neofs_env.neofs_adm()
        neofs_adm.fschain.refill_gas(
            rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
            alphabet_wallets=self.neofs_env.alphabet_wallets_dir,
            storage_wallet=default_wallet.path,
            gas="100.0",
        )

        is_soft = quota_type == "soft"
        with allure.step(f"Set container {quota_type} quota {quota_value}"):
            neofs_adm.fschain.container_quota(
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                cid=cid,
                wallet=default_wallet.path,
                wallet_password=default_wallet.password,
                soft=is_soft,
                post_data=str(quota_value),
            )
            expected_soft = quota_value if is_soft else 0
            expected_hard = quota_value if not is_soft else 0
            self.get_and_verify_container_quota(
                default_wallet, cid, expected_soft=expected_soft, expected_hard=expected_hard
            )

        self.tick_epochs_and_wait(2)

        with allure.step("Put objects that nearly fill the container quota"):
            file_path1 = generate_file(max_object_size_for_the_current_quota // 2)
            oid1 = put_object(default_wallet.path, file_path1, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
            get_object(
                default_wallet.path,
                cid,
                oid1,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

            file_path2 = generate_file(max_object_size_for_the_current_quota // 2 - 20)
            oid2 = put_object(default_wallet.path, file_path2, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
            get_object(
                default_wallet.path,
                cid,
                oid2,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

        self.tick_epochs_and_wait(1)
        prev_report = self.wait_until_quota_values_reported(cid, expected_objects=2 * (data_shards + parity_shards))

        with allure.step("Verify container quota is full"):
            file_path_exceed = generate_file(50)
            if is_soft:
                initial_line_count = self._get_log_line_count()
                oid_exceed = put_object(
                    default_wallet.path, file_path_exceed, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc
                )
                get_object(
                    default_wallet.path,
                    cid,
                    oid_exceed,
                    self.neofs_env.shell,
                    self.neofs_env.sn_rpc,
                )
                self._check_soft_quota_warning_in_logs(cid, start_line=initial_line_count, expect_warning=True)
            else:
                with pytest.raises(Exception, match=r".*size quota limits are exceeded.*"):
                    put_object(
                        default_wallet.path, file_path_exceed, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc
                    )

        with allure.step("Delete first object to reclaim container quota"):
            delete_object(
                default_wallet.path,
                cid,
                oid1,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

        self.tick_epochs_and_wait(1)
        expected_objects_after_delete = 2 if is_soft else 1
        prev_report = self.wait_until_quota_values_reported(
            cid,
            prev_report,
            expected_objects=expected_objects_after_delete * (data_shards + parity_shards),
        )

        with allure.step("Verify container quota is reclaimed - should be able to put object again"):
            file_path_after_delete = generate_file(max_object_size_for_the_current_quota // 2 - 10)
            oid3 = put_object(
                default_wallet.path, file_path_after_delete, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc
            )
            get_object(
                default_wallet.path,
                cid,
                oid3,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

        with allure.step("Delete second object as well"):
            delete_object(
                default_wallet.path,
                cid,
                oid2,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

        self.tick_epochs_and_wait(1)
        expected_objects_final = 2 if is_soft else 1
        self.wait_until_quota_values_reported(
            cid,
            prev_report,
            expected_objects=expected_objects_final * (data_shards + parity_shards),
        )

        with allure.step("Verify significant container quota space is available after deleting both objects"):
            file_path_large = generate_file(max_object_size_for_the_current_quota // 2 - 20)
            oid4 = put_object(
                default_wallet.path, file_path_large, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc
            )
            get_object(
                default_wallet.path,
                cid,
                oid4,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

    @pytest.mark.parametrize("quota_type,quota_value", [("hard", 200)])
    def test_ec_container_object_lifetime_and_quota_reclaim(
        self, default_wallet: NodeWallet, quota_type: str, quota_value: int
    ):
        data_shards = 3
        parity_shards = 1
        placement_rule = f"EC {data_shards}/{parity_shards} CBF 1"
        max_allowed_part_size = quota_value // (data_shards + parity_shards)
        max_object_size_for_the_current_quota = max_allowed_part_size * data_shards

        cid = create_container(
            default_wallet.path,
            rule=placement_rule,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )
        neofs_adm: NeofsAdm = self.neofs_env.neofs_adm()
        neofs_adm.fschain.refill_gas(
            rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
            alphabet_wallets=self.neofs_env.alphabet_wallets_dir,
            storage_wallet=default_wallet.path,
            gas="100.0",
        )

        is_soft = quota_type == "soft"
        with allure.step(f"Set container {quota_type} quota {quota_value}"):
            neofs_adm.fschain.container_quota(
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                cid=cid,
                wallet=default_wallet.path,
                wallet_password=default_wallet.password,
                soft=is_soft,
                post_data=str(quota_value),
            )
            expected_soft = quota_value if is_soft else 0
            expected_hard = quota_value if not is_soft else 0
            self.get_and_verify_container_quota(
                default_wallet, cid, expected_soft=expected_soft, expected_hard=expected_hard
            )

        self.tick_epochs_and_wait(2)

        with allure.step("Put objects with short lifetime that nearly fill the container quota"):
            file_path1 = generate_file(max_object_size_for_the_current_quota // 2)
            oid1 = put_object(
                default_wallet.path, file_path1, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc, lifetime=1
            )
            get_object(
                default_wallet.path,
                cid,
                oid1,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

            file_path2 = generate_file(max_object_size_for_the_current_quota // 2 - 20)
            oid2 = put_object(
                default_wallet.path, file_path2, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc, lifetime=1
            )
            get_object(
                default_wallet.path,
                cid,
                oid2,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

        self.tick_epochs_and_wait(1)
        time.sleep(5)
        self.tick_epochs_and_wait(1)
        prev_report = self.wait_until_quota_values_reported(
            cid,
            expected_objects=2 * (data_shards + parity_shards),
        )
        wait_for_gc_pass_on_storage_nodes()
        self.tick_epochs_and_wait(1)
        self.wait_until_quota_values_reported(
            cid,
            prev_report,
            expected_objects=0,
        )

        with allure.step("Verify quota is reclaimed after auto-deletion - should be able to put objects again"):
            file_path_after_auto_delete = generate_file(max_object_size_for_the_current_quota // 2 - 10)
            if is_soft:
                reclaim_line_count = self._get_log_line_count()

            oid3 = put_object(
                default_wallet.path, file_path_after_auto_delete, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc
            )
            get_object(
                default_wallet.path,
                cid,
                oid3,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

            if is_soft:
                self._check_soft_quota_warning_in_logs(cid, start_line=reclaim_line_count, expect_warning=False)

        with allure.step("Verify quota space is available after auto-deletion"):
            file_path_large = generate_file(max_object_size_for_the_current_quota // 2 - 20)
            if is_soft:
                large_file_line_count = self._get_log_line_count()

            oid4 = put_object(
                default_wallet.path, file_path_large, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc
            )
            get_object(
                default_wallet.path,
                cid,
                oid4,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )

            if is_soft:
                self._check_soft_quota_warning_in_logs(cid, start_line=large_file_line_count, expect_warning=False)

    @pytest.mark.parametrize("quota_type,initial_quota", [("hard", 100)])
    def test_ec_container_quota_removal(self, default_wallet: NodeWallet, quota_type: str, initial_quota: int):
        data_shards = 3
        parity_shards = 1
        placement_rule = f"EC {data_shards}/{parity_shards} CBF 1"
        max_allowed_part_size = initial_quota // (data_shards + parity_shards)
        max_object_size_for_the_current_quota = max_allowed_part_size * data_shards

        cid = create_container(
            default_wallet.path,
            rule=placement_rule,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )
        neofs_adm: NeofsAdm = self.neofs_env.neofs_adm()
        neofs_adm.fschain.refill_gas(
            rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
            alphabet_wallets=self.neofs_env.alphabet_wallets_dir,
            storage_wallet=default_wallet.path,
            gas="100.0",
        )

        is_soft = quota_type == "soft"
        with allure.step(f"Set initial container {quota_type} quota to {initial_quota}"):
            neofs_adm.fschain.container_quota(
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                cid=cid,
                wallet=default_wallet.path,
                wallet_password=default_wallet.password,
                soft=is_soft,
                post_data=str(initial_quota),
            )
            expected_soft = initial_quota if is_soft else 0
            expected_hard = initial_quota if not is_soft else 0
            self.get_and_verify_container_quota(
                default_wallet, cid, expected_soft=expected_soft, expected_hard=expected_hard
            )

        self.tick_epochs_and_wait(2)

        with allure.step("Verify quota enforcement with initial quota"):
            file_path = generate_file(max_object_size_for_the_current_quota * 2)
            if is_soft:
                put_object(default_wallet.path, file_path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
                self._check_soft_quota_warning_in_logs(cid, expect_warning=True)
            else:
                with pytest.raises(Exception, match=r".*size quota limits are exceeded.*"):
                    put_object(default_wallet.path, file_path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)

        with allure.step(f"Reset container {quota_type} quota to 0"):
            neofs_adm.fschain.container_quota(
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                cid=cid,
                wallet=default_wallet.path,
                wallet_password=default_wallet.password,
                soft=is_soft,
                post_data="0",
            )
            self.get_and_verify_container_quota(default_wallet, cid, expected_soft=0, expected_hard=0)

        self.tick_epochs_and_wait(1)
        if is_soft:
            self.wait_until_quota_values_reported(cid, expected_objects=1 * (data_shards + parity_shards))

        with allure.step("Verify no limits are enforced after quota reset to 0"):
            large_file_path = generate_file(max_object_size_for_the_current_quota * 5)
            if is_soft:
                line_count_before_large = self._get_log_line_count()
            oid_after_reset = put_object(
                default_wallet.path, large_file_path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc
            )
            get_object(
                default_wallet.path,
                cid,
                oid_after_reset,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )
            if is_soft:
                self._check_soft_quota_warning_in_logs(cid, start_line=line_count_before_large, expect_warning=False)

    @pytest.mark.parametrize("soft_quota,hard_quota", [(100, 200)])
    def test_ec_container_mixed_quotas(self, default_wallet: NodeWallet, soft_quota: int, hard_quota: int):
        data_shards = 3
        parity_shards = 1
        placement_rule = f"EC {data_shards}/{parity_shards} CBF 1"

        cid = create_container(
            default_wallet.path,
            rule=placement_rule,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )
        neofs_adm: NeofsAdm = self.neofs_env.neofs_adm()
        neofs_adm.fschain.refill_gas(
            rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
            alphabet_wallets=self.neofs_env.alphabet_wallets_dir,
            storage_wallet=default_wallet.path,
            gas="100.0",
        )

        with allure.step(f"Set container soft quota to {soft_quota}"):
            neofs_adm.fschain.container_quota(
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                cid=cid,
                wallet=default_wallet.path,
                wallet_password=default_wallet.password,
                soft=True,
                post_data=str(soft_quota),
            )

        with allure.step(f"Set container hard quota to {hard_quota}"):
            neofs_adm.fschain.container_quota(
                rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
                cid=cid,
                wallet=default_wallet.path,
                wallet_password=default_wallet.password,
                post_data=str(hard_quota),
            )
            self.get_and_verify_container_quota(default_wallet, cid, expected_soft=soft_quota, expected_hard=hard_quota)

        self.tick_epochs_and_wait(2)

        max_allowed_part_size_soft = soft_quota // (data_shards + parity_shards)
        max_object_size_soft = max_allowed_part_size_soft * data_shards

        with allure.step("Put object within soft quota limit - no warning expected"):
            initial_line_count = self._get_log_line_count()
            file_path = generate_file(max_object_size_soft // 2)
            oid1 = put_object(default_wallet.path, file_path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
            get_object(
                default_wallet.path,
                cid,
                oid1,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )
            self._check_soft_quota_warning_in_logs(cid, start_line=initial_line_count, expect_warning=False)

        self.tick_epochs_and_wait(1)
        prev_report = self.wait_until_quota_values_reported(cid, expected_objects=1 * (data_shards + parity_shards))

        with allure.step("Put object that exceeds soft quota but stays within hard quota - warning expected"):
            line_count_before_soft_exceed = self._get_log_line_count()
            file_path = generate_file(max_object_size_soft)
            oid2 = put_object(default_wallet.path, file_path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
            get_object(
                default_wallet.path,
                cid,
                oid2,
                self.neofs_env.shell,
                self.neofs_env.sn_rpc,
            )
            self._check_soft_quota_warning_in_logs(cid, start_line=line_count_before_soft_exceed, expect_warning=True)

        self.tick_epochs_and_wait(1)
        self.wait_until_quota_values_reported(
            cid,
            prev_report,
            expected_objects=2 * (data_shards + parity_shards),
        )

        with allure.step("Try to put object that would exceed hard quota - should fail"):
            file_path = generate_file(60)
            with pytest.raises(Exception, match=r".*size quota limits are exceeded.*"):
                put_object(default_wallet.path, file_path, cid, shell=self.shell, endpoint=self.neofs_env.sn_rpc)
