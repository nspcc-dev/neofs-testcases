import allure
import pytest
from helpers.acl import (
    EACLAccess,
    EACLOperation,
    EACLRole,
    EACLRule,
    create_eacl,
    get_eacl,
    set_eacl,
    wait_for_cache_expired,
)
from helpers.complex_object_actions import wait_object_replication
from helpers.container import create_container
from helpers.container_access import check_full_access_to_container, check_no_access_to_container
from helpers.grpc_responses import EACL_PROHIBITED_TO_MODIFY_SYSTEM_ACCESS, NOT_CONTAINER_OWNER
from helpers.neofs_verbs import put_object_to_random_node
from helpers.node_management import drop_object
from helpers.object_access import (
    can_delete_object,
    can_get_head_object,
    can_get_object,
    can_get_range_hash_of_object,
    can_get_range_of_object,
    can_put_object,
    can_search_object,
)
from helpers.wellknown_acl import PUBLIC_ACL
from neofs_env.neofs_env_test_base import NeofsEnvTestBase
from neofs_testlib.env.env import NeoFSEnv


@pytest.mark.acl
@pytest.mark.acl_extended
class TestEACLContainer(NeofsEnvTestBase):
    @pytest.fixture(scope="function")
    def eacl_full_placement_container_with_object(self, wallets, file_path) -> tuple[str, str, str]:
        user_wallet = wallets.get_wallet()
        storage_nodes = self.neofs_env.storage_nodes
        node_count = len(storage_nodes)
        with allure.step("Create eACL public container with full placement rule"):
            full_placement_rule = f"REP {node_count} IN X CBF 1 SELECT {node_count} FROM * AS X"
            cid = create_container(
                wallet=user_wallet.wallet_path,
                rule=full_placement_rule,
                basic_acl=PUBLIC_ACL,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Add test object to container"):
            oid = put_object_to_random_node(
                user_wallet.wallet_path, file_path, cid, shell=self.shell, neofs_env=self.neofs_env
            )
            wait_object_replication(
                cid,
                oid,
                node_count,
                shell=self.shell,
                nodes=storage_nodes,
                neofs_env=self.neofs_env,
            )

        yield cid, oid, file_path

    @pytest.mark.parametrize("deny_role", [EACLRole.USER, EACLRole.OTHERS])
    def test_extended_acl_deny_all_operations(
        self, wallets, eacl_container_with_objects, deny_role
    ):
        user_wallet = wallets.get_wallet()
        other_wallet = wallets.get_wallet(EACLRole.OTHERS)
        deny_role_wallet = other_wallet if deny_role == EACLRole.OTHERS else user_wallet
        not_deny_role_wallet = user_wallet if deny_role == EACLRole.OTHERS else other_wallet
        deny_role_str = "all others" if deny_role == EACLRole.OTHERS else "user"
        not_deny_role_str = "user" if deny_role == EACLRole.OTHERS else "all others"
        allure.dynamic.title(f"Testcase to deny NeoFS operations for {deny_role_str}.")
        cid, object_oids, file_path = eacl_container_with_objects

        with allure.step(f"Deny all operations for {deny_role_str} via eACL"):
            eacl_deny = [
                EACLRule(access=EACLAccess.DENY, role=deny_role, operation=op)
                for op in EACLOperation
            ]
            set_eacl(
                user_wallet.wallet_path,
                cid,
                create_eacl(
                    cid, eacl_deny, shell=self.shell, wallet_config=user_wallet.config_path
                ),
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            wait_for_cache_expired()

        with allure.step(f"Check only {not_deny_role_str} has full access to container"):
            with allure.step(
                f"Check {deny_role_str} has not access to any operations with container"
            ):
                check_no_access_to_container(
                    deny_role_wallet.wallet_path,
                    cid,
                    object_oids[0],
                    file_path,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                )

            with allure.step(
                f"Check {not_deny_role_wallet} has full access to eACL public container"
            ):
                check_full_access_to_container(
                    not_deny_role_wallet.wallet_path,
                    cid,
                    object_oids.pop(),
                    file_path,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                )

        with allure.step(f"Allow all operations for {deny_role_str} via eACL"):
            eacl_deny = [
                EACLRule(access=EACLAccess.ALLOW, role=deny_role, operation=op)
                for op in EACLOperation
            ]
            set_eacl(
                user_wallet.wallet_path,
                cid,
                create_eacl(
                    cid, eacl_deny, shell=self.shell, wallet_config=user_wallet.config_path
                ),
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            wait_for_cache_expired()

        with allure.step("Check all have full access to eACL public container"):
            check_full_access_to_container(
                user_wallet.wallet_path,
                cid,
                object_oids.pop(),
                file_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )
            check_full_access_to_container(
                other_wallet.wallet_path,
                cid,
                object_oids.pop(),
                file_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

    @allure.title("Testcase to allow NeoFS operations for only one other pubkey.")
    def test_extended_acl_deny_all_operations_exclude_pubkey(
        self, wallets, eacl_container_with_objects
    ):
        user_wallet = wallets.get_wallet()
        other_wallet, other_wallet_allow = wallets.get_wallets_list(EACLRole.OTHERS)[0:2]
        cid, object_oids, file_path = eacl_container_with_objects

        with allure.step("Deny all operations for others except single wallet via eACL"):
            eacl = [
                EACLRule(
                    access=EACLAccess.ALLOW,
                    role=other_wallet_allow.wallet_path,
                    operation=op,
                    password="password",
                )
                for op in EACLOperation
            ]
            eacl += [
                EACLRule(access=EACLAccess.DENY, role=EACLRole.OTHERS, operation=op)
                for op in EACLOperation
            ]
            set_eacl(
                user_wallet.wallet_path,
                cid,
                create_eacl(cid, eacl, shell=self.shell, wallet_config=user_wallet.config_path),
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            wait_for_cache_expired()

        with allure.step("Check only owner and allowed other have full access to public container"):
            with allure.step("Check other has not access to operations with container"):
                check_no_access_to_container(
                    other_wallet.wallet_path,
                    cid,
                    object_oids[0],
                    file_path,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                )

            with allure.step("Check owner has full access to public container"):
                check_full_access_to_container(
                    user_wallet.wallet_path,
                    cid,
                    object_oids.pop(),
                    file_path,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                )

            with allure.step("Check allowed other has full access to public container"):
                check_full_access_to_container(
                    other_wallet_allow.wallet_path,
                    cid,
                    object_oids.pop(),
                    file_path,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                )

    @allure.title("Testcase to validate NeoFS replication with eACL deny rules.")
    def test_extended_acl_deny_replication(
        self,
        wallets,
        eacl_full_placement_container_with_object,
    ):
        user_wallet = wallets.get_wallet()
        cid, oid, file_path = eacl_full_placement_container_with_object
        storage_nodes = self.neofs_env.storage_nodes
        storage_node = self.neofs_env.storage_nodes[0]

        with allure.step("Deny all operations for user via eACL"):
            eacl_deny = [
                EACLRule(access=EACLAccess.DENY, role=EACLRole.USER, operation=op)
                for op in EACLOperation
            ]
            eacl_deny += [
                EACLRule(access=EACLAccess.DENY, role=EACLRole.OTHERS, operation=op)
                for op in EACLOperation
            ]
            set_eacl(
                user_wallet.wallet_path,
                cid,
                create_eacl(
                    cid, eacl_deny, shell=self.shell, wallet_config=user_wallet.config_path
                ),
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            wait_for_cache_expired()

        with allure.step("Drop object to check replication"):
            drop_object(storage_node, cid=cid, oid=oid)

        with allure.step("Wait for dropped object replicated"):
            wait_object_replication(
                cid, oid, len(storage_nodes), self.shell, storage_nodes, self.neofs_env
            )

    @pytest.mark.trusted_party_proved
    @pytest.mark.system_eacl
    @allure.title("Test case for verifying the impossible to change system extended ACL")
    def test_deprecated_change_system_eacl(self, wallets, eacl_container_with_objects):
        user_wallet = wallets.get_wallet()
        cid, object_oids, file_path = eacl_container_with_objects
        endpoint = self.neofs_env.sn_rpc

        with allure.step("Try to deny the system extended ACL"):
            with pytest.raises(Exception, match=EACL_PROHIBITED_TO_MODIFY_SYSTEM_ACCESS):
                set_eacl(
                    user_wallet.wallet_path,
                    cid,
                    create_eacl(
                        cid=cid,
                        rules_list=[
                            EACLRule(access=EACLAccess.DENY, role=EACLRole.SYSTEM, operation=op)
                            for op in EACLOperation
                        ],
                        shell=self.shell,
                        wallet_config=user_wallet.config_path,
                    ),
                    shell=self.shell,
                    endpoint=endpoint,
                )
            wait_for_cache_expired()
            with allure.step("The eACL must be empty"):
                assert get_eacl(user_wallet.wallet_path, cid, self.shell, endpoint) is None

        with allure.step("Try to allow the system extended ACL"):
            with pytest.raises(Exception, match=EACL_PROHIBITED_TO_MODIFY_SYSTEM_ACCESS):
                set_eacl(
                    user_wallet.wallet_path,
                    cid,
                    create_eacl(
                        cid=cid,
                        rules_list=[
                            EACLRule(access=EACLAccess.ALLOW, role=EACLRole.SYSTEM, operation=op)
                            for op in EACLOperation
                        ],
                        shell=self.shell,
                        wallet_config=user_wallet.config_path,
                    ),
                    shell=self.shell,
                    endpoint=endpoint,
                )
            wait_for_cache_expired()
            with allure.step("The eACL must be empty"):
                assert get_eacl(user_wallet.wallet_path, cid, self.shell, endpoint) is None

    @pytest.mark.trusted_party_proved
    @pytest.mark.system_eacl
    @allure.title(
        "Test case for verifying the impossible to change system extended ACL if eACL already set"
    )
    def test_deprecated_change_system_eacl_if_eacl_already_set(
        self, wallets, eacl_container_with_objects
    ):
        user_wallet = wallets.get_wallet()
        cid, object_oids, file_path = eacl_container_with_objects
        endpoint = self.neofs_env.sn_rpc

        with allure.step("Set eACL"):
            set_eacl(
                user_wallet.wallet_path,
                cid,
                create_eacl(
                    cid=cid,
                    rules_list=[
                        EACLRule(access=EACLAccess.ALLOW, role=EACLRole.USER, operation=op)
                        for op in EACLOperation
                    ],
                    shell=self.shell,
                    wallet_config=user_wallet.config_path,
                ),
                shell=self.shell,
                endpoint=endpoint,
            )
            wait_for_cache_expired()

        old_eacl = get_eacl(user_wallet.wallet_path, cid, self.shell, endpoint)

        with allure.step("Try to change the system extended ACL"):
            with pytest.raises(Exception, match=EACL_PROHIBITED_TO_MODIFY_SYSTEM_ACCESS):
                set_eacl(
                    user_wallet.wallet_path,
                    cid,
                    create_eacl(
                        cid=cid,
                        rules_list=[
                            EACLRule(access=EACLAccess.DENY, role=EACLRole.SYSTEM, operation=op)
                            for op in EACLOperation
                        ],
                        shell=self.shell,
                        wallet_config=user_wallet.config_path,
                    ),
                    shell=self.shell,
                    endpoint=endpoint,
                )
        wait_for_cache_expired()
        with allure.step("The eACL should not be changed"):
            assert get_eacl(user_wallet.wallet_path, cid, self.shell, endpoint) == old_eacl

    @pytest.mark.trusted_party_proved
    @allure.title("Test case to check compliance with Check IR and STORAGE rules")
    def test_compliance_ir_and_storage_rules(self, wallets, eacl_container_with_objects):
        ir_wallet = wallets.get_ir_wallet()
        storage_wallet = wallets.get_storage_wallet()

        cid, object_oids, file_path = eacl_container_with_objects
        endpoint = self.neofs_env.sn_rpc

        with allure.step("Check IR and STORAGE rules compliance"):
            assert not can_put_object(
                ir_wallet.wallet_path,
                cid,
                file_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
                wallet_config=ir_wallet.config_path,
            )
            assert can_put_object(
                storage_wallet.wallet_path,
                cid,
                file_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
                wallet_config=storage_wallet.config_path,
            )

            assert can_get_object(
                ir_wallet.wallet_path,
                cid,
                object_oids[0],
                file_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
                wallet_config=ir_wallet.config_path,
            )
            assert can_get_object(
                storage_wallet.wallet_path,
                cid,
                object_oids[0],
                file_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
                wallet_config=storage_wallet.config_path,
            )

            assert can_get_head_object(
                ir_wallet.wallet_path,
                cid,
                object_oids[0],
                shell=self.shell,
                endpoint=endpoint,
                wallet_config=ir_wallet.config_path,
            )
            assert can_get_head_object(
                storage_wallet.wallet_path,
                cid,
                object_oids[0],
                shell=self.shell,
                endpoint=endpoint,
                wallet_config=storage_wallet.config_path,
            )

            assert can_search_object(
                ir_wallet.wallet_path,
                cid,
                shell=self.shell,
                endpoint=endpoint,
                oid=object_oids[0],
                wallet_config=ir_wallet.config_path,
            )
            assert can_search_object(
                storage_wallet.wallet_path,
                cid,
                shell=self.shell,
                endpoint=endpoint,
                oid=object_oids[0],
                wallet_config=storage_wallet.config_path,
            )

            with pytest.raises(AssertionError):
                assert can_get_range_of_object(
                    wallet=ir_wallet.wallet_path,
                    cid=cid,
                    oid=object_oids[0],
                    shell=self.shell,
                    endpoint=endpoint,
                    wallet_config=ir_wallet.config_path,
                )
            with pytest.raises(AssertionError):
                assert can_get_range_of_object(
                    wallet=storage_wallet.wallet_path,
                    cid=cid,
                    oid=object_oids[0],
                    shell=self.shell,
                    endpoint=endpoint,
                    wallet_config=storage_wallet.config_path,
                )

            assert can_get_range_hash_of_object(
                wallet=ir_wallet.wallet_path,
                cid=cid,
                oid=object_oids[0],
                shell=self.shell,
                endpoint=endpoint,
                wallet_config=ir_wallet.config_path,
            )

            assert can_get_range_hash_of_object(
                wallet=storage_wallet.wallet_path,
                cid=cid,
                oid=object_oids[0],
                shell=self.shell,
                endpoint=endpoint,
                wallet_config=storage_wallet.config_path,
            )

            with pytest.raises(AssertionError):
                assert can_delete_object(
                    wallet=ir_wallet.wallet_path,
                    cid=cid,
                    oid=object_oids[0],
                    shell=self.shell,
                    endpoint=endpoint,
                    wallet_config=ir_wallet.config_path,
                )
            with pytest.raises(AssertionError):
                assert can_delete_object(
                    wallet=storage_wallet.wallet_path,
                    cid=cid,
                    oid=object_oids[0],
                    shell=self.shell,
                    endpoint=endpoint,
                    wallet_config=storage_wallet.config_path,
                )

    @pytest.mark.trusted_party_proved
    @allure.title("Not owner and not trusted party can NOT set eacl")
    def test_only_owner_can_set_eacl(
        self,
        wallets,
        eacl_full_placement_container_with_object: tuple[str, str, str],
        not_owner_wallet: str,
    ):
        not_owner_wallet_config_path = NeoFSEnv._generate_temp_file(extension="yml")
        NeoFSEnv.generate_config_file(
            config_template="cli_cfg.yaml",
            config_path=not_owner_wallet_config_path,
            wallet=not_owner_wallet,
        )

        cid, oid, file_path = eacl_full_placement_container_with_object

        eacl = [
            EACLRule(access=EACLAccess.DENY, role=EACLRole.USER, operation=op)
            for op in EACLOperation
        ]

        with allure.step("Try to change EACL"):
            with pytest.raises(RuntimeError, match=NOT_CONTAINER_OWNER):
                set_eacl(
                    wallet_path=not_owner_wallet.path,
                    cid=cid,
                    eacl_table_path=create_eacl(
                        cid, eacl, shell=self.shell, wallet_config=not_owner_wallet_config_path
                    ),
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                )
