import random

import allure
import pytest
from helpers.acl import (
    EACLAccess,
    EACLFilter,
    EACLFilters,
    EACLHeaderType,
    EACLOperation,
    EACLRole,
    EACLRoleExtended,
    EACLRoleExtendedType,
    EACLRule,
    create_eacl,
    get_eacl,
    set_eacl,
    wait_for_cache_expired,
)
from helpers.complex_object_actions import wait_object_replication
from helpers.container import EC_3_1_PLACEMENT_RULE, create_container
from helpers.container_access import (
    check_custom_access_to_container,
    check_full_access_to_container,
    check_no_access_to_container,
)
from helpers.grpc_responses import (
    EACL_CHANGE_PROHIBITED,
    EACL_CHANGE_RPC_ERROR,
    EACL_CHANGE_TIMEOUT,
    EACL_PROHIBITED_TO_MODIFY_SYSTEM_ACCESS,
    NOT_CONTAINER_OWNER,
)
from helpers.neofs_verbs import put_object_to_random_node
from helpers.node_management import drop_object
from helpers.object_access import (
    can_delete_object,
    can_get_head_object,
    can_get_object,
    can_get_range_of_object,
    can_put_object,
    can_search_object,
)
from helpers.utility import parse_version
from helpers.wellknown_acl import PUBLIC_ACL, PUBLIC_ACL_F
from neofs_env.neofs_env_test_base import TestNeofsBase
from neofs_testlib.env.env import NeoFSEnv


class TestEACLContainer(TestNeofsBase):
    @pytest.fixture(
        scope="function",
        params=[
            pytest.param("REP 4 IN X CBF 1 SELECT 4 FROM * AS X", id="regular policy"),
            pytest.param(EC_3_1_PLACEMENT_RULE, id="ec policy"),
        ],
    )
    def eacl_full_placement_container_with_object(self, request, wallets, file_path) -> tuple[str, str, str]:
        user_wallet = wallets.get_wallet()
        storage_nodes = self.neofs_env.storage_nodes
        node_count = len(storage_nodes)
        with allure.step("Create eACL public container with full placement rule"):
            cid = create_container(
                wallet=user_wallet.wallet_path,
                rule=request.param,
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

    @pytest.mark.parametrize("placement_rule", ["REP 4 IN X CBF 1 SELECT 4 FROM * AS X", EC_3_1_PLACEMENT_RULE])
    def test_eacl_can_not_be_set_with_final_bit(self, wallets, placement_rule):
        with allure.step("Create container with the final bit set"):
            user_wallet = wallets.get_wallet()

            cid = create_container(
                wallet=user_wallet.wallet_path,
                rule=placement_rule,
                basic_acl=PUBLIC_ACL_F,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Try to set eacl for such container"):
            eacl_deny = [EACLRule(access=EACLAccess.DENY, role=EACLRole.USER, operation=op) for op in EACLOperation]

            with pytest.raises(RuntimeError, match=EACL_CHANGE_PROHIBITED):
                set_eacl(
                    user_wallet.wallet_path,
                    cid,
                    create_eacl(cid, eacl_deny, shell=self.shell, wallet_config=user_wallet.config_path),
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                )

        with allure.step("eACL must be empty"):
            assert get_eacl(user_wallet.wallet_path, cid, self.shell, self.neofs_env.sn_rpc) is None

        with allure.step("Try to set eacl for such container with force"):
            eacl_deny = [EACLRule(access=EACLAccess.DENY, role=EACLRole.USER, operation=op) for op in EACLOperation]

            with pytest.raises(RuntimeError, match=rf"{EACL_CHANGE_TIMEOUT}|{EACL_CHANGE_RPC_ERROR}"):
                set_eacl(
                    user_wallet.wallet_path,
                    cid,
                    create_eacl(cid, eacl_deny, shell=self.shell, wallet_config=user_wallet.config_path),
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    force=True,
                )

        with allure.step("eACL must be empty"):
            assert get_eacl(user_wallet.wallet_path, cid, self.shell, self.neofs_env.sn_rpc) is None

    @pytest.mark.parametrize("address", [EACLRoleExtendedType.ADDRESS, None])
    @pytest.mark.parametrize("deny_role", [EACLRole.USER, EACLRole.OTHERS])
    def test_extended_acl_deny_all_operations(self, wallets, eacl_container_with_objects, deny_role, address):
        user_wallet = wallets.get_wallet()
        other_wallet = wallets.get_wallet(EACLRole.OTHERS)
        deny_role_wallet = other_wallet if deny_role == EACLRole.OTHERS else user_wallet
        not_deny_role_wallet = user_wallet if deny_role == EACLRole.OTHERS else other_wallet
        deny_role_str = "all others" if deny_role == EACLRole.OTHERS else "user"
        not_deny_role_str = "user" if deny_role == EACLRole.OTHERS else "all others"
        allure.dynamic.title(f"Testcase to deny NeoFS operations for {deny_role_str}.")
        cid, object_oids, file_path = eacl_container_with_objects

        with allure.step(f"Deny all operations for {deny_role_str} via eACL"):
            if address:
                deny_role = EACLRoleExtended(
                    address, address.get_value(deny_role_wallet.wallet_path, self.neofs_env.default_password)
                )
            eacl_deny = [EACLRule(access=EACLAccess.DENY, role=deny_role, operation=op) for op in EACLOperation]
            set_eacl(
                user_wallet.wallet_path,
                cid,
                create_eacl(cid, eacl_deny, shell=self.shell, wallet_config=user_wallet.config_path),
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            wait_for_cache_expired()

        with allure.step(f"Check only {not_deny_role_str} has full access to container"):
            with allure.step(f"Check {deny_role_str} has not access to any operations with container"):
                check_no_access_to_container(
                    deny_role_wallet.wallet_path,
                    cid,
                    object_oids[0],
                    file_path,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                )

            with allure.step(f"Check {not_deny_role_wallet} has full access to eACL public container"):
                check_full_access_to_container(
                    not_deny_role_wallet.wallet_path,
                    cid,
                    object_oids.pop(),
                    file_path,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                )

        with allure.step(f"Allow all operations for {deny_role_str} via eACL"):
            if address:
                deny_role = EACLRoleExtended(
                    address, address.get_value(deny_role_wallet.wallet_path, self.neofs_env.default_password)
                )
            eacl_deny = [EACLRule(access=EACLAccess.ALLOW, role=deny_role, operation=op) for op in EACLOperation]
            set_eacl(
                user_wallet.wallet_path,
                cid,
                create_eacl(cid, eacl_deny, shell=self.shell, wallet_config=user_wallet.config_path),
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

    @pytest.mark.parametrize("address", [EACLRoleExtendedType.ADDRESS, None])
    @pytest.mark.parametrize("operation", list(EACLOperation))
    def test_extended_acl_operations_enforcement(self, wallets, eacl_container_with_objects, operation, address):
        user_wallet = wallets.get_wallet()
        cid, object_oids, file_path = eacl_container_with_objects

        with allure.step(f"Deny all operations via eACL except {operation.value}"):
            role = EACLRole.USER
            if address:
                role = EACLRoleExtended(
                    address, address.get_value(user_wallet.wallet_path, self.neofs_env.default_password)
                )
            eacl = [
                EACLRule(access=EACLAccess.DENY, role=role, operation=op) for op in EACLOperation if op != operation
            ]
            eacl.append(EACLRule(access=EACLAccess.ALLOW, role=role, operation=operation))
            eacl_file = create_eacl(cid, eacl, shell=self.shell)
            set_eacl(
                user_wallet.wallet_path,
                cid,
                eacl_file,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            wait_for_cache_expired()

        with allure.step(f"Check {operation.value} is available"):
            if operation == EACLOperation.PUT:
                assert can_put_object(
                    user_wallet.wallet_path,
                    cid,
                    file_path,
                    self.shell,
                    neofs_env=self.neofs_env,
                ), f"{operation.value} is not allowed, while it should be"
            elif operation == EACLOperation.GET:
                assert can_get_object(
                    user_wallet.wallet_path,
                    cid,
                    object_oids.pop(),
                    file_path,
                    self.shell,
                    neofs_env=self.neofs_env,
                ), f"{operation.value} is not allowed, while it should be"
            elif operation == EACLOperation.HEAD:
                assert can_get_head_object(
                    user_wallet.wallet_path,
                    cid,
                    object_oids.pop(),
                    self.shell,
                    endpoint=random.choice(self.neofs_env.storage_nodes).endpoint,
                ), f"{operation.value} is not allowed, while it should be"
            elif operation == EACLOperation.GET_RANGE:
                assert can_get_range_of_object(
                    user_wallet.wallet_path,
                    cid,
                    object_oids.pop(),
                    self.shell,
                    endpoint=random.choice(self.neofs_env.storage_nodes).endpoint,
                ), f"{operation.value} is not allowed, while it should be"
            elif operation == EACLOperation.SEARCH:
                assert can_search_object(
                    user_wallet.wallet_path,
                    cid,
                    self.shell,
                    endpoint=random.choice(self.neofs_env.storage_nodes).endpoint,
                ), f"{operation.value} is not allowed, while it should be"
            elif operation == EACLOperation.DELETE:
                assert can_delete_object(
                    user_wallet.wallet_path,
                    cid,
                    object_oids.pop(),
                    self.shell,
                    endpoint=random.choice(self.neofs_env.storage_nodes).endpoint,
                ), f"{operation.value} is not allowed, while it should be"
        with allure.step("Check all other operations are not available"):
            not_allowed_operations = [op for op in EACLOperation if op != operation]
            for not_allowed_op in not_allowed_operations:
                if not_allowed_op == EACLOperation.PUT:
                    assert not can_put_object(
                        user_wallet.wallet_path,
                        cid,
                        file_path,
                        self.shell,
                        neofs_env=self.neofs_env,
                    ), f"{not_allowed_op.value} is allowed, while it shouldn't"
                elif not_allowed_op == EACLOperation.GET:
                    assert not can_get_object(
                        user_wallet.wallet_path,
                        cid,
                        object_oids.pop(),
                        file_path,
                        self.shell,
                        neofs_env=self.neofs_env,
                    ), f"{not_allowed_op.value} is allowed, while it shouldn't"
                elif not_allowed_op == EACLOperation.HEAD:
                    assert not can_get_head_object(
                        user_wallet.wallet_path,
                        cid,
                        object_oids.pop(),
                        self.shell,
                        endpoint=random.choice(self.neofs_env.storage_nodes).endpoint,
                    ), f"{not_allowed_op.value} is allowed, while it shouldn't"
                elif not_allowed_op == EACLOperation.GET_RANGE:
                    assert not can_get_range_of_object(
                        user_wallet.wallet_path,
                        cid,
                        object_oids.pop(),
                        self.shell,
                        endpoint=random.choice(self.neofs_env.storage_nodes).endpoint,
                    ), f"{not_allowed_op.value} is allowed, while it shouldn't"
                elif not_allowed_op == EACLOperation.SEARCH:
                    assert not can_search_object(
                        user_wallet.wallet_path,
                        cid,
                        self.shell,
                        endpoint=random.choice(self.neofs_env.storage_nodes).endpoint,
                    ), f"{not_allowed_op.value} is allowed, while it shouldn't"
                elif not_allowed_op == EACLOperation.DELETE:
                    assert not can_delete_object(
                        user_wallet.wallet_path,
                        cid,
                        object_oids.pop(),
                        self.shell,
                        endpoint=random.choice(self.neofs_env.storage_nodes).endpoint,
                    ), f"{not_allowed_op.value} is allowed, while it shouldn't"

    @allure.title("Testcase to validate NeoFS replication with eACL deny rules.")
    @pytest.mark.parametrize("address", [EACLRoleExtendedType.ADDRESS, None])
    def test_extended_acl_deny_replication(self, wallets, eacl_full_placement_container_with_object, address):
        user_wallet = wallets.get_wallet()
        other_wallet = wallets.get_wallet(EACLRole.OTHERS)
        cid, oid, file_path = eacl_full_placement_container_with_object
        storage_nodes = self.neofs_env.storage_nodes
        storage_node = self.neofs_env.storage_nodes[0]

        with allure.step("Deny all operations for user via eACL"):
            user_role = EACLRole.USER
            others_role = EACLRole.OTHERS

            if address:
                user_role = EACLRoleExtended(
                    address, address.get_value(user_wallet.wallet_path, self.neofs_env.default_password)
                )
                others_role = EACLRoleExtended(
                    address, address.get_value(other_wallet.wallet_path, self.neofs_env.default_password)
                )

            eacl_deny = [EACLRule(access=EACLAccess.DENY, role=user_role, operation=op) for op in EACLOperation]
            eacl_deny += [EACLRule(access=EACLAccess.DENY, role=others_role, operation=op) for op in EACLOperation]
            set_eacl(
                user_wallet.wallet_path,
                cid,
                create_eacl(cid, eacl_deny, shell=self.shell, wallet_config=user_wallet.config_path),
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            wait_for_cache_expired()

        with allure.step("Drop object to check replication"):
            drop_object(storage_node, cid=cid, oid=oid)

        with allure.step("Wait for dropped object replicated"):
            wait_object_replication(cid, oid, len(storage_nodes), self.shell, storage_nodes, self.neofs_env)

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
                            EACLRule(access=EACLAccess.DENY, role=EACLRole.SYSTEM, operation=op) for op in EACLOperation
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

    @allure.title("Test case for verifying the impossible to change system extended ACL if eACL already set")
    @pytest.mark.parametrize("address", [EACLRoleExtendedType.ADDRESS, None])
    def test_deprecated_change_system_eacl_if_eacl_already_set(self, wallets, eacl_container_with_objects, address):
        user_wallet = wallets.get_wallet()
        cid, object_oids, file_path = eacl_container_with_objects
        endpoint = self.neofs_env.sn_rpc

        with allure.step("Set eACL"):
            user_role = EACLRole.USER
            if address:
                user_role = EACLRoleExtended(
                    address, address.get_value(user_wallet.wallet_path, self.neofs_env.default_password)
                )
            set_eacl(
                user_wallet.wallet_path,
                cid,
                create_eacl(
                    cid=cid,
                    rules_list=[
                        EACLRule(access=EACLAccess.ALLOW, role=user_role, operation=op) for op in EACLOperation
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
                            EACLRule(access=EACLAccess.DENY, role=EACLRole.SYSTEM, operation=op) for op in EACLOperation
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

    @allure.title("Not owner and not trusted party can NOT set eacl")
    @pytest.mark.parametrize("address", [EACLRoleExtendedType.ADDRESS, None])
    def test_only_owner_can_set_eacl(
        self, wallets, eacl_full_placement_container_with_object: tuple[str, str, str], not_owner_wallet: str, address
    ):
        not_owner_wallet_config_path = self.neofs_env._generate_temp_file(self.neofs_env._env_dir, extension="yml")
        NeoFSEnv.generate_config_file(
            config_template="cli_cfg.yaml",
            config_path=not_owner_wallet_config_path,
            wallet=not_owner_wallet,
        )

        cid, oid, file_path = eacl_full_placement_container_with_object

        user_role = EACLRole.USER
        if address:
            user_role = EACLRoleExtended(address, address.get_value(not_owner_wallet.path, not_owner_wallet.password))
        eacl = [EACLRule(access=EACLAccess.DENY, role=user_role, operation=op) for op in EACLOperation]

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


class TestEACLOnContainerCreation(TestNeofsBase):
    @pytest.fixture(autouse=True)
    def _skip_old_node(self):
        if parse_version(self.neofs_env.get_binary_version(self.neofs_env.neofs_node_path)) <= parse_version("0.53.0"):
            pytest.skip("setting eACL on container creation is not supported before neofs-node 0.53.0")

    @allure.title("eACL denying everyone is applied right on container creation ({placement_rule})")
    @pytest.mark.parametrize(
        "placement_rule",
        [
            pytest.param("REP 4 IN X CBF 1 SELECT 4 FROM * AS X", id="regular policy"),
            pytest.param(EC_3_1_PLACEMENT_RULE, id="ec policy"),
        ],
    )
    def test_set_eacl_on_container_creation(self, wallets, file_path, placement_rule):
        user_wallet = wallets.get_wallet()
        other_wallet = wallets.get_wallet(EACLRole.OTHERS)

        with allure.step("Build eACL table denying all operations for OTHERS"):
            eacl_deny = [EACLRule(access=EACLAccess.DENY, role=EACLRole.OTHERS, operation=op) for op in EACLOperation]
            eacl_table = create_eacl(
                cid="", rules_list=eacl_deny, shell=self.shell, wallet_config=user_wallet.config_path
            )

        with allure.step("Create public container with the eACL table within the same create call"):
            cid = create_container(
                wallet=user_wallet.wallet_path,
                rule=placement_rule,
                basic_acl=PUBLIC_ACL,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                eacl=eacl_table,
            )

        with allure.step("eACL must be set already, without an extra set-eacl call"):
            assert get_eacl(user_wallet.wallet_path, cid, self.shell, self.neofs_env.sn_rpc) is not None, (
                "eACL was not set during container creation"
            )

        with allure.step("Owner puts a test object into the container"):
            oid = put_object_to_random_node(
                user_wallet.wallet_path, file_path, cid, shell=self.shell, neofs_env=self.neofs_env
            )

        with allure.step("OTHERS has no access to the container"):
            check_no_access_to_container(
                other_wallet.wallet_path,
                cid,
                oid,
                file_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step("Owner still has full access to the container"):
            check_full_access_to_container(
                user_wallet.wallet_path,
                cid,
                oid,
                file_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

    @allure.title("Single simple eACL set on container creation enforces only the denied operation")
    @pytest.mark.parametrize("denied_operation", [EACLOperation.PUT, EACLOperation.DELETE])
    def test_set_simple_eacl_on_container_creation(self, wallets, file_path, denied_operation):
        user_wallet = wallets.get_wallet()
        other_wallet = wallets.get_wallet(EACLRole.OTHERS)

        with allure.step(f"Build eACL table denying only {denied_operation.value} for OTHERS"):
            eacl_deny = [EACLRule(access=EACLAccess.DENY, role=EACLRole.OTHERS, operation=denied_operation)]
            eacl_table = create_eacl(
                cid="", rules_list=eacl_deny, shell=self.shell, wallet_config=user_wallet.config_path
            )

        with allure.step("Create public container with the eACL table within the same create call"):
            cid = create_container(
                wallet=user_wallet.wallet_path,
                basic_acl=PUBLIC_ACL,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                eacl=eacl_table,
            )

        with allure.step("eACL must be set already, without an extra set-eacl call"):
            assert get_eacl(user_wallet.wallet_path, cid, self.shell, self.neofs_env.sn_rpc) is not None, (
                "eACL was not set during container creation"
            )

        with allure.step("Owner puts a test object into the container"):
            oid = put_object_to_random_node(
                user_wallet.wallet_path, file_path, cid, shell=self.shell, neofs_env=self.neofs_env
            )

        with allure.step(f"OTHERS can do everything but {denied_operation.value}"):
            check_custom_access_to_container(
                other_wallet.wallet_path,
                cid,
                oid,
                file_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
                deny_operations=[denied_operation],
            )

    @allure.title("Filter-based eACL set on creation is enforced per object header")
    def test_set_eacl_with_object_filter_on_container_creation(self, wallets, file_path):
        user_wallet = wallets.get_wallet()
        other_wallet = wallets.get_wallet(EACLRole.OTHERS)
        attribute = {"check_key": "check_value"}

        with allure.step("Build eACL denying GET/HEAD for OTHERS only for objects with a given attribute"):
            obj_filter = EACLFilter(key="check_key", value="check_value", header_type=EACLHeaderType.OBJECT)
            eacl_deny = [
                EACLRule(
                    access=EACLAccess.DENY,
                    role=EACLRole.OTHERS,
                    filters=EACLFilters([obj_filter]),
                    operation=op,
                )
                for op in (EACLOperation.GET, EACLOperation.HEAD)
            ]
            eacl_table = create_eacl(
                cid="", rules_list=eacl_deny, shell=self.shell, wallet_config=user_wallet.config_path
            )

        with allure.step("Create public container with the filter-based eACL within the same create call"):
            cid = create_container(
                wallet=user_wallet.wallet_path,
                basic_acl=PUBLIC_ACL,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                eacl=eacl_table,
            )

        with allure.step("Owner puts one object with the filtered attribute and one without"):
            oid_with_attr = put_object_to_random_node(
                user_wallet.wallet_path,
                file_path,
                cid,
                attributes=attribute,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )
            oid_without_attr = put_object_to_random_node(
                user_wallet.wallet_path, file_path, cid, shell=self.shell, neofs_env=self.neofs_env
            )

        with allure.step("OTHERS can GET/HEAD the object without the filtered attribute"):
            assert can_get_object(
                other_wallet.wallet_path, cid, oid_without_attr, file_path, self.shell, neofs_env=self.neofs_env
            ), "GET must be allowed for an object that does not match the filter"
            assert can_get_head_object(
                other_wallet.wallet_path, cid, oid_without_attr, self.shell, endpoint=self.neofs_env.sn_rpc
            ), "HEAD must be allowed for an object that does not match the filter"

        with allure.step("OTHERS cannot GET/HEAD the object carrying the filtered attribute"):
            assert not can_get_object(
                other_wallet.wallet_path, cid, oid_with_attr, file_path, self.shell, neofs_env=self.neofs_env
            ), "GET must be denied for an object that matches the filter"
            assert not can_get_head_object(
                other_wallet.wallet_path, cid, oid_with_attr, self.shell, endpoint=self.neofs_env.sn_rpc
            ), "HEAD must be denied for an object that matches the filter"

    @allure.title("Container is not created when its eACL can not be applied: final bit set")
    def test_eacl_on_container_creation_rejected_with_final_bit(self, wallets):
        user_wallet = wallets.get_wallet()

        with allure.step("Build a valid eACL table"):
            eacl_deny = [EACLRule(access=EACLAccess.DENY, role=EACLRole.OTHERS, operation=op) for op in EACLOperation]
            eacl_table = create_eacl(
                cid="", rules_list=eacl_deny, shell=self.shell, wallet_config=user_wallet.config_path
            )

        with allure.step("Creating a final-bit container with an eACL must be rejected by the Inner Ring"):
            with pytest.raises(
                RuntimeError, match="'eacl' flag is not empty, but container does not allow extended ACL"
            ):
                create_container(
                    wallet=user_wallet.wallet_path,
                    basic_acl=PUBLIC_ACL_F,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    eacl=eacl_table,
                )

    @allure.title("Container is not created when eACL table has a different container ID set")
    def test_eacl_with_foreign_cid_rejected_on_container_creation(self, wallets):
        user_wallet = wallets.get_wallet()

        with allure.step("Create a container whose CID will be baked into the eACL table"):
            foreign_cid = create_container(
                wallet=user_wallet.wallet_path,
                basic_acl=PUBLIC_ACL,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Build an eACL table that explicitly targets the foreign container ID"):
            eacl_deny = [EACLRule(access=EACLAccess.DENY, role=EACLRole.OTHERS, operation=op) for op in EACLOperation]
            eacl_table = create_eacl(
                cid=foreign_cid, rules_list=eacl_deny, shell=self.shell, wallet_config=user_wallet.config_path
            )

        with allure.step("Creating a new container with this eACL table must be rejected"):
            with pytest.raises(RuntimeError, match="container ID set, but calculated container ID is"):
                create_container(
                    wallet=user_wallet.wallet_path,
                    basic_acl=PUBLIC_ACL,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    eacl=eacl_table,
                )

    @allure.title("The 'force' flag overrides a different container ID set in the eACL table on creation")
    def test_eacl_with_foreign_cid_overridden_by_force_on_container_creation(self, wallets, file_path):
        user_wallet = wallets.get_wallet()
        other_wallet = wallets.get_wallet(EACLRole.OTHERS)

        with allure.step("Create a container whose CID will be baked into the eACL table"):
            foreign_cid = create_container(
                wallet=user_wallet.wallet_path,
                basic_acl=PUBLIC_ACL,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Build an eACL table that explicitly targets the foreign container ID"):
            eacl_deny = [EACLRule(access=EACLAccess.DENY, role=EACLRole.OTHERS, operation=op) for op in EACLOperation]
            eacl_table = create_eacl(
                cid=foreign_cid, rules_list=eacl_deny, shell=self.shell, wallet_config=user_wallet.config_path
            )

        with allure.step("Creating a new container with '--force' overrides the table's container ID"):
            cid = create_container(
                wallet=user_wallet.wallet_path,
                basic_acl=PUBLIC_ACL,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                eacl=eacl_table,
                force=True,
            )
            assert cid != foreign_cid, "a brand new container ID is expected"

        with allure.step("eACL must be set on the new container, not on the foreign one"):
            assert get_eacl(user_wallet.wallet_path, cid, self.shell, self.neofs_env.sn_rpc) is not None, (
                "eACL was not set during container creation"
            )

        with allure.step("Owner puts a test object into the new container"):
            oid = put_object_to_random_node(
                user_wallet.wallet_path, file_path, cid, shell=self.shell, neofs_env=self.neofs_env
            )

        with allure.step("OTHERS has no access to the new container, confirming the overridden eACL is enforced"):
            check_no_access_to_container(
                other_wallet.wallet_path,
                cid,
                oid,
                file_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

    @allure.title("eACL set on container creation can still be replaced in the usual manner")
    def test_eacl_set_on_creation_can_be_replaced(self, wallets, file_path):
        user_wallet = wallets.get_wallet()
        other_wallet = wallets.get_wallet(EACLRole.OTHERS)

        with allure.step("Build eACL table denying all operations for OTHERS"):
            eacl_deny = [EACLRule(access=EACLAccess.DENY, role=EACLRole.OTHERS, operation=op) for op in EACLOperation]
            eacl_table = create_eacl(
                cid="", rules_list=eacl_deny, shell=self.shell, wallet_config=user_wallet.config_path
            )

        with allure.step("Create public container with the deny-all eACL within the same create call"):
            cid = create_container(
                wallet=user_wallet.wallet_path,
                basic_acl=PUBLIC_ACL,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                eacl=eacl_table,
            )

        with allure.step("Owner puts a test object into the container"):
            oid = put_object_to_random_node(
                user_wallet.wallet_path, file_path, cid, shell=self.shell, neofs_env=self.neofs_env
            )

        with allure.step("OTHERS has no access while the creation-time eACL is in force"):
            check_no_access_to_container(
                other_wallet.wallet_path,
                cid,
                oid,
                file_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step("Replace the eACL via the usual set-eacl call, allowing everything for OTHERS"):
            eacl_allow = [EACLRule(access=EACLAccess.ALLOW, role=EACLRole.OTHERS, operation=op) for op in EACLOperation]
            set_eacl(
                user_wallet.wallet_path,
                cid,
                create_eacl(cid, eacl_allow, shell=self.shell, wallet_config=user_wallet.config_path),
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            wait_for_cache_expired()

        with allure.step("OTHERS now has full access, confirming the eACL was replaced"):
            check_full_access_to_container(
                other_wallet.wallet_path,
                cid,
                oid,
                file_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )
