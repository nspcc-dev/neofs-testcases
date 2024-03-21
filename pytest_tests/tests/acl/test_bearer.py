import os
import random
import uuid
from collections import namedtuple

import allure
import neofs_env.neofs_epoch as neofs_epoch
import pytest
from helpers.acl import (
    EACLAccess,
    EACLOperation,
    EACLRole,
    EACLRule,
    create_bearer_token,
    create_eacl,
    form_bearertoken_file,
    set_eacl,
    sign_bearer,
    wait_for_cache_expired,
)
from helpers.common import ASSETS_DIR, TEST_FILES_DIR
from helpers.container import create_container
from helpers.container_access import (
    check_custom_access_to_container,
    check_full_access_to_container,
    check_no_access_to_container,
)
from helpers.neofs_verbs import put_object_to_random_node
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
from neofs_testlib.shell import Shell
from neofs_testlib.utils.wallet import get_last_address_from_wallet

ContainerTuple = namedtuple("ContainerTuple", ["cid", "objects_oids"])


@pytest.mark.acl
@pytest.mark.acl_bearer
class TestACLBearer(NeofsEnvTestBase):
    @pytest.mark.parametrize("role", [EACLRole.USER, EACLRole.OTHERS])
    def test_bearer_token_operations(self, wallets, eacl_container_with_objects, role):
        allure.dynamic.title(f"Testcase to validate NeoFS operations with {role.value} BearerToken")
        cid, objects_oids, file_path = eacl_container_with_objects
        user_wallet = wallets.get_wallet()
        deny_wallet = wallets.get_wallet(role)
        endpoint = self.neofs_env.sn_rpc

        with allure.step(f"Check {role.value} has full access to container without bearer token"):
            check_full_access_to_container(
                wallet=deny_wallet.wallet_path,
                cid=cid,
                oid=objects_oids.pop(),
                file_name=file_path,
                wallet_config=deny_wallet.config_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step(f"Set deny all operations for {role.value} via eACL"):
            eacl = [
                EACLRule(access=EACLAccess.DENY, role=role, operation=op) for op in EACLOperation
            ]
            eacl_file = create_eacl(cid, eacl, shell=self.shell)
            set_eacl(user_wallet.wallet_path, cid, eacl_file, shell=self.shell, endpoint=endpoint)
            wait_for_cache_expired()

        with allure.step(f"Create bearer token for {role.value} with all operations allowed"):
            bearer = form_bearertoken_file(
                user_wallet.wallet_path,
                cid,
                [
                    EACLRule(operation=op, access=EACLAccess.ALLOW, role=role)
                    for op in EACLOperation
                ],
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step(
            f"Check {role.value} without token has no access to all operations with container"
        ):
            check_no_access_to_container(
                deny_wallet.wallet_path,
                cid,
                objects_oids.pop(),
                file_path,
                wallet_config=deny_wallet.config_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step(
            f"Check {role.value} with token has access to all operations with container"
        ):
            check_full_access_to_container(
                deny_wallet.wallet_path,
                cid,
                objects_oids.pop(),
                file_path,
                bearer=bearer,
                wallet_config=deny_wallet.config_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step(f"Set allow all operations for {role.value} via eACL"):
            eacl = [
                EACLRule(access=EACLAccess.ALLOW, role=role, operation=op) for op in EACLOperation
            ]
            eacl_file = create_eacl(cid, eacl, shell=self.shell)
            set_eacl(user_wallet.wallet_path, cid, eacl_file, shell=self.shell, endpoint=endpoint)
            wait_for_cache_expired()

        with allure.step(
            f"Check {role.value} without token has access to all operations with container"
        ):
            check_full_access_to_container(
                deny_wallet.wallet_path,
                cid,
                objects_oids.pop(),
                file_path,
                wallet_config=deny_wallet.config_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

    @allure.title("BearerToken Operations for compound Operations")
    def test_bearer_token_compound_operations(self, wallets, eacl_container_with_objects):
        endpoint = self.neofs_env.sn_rpc
        cid, objects_oids, file_path = eacl_container_with_objects
        user_wallet = wallets.get_wallet()
        other_wallet = wallets.get_wallet(role=EACLRole.OTHERS)

        # Operations that we will deny for each role via eACL
        deny_map = {
            EACLRole.USER: [EACLOperation.DELETE],
            EACLRole.OTHERS: [
                EACLOperation.SEARCH,
                EACLOperation.GET_RANGE_HASH,
                EACLOperation.GET_RANGE,
            ],
        }

        # Operations that we will allow for each role with bearer token
        bearer_map = {
            EACLRole.USER: [
                EACLOperation.DELETE,
                EACLOperation.PUT,
                EACLOperation.GET_RANGE,
            ],
            EACLRole.OTHERS: [
                EACLOperation.GET,
                EACLOperation.GET_RANGE,
                EACLOperation.GET_RANGE_HASH,
            ],
        }

        deny_map_with_bearer = {
            EACLRole.USER: [
                op for op in deny_map[EACLRole.USER] if op not in bearer_map[EACLRole.USER]
            ],
            EACLRole.OTHERS: [
                op for op in deny_map[EACLRole.OTHERS] if op not in bearer_map[EACLRole.OTHERS]
            ],
        }

        eacl_deny = []
        for role, operations in deny_map.items():
            eacl_deny += [
                EACLRule(access=EACLAccess.DENY, role=role, operation=op) for op in operations
            ]
        set_eacl(
            user_wallet.wallet_path,
            cid,
            eacl_table_path=create_eacl(cid, eacl_deny, shell=self.shell),
            shell=self.shell,
            endpoint=endpoint,
        )
        wait_for_cache_expired()

        with allure.step("Check rule consistency without bearer"):
            check_custom_access_to_container(
                user_wallet.wallet_path,
                cid,
                objects_oids.pop(),
                file_path,
                deny_operations=deny_map[EACLRole.USER],
                wallet_config=user_wallet.config_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )
            check_custom_access_to_container(
                other_wallet.wallet_path,
                cid,
                objects_oids.pop(),
                file_path,
                deny_operations=deny_map[EACLRole.OTHERS],
                wallet_config=other_wallet.config_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step("Check rule consistency using bearer token"):
            bearer_user = form_bearertoken_file(
                user_wallet.wallet_path,
                cid,
                [
                    EACLRule(operation=op, access=EACLAccess.ALLOW, role=EACLRole.USER)
                    for op in bearer_map[EACLRole.USER]
                ],
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

            bearer_other = form_bearertoken_file(
                user_wallet.wallet_path,
                cid,
                [
                    EACLRule(operation=op, access=EACLAccess.ALLOW, role=EACLRole.OTHERS)
                    for op in bearer_map[EACLRole.OTHERS]
                ],
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

            check_custom_access_to_container(
                user_wallet.wallet_path,
                cid,
                objects_oids.pop(),
                file_path,
                deny_operations=deny_map_with_bearer[EACLRole.USER],
                bearer=bearer_user,
                wallet_config=user_wallet.config_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )
            check_custom_access_to_container(
                other_wallet.wallet_path,
                cid,
                objects_oids.pop(),
                file_path,
                deny_operations=deny_map_with_bearer[EACLRole.OTHERS],
                bearer=bearer_other,
                wallet_config=other_wallet.config_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

    @pytest.mark.parametrize("expiration_flag", ["lifetime", "expire_at"])
    def test_bearer_token_expiration(self, wallets, eacl_container_with_objects, expiration_flag):
        self.tick_epochs_and_wait(1)
        current_epoch = neofs_epoch.get_epoch(self.neofs_env)
        cid, objects_oids, file_path = eacl_container_with_objects
        user_wallet = wallets.get_wallet()

        with allure.step("Create and sign bearer token via cli"):
            eacl = [
                EACLRule(access=EACLAccess.ALLOW, role=EACLRole.USER, operation=op)
                for op in EACLOperation
            ]

            path_to_bearer = os.path.join(
                os.getcwd(), ASSETS_DIR, TEST_FILES_DIR, f"bearer_token_{str(uuid.uuid4())}"
            )

            create_bearer_token(
                self.shell,
                issued_at=1,
                not_valid_before=1,
                owner=get_last_address_from_wallet(user_wallet.wallet_path, "password"),
                out=path_to_bearer,
                rpc_endpoint=self.neofs_env.sn_rpc,
                eacl=create_eacl(cid, eacl, shell=self.shell),
                lifetime=1 if expiration_flag == "lifetime" else None,
                expire_at=current_epoch + 1 if expiration_flag == "expire_at" else None,
            )

            sign_bearer(
                shell=self.shell,
                wallet_path=user_wallet.wallet_path,
                eacl_rules_file_from=path_to_bearer,
                eacl_rules_file_to=path_to_bearer,
                json=True,
            )

        self.tick_epochs_and_wait(1)

        with allure.step(
            f"Check {EACLRole.USER.value} with token has access to all operations with container"
        ):
            check_full_access_to_container(
                user_wallet.wallet_path,
                cid,
                objects_oids.pop(),
                file_path,
                bearer=path_to_bearer,
                wallet_config=user_wallet.config_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        self.tick_epochs_and_wait(1)

        with allure.step(
            f"Check {EACLRole.USER.value} has no access to all operations with container"
        ):
            check_no_access_to_container(
                user_wallet.wallet_path,
                cid,
                objects_oids.pop(),
                file_path,
                bearer=path_to_bearer,
                wallet_config=user_wallet.config_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

    @allure.title("Check bearer token with ContainerID specified")
    def test_bearer_token_with_container_id(
        self, wallets, client_shell: Shell, neofs_env: NeoFSEnv, file_path: str
    ):
        user_wallet = wallets.get_wallet()
        container1, container2 = self._create_containers_with_objects(
            containers_count=2,
            objects_count=1,
            user_wallet=user_wallet,
            client_shell=client_shell,
            neofs_env=neofs_env,
            file_path=file_path,
        )

        with allure.step(
            f"Create bearer token with all operations allowed for cid: {container1.cid}"
        ):
            bearer = form_bearertoken_file(
                user_wallet.wallet_path,
                container1.cid,
                [
                    EACLRule(operation=op, access=EACLAccess.ALLOW, role=EACLRole.USER)
                    for op in EACLOperation
                ],
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step(
            f"Check {EACLRole.USER.value} with token has access to all operations with container {container1.cid}"
        ):
            check_full_access_to_container(
                user_wallet.wallet_path,
                container1.cid,
                container1.objects_oids.pop(),
                file_path,
                bearer=bearer,
                wallet_config=user_wallet.config_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step(
            f"Check {EACLRole.USER.value} has no access to all operations with another container {container2.cid}"
        ):
            check_no_access_to_container(
                user_wallet.wallet_path,
                container2.cid,
                container2.objects_oids.pop(),
                file_path,
                bearer=bearer,
                wallet_config=user_wallet.config_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

    @allure.title("Check bearer token without ContainerID specified")
    def test_bearer_token_without_container_id(
        self, wallets, client_shell: Shell, neofs_env: NeoFSEnv, file_path: str
    ):
        user_wallet = wallets.get_wallet()
        container1, container2 = self._create_containers_with_objects(
            containers_count=2,
            objects_count=1,
            user_wallet=user_wallet,
            client_shell=client_shell,
            neofs_env=neofs_env,
            file_path=file_path,
        )

        with allure.step(f"Create bearer token with all operations allowed for all containers"):
            bearer = form_bearertoken_file(
                user_wallet.wallet_path,
                None,
                [
                    EACLRule(operation=op, access=EACLAccess.ALLOW, role=EACLRole.USER)
                    for op in EACLOperation
                ],
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step(
            f"Check {EACLRole.USER.value} with token has access to all operations with container {container1.cid}"
        ):
            check_full_access_to_container(
                user_wallet.wallet_path,
                container1.cid,
                container1.objects_oids.pop(),
                file_path,
                bearer=bearer,
                wallet_config=user_wallet.config_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step(
            f"Check {EACLRole.USER.value} with token has access to all operations with container {container2.cid}"
        ):
            check_full_access_to_container(
                user_wallet.wallet_path,
                container2.cid,
                container2.objects_oids.pop(),
                file_path,
                bearer=bearer,
                wallet_config=user_wallet.config_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

    @pytest.mark.parametrize("operation", list(EACLOperation))
    def test_bearer_token_separate_operations(
        self, wallets, eacl_container_with_objects, operation
    ):
        role = EACLRole.USER
        not_allowed_operations = [op for op in EACLOperation if op != operation]

        allure.dynamic.title(
            f"Testcase to validate NeoFS {operation.value} with {role.value} BearerToken"
        )
        cid, objects_oids, file_path = eacl_container_with_objects
        user_wallet = wallets.get_wallet()
        deny_wallet = wallets.get_wallet(role)
        endpoint = self.neofs_env.sn_rpc

        with allure.step(f"Set deny all operations for {role.value} via eACL"):
            eacl = [
                EACLRule(access=EACLAccess.DENY, role=role, operation=op) for op in EACLOperation
            ]
            eacl_file = create_eacl(cid, eacl, shell=self.shell)
            set_eacl(user_wallet.wallet_path, cid, eacl_file, shell=self.shell, endpoint=endpoint)
            wait_for_cache_expired()

        with allure.step(f"Create bearer token for {role.value} with {operation.value} allowed"):
            bearer = form_bearertoken_file(
                user_wallet.wallet_path,
                cid,
                [EACLRule(operation=operation, access=EACLAccess.ALLOW, role=role)],
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step(
            f"Check {role.value} with token has access to {operation.value} within container"
        ):
            if operation == EACLOperation.PUT:
                assert can_put_object(
                    deny_wallet.wallet_path,
                    cid,
                    file_path,
                    self.shell,
                    neofs_env=self.neofs_env,
                    bearer=bearer,
                    wallet_config=deny_wallet.config_path,
                ), f"{operation.value} is not allowed, while it should be"
            elif operation == EACLOperation.GET:
                assert can_get_object(
                    deny_wallet.wallet_path,
                    cid,
                    objects_oids.pop(),
                    file_path,
                    self.shell,
                    neofs_env=self.neofs_env,
                    bearer=bearer,
                    wallet_config=deny_wallet.config_path,
                ), f"{operation.value} is not allowed, while it should be"
            elif operation == EACLOperation.HEAD:
                assert can_get_head_object(
                    deny_wallet.wallet_path,
                    cid,
                    objects_oids.pop(),
                    self.shell,
                    endpoint=random.choice(self.neofs_env.storage_nodes).endpoint,
                    bearer=bearer,
                    wallet_config=deny_wallet.config_path,
                ), f"{operation.value} is not allowed, while it should be"
            elif operation == EACLOperation.GET_RANGE:
                assert can_get_range_of_object(
                    deny_wallet.wallet_path,
                    cid,
                    objects_oids.pop(),
                    self.shell,
                    endpoint=random.choice(self.neofs_env.storage_nodes).endpoint,
                    bearer=bearer,
                    wallet_config=deny_wallet.config_path,
                ), f"{operation.value} is not allowed, while it should be"
            elif operation == EACLOperation.GET_RANGE_HASH:
                assert can_get_range_hash_of_object(
                    deny_wallet.wallet_path,
                    cid,
                    objects_oids.pop(),
                    self.shell,
                    endpoint=random.choice(self.neofs_env.storage_nodes).endpoint,
                    bearer=bearer,
                    wallet_config=deny_wallet.config_path,
                ), f"{operation.value} is not allowed, while it should be"
            elif operation == EACLOperation.SEARCH:
                assert can_search_object(
                    deny_wallet.wallet_path,
                    cid,
                    self.shell,
                    endpoint=random.choice(self.neofs_env.storage_nodes).endpoint,
                    bearer=bearer,
                    wallet_config=deny_wallet.config_path,
                ), f"{operation.value} is not allowed, while it should be"
            elif operation == EACLOperation.DELETE:
                assert can_delete_object(
                    deny_wallet.wallet_path,
                    cid,
                    objects_oids.pop(),
                    self.shell,
                    endpoint=random.choice(self.neofs_env.storage_nodes).endpoint,
                    bearer=bearer,
                    wallet_config=deny_wallet.config_path,
                ), f"{operation.value} is not allowed, while it should be"

        with allure.step(
            f"Check {role.value} with token has no access to all other operations within container"
        ):
            for not_allowed_op in not_allowed_operations:
                if not_allowed_op == EACLOperation.PUT:
                    assert not can_put_object(
                        deny_wallet.wallet_path,
                        cid,
                        file_path,
                        self.shell,
                        neofs_env=self.neofs_env,
                        bearer=bearer,
                        wallet_config=deny_wallet.config_path,
                    ), f"{not_allowed_op.value} is allowed, while it shouldn't"
                elif not_allowed_op == EACLOperation.GET:
                    assert not can_get_object(
                        deny_wallet.wallet_path,
                        cid,
                        objects_oids.pop(),
                        file_path,
                        self.shell,
                        neofs_env=self.neofs_env,
                        bearer=bearer,
                        wallet_config=deny_wallet.config_path,
                    ), f"{not_allowed_op.value} is allowed, while it shouldn't"
                elif not_allowed_op == EACLOperation.HEAD:
                    assert not can_get_head_object(
                        deny_wallet.wallet_path,
                        cid,
                        objects_oids.pop(),
                        self.shell,
                        endpoint=random.choice(self.neofs_env.storage_nodes).endpoint,
                        bearer=bearer,
                        wallet_config=deny_wallet.config_path,
                    ), f"{not_allowed_op.value} is allowed, while it shouldn't"
                elif not_allowed_op == EACLOperation.GET_RANGE:
                    assert not can_get_range_of_object(
                        deny_wallet.wallet_path,
                        cid,
                        objects_oids.pop(),
                        self.shell,
                        endpoint=random.choice(self.neofs_env.storage_nodes).endpoint,
                        bearer=bearer,
                        wallet_config=deny_wallet.config_path,
                    ), f"{not_allowed_op.value} is allowed, while it shouldn't"
                elif not_allowed_op == EACLOperation.GET_RANGE_HASH:
                    assert not can_get_range_hash_of_object(
                        deny_wallet.wallet_path,
                        cid,
                        objects_oids.pop(),
                        self.shell,
                        endpoint=random.choice(self.neofs_env.storage_nodes).endpoint,
                        bearer=bearer,
                        wallet_config=deny_wallet.config_path,
                    ), f"{not_allowed_op.value} is allowed, while it shouldn't"
                elif not_allowed_op == EACLOperation.SEARCH:
                    assert not can_search_object(
                        deny_wallet.wallet_path,
                        cid,
                        self.shell,
                        endpoint=random.choice(self.neofs_env.storage_nodes).endpoint,
                        bearer=bearer,
                        wallet_config=deny_wallet.config_path,
                    ), f"{not_allowed_op.value} is allowed, while it shouldn't"
                elif not_allowed_op == EACLOperation.DELETE:
                    assert not can_delete_object(
                        deny_wallet.wallet_path,
                        cid,
                        objects_oids.pop(),
                        self.shell,
                        endpoint=random.choice(self.neofs_env.storage_nodes).endpoint,
                        bearer=bearer,
                        wallet_config=deny_wallet.config_path,
                    ), f"{not_allowed_op.value} is allowed, while it shouldn't"

    def _create_containers_with_objects(
        self,
        containers_count: int,
        objects_count: int,
        user_wallet,
        client_shell: Shell,
        neofs_env: NeoFSEnv,
        file_path: str,
    ) -> list[ContainerTuple]:
        result = []
        for _ in range(containers_count):
            with allure.step("Create eACL public container"):
                cid = create_container(
                    user_wallet.wallet_path,
                    basic_acl=PUBLIC_ACL,
                    shell=client_shell,
                    endpoint=neofs_env.sn_rpc,
                )

            with allure.step("Add test objects to container"):
                objects_oids = [
                    put_object_to_random_node(
                        user_wallet.wallet_path,
                        file_path,
                        cid,
                        attributes={"key1": "val1", "key": val, "key2": "abc"},
                        shell=client_shell,
                        neofs_env=neofs_env,
                    )
                    for val in range(objects_count)
                ]
            result.append(ContainerTuple(cid=cid, objects_oids=objects_oids))
        return result
