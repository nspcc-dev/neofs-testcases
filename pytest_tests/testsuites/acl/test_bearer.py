import os
import uuid
from collections import namedtuple

import allure
import pytest
from cluster import Cluster
from cluster_test_base import ClusterTestBase
from common import ASSETS_DIR, TEST_FILES_DIR, WALLET_PASS
from epoch import get_epoch
from neofs_testlib.shell import Shell
from neofs_testlib.utils.wallet import get_last_address_from_wallet
from python_keywords.acl import (
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
from python_keywords.container import create_container
from python_keywords.container_access import (
    check_custom_access_to_container,
    check_full_access_to_container,
    check_no_access_to_container,
)
from python_keywords.neofs_verbs import put_object_to_random_node
from wellknown_acl import PUBLIC_ACL

ContainerTuple = namedtuple("ContainerTuple", ["cid", "objects_oids"])


@pytest.mark.acl
@pytest.mark.acl_bearer
class TestACLBearer(ClusterTestBase):
    @pytest.mark.parametrize("role", [EACLRole.USER, EACLRole.OTHERS])
    def test_bearer_token_operations(self, wallets, eacl_container_with_objects, role):
        allure.dynamic.title(f"Testcase to validate NeoFS operations with {role.value} BearerToken")
        cid, objects_oids, file_path = eacl_container_with_objects
        user_wallet = wallets.get_wallet()
        deny_wallet = wallets.get_wallet(role)
        endpoint = self.cluster.default_rpc_endpoint

        with allure.step(f"Check {role.value} has full access to container without bearer token"):
            check_full_access_to_container(
                deny_wallet.wallet_path,
                cid,
                objects_oids.pop(),
                file_path,
                wallet_config=deny_wallet.config_path,
                shell=self.shell,
                cluster=self.cluster,
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
                endpoint=self.cluster.default_rpc_endpoint,
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
                cluster=self.cluster,
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
                cluster=self.cluster,
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
                cluster=self.cluster,
            )

    @allure.title("BearerToken Operations for compound Operations")
    def test_bearer_token_compound_operations(self, wallets, eacl_container_with_objects):
        endpoint = self.cluster.default_rpc_endpoint
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
                cluster=self.cluster,
            )
            check_custom_access_to_container(
                other_wallet.wallet_path,
                cid,
                objects_oids.pop(),
                file_path,
                deny_operations=deny_map[EACLRole.OTHERS],
                wallet_config=other_wallet.config_path,
                shell=self.shell,
                cluster=self.cluster,
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
                endpoint=self.cluster.default_rpc_endpoint,
            )

            bearer_other = form_bearertoken_file(
                user_wallet.wallet_path,
                cid,
                [
                    EACLRule(operation=op, access=EACLAccess.ALLOW, role=EACLRole.OTHERS)
                    for op in bearer_map[EACLRole.OTHERS]
                ],
                shell=self.shell,
                endpoint=self.cluster.default_rpc_endpoint,
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
                cluster=self.cluster,
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
                cluster=self.cluster,
            )

    @pytest.mark.parametrize("expiration_flag", ["lifetime", "expire_at"])
    def test_bearer_token_expiration(self, wallets, eacl_container_with_objects, expiration_flag):
        self.tick_epochs_and_wait(1)
        current_epoch = get_epoch(self.shell, self.cluster)
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
                owner=get_last_address_from_wallet(user_wallet.wallet_path, WALLET_PASS),
                out=path_to_bearer,
                rpc_endpoint=self.cluster.default_rpc_endpoint,
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
                cluster=self.cluster,
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
                cluster=self.cluster,
            )

    @allure.title("Check bearer token with ContainerID specified")
    def test_bearer_token_with_container_id(
        self, wallets, client_shell: Shell, cluster: Cluster, file_path: str
    ):
        user_wallet = wallets.get_wallet()
        container1, container2 = self._create_containers_with_objects(
            containers_count=2,
            objects_count=1,
            user_wallet=user_wallet,
            client_shell=client_shell,
            cluster=cluster,
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
                endpoint=self.cluster.default_rpc_endpoint,
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
                cluster=self.cluster,
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
                cluster=self.cluster,
            )

    @allure.title("Check bearer token without ContainerID specified")
    def test_bearer_token_without_container_id(
        self, wallets, client_shell: Shell, cluster: Cluster, file_path: str
    ):
        user_wallet = wallets.get_wallet()
        container1, container2 = self._create_containers_with_objects(
            containers_count=2,
            objects_count=1,
            user_wallet=user_wallet,
            client_shell=client_shell,
            cluster=cluster,
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
                endpoint=self.cluster.default_rpc_endpoint,
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
                cluster=self.cluster,
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
                cluster=self.cluster,
            )

    def _create_containers_with_objects(
        self,
        containers_count: int,
        objects_count: int,
        user_wallet,
        client_shell: Shell,
        cluster: Cluster,
        file_path: str,
    ) -> list[ContainerTuple]:
        result = []
        for _ in range(containers_count):
            with allure.step("Create eACL public container"):
                cid = create_container(
                    user_wallet.wallet_path,
                    basic_acl=PUBLIC_ACL,
                    shell=client_shell,
                    endpoint=cluster.default_rpc_endpoint,
                )

            with allure.step("Add test objects to container"):
                objects_oids = [
                    put_object_to_random_node(
                        user_wallet.wallet_path,
                        file_path,
                        cid,
                        attributes={"key1": "val1", "key": val, "key2": "abc"},
                        shell=client_shell,
                        cluster=cluster,
                    )
                    for val in range(objects_count)
                ]
            result.append(ContainerTuple(cid=cid, objects_oids=objects_oids))
        return result
