import os
import uuid

import allure
import neofs_env.neofs_epoch as neofs_epoch
import pytest
from helpers.acl import (
    EACLAccess,
    EACLFilter,
    EACLFilters,
    EACLHeaderType,
    EACLOperation,
    EACLRole,
    EACLRule,
    create_bearer_token,
    create_eacl,
    form_bearertoken_file,
    set_eacl,
    sign_bearer,
)
from helpers.common import TEST_FILES_DIR, get_assets_dir_path
from helpers.container import (
    DEFAULT_PLACEMENT_RULE,
    REP_2_FOR_3_NODES_PLACEMENT_RULE,
    SINGLE_PLACEMENT_RULE,
    create_container,
)
from helpers.file_helper import generate_file
from helpers.grpc_responses import OBJECT_ACCESS_DENIED
from helpers.neofs_verbs import delete_object, get_object, put_object
from helpers.storage_container import StorageContainer, StorageContainerInfo
from helpers.storage_object_info import StorageObjectInfo
from helpers.test_control import expect_not_raises
from helpers.wellknown_acl import EACL_PUBLIC_READ_WRITE
from neofs_testlib.env.env import NeoFSEnv, NodeWallet
from neofs_testlib.utils.wallet import get_last_address_from_wallet
from pytest import FixtureRequest
from s3.s3_base import TestNeofsS3Base


def pytest_generate_tests(metafunc):
    if "s3_client" in metafunc.fixturenames:
        metafunc.parametrize("s3_client", ["boto3"], indirect=True)


@pytest.fixture(scope="module")
@allure.title("Create bearer token for OTHERS with all operations allowed for all containers")
def bearer_token_file_all_allow(default_wallet: NodeWallet, neofs_env_s3_gw: NeoFSEnv) -> str:
    bearer = form_bearertoken_file(
        default_wallet.path,
        "",
        [EACLRule(operation=op, access=EACLAccess.ALLOW, role=EACLRole.OTHERS) for op in EACLOperation],
        shell=neofs_env_s3_gw.shell,
        endpoint=neofs_env_s3_gw.sn_rpc,
    )

    return bearer


@pytest.fixture(scope="module")
@allure.title("Create user container for bearer token usage")
def user_container(default_wallet: NodeWallet, neofs_env_s3_gw: NeoFSEnv, request: FixtureRequest) -> StorageContainer:
    container_id = create_container(
        default_wallet.path,
        shell=neofs_env_s3_gw.shell,
        rule=request.param,
        basic_acl=EACL_PUBLIC_READ_WRITE,
        endpoint=neofs_env_s3_gw.sn_rpc,
    )
    # Deliberately using s3gate wallet here to test bearer token
    return StorageContainer(
        StorageContainerInfo(container_id, neofs_env_s3_gw.s3_gw.wallet),
        neofs_env_s3_gw.shell,
        neofs_env_s3_gw,
    )


@pytest.fixture()
def storage_objects(
    user_container: StorageContainer,
    bearer_token_file_all_allow: str,
    request: FixtureRequest,
    neofs_env_s3_gw: NeoFSEnv,
) -> list[StorageObjectInfo]:
    epoch = neofs_epoch.get_epoch(neofs_env_s3_gw)
    storage_objects: list[StorageObjectInfo] = []
    for node in neofs_env_s3_gw.storage_nodes:
        storage_objects.append(
            user_container.generate_object(
                neofs_env_s3_gw.get_object_size(request.param),
                epoch + 3,
                bearer_token=bearer_token_file_all_allow,
                endpoint=node.endpoint,
            )
        )
    return storage_objects


class TestObjectApiWithBearerToken(TestNeofsS3Base):
    @pytest.mark.parametrize(
        "user_container",
        [SINGLE_PLACEMENT_RULE],
        ids=["single replica for all nodes placement rule"],
        indirect=True,
    )
    @pytest.mark.parametrize(
        "storage_objects",
        [
            pytest.param("simple_object_size", id="simple object", marks=pytest.mark.simple),
            pytest.param("complex_object_size", id="complex object", marks=pytest.mark.complex),
        ],
        indirect=True,
    )
    def test_delete_object_with_s3_wallet_bearer(
        self,
        storage_objects: list[StorageObjectInfo],
        bearer_token_file_all_allow: str,
        request: FixtureRequest,
    ):
        allure.dynamic.title(
            f"Object can be deleted from any node using s3gate wallet with bearer token for {request.node.callspec.id}"
        )

        s3_gate_wallet = self.neofs_env.s3_gw.wallet
        s3_gate_wallet_config_path = self.neofs_env.generate_cli_config(s3_gate_wallet)

        with allure.step("Try to fetch each object from first storage node"):
            for storage_object in storage_objects:
                with expect_not_raises():
                    get_object(
                        s3_gate_wallet.path,
                        storage_object.cid,
                        storage_object.oid,
                        self.shell,
                        endpoint=self.neofs_env.sn_rpc,
                        bearer=bearer_token_file_all_allow,
                        wallet_config=s3_gate_wallet_config_path,
                    )

        with allure.step("Try to delete each object from first storage node"):
            for storage_object in storage_objects:
                with expect_not_raises():
                    delete_object(
                        s3_gate_wallet.path,
                        storage_object.cid,
                        storage_object.oid,
                        self.shell,
                        endpoint=self.neofs_env.sn_rpc,
                        bearer=bearer_token_file_all_allow,
                        wallet_config=s3_gate_wallet_config_path,
                    )

    @pytest.mark.parametrize(
        "user_container",
        [REP_2_FOR_3_NODES_PLACEMENT_RULE],
        ids=["2 replicas for 3 nodes placement rule"],
        indirect=True,
    )
    @pytest.mark.parametrize(
        "file_size",
        [
            pytest.param("simple_object_size", id="simple object", marks=pytest.mark.simple),
            pytest.param("complex_object_size", id="complex object", marks=pytest.mark.complex),
        ],
    )
    def test_get_object_with_s3_wallet_bearer_from_all_nodes(
        self,
        user_container: StorageContainer,
        file_size: str,
        bearer_token_file_all_allow: str,
        request: FixtureRequest,
    ):
        allure.dynamic.title(
            f"Object can be fetched from any node using s3gate wallet with bearer token for {request.node.callspec.id}"
        )

        s3_gate_wallet = self.neofs_env.s3_gw.wallet
        with allure.step("Put one object to container"):
            epoch = self.ensure_fresh_epoch()
            storage_object = user_container.generate_object(
                self.neofs_env.get_object_size(file_size), epoch + 3, bearer_token=bearer_token_file_all_allow
            )

        with allure.step("Try to fetch object from each storage node"):
            for node in self.neofs_env.storage_nodes:
                with expect_not_raises():
                    get_object(
                        s3_gate_wallet.path,
                        storage_object.cid,
                        storage_object.oid,
                        self.shell,
                        endpoint=node.endpoint,
                        bearer=bearer_token_file_all_allow,
                        wallet_config=self.neofs_env.generate_cli_config(s3_gate_wallet),
                    )

    @pytest.mark.parametrize(
        "user_container",
        [DEFAULT_PLACEMENT_RULE],
        indirect=True,
    )
    @pytest.mark.parametrize(
        "file_size",
        [
            pytest.param("simple_object_size", id="simple object", marks=pytest.mark.simple),
            pytest.param("complex_object_size", id="complex object", marks=pytest.mark.complex),
        ],
    )
    def test_attributes_bearer_rules(
        self,
        default_wallet: NodeWallet,
        file_size: str,
        user_container: StorageContainer,
    ):
        # what? user_container has "s3 GW wallet to test bearer", so much magic...
        other_wallet = user_container.get_wallet_path()
        container_owner = default_wallet.path
        cid = user_container.get_id()
        test_file = generate_file(self.neofs_env.get_object_size(file_size))
        ATTRIBUTE_KEY = "test_attribute"
        ATTRIBUTE_VALUE = "allowed_value"

        with allure.step("Create eACL that prohibits PUT operation for OTHERS"):
            eacl_deny = create_eacl(
                cid,
                [
                    EACLRule(
                        role=EACLRole.OTHERS,
                        access=EACLAccess.DENY,
                        operation=EACLOperation.PUT,
                    )
                ],
                shell=self.shell,
            )

        with allure.step("Set container-wise eACL"):
            set_eacl(
                container_owner,
                cid,
                eacl_deny,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Try to PUT object with OTHER role"):
            with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
                put_object(
                    wallet=other_wallet,
                    path=test_file,
                    cid=cid,
                    shell=self.shell,
                    endpoint=user_container.neofs_env.sn_rpc,
                )

        with allure.step(f"Create exception for {{{ATTRIBUTE_KEY}: {ATTRIBUTE_VALUE}}} objects"):
            eacl_allow_exception = create_eacl(
                cid,
                [
                    EACLRule(
                        role=EACLRole.OTHERS,
                        access=EACLAccess.ALLOW,
                        operation=EACLOperation.PUT,
                        filters=EACLFilters(
                            filters=[
                                EACLFilter(key=ATTRIBUTE_KEY, value=ATTRIBUTE_VALUE, header_type=EACLHeaderType.OBJECT)
                            ]
                        ),
                    )
                ],
                shell=self.shell,
            )

        path_to_bearer = os.path.join(get_assets_dir_path(), TEST_FILES_DIR, f"bearer_token_{str(uuid.uuid4())}")

        create_bearer_token(
            self.shell,
            issued_at=1,
            not_valid_before=1,
            owner=get_last_address_from_wallet(other_wallet, user_container.neofs_env.default_password),
            out=path_to_bearer,
            rpc_endpoint=self.neofs_env.sn_rpc,
            eacl=eacl_allow_exception,
            expire_at=(1 << 32) - 1,
        )

        sign_bearer(
            shell=self.shell,
            wallet_path=container_owner,
            eacl_rules_file_from=path_to_bearer,
            eacl_rules_file_to=path_to_bearer,
            json=True,
        )

        with allure.step("Try to PUT object with exceptional bearer"):
            put_object(
                wallet=other_wallet,
                path=test_file,
                cid=cid,
                shell=self.shell,
                attributes={ATTRIBUTE_KEY: ATTRIBUTE_VALUE},
                bearer=path_to_bearer,
                endpoint=user_container.neofs_env.sn_rpc,
            )
