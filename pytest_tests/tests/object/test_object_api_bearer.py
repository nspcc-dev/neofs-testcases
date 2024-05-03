import allure
import neofs_env.neofs_epoch as neofs_epoch
import pytest
from helpers.acl import EACLAccess, EACLOperation, EACLRole, EACLRule, form_bearertoken_file
from helpers.container import (
    REP_2_FOR_3_NODES_PLACEMENT_RULE,
    SINGLE_PLACEMENT_RULE,
    create_container,
)
from helpers.neofs_verbs import delete_object, get_object
from helpers.storage_container import StorageContainer, StorageContainerInfo
from helpers.storage_object_info import StorageObjectInfo
from helpers.test_control import expect_not_raises
from helpers.wellknown_acl import EACL_PUBLIC_READ_WRITE
from neofs_env.neofs_env_test_base import NeofsEnvTestBase
from neofs_testlib.env.env import NeoFSEnv, NodeWallet
from neofs_testlib.shell import Shell
from pytest import FixtureRequest


@pytest.fixture(scope="module")
@allure.title("Create bearer token for OTHERS with all operations allowed for all containers")
def bearer_token_file_all_allow(default_wallet: NodeWallet, client_shell: Shell, neofs_env: NeoFSEnv) -> str:
    bearer = form_bearertoken_file(
        default_wallet.path,
        "",
        [EACLRule(operation=op, access=EACLAccess.ALLOW, role=EACLRole.OTHERS) for op in EACLOperation],
        shell=client_shell,
        endpoint=neofs_env.sn_rpc,
    )

    return bearer


@pytest.fixture(scope="module")
@allure.title("Create user container for bearer token usage")
def user_container(
    default_wallet: NodeWallet, client_shell: Shell, neofs_env: NeoFSEnv, request: FixtureRequest
) -> StorageContainer:
    container_id = create_container(
        default_wallet.path,
        shell=client_shell,
        rule=request.param,
        basic_acl=EACL_PUBLIC_READ_WRITE,
        endpoint=neofs_env.sn_rpc,
    )
    # Deliberately using s3gate wallet here to test bearer token
    return StorageContainer(
        StorageContainerInfo(container_id, neofs_env.s3_gw.wallet),
        client_shell,
        neofs_env,
    )


@pytest.fixture()
def storage_objects(
    user_container: StorageContainer,
    bearer_token_file_all_allow: str,
    request: FixtureRequest,
    neofs_env: NeoFSEnv,
) -> list[StorageObjectInfo]:
    epoch = neofs_epoch.get_epoch(neofs_env)
    storage_objects: list[StorageObjectInfo] = []
    for node in neofs_env.storage_nodes:
        storage_objects.append(
            user_container.generate_object(
                request.param,
                epoch + 3,
                bearer_token=bearer_token_file_all_allow,
                endpoint=node.endpoint,
            )
        )
    return storage_objects


@pytest.mark.smoke
@pytest.mark.bearer
class TestObjectApiWithBearerToken(NeofsEnvTestBase):
    @pytest.mark.parametrize(
        "user_container",
        [SINGLE_PLACEMENT_RULE],
        ids=["single replica for all nodes placement rule"],
        indirect=True,
    )
    @pytest.mark.parametrize(
        "storage_objects",
        [pytest.lazy_fixture("simple_object_size"), pytest.lazy_fixture("complex_object_size")],
        ids=["simple object", "complex object"],
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
        [pytest.lazy_fixture("simple_object_size"), pytest.lazy_fixture("complex_object_size")],
        ids=["simple object", "complex object"],
    )
    def test_get_object_with_s3_wallet_bearer_from_all_nodes(
        self,
        user_container: StorageContainer,
        file_size: int,
        bearer_token_file_all_allow: str,
        request: FixtureRequest,
    ):
        allure.dynamic.title(
            f"Object can be fetched from any node using s3gate wallet with bearer token for {request.node.callspec.id}"
        )

        s3_gate_wallet = self.neofs_env.s3_gw.wallet
        with allure.step("Put one object to container"):
            epoch = self.get_epoch()
            storage_object = user_container.generate_object(
                file_size, epoch + 3, bearer_token=bearer_token_file_all_allow
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
