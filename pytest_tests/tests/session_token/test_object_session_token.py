import random

import allure
import neofs_env.neofs_epoch as neofs_epoch
import pytest
from helpers.container import create_container
from helpers.file_helper import generate_file
from helpers.grpc_responses import EXPIRED_SESSION_TOKEN, SESSION_NOT_FOUND
from helpers.neofs_verbs import delete_object, put_object, put_object_to_random_node
from helpers.session_token import create_session_token
from neofs_env.neofs_env_test_base import TestNeofsBase
from neofs_testlib.utils.wallet import get_last_address_from_wallet


class TestDynamicObjectSession(TestNeofsBase):
    @allure.title("Test Object Operations with Session Token")
    @pytest.mark.parametrize(
        "object_size",
        [
            pytest.param("simple_object_size", id="simple object", marks=pytest.mark.simple),
            pytest.param("complex_object_size", id="complex object", marks=pytest.mark.complex),
        ],
    )
    def test_object_session_token(self, default_wallet, object_size):
        """
        Test how operations over objects are executed with a session token

        Steps:
        1. Create a private container
        2. Obj operation requests to the node which IS NOT in the container but granted
            with a session token
        3. Obj operation requests to the node which IS in the container and NOT granted
            with a session token
        4. Obj operation requests to the node which IS NOT in the container and NOT granted
            with a session token
        """

        with allure.step("Init wallet"):
            wallet = default_wallet
            address = get_last_address_from_wallet(wallet.path, wallet.password)

        with allure.step("Nodes Settlements"):
            (
                session_token_node,
                container_node,
                non_container_node,
            ) = random.sample(self.neofs_env.storage_nodes, 3)

        with allure.step("Create Session Token"):
            session_token = create_session_token(
                shell=self.shell,
                owner=address,
                wallet_path=wallet.path,
                wallet_password=wallet.password,
                rpc_endpoint=session_token_node.endpoint,
                lifetime=2,
            )

        with allure.step("Create Private Container"):
            un_locode = container_node.node_attrs[0].split(":")[1].strip()
            locode = "SPB" if un_locode == "RU LED" else un_locode.split()[1]
            placement_policy = (
                f"REP 1 IN LOC_{locode}_PLACE CBF 1 SELECT 1 FROM LOC_{locode} "
                f'AS LOC_{locode}_PLACE FILTER "UN-LOCODE" '
                f'EQ "{un_locode}" AS LOC_{locode}'
            )
            cid = create_container(
                wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                rule=placement_policy,
            )

        with allure.step("Put Objects"):
            file_path = generate_file(self.neofs_env.get_object_size(object_size))
            oid = put_object_to_random_node(
                wallet=wallet.path,
                path=file_path,
                cid=cid,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )
            oid_delete = put_object_to_random_node(
                wallet=wallet.path,
                path=file_path,
                cid=cid,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step("Node not in container but granted a session token"):
            put_object(
                wallet=wallet.path,
                path=file_path,
                cid=cid,
                shell=self.shell,
                endpoint=session_token_node.endpoint,
                session=session_token,
            )
            delete_object(
                wallet=wallet.path,
                cid=cid,
                oid=oid_delete,
                shell=self.shell,
                endpoint=session_token_node.endpoint,
                session=session_token,
            )

        with allure.step("Node in container and not granted a session token"):
            with pytest.raises(Exception, match=SESSION_NOT_FOUND):
                put_object(
                    wallet=wallet.path,
                    path=file_path,
                    cid=cid,
                    shell=self.shell,
                    endpoint=container_node.endpoint,
                    session=session_token,
                )
            with pytest.raises(Exception, match=SESSION_NOT_FOUND):
                delete_object(
                    wallet=wallet.path,
                    cid=cid,
                    oid=oid,
                    shell=self.shell,
                    endpoint=container_node.endpoint,
                    session=session_token,
                )

        with allure.step("Node not in container and not granted a session token"):
            with pytest.raises(Exception, match=SESSION_NOT_FOUND):
                put_object(
                    wallet=wallet.path,
                    path=file_path,
                    cid=cid,
                    shell=self.shell,
                    endpoint=non_container_node.endpoint,
                    session=session_token,
                )
            with pytest.raises(Exception, match=SESSION_NOT_FOUND):
                delete_object(
                    wallet=wallet.path,
                    cid=cid,
                    oid=oid,
                    shell=self.shell,
                    endpoint=non_container_node.endpoint,
                    session=session_token,
                )

    @allure.title("Verify session token expiration flags")
    @pytest.mark.parametrize("expiration_flag", ["lifetime", "expire_at"])
    @pytest.mark.simple
    def test_session_token_expiration_flags(self, default_wallet, expiration_flag):
        rpc_endpoint = self.neofs_env.storage_nodes[0].endpoint

        with allure.step("Create Session Token with Lifetime param"):
            current_epoch = neofs_epoch.get_epoch(self.neofs_env)

            session_token = create_session_token(
                shell=self.shell,
                owner=get_last_address_from_wallet(default_wallet.path, default_wallet.password),
                wallet_path=default_wallet.path,
                wallet_password=default_wallet.password,
                rpc_endpoint=rpc_endpoint,
                lifetime=1 if expiration_flag == "lifetime" else None,
                expire_at=current_epoch + 1 if expiration_flag == "expire_at" else None,
            )

        with allure.step("Create Private Container"):
            first_node = self.neofs_env.storage_nodes[0]
            un_locode = first_node.node_attrs[0].split(":")[1].strip()
            locode = "SPB" if un_locode == "RU LED" else un_locode.split()[1]
            placement_policy = (
                f"REP 1 IN LOC_{locode}_PLACE CBF 1 SELECT 1 FROM LOC_{locode} "
                f'AS LOC_{locode}_PLACE FILTER "UN-LOCODE" '
                f'EQ "{un_locode}" AS LOC_{locode}'
            )
            cid = create_container(
                default_wallet.path,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                rule=placement_policy,
            )

        with allure.step("Verify object operations with created session token are allowed"):
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oid = put_object(
                wallet=default_wallet.path,
                path=file_path,
                cid=cid,
                shell=self.shell,
                endpoint=rpc_endpoint,
                session=session_token,
            )
            delete_object(
                wallet=default_wallet.path,
                cid=cid,
                oid=oid,
                shell=self.shell,
                endpoint=rpc_endpoint,
                session=session_token,
            )

        self.tick_epochs_and_wait(2)

        with allure.step("Verify object operations with created session token are not allowed"):
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            with pytest.raises(RuntimeError, match=EXPIRED_SESSION_TOKEN):
                oid = put_object(
                    wallet=default_wallet.path,
                    path=file_path,
                    cid=cid,
                    shell=self.shell,
                    endpoint=rpc_endpoint,
                    session=session_token,
                )
