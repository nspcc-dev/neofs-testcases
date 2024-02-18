import logging
import random
import string

import allure
import pytest
from file_helper import generate_file
from neofs_env.neofs_env_test_base import NeofsEnvTestBase
from neofs_testlib.env.env import NodeWallet
from python_keywords.container import (
    create_container,
    delete_container,
    get_container,
    list_containers,
    wait_for_container_deletion,
)
from python_keywords.neofs_verbs import get_netmap_netinfo, head_object, put_object_to_random_node

logger = logging.getLogger("NeoLogger")
CONTAINERS_NAME_PREFIX = "homo_hash_container_"


@allure.title("Homomorphic hash disabling/enabling")
@pytest.mark.homo_hash
class TestHomomorphicHash(NeofsEnvTestBase):
    @allure.title("Verify homomorphic hash disabling/enabling")
    def test_homomorphic_hash_enable_disable(self):
        self.switch_homomorphic_hash_value()
        self.switch_homomorphic_hash_value()

    @allure.title("New containers should have specified homomorphic hash value")
    def test_new_containers_created_with_specified_homomorphic_hash_value(
        self, default_wallet: NodeWallet, simple_object_size: int, containers_cleanup
    ):
        with allure.step("Set homomorphic hash value to the opposite one "):
            new_hash_value = self.switch_homomorphic_hash_value()
            with allure.step("Verify objects metadata"):
                if new_hash_value:
                    assert self.is_homomorphic_hash_disabled_in_metadata_of_new_containers(
                        default_wallet, simple_object_size
                    )
                else:
                    assert not self.is_homomorphic_hash_disabled_in_metadata_of_new_containers(
                        default_wallet, simple_object_size
                    )

        with allure.step("Set homomorphic hash value back to the original state"):
            new_hash_value = self.switch_homomorphic_hash_value()
            with allure.step("Verify objects metadata"):
                if new_hash_value:
                    assert self.is_homomorphic_hash_disabled_in_metadata_of_new_containers(
                        default_wallet, simple_object_size
                    )
                else:
                    assert not self.is_homomorphic_hash_disabled_in_metadata_of_new_containers(
                        default_wallet, simple_object_size
                    )

    @allure.title("Old containers should not be affected by new hash value")
    def test_old_containers_have_old_homomorphic_hash_value(
        self, default_wallet: NodeWallet, simple_object_size: int, containers_cleanup
    ):
        cid, oid = self.create_container_with_single_object(default_wallet, simple_object_size)
        current_object_has_hash = self.object_has_homomorphic_hash_value(default_wallet, cid, oid)

        self.switch_homomorphic_hash_value()

        with allure.step("Verify new objects still have the old hash setting"):
            file_path = generate_file(simple_object_size)
            new_oid = put_object_to_random_node(
                default_wallet.path, file_path, cid, self.shell, neofs_env=self.neofs_env
            )
            new_object_has_hash = self.object_has_homomorphic_hash_value(
                default_wallet, cid, new_oid
            )
            assert current_object_has_hash == new_object_has_hash

    @pytest.fixture(scope="function")
    def containers_cleanup(self, default_wallet: NodeWallet):
        yield
        with allure.step("Delete containers and check they were deleted"):
            list_cids = list_containers(default_wallet.path, self.shell, self.neofs_env.sn_rpc)
            for cid in list_cids:
                cont_info = get_container(
                    default_wallet.path, cid, self.shell, self.neofs_env.sn_rpc, True
                )
                if cont_info.get("attributes").get("Name", "").startswith(CONTAINERS_NAME_PREFIX):
                    delete_container(
                        default_wallet.path,
                        cid,
                        shell=self.shell,
                        endpoint=self.neofs_env.sn_rpc,
                    )
                    self.tick_epochs_and_wait(1)
                    wait_for_container_deletion(
                        default_wallet.path,
                        cid,
                        shell=self.shell,
                        endpoint=self.neofs_env.sn_rpc,
                    )

    @allure.step("Switch homomorphic hash value to the opposite")
    def switch_homomorphic_hash_value(self) -> bool:
        prev_hash_value = self.get_homomorphic_hash()
        self.set_homomorphic_hash(not prev_hash_value)
        new_hash_value = self.get_homomorphic_hash()
        assert new_hash_value != prev_hash_value
        return new_hash_value

    def is_homomorphic_hash_disabled_in_metadata_of_new_containers(
        self, default_wallet: NodeWallet, simple_object_size: int
    ) -> bool:
        cid, oid = self.create_container_with_single_object(default_wallet, simple_object_size)
        return not self.object_has_homomorphic_hash_value(default_wallet, cid, oid)

    @allure.step("Set HomomorphicHashingDisabled to '{value}'")
    def set_homomorphic_hash(self, value: bool) -> None:
        logger.info(f"Set HomomorphicHashingDisabled to '{value}'")

        ir_node = self.neofs_env.inner_ring_nodes[0]

        neofsadm = self.neofs_env.neofs_adm()

        neofsadm.morph.set_config(
            rpc_endpoint=f"http://{ir_node.rpc_address}",
            alphabet_wallets=self.neofs_env.alphabet_wallets_dir,
            post_data=f"HomomorphicHashingDisabled={str(value).lower()}",
        )

    @allure.step("Get HomomorphicHashingDisabled value from netmap netinfo")
    def get_homomorphic_hash(self) -> bool:
        storage_node = self.neofs_env.storage_nodes[0]
        net_info = get_netmap_netinfo(
            wallet=storage_node.wallet.path,
            wallet_config=storage_node.cli_config,
            endpoint=storage_node.endpoint,
            shell=self.shell,
        )
        logger.info(f"netmap netinfo: \n{net_info}\n")
        return net_info["homomorphic_hashing_disabled"]

    @allure.step("Create container with single object in it")
    def create_container_with_single_object(
        self, default_wallet: NodeWallet, simple_object_size: int
    ) -> tuple[int, int]:
        placement_rule = "REP 2 IN X CBF 1 SELECT 2 FROM * AS X"
        container_name = (
            f"{CONTAINERS_NAME_PREFIX}{''.join(random.choices(string.ascii_lowercase, k=5))}"
        )
        cid = create_container(
            default_wallet.path,
            rule=placement_rule,
            name=container_name,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )

        file_path = generate_file(simple_object_size)
        oid = put_object_to_random_node(
            default_wallet.path, file_path, cid, self.shell, neofs_env=self.neofs_env
        )
        return cid, oid

    @allure.step("Get object homomorphic hash value")
    def object_has_homomorphic_hash_value(
        self, default_wallet: NodeWallet, cid: int, oid: int
    ) -> bool:
        meta = head_object(
            default_wallet.path,
            cid,
            oid,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
        )
        logger.info(f"object metadata: \n{meta}\n")
        return meta["header"]["homomorphicHash"] is not None
