import allure
import pytest
from neofs_testlib.env.env import NeoFSEnv, StorageNode
from tenacity import retry, stop_after_attempt, wait_fixed


@retry(wait=wait_fixed(1), stop=stop_after_attempt(50), reraise=True)
def _wait_until_ready(neofs_env: NeoFSEnv, sn: StorageNode):
    neofs_cli = neofs_env.neofs_cli(sn.cli_config)
    result = neofs_cli.control.healthcheck(endpoint=sn.control_endpoint)
    assert "Health status: READY" in result.stdout, "Health is not ready"
    assert "Network status: ONLINE" in result.stdout, "Network is not online"


def test_verified_nodes_domain(neofs_env_ir_only: NeoFSEnv):
    neofs_adm = neofs_env_ir_only.neofs_adm()
    domain = "nodes.neofs"

    with allure.step("Get SN address"):
        new_storage_node = StorageNode(
            neofs_env_ir_only,
            len(neofs_env_ir_only.storage_nodes) + 1,
            node_attrs=[f"VerifiedNodesDomain:{domain}"],
        )
        neofs_env_ir_only.storage_nodes.append(new_storage_node)
        neofs_env_ir_only.generate_storage_wallet(new_storage_node.wallet, label=f"sn{new_storage_node.sn_number}")

    with allure.step("Add SN address to the verified nodes domain access list"):
        neofs_adm.fschain.verified_nodes_domain_set_access_list(
            rpc_endpoint=f"http://{neofs_env_ir_only.fschain_rpc}",
            domain=domain,
            wallet=neofs_env_ir_only.inner_ring_nodes[0].alphabet_wallet.path,
            wallet_password=neofs_env_ir_only.inner_ring_nodes[0].alphabet_wallet.password,
            neo_addresses=[new_storage_node.wallet.address],
        )

    with allure.step("Check that SN address is in the verified nodes domain access list"):
        result = neofs_adm.fschain.verified_nodes_domain_access_list(
            rpc_endpoint=f"http://{neofs_env_ir_only.fschain_rpc}",
            domain=domain,
        )
        assert new_storage_node.wallet.address in result.stdout, "New storage node not in access list"

    with allure.step("Start new storage node and ensure it is registered in the network"):
        new_storage_node.start(prepared_wallet=new_storage_node.wallet, wait_until_ready=False)
        neofs_env_ir_only._wait_until_all_storage_nodes_are_ready()
        neofs_env_ir_only.neofs_adm().fschain.force_new_epoch(
            rpc_endpoint=f"http://{neofs_env_ir_only.fschain_rpc}",
            alphabet_wallets=neofs_env_ir_only.alphabet_wallets_dir,
        )
        _wait_until_ready(neofs_env_ir_only, new_storage_node)

    with allure.step("Deploy another SN and ensure it won't be accepted by the network"):
        another_storage_node = StorageNode(
            neofs_env_ir_only,
            len(neofs_env_ir_only.storage_nodes) + 1,
            node_attrs=[f"VerifiedNodesDomain:{domain}"],
        )
        neofs_env_ir_only.storage_nodes.append(another_storage_node)
        another_storage_node.start(wait_until_ready=False)
        neofs_env_ir_only._wait_until_all_storage_nodes_are_ready()
        neofs_env_ir_only.neofs_adm().fschain.force_new_epoch(
            rpc_endpoint=f"http://{neofs_env_ir_only.fschain_rpc}",
            alphabet_wallets=neofs_env_ir_only.alphabet_wallets_dir,
        )
        with pytest.raises(Exception):
            _wait_until_ready(neofs_env_ir_only, another_storage_node)
