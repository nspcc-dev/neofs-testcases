import os

import allure
import pytest
from neofs_env.neofs_env_test_base import NeofsEnvTestBase
from neofs_testlib.cli import NeoGo
from neofs_testlib.env.env import NeoFSEnv
from neofs_testlib.shell import Shell


class TestContract(NeofsEnvTestBase):
    @allure.title("Test operations with external smart contracts")
    def test_contract(self, datadir, client_shell: Shell, neofs_env: NeoFSEnv):
        neogo = NeoGo(client_shell, neo_go_exec_path=neofs_env.neo_go_path)
        with allure.step("Compile new contract"):
            neogo.contract.contract_compile(
                i=os.path.join(datadir, "deploy"),
                out=os.path.join(datadir, "deploy", "testctr_contract.nef"),
                manifest=os.path.join(datadir, "deploy", "config.json"),
                config=os.path.join(datadir, "deploy", "neo-go.yml"),
            )

        neofsadm = neofs_env.neofs_adm()

        with allure.step("Try to deploy contract with wrong arguments"):
            with pytest.raises(RuntimeError, match=".*deploy has failed.*"):
                neofsadm.morph.deploy(
                    rpc_endpoint=f"http://{neofs_env.inner_ring_nodes[0].rpc_address}",
                    alphabet_wallets="/".join(neofs_env.inner_ring_nodes[0].alphabet_wallet.path.split("/")[:-1]),
                    domain="myzone",
                    contract=os.path.join(datadir, "deploy"),
                    post_data="string:shouldFail",
                )

        with allure.step("Try to deploy contract with valid arguments"):
            neofsadm.morph.deploy(
                rpc_endpoint=f"http://{neofs_env.inner_ring_nodes[0].rpc_address}",
                alphabet_wallets="/".join(neofs_env.inner_ring_nodes[0].alphabet_wallet.path.split("/")[:-1]),
                domain="myzone",
                contract=os.path.join(datadir, "deploy"),
                post_data="string:ok",
            )

        with allure.step("Try to update deployed contract"):
            with allure.step("Compile new contract"):
                neogo.contract.contract_compile(
                    i=os.path.join(datadir, "update"),
                    out=os.path.join(datadir, "update", "testctr_contract.nef"),
                    manifest=os.path.join(datadir, "update", "config.json"),
                    config=os.path.join(datadir, "update", "neo-go.yml"),
                )

            with allure.step("Try to deploy updated contract with wrong arguments"):
                with pytest.raises(RuntimeError, match=".*update has failed.*"):
                    neofsadm.morph.deploy(
                        rpc_endpoint=f"http://{neofs_env.inner_ring_nodes[0].rpc_address}",
                        alphabet_wallets="/".join(neofs_env.inner_ring_nodes[0].alphabet_wallet.path.split("/")[:-1]),
                        domain="myzone",
                        update=True,
                        contract=os.path.join(datadir, "update"),
                        post_data="string:shouldFail",
                    )

            with allure.step("Try to deploy updated contract with valid arguments"):
                neofsadm.morph.deploy(
                    rpc_endpoint=f"http://{neofs_env.inner_ring_nodes[0].rpc_address}",
                    alphabet_wallets="/".join(neofs_env.inner_ring_nodes[0].alphabet_wallet.path.split("/")[:-1]),
                    domain="myzone",
                    update=True,
                    contract=os.path.join(datadir, "update"),
                    post_data="string:ok",
                )

        hashes = neofsadm.morph.dump_hashes(
            rpc_endpoint=f"http://{neofs_env.inner_ring_nodes[0].rpc_address}",
            domain="myzone",
        )
        assert hashes != ""
