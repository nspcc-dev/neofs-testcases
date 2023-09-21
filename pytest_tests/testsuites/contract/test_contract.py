import os

import allure
import pytest
from cluster import Cluster
from common import NEOFS_ADM_CONFIG_PATH, NEOFS_ADM_EXEC, NEOGO_EXECUTABLE
from neofs_testlib.cli import NeofsAdm, NeoGo
from neofs_testlib.shell import Shell

from steps.cluster_test_base import ClusterTestBase


@pytest.mark.additional_contracts
class TestContract(ClusterTestBase):
    @allure.title("Test operations with external smart contracts")
    def test_contract(self, datadir, client_shell: Shell, cluster: Cluster):
        neogo = NeoGo(client_shell, neo_go_exec_path=NEOGO_EXECUTABLE)
        with allure.step("Compile new contract"):
            neogo.contract.contract_compile(
                i=os.path.join(datadir, "deploy"),
                out=os.path.join(datadir, "deploy", "testctr_contract.nef"),
                manifest=os.path.join(datadir, "deploy", "config.json"),
                config=os.path.join(datadir, "deploy", "neo-go.yml"),
            )

        neofsadm = NeofsAdm(
            shell=client_shell,
            neofs_adm_exec_path=NEOFS_ADM_EXEC,
            config_file=NEOFS_ADM_CONFIG_PATH,
        )

        with allure.step("Try to deploy contract with wrong arguments"):
            with pytest.raises(RuntimeError, match=".*deploy has failed.*"):
                neofsadm.morph.deploy(
                    rpc_endpoint=cluster.morph_chain_nodes[0].get_endpoint(),
                    alphabet_wallets="/".join(
                        cluster.ir_nodes[0].get_wallet_path().split("/")[:-1]
                    ),
                    domain="myzone",
                    contract=os.path.join(datadir, "deploy"),
                    post_data="string:shouldFail",
                )

        with allure.step("Try to deploy contract with valid arguments"):
            neofsadm.morph.deploy(
                rpc_endpoint=cluster.morph_chain_nodes[0].get_endpoint(),
                alphabet_wallets="/".join(cluster.ir_nodes[0].get_wallet_path().split("/")[:-1]),
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
                        rpc_endpoint=cluster.morph_chain_nodes[0].get_endpoint(),
                        alphabet_wallets="/".join(
                            cluster.ir_nodes[0].get_wallet_path().split("/")[:-1]
                        ),
                        domain="myzone",
                        update=True,
                        contract=os.path.join(datadir, "update"),
                        post_data="string:shouldFail",
                    )

            with allure.step("Try to deploy updated contract with valid arguments"):
                neofsadm.morph.deploy(
                    rpc_endpoint=cluster.morph_chain_nodes[0].get_endpoint(),
                    alphabet_wallets="/".join(
                        cluster.ir_nodes[0].get_wallet_path().split("/")[:-1]
                    ),
                    domain="myzone",
                    update=True,
                    contract=os.path.join(datadir, "update"),
                    post_data="string:ok",
                )

        hashes = neofsadm.morph.dump_hashes(
            rpc_endpoint=cluster.morph_chain_nodes[0].get_endpoint(),
            domain="myzone",
        )
        assert hashes != ""
