import os

import allure
import pytest
from neofs_env.neofs_env_test_base import TestNeofsBase
from neofs_testlib.cli import NeoGo
from neofs_testlib.env.env import NeoFSEnv


class TestN3(TestNeofsBase):
    @allure.title("Test N3 contract witnesses in container ops")
    def test_n3_contract_witnesses_in_container_ops(self, datadir, neofs_env: NeoFSEnv):
        neogo = NeoGo(neofs_env.shell, neo_go_exec_path=neofs_env.neo_go_path)

        neofsadm = neofs_env.neofs_adm()

        neofsadm.fschain.refill_gas(
            rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
            alphabet_wallets=neofs_env.alphabet_wallets_dir,
            storage_wallet=os.path.join(datadir, "deployer_wallet.json"),
            gas="100",
        )

        with allure.step("Deploy user management contract"):
            result = neogo.contract.deploy(
                input_file=os.path.join(datadir, "contract", "usermgt.nef"),
                manifest=os.path.join(datadir, "contract", "usermgt.manifest.json"),
                rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
                wallet=os.path.join(datadir, "deployer_wallet.json"),
                wallet_password="deployer",
                force=True,
                await_mode=True,
            )
            assert "1b4012d2aba18230a8ada77540f64d190480cbb0" in result.stdout, (
                "Expected contract address not found in deployment output"
            )

        with allure.step("Create container"):
            result = neofs_env.neofs_cli(None).request.create_container(
                body=os.path.join(datadir, "create_container_request.json"),
                endpoint=neofs_env.storage_nodes[0].endpoint,
            )
            assert "DZCBvXHg7PtXnRHusFSFWcy2JomcPSiGyM1Zszc5ZTk5" in result.stdout, (
                "Expected container ID not found in create container output"
            )

        with allure.step("Create container with wrong user"):
            with pytest.raises(Exception, match=".*container not saved within 10s.*"):
                result = neofs_env.neofs_cli(None).request.create_container(
                    body=os.path.join(datadir, "wrong_user_create_container_request.json"),
                    endpoint=neofs_env.storage_nodes[0].endpoint,
                )

        with allure.step("Create container with wrong signature"):
            with pytest.raises(Exception, match=".*container not saved within 10s.*"):
                result = neofs_env.neofs_cli(None).request.create_container(
                    body=os.path.join(datadir, "wrong_signature_create_container_request.json"),
                    endpoint=neofs_env.storage_nodes[0].endpoint,
                )

        with allure.step("Create container with wrong contract method"):
            with pytest.raises(Exception, match=".*container not saved within 10s.*"):
                result = neofs_env.neofs_cli(None).request.create_container(
                    body=os.path.join(datadir, "wrong_method_create_container_request.json"),
                    endpoint=neofs_env.storage_nodes[0].endpoint,
                )
