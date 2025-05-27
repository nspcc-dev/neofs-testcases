from typing import Optional, Union

import allure
import pytest
from helpers.neofs_verbs import CONFIG_KEYS_MAPPING, get_netmap_netinfo
from neofs_env.neofs_env_test_base import TestNeofsBase


@allure.title("Network configuration changes via neofs-adm")
class TestNetworkConfigChange(TestNeofsBase):
    @pytest.mark.parametrize(
        "key, value",
        [
            ("MaxObjectSize", 1048576),
            ("BasicIncomeRate", 50000000),
            ("AuditFee", 5000),
            ("EpochDuration", 480),
            ("ContainerFee", 2000),
            # ("ContainerAliasFee", 5000),
            ("EigenTrustIterations", 8),
            ("EigenTrustAlpha", 0.2),
            ("InnerRingCandidateFee", 5000000000),
            ("WithdrawFee", 200000000),
            ("HomomorphicHashingDisabled", True),
        ],
    )
    def test_config_update_single_value(self, key: str, value: Union[str, int, bool], clean_config: None):
        allure.dynamic.title(f"Set '{key}' to '{value}'")
        self._set_and_verify_config_keys(**{key: value})

    @allure.title("Multiple network config keys can be changed at once")
    def test_config_update_multiple_values(self, clean_config: None):
        new_key_value_pairs = {
            "MaxObjectSize": 1048576,
            "BasicIncomeRate": 50000000,
            "AuditFee": 5000,
        }
        self._set_and_verify_config_keys(**new_key_value_pairs)

    @pytest.mark.parametrize(
        "key, value, expected_type",
        [
            ("MaxObjectSize", "VeryBigSize", int),
            ("BasicIncomeRate", False, int),
            ("HomomorphicHashingDisabled", 0.2, bool),
        ],
    )
    @allure.title("Set network config key to invalid value")
    def test_config_set_invalid_value(self, key: str, value: Union[str, int, bool], expected_type: type):
        with pytest.raises(
            RuntimeError,
            match=f"Error: invalid value for {key} key, expected {expected_type.__name__}, got '{str(value).lower()}'",
        ):
            self._set_and_verify_config_keys(**{key: value})

    @allure.title("Set multiple network config keys to invalid values with force")
    def test_config_set_multiple_invalid_values(self):
        with pytest.raises(
            RuntimeError,
            match="Error: invalid value for MaxObjectSize key, expected int, got 'verybigsize'",
        ):
            self._set_and_verify_config_keys(**{"MaxObjectSize": "VeryBigSize", "BasicIncomeRate": False}, force=True)

    @allure.title("Set network config unknown key")
    def test_config_set_unknown_key(self):
        with pytest.raises(RuntimeError, match=".*key is not well-known.*"):
            self._set_and_verify_config_keys(**{"unknown_key": 120})

    @allure.title("Set network config unknown key with force")
    def test_config_force_set_unknown_key(self):
        with pytest.raises(AssertionError):
            self._set_and_verify_config_keys(
                **{"unknown_key": 120},
                force=True,
                config_keys_mapping={"unknown_key": "unknown_key"},
            )

    def _set_and_verify_config_keys(
        self,
        force: bool = False,
        config_keys_mapping: Optional[dict[str]] = None,
        **key_value_pairs: dict[str, Union[str, int, bool]],
    ):
        if config_keys_mapping is None:
            config_keys_mapping = CONFIG_KEYS_MAPPING

        ir_node = self.neofs_env.inner_ring_nodes[0]
        neofsadm = self.neofs_env.neofs_adm()

        with allure.step(f"Set {key_value_pairs} via neofs-adm"):
            force_str = "--force " if force else ""
            keys_values_str = " ".join([f"{key}={str(value).lower()}" for key, value in key_value_pairs.items()])

            neofsadm.fschain.set_config(
                rpc_endpoint=f"http://{ir_node.endpoint}",
                alphabet_wallets=self.neofs_env.alphabet_wallets_dir,
                post_data=f"{force_str}{keys_values_str}",
            )

            storage_node = self.neofs_env.storage_nodes[0]
            net_info = get_netmap_netinfo(
                wallet=storage_node.wallet.path,
                wallet_config=storage_node.cli_config,
                endpoint=storage_node.endpoint,
                shell=self.shell,
            )

            for key, value in key_value_pairs.items():
                assert net_info[config_keys_mapping[key]] == value

    @pytest.fixture(scope="function")
    def clean_config(self):
        with allure.step("Get config contents before test"):
            storage_node = self.neofs_env.storage_nodes[0]
            net_info = get_netmap_netinfo(
                wallet=storage_node.wallet.path,
                wallet_config=storage_node.cli_config,
                endpoint=storage_node.endpoint,
                shell=self.shell,
            )
            original_key_value_pairs = {}
            for key, value in CONFIG_KEYS_MAPPING.items():
                original_key_value_pairs[key] = net_info[value]

        yield

        with allure.step("Change config contents back to original via neofs-adm"):
            self._set_and_verify_config_keys(**original_key_value_pairs)
