from typing import Optional, Union

import allure
import pytest
from neofs_env.neofs_env_test_base import NeofsEnvTestBase
from python_keywords.neofs_verbs import get_netmap_netinfo

CONFIG_KEYS_MAPPING = {
    "MaxObjectSize": "maximum_object_size",
    "BasicIncomeRate": "storage_price",
    "AuditFee": "audit_fee",
    "EpochDuration": "epoch_duration",
    "ContainerFee": "container_fee",
    # "ContainerAliasFee": "container_alias_fee",
    "EigenTrustIterations": "number_of_eigentrust_iterations",
    "EigenTrustAlpha": "eigentrust_alpha",
    "InnerRingCandidateFee": "inner_ring_candidate_fee",
    "WithdrawFee": "withdrawal_fee",
    "HomomorphicHashingDisabled": "homomorphic_hashing_disabled",
    "MaintenanceModeAllowed": "maintenance_mode_allowed",
}


@allure.title("Network configuration changes via neofs-adm")
@pytest.mark.network_config
class TestNetworkConfigChange(NeofsEnvTestBase):
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
            ("MaintenanceModeAllowed", True),
        ],
    )
    def test_config_update_single_value(
        self, key: str, value: Union[str, int, bool], clean_config: None
    ):
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
    def test_config_set_invalid_value(
        self, key: str, value: Union[str, int, bool], expected_type: type
    ):
        with pytest.raises(
            RuntimeError,
            match=f"Error: invalid value for {key} key, "
            f"expected {expected_type.__name__}, got '{str(value).lower()}'",
        ):
            self._set_and_verify_config_keys(**{key: value})

    @allure.title("Set multiple network config keys to invalid values with force")
    def test_config_set_multiple_invalid_values(self):
        with pytest.raises(
            RuntimeError,
            match="Error: invalid value for MaxObjectSize key, " "expected int, got 'verybigsize'",
        ):
            self._set_and_verify_config_keys(
                **{"MaxObjectSize": "VeryBigSize", "BasicIncomeRate": False}, force=True
            )

    @allure.title("Set network config unknown key")
    def test_config_set_unknown_key(self):
        with pytest.raises(RuntimeError, match=f".*key is not well-known.*"):
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
            force_str = f"--force " if force else ""
            keys_values_str = " ".join(
                [f"{key}={str(value).lower()}" for key, value in key_value_pairs.items()]
            )

            neofsadm.morph.set_config(
                rpc_endpoint=f"http://{ir_node.rpc_address}",
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
        with allure.step(f"Get config contents before test"):
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

        with allure.step(f"Change config contents back to original via neofs-adm"):
            self._set_and_verify_config_keys(**original_key_value_pairs)
