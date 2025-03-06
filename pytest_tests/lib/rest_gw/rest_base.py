import pytest
from helpers.neofs_verbs import get_netmap_netinfo
from neofs_env.neofs_env_test_base import NeofsEnvTestBase
from neofs_testlib.env.env import NeoFSEnv


class TestNeofsRestBase(NeofsEnvTestBase):
    @pytest.fixture(scope="session")
    def max_object_size(self, neofs_env_rest_gw: NeoFSEnv) -> int:
        storage_node = neofs_env_rest_gw.storage_nodes[0]
        net_info = get_netmap_netinfo(
            wallet=storage_node.wallet.path,
            wallet_config=storage_node.cli_config,
            endpoint=storage_node.endpoint,
            shell=neofs_env_rest_gw.shell,
        )
        yield net_info["maximum_object_size"]

    @pytest.fixture(scope="class", autouse=True)
    def fill_mandatory_dependencies(self, request, neofs_env_rest_gw: NeoFSEnv):
        request.cls.shell = neofs_env_rest_gw.shell
        request.cls.neofs_env = neofs_env_rest_gw
        yield
