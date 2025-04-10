import pytest
from neofs_env.neofs_env_test_base import NeofsEnvTestBase
from neofs_testlib.env.env import NeoFSEnv


class TestNeofsRestBase(NeofsEnvTestBase):
    @pytest.fixture(scope="class", autouse=True)
    def fill_mandatory_dependencies(self, request, neofs_env_rest_gw: NeoFSEnv):
        request.cls.shell = neofs_env_rest_gw.shell
        request.cls.neofs_env = neofs_env_rest_gw
        yield
