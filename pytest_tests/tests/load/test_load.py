import pytest
from neofs_env.neofs_env_test_base import NeofsEnvTestBase
from neofs_testlib.env.env import XK6


@pytest.mark.load
class TestLoad(NeofsEnvTestBase):
    def test_custom_load(self):
        xk6 = XK6(self.neofs_env)
        endpoints = [sn.endpoint for sn in self.neofs_env.storage_nodes]
        xk6.prepare(endpoints)
        xk6.run(endpoints)
