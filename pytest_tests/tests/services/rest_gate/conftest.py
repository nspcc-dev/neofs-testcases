import logging

import pytest
from neofs_testlib.env.env import NeoFSEnv

logger = logging.getLogger("NeoLogger")


@pytest.fixture(scope="session")
def gw_endpoint(neofs_env_rest_gw: NeoFSEnv):
    return f"http://{neofs_env_rest_gw.rest_gw.address}/v1"
