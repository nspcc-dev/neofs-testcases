import logging

import pytest
from neofs_testlib.env.env import NeoFSEnv

logger = logging.getLogger("NeoLogger")


@pytest.fixture(scope="session")
def gw_endpoint(neofs_env: NeoFSEnv):
    return f"http://{neofs_env.rest_gw.endpoint}/v1"
