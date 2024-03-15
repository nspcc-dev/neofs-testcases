import logging

import pytest
from neofs_testlib.env.env import NeoFSEnv
from neofs_testlib.reporter import AllureHandler, get_reporter

get_reporter().register_handler(AllureHandler())
logger = logging.getLogger("NeoLogger")


@pytest.fixture(scope="session", params=["HTTP", "REST"])
def gw_endpoint(neofs_env: NeoFSEnv, request):
    gw_type = request.param
    if gw_type == "HTTP":
        return f"http://{neofs_env.http_gw.address}"
    else:  # Assuming REST
        return f"http://{neofs_env.rest_gw.address}/v1"
