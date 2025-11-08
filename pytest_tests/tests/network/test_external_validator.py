import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import allure
import pytest
from helpers import wallet_helpers
from helpers.common import SN_VALIDATOR_DEFAULT_PORT
from neofs_testlib.env.env import NeoFSEnv, StorageNode
from tenacity import retry, stop_after_attempt, wait_fixed

logger = logging.getLogger("NeoLogger")

ir_private_key = None
invalid_nonce = False
invalid_signature = False

response_config = {
    "status_code": 200,
    "response_body": {
        "body": {"verified": True, "details": "", "nonce": 0},
        "signature": {"sign": "<bytes: signed body>"},
    },
}


class Validator(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/verify":
            self.send_response(404)
            self.end_headers()
            return

        content_length = int(self.headers.get("Content-Length", 0))
        post_data = self.rfile.read(content_length).decode()
        try:
            request_json = json.loads(post_data)
            logger.info(f"Validator received {request_json=}")

            if invalid_nonce:
                response_config["response_body"]["body"]["nonce"] = 123456789
            else:
                response_config["response_body"]["body"]["nonce"] = request_json["body"]["nonce"]

            if invalid_signature:
                response_config["response_body"]["signature"]["sign"] = wallet_helpers.sign_string(
                    json.dumps(response_config["response_body"]["body"]), ir_private_key
                )[::-1]
            else:
                response_config["response_body"]["signature"]["sign"] = wallet_helpers.sign_string(
                    json.dumps(response_config["response_body"]["body"]), ir_private_key
                )
        except json.JSONDecodeError:
            self.send_response(400)
            self.end_headers()
            return

        self.send_response(response_config["status_code"])
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        logger.info(f"Validator sends response {response_config=}")
        self.wfile.write(json.dumps(response_config["response_body"]).encode())


@pytest.fixture
def validator():
    server = HTTPServer(("localhost", SN_VALIDATOR_DEFAULT_PORT), Validator)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    yield
    server.shutdown()
    thread.join()


@pytest.fixture
def invalid_nonce_setting():
    global invalid_nonce
    invalid_nonce = True
    yield
    invalid_nonce = False


@pytest.fixture
def invalid_signature_setting():
    global invalid_signature
    invalid_signature = True
    yield
    invalid_signature = False


@pytest.fixture
def invalid_response_code():
    response_config["status_code"] = 400
    yield
    response_config["status_code"] = 200


@retry(wait=wait_fixed(1), stop=stop_after_attempt(50), reraise=True)
def _wait_until_ready(neofs_env: NeoFSEnv, sn: StorageNode):
    neofs_cli = neofs_env.neofs_cli(sn.cli_config)
    result = neofs_cli.control.healthcheck(endpoint=sn.control_endpoint)
    assert "Health status: READY" in result.stdout, "Health is not ready"
    assert "Network status: ONLINE" in result.stdout, "Network is not online"


@allure.title("Verify SN is accepted by the network with external validator")
def test_sn_validator_happy_path(neofs_env_ir_only_with_sn_validator: NeoFSEnv, validator):
    neofs_env = neofs_env_ir_only_with_sn_validator
    with allure.step("Get private key from IR"):
        global ir_private_key
        ir_private_key = wallet_helpers.get_private_key(neofs_env.inner_ring_nodes[0].alphabet_wallet)

    with allure.step("Start storage node and wait until ready"):
        new_storage_node = StorageNode(
            neofs_env,
            len(neofs_env.storage_nodes) + 1,
            node_attrs=["UN-LOCODE:RU MOW", "Price:22"],
        )
        neofs_env.storage_nodes.append(new_storage_node)
        new_storage_node.start(wait_until_ready=False)
        neofs_env._wait_until_all_storage_nodes_are_ready()
        neofs_env.neofs_adm().fschain.force_new_epoch(
            rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
            alphabet_wallets=neofs_env.alphabet_wallets_dir,
        )
        _wait_until_ready(neofs_env, new_storage_node)


@allure.title("Verify SN is not accepted by the network with invalid nonce")
def test_sn_validator_invalid_nonce(neofs_env_ir_only_with_sn_validator: NeoFSEnv, validator, invalid_nonce_setting):
    neofs_env = neofs_env_ir_only_with_sn_validator
    with allure.step("Get private key from IR"):
        global ir_private_key
        ir_private_key = wallet_helpers.get_private_key(neofs_env.inner_ring_nodes[0].alphabet_wallet)

    with allure.step("Start storage node and wait until ready"):
        new_storage_node = StorageNode(
            neofs_env,
            len(neofs_env.storage_nodes) + 1,
            node_attrs=["UN-LOCODE:RU MOW", "Price:22"],
        )
        neofs_env.storage_nodes.append(new_storage_node)
        new_storage_node.start(wait_until_ready=False)
        neofs_env._wait_until_all_storage_nodes_are_ready()
        neofs_env.neofs_adm().fschain.force_new_epoch(
            rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
            alphabet_wallets=neofs_env.alphabet_wallets_dir,
        )
        with pytest.raises(Exception):
            _wait_until_ready(neofs_env, new_storage_node)


@allure.title("Verify SN is not accepted by the network with invalid signature")
def test_sn_validator_invalid_signature(
    neofs_env_ir_only_with_sn_validator: NeoFSEnv, validator, invalid_signature_setting
):
    neofs_env = neofs_env_ir_only_with_sn_validator
    with allure.step("Get private key from IR"):
        global ir_private_key
        ir_private_key = wallet_helpers.get_private_key(neofs_env.inner_ring_nodes[0].alphabet_wallet)

    with allure.step("Start storage node and wait until ready"):
        new_storage_node = StorageNode(
            neofs_env,
            len(neofs_env.storage_nodes) + 1,
            node_attrs=["UN-LOCODE:RU MOW", "Price:22"],
        )
        neofs_env.storage_nodes.append(new_storage_node)
        new_storage_node.start(wait_until_ready=False)
        neofs_env._wait_until_all_storage_nodes_are_ready()
        neofs_env.neofs_adm().fschain.force_new_epoch(
            rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
            alphabet_wallets=neofs_env.alphabet_wallets_dir,
        )
        with pytest.raises(Exception):
            _wait_until_ready(neofs_env, new_storage_node)


@allure.title("Verify SN is not accepted by the network with invalid response code")
def test_sn_validator_invalid_response_code(
    neofs_env_ir_only_with_sn_validator: NeoFSEnv, validator, invalid_response_code
):
    neofs_env = neofs_env_ir_only_with_sn_validator
    with allure.step("Get private key from IR"):
        global ir_private_key
        ir_private_key = wallet_helpers.get_private_key(neofs_env.inner_ring_nodes[0].alphabet_wallet)

    with allure.step("Start storage node and wait until ready"):
        new_storage_node = StorageNode(
            neofs_env,
            len(neofs_env.storage_nodes) + 1,
            node_attrs=["UN-LOCODE:RU MOW", "Price:22"],
        )
        neofs_env.storage_nodes.append(new_storage_node)
        new_storage_node.start(wait_until_ready=False)
        neofs_env._wait_until_all_storage_nodes_are_ready()
        neofs_env.neofs_adm().fschain.force_new_epoch(
            rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
            alphabet_wallets=neofs_env.alphabet_wallets_dir,
        )
        with pytest.raises(Exception):
            _wait_until_ready(neofs_env, new_storage_node)
