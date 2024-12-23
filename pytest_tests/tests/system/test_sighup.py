import os
import signal
import socket
import time
from importlib.resources import files

import allure
import pytest
import yaml
from neofs_testlib.env.env import NeoFSEnv


def is_port_in_use(host: str, port: str) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.settimeout(1)
            s.connect((host, int(port)))
            return True
        except (socket.timeout, ConnectionRefusedError):
            return False


@pytest.fixture
def clear_neofs_env():
    neofs_env_config = yaml.safe_load(
        files("neofs_testlib.env.templates").joinpath("neofs_env_config.yaml").read_text()
    )
    neofs_env = NeoFSEnv(neofs_env_config=neofs_env_config)
    neofs_env.download_binaries()
    neofs_env.deploy_inner_ring_nodes()
    neofs_env.deploy_storage_nodes(
        count=1,
        node_attrs={
            0: ["UN-LOCODE:RU MOW", "Price:22"],
        },
    )
    yield neofs_env
    neofs_env.kill()


def test_sighup_fschain_endpoint_reload(clear_neofs_env: NeoFSEnv):
    neofs_env = clear_neofs_env

    with allure.step("Update IR port"):
        ir_config_path = neofs_env.inner_ring_nodes[0].ir_node_config_path
        ir_config = yaml.safe_load(neofs_env.shell.exec(f"cat {ir_config_path}").stdout)
        ir_config["fschain"]["consensus"]["rpc"]["listen"][0] = f"{neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        with open(ir_config_path, "w") as config_file:
            yaml.dump(ir_config, config_file)
        neofs_env.inner_ring_nodes[0].process.kill()
        neofs_env.inner_ring_nodes[0]._launch_process()
        neofs_env.inner_ring_nodes[0]._wait_until_ready()

    with allure.step("Update SN to use new IR"):
        sn_config_path = neofs_env.storage_nodes[0].storage_node_config_path
        sn_config = yaml.safe_load(neofs_env.shell.exec(f"cat {sn_config_path}").stdout)
        sn_config["fschain"]["endpoints"][0] = f"ws://{ neofs_env.fschain_rpc }/ws"
        with open(sn_config_path, "w") as config_file:
            yaml.dump(sn_config, config_file)
        os.kill(neofs_env.storage_nodes[0].process.pid, signal.SIGHUP)
        time.sleep(5)
        neofs_env.storage_nodes[0]._wait_until_ready()


def test_sighup_node_attrs_update(clear_neofs_env: NeoFSEnv):
    neofs_env = clear_neofs_env

    with allure.step("Get current node attributes"):
        node_info = (
            neofs_env.neofs_cli(neofs_env.storage_nodes[0].cli_config)
            .netmap.nodeinfo(
                rpc_endpoint=neofs_env.sn_rpc,
                wallet=neofs_env.storage_nodes[0].wallet.path,
            )
            .stdout.strip()
        )
        assert "UN-LOCODE=RU MOW" in node_info, "node info doesn't contain required attributes"

    with allure.step("Update node attributes in config file"):
        config_path = neofs_env.storage_nodes[0].storage_node_config_path
        sn_config = yaml.safe_load(neofs_env.shell.exec(f"cat {config_path}").stdout)
        sn_config["node"]["attribute_0"] = "UN-LOCODE:FI HEL"
        with open(config_path, "w") as config_file:
            yaml.dump(sn_config, config_file)
        os.kill(neofs_env.storage_nodes[0].process.pid, signal.SIGHUP)
        neofs_env.storage_nodes[0]._wait_until_ready()

    with allure.step("Ensure new config attributes applied"):
        node_info = (
            neofs_env.neofs_cli(neofs_env.storage_nodes[0].cli_config)
            .netmap.nodeinfo(
                rpc_endpoint=neofs_env.sn_rpc,
                wallet=neofs_env.storage_nodes[0].wallet.path,
            )
            .stdout.strip()
        )
        assert "UN-LOCODE=RU MOW" not in node_info, "node info doesn't contain required attributes"
        assert "UN-LOCODE=FI HEL" in node_info, "node info doesn't contain required attributes"


def test_sighup_disable_metrics(clear_neofs_env: NeoFSEnv):
    neofs_env = clear_neofs_env

    with allure.step("Disable pprof and prometheus"):
        config_path = neofs_env.storage_nodes[0].storage_node_config_path
        sn_config = yaml.safe_load(neofs_env.shell.exec(f"cat {config_path}").stdout)
        sn_config["prometheus"]["enabled"] = False
        sn_config["pprof"]["enabled"] = False
        with open(config_path, "w") as config_file:
            yaml.dump(sn_config, config_file)
        os.kill(neofs_env.storage_nodes[0].process.pid, signal.SIGHUP)
        neofs_env.storage_nodes[0]._wait_until_ready()
        assert not is_port_in_use(
            neofs_env.storage_nodes[0].pprof_address.split(":")[0],
            neofs_env.storage_nodes[0].pprof_address.split(":")[1],
        ), "pprof port is busy, but should not be"
        assert not is_port_in_use(
            neofs_env.storage_nodes[0].prometheus_address.split(":")[0],
            neofs_env.storage_nodes[0].prometheus_address.split(":")[1],
        ), "prometheus port is busy, but should not be"

    with allure.step("Enable pprof and prometheus"):
        sn_config["prometheus"]["enabled"] = True
        sn_config["pprof"]["enabled"] = True
        with open(config_path, "w") as config_file:
            yaml.dump(sn_config, config_file)
        os.kill(neofs_env.storage_nodes[0].process.pid, signal.SIGHUP)
        neofs_env.storage_nodes[0]._wait_until_ready()
        assert is_port_in_use(
            neofs_env.storage_nodes[0].pprof_address.split(":")[0],
            neofs_env.storage_nodes[0].pprof_address.split(":")[1],
        ), "pprof port is not busy, but should be"
        assert is_port_in_use(
            neofs_env.storage_nodes[0].prometheus_address.split(":")[0],
            neofs_env.storage_nodes[0].prometheus_address.split(":")[1],
        ), "prometheus port is not busy, but should be"
