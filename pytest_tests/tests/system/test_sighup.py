import os
import signal
import socket
import ssl
import time
from collections.abc import Callable

import allure
import pytest
import yaml
from helpers.utility import parse_version
from neofs_testlib.env.env import NeoFSEnv


@pytest.fixture(autouse=True)
def skip_for_older_node(neofs_env_single_sn: NeoFSEnv) -> None:
    node_version = neofs_env_single_sn.get_binary_version(neofs_env_single_sn.neofs_node_path)
    if parse_version(node_version) <= parse_version("0.55.0"):
        pytest.skip(f"current SIGHUP tests are not supported by neofs-node {node_version} (<= 0.55.0)")


def is_port_in_use(host: str, port: str | int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.settimeout(1)
            s.connect((host, int(port)))
            return True
        except (socket.timeout, ConnectionRefusedError, OSError):
            return False


def load_sn_config(neofs_env: NeoFSEnv) -> dict:
    config_path = neofs_env.storage_nodes[0].storage_node_config_path
    return yaml.safe_load(neofs_env.shell.exec(f"cat {config_path}").stdout)


def save_sn_config(neofs_env: NeoFSEnv, sn_config: dict) -> None:
    config_path = neofs_env.storage_nodes[0].storage_node_config_path
    with open(config_path, "w") as config_file:
        yaml.dump(sn_config, config_file)


def read_sn_logs(neofs_env: NeoFSEnv) -> str:
    with open(neofs_env.storage_nodes[0].stderr) as sn_logs:
        return sn_logs.read()


def send_sighup(neofs_env: NeoFSEnv) -> None:
    os.kill(neofs_env.storage_nodes[0].process.pid, signal.SIGHUP)
    time.sleep(3)


def assert_storage_node_alive(neofs_env: NeoFSEnv) -> None:
    sn = neofs_env.storage_nodes[0]
    assert sn.process.poll() is None, f"storage node process exited unexpectedly, stderr={sn.stderr}"
    sn._wait_until_ready()


def assert_storage_node_stopped(neofs_env: NeoFSEnv, timeout: float = 20) -> None:
    sn = neofs_env.storage_nodes[0]
    deadline = time.time() + timeout
    while time.time() < deadline:
        if sn.process.poll() is not None:
            return
        time.sleep(0.5)
    raise AssertionError(f"storage node process did not exit after failed SIGHUP, stderr={sn.stderr}")


def assert_public_api_works(neofs_env: NeoFSEnv) -> str:
    sn = neofs_env.storage_nodes[0]
    host, port = sn.endpoint.split(":")
    assert is_port_in_use(host, port), f"public API endpoint {sn.endpoint} is not listening"
    node_info = (
        neofs_env.neofs_cli(sn.cli_config)
        .netmap.nodeinfo(
            rpc_endpoint=neofs_env.sn_rpc,
            wallet=sn.wallet.path,
        )
        .stdout.strip()
    )
    assert node_info, "public API nodeinfo response is empty"
    return node_info


def interesting_sn_logs(logs: str) -> str:
    return "\n".join(
        line
        for line in logs.splitlines()
        if any(token in line.lower() for token in ("error", "warn", "reload", "sighup", "tls", "certificate"))
    )


def assert_logs_contain(neofs_env: NeoFSEnv, expected_log_fragments: list[str]) -> None:
    logs = read_sn_logs(neofs_env)
    for fragment in expected_log_fragments:
        assert fragment in logs, (
            f"expected '{fragment}' in storage node logs: {neofs_env.storage_nodes[0].stderr}\n"
            f"{interesting_sn_logs(logs)}"
        )


def assert_reload_failed(neofs_env: NeoFSEnv, expected_log_fragments: list[str]) -> None:
    logs = read_sn_logs(neofs_env)
    assert "configuration reload" in logs, (
        f"expected failed configuration reload in logs: {neofs_env.storage_nodes[0].stderr}\n"
        f"{interesting_sn_logs(logs)}"
    )
    assert_logs_contain(neofs_env, expected_log_fragments)


def assert_reload_succeeded(neofs_env: NeoFSEnv, expected_log_fragments: list[str] | None = None) -> None:
    logs = read_sn_logs(neofs_env)
    assert "configuration has been reloaded successfully" in logs, (
        f"expected successful configuration reload in logs: {neofs_env.storage_nodes[0].stderr}"
    )
    if expected_log_fragments:
        assert_logs_contain(neofs_env, expected_log_fragments)


def fetch_tls_peer_certificate_der(host: str, port: str | int) -> bytes:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    with socket.create_connection((host, int(port)), timeout=5) as sock:
        with context.wrap_socket(sock, server_hostname=host) as tls_sock:
            der_cert = tls_sock.getpeercert(binary_form=True)
    assert der_cert, f"no TLS certificate presented by {host}:{port}"
    return der_cert


def assert_generated_self_signed_certificate(neofs_env: NeoFSEnv, endpoint: str) -> None:
    host, port = endpoint.split(":")
    fetch_tls_peer_certificate_der(host, port)
    assert_logs_contain(neofs_env, ["using self-signed TLS certificate"])


def test_sighup_fschain_endpoint_reload(neofs_env_single_sn: NeoFSEnv):
    neofs_env = neofs_env_single_sn
    current_endpoint = f"ws://{neofs_env.fschain_rpc}/ws"
    unreachable_endpoint = f"ws://{neofs_env.domain}:{NeoFSEnv.get_available_port()}/ws"

    with allure.step("Reload FS chain endpoints to include an extra unreachable RPC"):
        sn_config = load_sn_config(neofs_env)
        sn_config["fschain"]["endpoints"] = [unreachable_endpoint, current_endpoint]
        save_sn_config(neofs_env, sn_config)
        send_sighup(neofs_env)
        assert_storage_node_alive(neofs_env)
        assert_reload_succeeded(neofs_env)

    with allure.step("Reload FS chain endpoints back to the working IR only"):
        sn_config = load_sn_config(neofs_env)
        sn_config["fschain"]["endpoints"] = [current_endpoint]
        save_sn_config(neofs_env, sn_config)
        send_sighup(neofs_env)
        assert_storage_node_alive(neofs_env)
        assert_public_api_works(neofs_env)


def test_sighup_node_attrs_update(neofs_env_single_sn: NeoFSEnv):
    neofs_env = neofs_env_single_sn

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
        sn_config = load_sn_config(neofs_env)
        sn_config["node"]["attributes"][0] = "UN-LOCODE:FI HEL"
        save_sn_config(neofs_env, sn_config)
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


def test_sighup_disable_metrics(neofs_env_single_sn: NeoFSEnv):
    neofs_env = neofs_env_single_sn

    with allure.step("Disable pprof and prometheus"):
        sn_config = load_sn_config(neofs_env)
        sn_config["prometheus"]["enabled"] = False
        sn_config["pprof"]["enabled"] = False
        save_sn_config(neofs_env, sn_config)
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
        save_sn_config(neofs_env, sn_config)
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


def test_sighup_invalid_tls_certificate_stops_node(neofs_env_single_sn: NeoFSEnv):
    neofs_env = neofs_env_single_sn
    sn = neofs_env.storage_nodes[0]

    with allure.step("Ensure public API works before reload"):
        assert_public_api_works(neofs_env)

    with allure.step("Apply gRPC TLS config with an invalid certificate"):
        invalid_cert_path = neofs_env._generate_temp_file(sn.sn_dir, extension="pem", prefix="invalid_tls_cert")
        with open(invalid_cert_path, "w") as cert_file:
            cert_file.write("not-a-tls-certificate\n")
        sn_config = load_sn_config(neofs_env)
        sn_config["grpc"] = [
            {
                "endpoint": sn.endpoint,
                "tls": {
                    "enabled": True,
                    "certificate": str(invalid_cert_path),
                },
            }
        ]
        save_sn_config(neofs_env, sn_config)
        send_sighup(neofs_env)

    with allure.step("Failed TLS reload must shut the node down"):
        assert_storage_node_stopped(neofs_env)
        assert_reload_failed(neofs_env, ["reload gRPC configuration"])


def test_sighup_self_signed_tls_change_requires_restart(neofs_env_single_sn: NeoFSEnv):
    neofs_env = neofs_env_single_sn
    sn = neofs_env.storage_nodes[0]

    with allure.step("Start SN with TLS enabled and no configured certificate"):
        sn.stop()
        sn_config = load_sn_config(neofs_env)
        sn_config["grpc"] = [
            {
                "endpoint": sn.endpoint,
                "tls": {"enabled": True},
            }
        ]
        save_sn_config(neofs_env, sn_config)
        sn.tls_enabled = True
        sn.tls_endpoint = sn.endpoint
        sn.start(fresh=False)

    with allure.step("SN starts successfully and serves the generated self-signed certificate"):
        assert_storage_node_alive(neofs_env)
        assert_generated_self_signed_certificate(neofs_env, sn.endpoint)

    with allure.step("Changing self-signed TLS endpoints via SIGHUP requires a restart"):
        changed_endpoint = f"{neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        sn_config = load_sn_config(neofs_env)
        sn_config["grpc"] = [
            {
                "endpoint": changed_endpoint,
                "tls": {"enabled": True},
            }
        ]
        save_sn_config(neofs_env, sn_config)
        send_sighup(neofs_env)
        assert_storage_node_stopped(neofs_env)
        assert_reload_failed(neofs_env, ["changing self-signed TLS endpoints requires node restart"])


def test_sighup_occupied_endpoint_keeps_public_api(neofs_env_single_sn: NeoFSEnv):
    neofs_env = neofs_env_single_sn
    sn = neofs_env.storage_nodes[0]
    occupied_port = NeoFSEnv.get_available_port()
    occupied_endpoint = f"{neofs_env.domain}:{occupied_port}"

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as occupied_socket:
        occupied_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        occupied_socket.bind(("127.0.0.1", int(occupied_port)))
        occupied_socket.listen(1)

        with allure.step("Ensure public API works before reload"):
            assert_public_api_works(neofs_env)

        with allure.step("Add occupied gRPC endpoint via SIGHUP"):
            sn_config = load_sn_config(neofs_env)
            sn_config["grpc"] = [
                {"endpoint": sn.endpoint},
                {"endpoint": occupied_endpoint},
            ]
            save_sn_config(neofs_env, sn_config)
            send_sighup(neofs_env)

        with allure.step("Failed bind of an extra endpoint must keep existing public API"):
            assert_storage_node_alive(neofs_env)
            assert_public_api_works(neofs_env)
            assert_reload_succeeded(
                neofs_env,
                ["failed to start gRPC server", occupied_endpoint, "address already in use"],
            )


def test_sighup_retries_grpc_config_after_failed_reload(neofs_env_single_sn: NeoFSEnv):
    neofs_env = neofs_env_single_sn
    sn = neofs_env.storage_nodes[0]
    occupied_port = NeoFSEnv.get_available_port()
    occupied_endpoint = f"{neofs_env.domain}:{occupied_port}"
    retry_port = NeoFSEnv.get_available_port()
    retry_endpoint = f"{neofs_env.domain}:{retry_port}"

    with allure.step("Fail to bind a new gRPC endpoint while keeping the existing one"):
        occupied_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        occupied_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        occupied_socket.bind(("127.0.0.1", int(occupied_port)))
        occupied_socket.listen(1)
        try:
            sn_config = load_sn_config(neofs_env)
            sn_config["grpc"] = [
                {"endpoint": sn.endpoint},
                {"endpoint": occupied_endpoint},
            ]
            save_sn_config(neofs_env, sn_config)
            send_sighup(neofs_env)
            assert_storage_node_alive(neofs_env)
            assert_public_api_works(neofs_env)
            assert_reload_succeeded(
                neofs_env,
                ["failed to start gRPC server", occupied_endpoint, "address already in use"],
            )
        finally:
            occupied_socket.close()

    with allure.step("Next SIGHUP retries and applies a valid new endpoint"):
        sn_config = load_sn_config(neofs_env)
        sn_config["grpc"] = [
            {"endpoint": sn.endpoint},
            {"endpoint": retry_endpoint},
        ]
        save_sn_config(neofs_env, sn_config)
        send_sighup(neofs_env)
        assert_storage_node_alive(neofs_env)
        assert_public_api_works(neofs_env)
        assert is_port_in_use(neofs_env.domain, retry_port), (
            f"new gRPC endpoint {retry_endpoint} was not applied after retry"
        )


@pytest.mark.parametrize(
    ("case_name", "mutate_config", "expected_log_fragments"),
    [
        (
            "no_public_grpc_endpoints",
            lambda cfg, env: cfg.__setitem__(
                "grpc",
                [
                    {"endpoint": env.storage_nodes[0].endpoint},
                    {"endpoint": ""},
                ],
            ),
            ["validate configuration", "empty/not set endpoint"],
        ),
        (
            "no_fschain_endpoints",
            lambda cfg, _env: cfg["fschain"].__setitem__("endpoints", []),
            ["validate configuration", "no FS chain RPC endpoints"],
        ),
    ],
    ids=["no_public_grpc_endpoints", "no_fschain_endpoints"],
)
def test_sighup_rejects_invalid_required_settings(
    neofs_env_single_sn: NeoFSEnv,
    case_name: str,
    mutate_config: Callable[[dict, NeoFSEnv], None],
    expected_log_fragments: list[str],
):
    neofs_env = neofs_env_single_sn

    with allure.step(f"Apply invalid config: {case_name}"):
        sn_config = load_sn_config(neofs_env)
        mutate_config(sn_config, neofs_env)
        save_sn_config(neofs_env, sn_config)
        send_sighup(neofs_env)

    with allure.step("Invalid config must shut the node down"):
        assert_storage_node_stopped(neofs_env)
        assert_reload_failed(neofs_env, expected_log_fragments)


@pytest.mark.parametrize(
    ("case_name", "invalid_attribute"),
    [
        ("malformed_attribute", "not-a-key-value"),
        ("invalid_unlocode", "UN-LOCODE:ZZ ZZZ"),
    ],
)
def test_sighup_invalid_node_attributes_keep_previous(
    neofs_env_single_sn: NeoFSEnv,
    case_name: str,
    invalid_attribute: str,
):
    neofs_env = neofs_env_single_sn

    with allure.step("Capture currently applied attributes"):
        node_info_before = assert_public_api_works(neofs_env)
        assert "UN-LOCODE=RU MOW" in node_info_before

    with allure.step(f"Apply invalid node attributes via SIGHUP: {case_name}"):
        sn_config = load_sn_config(neofs_env)
        sn_config["node"]["attributes"] = [invalid_attribute, "Price:22"]
        save_sn_config(neofs_env, sn_config)
        send_sighup(neofs_env)

    with allure.step("Invalid attributes must shut the node down"):
        assert_storage_node_stopped(neofs_env)
        assert_reload_failed(neofs_env, ["update node attributes"])
