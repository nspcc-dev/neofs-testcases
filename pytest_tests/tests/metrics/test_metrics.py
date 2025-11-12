import socket

from neofs_testlib.env.env import NeoFSEnv
from tenacity import retry, stop_after_attempt, wait_fixed


@retry(wait=wait_fixed(1), stop=stop_after_attempt(50), reraise=True)
def is_port_in_use(host: str, port: str, error_msg: str):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.settimeout(1)
            s.connect((host, int(port)))
        except (socket.timeout, ConnectionRefusedError):
            raise AssertionError(error_msg)


def test_ports_for_metrics_are_utilized(neofs_env_with_mainchain: NeoFSEnv):
    is_port_in_use(
        neofs_env_with_mainchain.main_chain.pprof_address.split(":")[0],
        neofs_env_with_mainchain.main_chain.pprof_address.split(":")[1],
        "main chain pprof port is not utilized",
    )

    is_port_in_use(
        neofs_env_with_mainchain.main_chain.prometheus_address.split(":")[0],
        neofs_env_with_mainchain.main_chain.prometheus_address.split(":")[1],
        "main chain prometheus port is not utilized",
    )

    is_port_in_use(
        neofs_env_with_mainchain.inner_ring_nodes[0].pprof_address.split(":")[0],
        neofs_env_with_mainchain.inner_ring_nodes[0].pprof_address.split(":")[1],
        "inner ring node pprof port is not utilized",
    )

    is_port_in_use(
        neofs_env_with_mainchain.inner_ring_nodes[0].prometheus_address.split(":")[0],
        neofs_env_with_mainchain.inner_ring_nodes[0].prometheus_address.split(":")[1],
        "inner ring node prometheus port is not utilized",
    )

    is_port_in_use(
        neofs_env_with_mainchain.s3_gw.pprof_address.split(":")[0],
        neofs_env_with_mainchain.s3_gw.pprof_address.split(":")[1],
        "s3 gw pprof port is not utilized",
    )

    is_port_in_use(
        neofs_env_with_mainchain.s3_gw.prometheus_address.split(":")[0],
        neofs_env_with_mainchain.s3_gw.prometheus_address.split(":")[1],
        "s3 gw prometheus port is not utilized",
    )

    is_port_in_use(
        neofs_env_with_mainchain.rest_gw.pprof_address.split(":")[0],
        neofs_env_with_mainchain.rest_gw.pprof_address.split(":")[1],
        "rest gw pprof port is not utilized",
    )

    is_port_in_use(
        neofs_env_with_mainchain.rest_gw.prometheus_address.split(":")[0],
        neofs_env_with_mainchain.rest_gw.prometheus_address.split(":")[1],
        "rest gw prometheus port is not utilized",
    )

    for sn in neofs_env_with_mainchain.storage_nodes:
        is_port_in_use(
            sn.pprof_address.split(":")[0], sn.pprof_address.split(":")[1], "storage node pprof port is not utilized"
        )

        is_port_in_use(
            sn.prometheus_address.split(":")[0],
            sn.prometheus_address.split(":")[1],
            "storage node prometheus port is not utilized",
        )
