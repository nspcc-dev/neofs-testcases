import time
from datetime import datetime, timezone

from neofs_testlib.blockchain.rpc_client import RPCClient
from neofs_testlib.env.env import NeoFSEnv


def fs_chain_unix_time(neofs_env: NeoFSEnv) -> int:
    """Unix time of the latest FS chain block, in seconds."""
    client = RPCClient(f"http://{neofs_env.fschain_rpc}")
    height = client.get_block_count()
    header = client.get_block_header(height - 1)
    return int(header["time"]) // 1000


def rfc3339_utc(unix_seconds: int) -> str:
    """Format a Unix timestamp as UTC RFC3339 with a Z suffix."""
    return datetime.fromtimestamp(unix_seconds, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def wait_until_chain_time_after(neofs_env: NeoFSEnv, unix_seconds: int) -> None:
    """Wait until FS chain time is later than the given Unix timestamp."""
    target = unix_seconds + 1
    deadline = time.monotonic() + max(60, target - int(time.time()) + 90)
    last = 0
    while time.monotonic() < deadline:
        last = fs_chain_unix_time(neofs_env)
        if last > target:
            return
        time.sleep(1)
    raise AssertionError(f"FS chain time {last} did not pass {target}")
