import re
import time

import allure
from helpers.common import STORAGE_GC_TIME


def parse_time(value: str) -> int:
    """Converts time interval in text form into time interval as number of seconds.

    Args:
        value: time interval as text.

    Returns:
        Number of seconds in the parsed time interval.
    """
    value = value.lower()

    for suffix in ["s", "sec"]:
        if value.endswith(suffix):
            return int(value[: -len(suffix)])

    for suffix in ["m", "min"]:
        if value.endswith(suffix):
            return int(value[: -len(suffix)]) * 60

    for suffix in ["h", "hr", "hour"]:
        if value.endswith(suffix):
            return int(value[: -len(suffix)]) * 60 * 60

    raise ValueError(f"Unknown units in time value '{value}'")


def parse_version(version):
    """Parses a version string like '0.44.2-231-g1df00450-dirty' into a sortable tuple."""
    match = re.match(r"(\d+)\.(\d+)\.(\d+)(?:-(\d+)-g([a-f0-9]+))?(?:-dirty)?", version)
    if not match:
        raise ValueError(f"Invalid version format: {version}")

    major, minor, patch, commits, commit_hash = match.groups()
    return (
        int(major),
        int(minor),
        int(patch),
        int(commits) if commits else 0,
        commit_hash or "",
    )


def placement_policy_from_container(container_info: str) -> str:
    """
    Get placement policy from container info:

        container ID: j7k4auNHRmiPMSmnH2qENLECD2au2y675fvTX6csDwd
        version: 2.12
        owner ID: NQ8HUxE5qEj7UUvADj7z9Z7pcvJdjtPwuw
        basic ACL: 0fbfbfff (eacl-public-read-write)
        attribute: Timestamp=1656340345 (2022-06-27 17:32:25 +0300 MSK)
        nonce: 1c511e88-efd7-4004-8dbf-14391a5d375a
        placement policy:
        REP 1 IN LOC_PLACE
        CBF 1
        SELECT 1 FROM LOC_SW AS LOC_PLACE
        FILTER Country EQ Sweden AS LOC_SW

    Args:
        container_info: output from neofs-cli container get command

    Returns:
        placement policy as a string
    """
    assert ":" in container_info, f"Could not find placement rule in the output {container_info}"
    return container_info.split(":")[-1].replace("\n", " ").strip()


def wait_for_gc_pass_on_storage_nodes() -> None:
    wait_time = parse_time(STORAGE_GC_TIME)
    with allure.step(f"Wait {wait_time}s until GC completes on storage nodes"):
        time.sleep(wait_time)


def parse_node_height(stdout: str) -> tuple[float, float]:
    lines = stdout.strip().split("\n")
    block_height = float(lines[0].split(": ")[1].strip())
    state = float(lines[1].split(": ")[1].strip())
    return block_height, state
