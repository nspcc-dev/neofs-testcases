import os
import time
import uuid

import allure
from common import ASSETS_DIR, SIMPLE_OBJ_SIZE, STORAGE_GC_TIME


def create_file_with_content(file_path: str = None, content: str = None) -> str:
    mode = "w+"
    if not content:
        content = os.urandom(SIMPLE_OBJ_SIZE)
        mode = "wb"

    if not file_path:
        file_path = f"{os.getcwd()}/{ASSETS_DIR}/{str(uuid.uuid4())}"
    else:
        if not os.path.exists(os.path.dirname(file_path)):
            os.makedirs(os.path.dirname(file_path))

    with open(file_path, mode) as out_file:
        out_file.write(content)

    return file_path


def get_file_content(file_path: str, content_len: int = None, mode="r") -> str:
    with open(file_path, mode) as out_file:
        if content_len:
            content = out_file.read(content_len)
        else:
            content = out_file.read()

    return content


def split_file(file_path: str, parts: int) -> list[str]:
    files = []
    with open(file_path, "rb") as in_file:
        data = in_file.read()

    content_size = len(data)

    chunk_size = int((content_size + parts) / parts)
    part_id = 1
    for start_position in range(0, content_size + 1, chunk_size):
        part_file_name = f"{file_path}_part_{part_id}"
        files.append(part_file_name)
        with open(part_file_name, "wb") as out_file:
            out_file.write(data[start_position : start_position + chunk_size])
        part_id += 1

    return files


def parse_time(value: str) -> int:
    if value.endswith("s"):
        return int(value[:-1])

    if value.endswith("m"):
        return int(value[:-1]) * 60


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
    # We add 15 seconds to allow some time for GC process itself
    wait_time = parse_time(STORAGE_GC_TIME)
    with allure.step(f"Wait {wait_time}s until GC completes on storage nodes"):
        time.sleep(wait_time)
