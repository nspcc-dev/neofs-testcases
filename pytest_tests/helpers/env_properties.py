import logging
import re

import allure
from pytest import Config

logger = logging.getLogger("NeoLogger")


@allure.step("Read environment.properties")
def read_env_properties(config: Config) -> dict:
    environment_dir = config.getoption("--alluredir")
    if not environment_dir:
        return None

    file_path = f"{environment_dir}/environment.properties"
    with open(file_path, "r") as file:
        raw_content = file.read()

    env_properties = {}
    for line in raw_content.split("\n"):
        m = re.match("(.*?)=(.*)", line)
        if not m:
            logger.warning(f"Could not parse env property from {line}")
            continue
        key, value = m.group(1), m.group(2)
        env_properties[key] = value
    return env_properties


@allure.step("Update data in environment.properties")
def save_env_properties(config: Config, env_data: dict) -> None:
    environment_dir = config.getoption("--alluredir")
    if not environment_dir:
        return None

    file_path = f"{environment_dir}/environment.properties"
    with open(file_path, "a+") as env_file:
        for env, env_value in env_data.items():
            env_file.write(f"{env}={env_value}\n")
