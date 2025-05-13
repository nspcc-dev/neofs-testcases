import json
import pathlib
import re
from dataclasses import dataclass
from io import StringIO

import allure
import pytest
import yaml
from dotenv import dotenv_values
from helpers.utility import parse_version
from neofs_testlib.env.env import NeoFSEnv, StorageNode

SHARD_PREFIX = "NEOFS_STORAGE_SHARD_"
SHARD_PREFIX_POST_0_45_2 = "NEOFS_STORAGE_SHARDS_"
BLOBSTOR_PREFIX = "_BLOBSTOR_"


@dataclass
class Blobstor:
    path: str
    path_type: str

    def __eq__(self, other) -> bool:
        if not isinstance(other, self.__class__):
            raise RuntimeError(f"Only two {self.__class__.__name__} instances can be compared")
        return self.path == other.path and self.path_type == other.path_type

    def __hash__(self):
        return hash((self.path, self.path_type))

    @staticmethod
    def from_config_object(section: dict, shard_id: str, blobstor_id: str, shard_prefix: str = SHARD_PREFIX):
        var_prefix = f"{shard_prefix}{shard_id}{BLOBSTOR_PREFIX}{blobstor_id}"
        return Blobstor(section[f"{var_prefix}_PATH"], section[f"{var_prefix}_TYPE"])


@dataclass
class Shard:
    blobstor: list[Blobstor]
    metabase: str
    writecache: str

    def __eq__(self, other) -> bool:
        if not isinstance(other, self.__class__):
            raise RuntimeError(f"Only two {self.__class__.__name__} instances can be compared")
        return (
            set(self.blobstor) == set(other.blobstor)
            and self.metabase == other.metabase
            and self.writecache == other.writecache
        )

    def __hash__(self):
        return hash((self.metabase, self.writecache))

    @staticmethod
    def _get_blobstor_count_from_section(config_object: dict, shard_id: int, shard_prefix: str = SHARD_PREFIX):
        pattern = f"{shard_prefix}{shard_id}{BLOBSTOR_PREFIX}"
        blobstors = {key[: len(pattern) + 2] for key in config_object.keys() if pattern in key}
        return len(blobstors)

    @staticmethod
    def from_config_object(config_object: dict, shard_id: int, shard_prefix: str = SHARD_PREFIX):
        var_prefix = f"{shard_prefix}{shard_id}"

        blobstor_count = Shard._get_blobstor_count_from_section(config_object, shard_id, shard_prefix)
        blobstors = [
            Blobstor.from_config_object(config_object, shard_id, blobstor_id, shard_prefix)
            for blobstor_id in range(blobstor_count)
        ]

        write_cache_enabled = config_object[f"{var_prefix}_WRITECACHE_ENABLED"].lower() in (
            "true",
            "1",
            "yes",
            "y",
            "on",
        )

        return Shard(
            blobstors,
            config_object[f"{var_prefix}_METABASE_PATH"],
            config_object[f"{var_prefix}_WRITECACHE_PATH"] if write_cache_enabled else "",
        )

    @staticmethod
    def from_object(shard):
        metabase = shard["metabase"]["path"] if "path" in shard["metabase"] else shard["metabase"]
        if "enabled" in shard["writecache"]:
            writecache = shard["writecache"]["path"] if shard["writecache"]["enabled"] else ""
        else:
            writecache = shard["writecache"]["path"] if "path" in shard["writecache"] else shard["writecache"]

        return Shard(
            blobstor=[Blobstor(path=blobstor["path"], path_type=blobstor["type"]) for blobstor in shard["blobstor"]],
            metabase=metabase,
            writecache=writecache,
        )

    @staticmethod
    def from_object_post_0_45_2(shard):
        metabase = shard["metabase"]["path"] if "path" in shard["metabase"] else shard["metabase"]
        if "enabled" in shard["writecache"]:
            writecache = shard["writecache"]["path"] if shard["writecache"]["enabled"] else ""
        else:
            writecache = shard["writecache"]["path"] if "path" in shard["writecache"] else shard["writecache"]

        return Shard(
            blobstor=[Blobstor(path=shard["blobstor"]["path"], path_type=shard["blobstor"]["type"])],
            metabase=metabase,
            writecache=writecache,
        )


def shards_from_yaml(contents: str) -> list[Shard]:
    config = yaml.safe_load(contents)
    return [Shard.from_object(shard) for shard in config["storage"]["shard"].values()]


def shards_from_yaml_post_0_45_2(contents: str) -> list[Shard]:
    config = yaml.safe_load(contents)
    return [Shard.from_object_post_0_45_2(shard) for shard in config["storage"]["shards"]]


def shards_from_env(contents: str) -> list[Shard]:
    configObj = dotenv_values(stream=StringIO(contents))

    pattern = rf"{SHARD_PREFIX}\d*"
    num_shards = len(set(re.findall(pattern, contents)))

    return [Shard.from_config_object(configObj, shard_id) for shard_id in range(num_shards)]


def shards_from_env_post_0_45_2(contents: str) -> list[Shard]:
    configObj = dotenv_values(stream=StringIO(contents))

    pattern = rf"{SHARD_PREFIX_POST_0_45_2}\d*"
    num_shards = len(set(re.findall(pattern, contents)))

    return [Shard.from_config_object(configObj, shard_id, SHARD_PREFIX_POST_0_45_2) for shard_id in range(num_shards)]


class TestControlShard:
    @staticmethod
    def get_shards_from_config(neofs_env: NeoFSEnv, node: StorageNode) -> list[Shard]:
        config_file = node.storage_node_config_path
        file_type = pathlib.Path(config_file).suffix
        contents = neofs_env.shell.exec(f"cat {config_file}").stdout

        yaml_parser = shards_from_yaml
        if parse_version(neofs_env.get_binary_version(neofs_env.neofs_node_path)) > parse_version("0.45.2"):
            yaml_parser = shards_from_yaml_post_0_45_2

        shards_from_env_parser = shards_from_env
        if parse_version(neofs_env.get_binary_version(neofs_env.neofs_node_path)) > parse_version("0.45.2"):
            shards_from_env_parser = shards_from_env_post_0_45_2

        parser_method = {
            ".env": shards_from_env_parser,
            ".yaml": yaml_parser,
            ".yml": yaml_parser,
        }

        shards = parser_method[file_type](contents)
        return shards

    @staticmethod
    def get_shards_from_cli(neofs_env: NeoFSEnv, node: StorageNode) -> list[Shard]:
        cli = neofs_env.neofs_cli(node.cli_config)
        result = cli.shards.list(
            endpoint=node.control_endpoint,
            wallet=node.wallet.path,
            json_mode=True,
        )
        if parse_version(neofs_env.get_binary_version(neofs_env.neofs_node_path)) > parse_version("0.45.2"):
            return [Shard.from_object_post_0_45_2(shard) for shard in json.loads(result.stdout.strip())]
        return [Shard.from_object(shard) for shard in json.loads(result.stdout.strip())]

    @pytest.mark.sanity
    @allure.title("All shards are available")
    def test_control_shard(self, neofs_env: NeoFSEnv):
        for storage_node in neofs_env.storage_nodes:
            shards_from_config = self.get_shards_from_config(neofs_env, storage_node)
            shards_from_cli = self.get_shards_from_cli(neofs_env, storage_node)
            assert set(shards_from_config) == set(shards_from_cli)
