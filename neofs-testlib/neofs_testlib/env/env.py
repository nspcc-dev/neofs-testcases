import datetime
import fcntl
import json
import logging
import os
import pickle
import platform
import random
import shutil
import socket
import stat
import string
import subprocess
import sys
import tarfile
import threading
import time
import uuid
from collections import namedtuple
from dataclasses import dataclass
from enum import Enum
from importlib.resources import files
from pathlib import Path
from subprocess import Popen, TimeoutExpired
from typing import Optional

import allure
import jinja2
import psutil
import pytest
import requests
import yaml
from helpers.common import (
    ALLOCATED_PORTS_FILE,
    ALLOCATED_PORTS_LOCK_FILE,
    BINARY_DOWNLOADS_LOCK_FILE,
    COMPLEX_OBJECT_CHUNKS_COUNT,
    COMPLEX_OBJECT_TAIL_SIZE,
    DEFAULT_OBJECT_OPERATION_TIMEOUT,
    DEFAULT_REST_OPERATION_TIMEOUT,
    SIMPLE_OBJECT_SIZE,
    get_assets_dir_path,
)
from helpers.neofs_verbs import get_netmap_netinfo
from helpers.utility import parse_version
from tenacity import retry, stop_after_attempt, wait_fixed

from neofs_testlib.cli import NeofsAdm, NeofsCli, NeofsLens, NeoGo
from neofs_testlib.shell import LocalShell
from neofs_testlib.utils import wallet as wallet_utils

logger = logging.getLogger("neofs.testlib.env")
_thread_lock = threading.Lock()


@dataclass
class NodeWallet:
    path: str
    address: str
    password: str
    cli_config: str = None
    neo_go_config: str = None


class WalletType(Enum):
    STORAGE = 1
    ALPHABET = 2


class ObjectType(Enum):
    SIMPLE = "simple_object_size"
    COMPLEX = "complex_object_size"


def terminate_process(process: Popen):
    process.terminate()
    try:
        process.wait(timeout=60)
    except TimeoutExpired as e:
        logger.info(f"Didn't manage to terminate process gracefully: {e}")
        process.kill()
        process.wait(timeout=60)


class NeoFSEnv:
    def __init__(self, neofs_env_config: dict = None):
        self._id = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d-%H-%M-%S")
        self._env_dir = f"{get_assets_dir_path()}/env_files/neofs-env-{self._id}"

        self.domain = "localhost"
        self.default_password = "password"
        self.shell = LocalShell()
        # utilities
        self.neofs_env_config = neofs_env_config
        self.neofs_adm_path = os.getenv("NEOFS_ADM_BIN", "./neofs-adm")
        self.neofs_cli_path = os.getenv("NEOFS_CLI_BIN", "./neofs-cli")
        self.neofs_lens_path = os.getenv("NEOFS_LENS_BIN", "./neofs-lens")
        self.neo_go_path = os.getenv("NEO_GO_BIN", "./neo-go")
        self.neofs_ir_path = os.getenv("NEOFS_IR_BIN", "./neofs-ir")
        self.neofs_node_path = os.getenv("NEOFS_NODE_BIN", "./neofs-node")
        self.neofs_s3_authmate_path = os.getenv("NEOFS_S3_AUTHMATE_BIN", "./neofs-s3-authmate")
        self.neofs_s3_gw_path = os.getenv("NEOFS_S3_GW_BIN", "./neofs-s3-gw")
        self.neofs_rest_gw_path = os.getenv("NEOFS_REST_GW_BIN", "./neofs-rest-gw")
        self.warp_path = os.getenv("WARP_BIN", "./warp")
        self.alphabet_wallets_dir = self._generate_temp_dir(prefix="ir_alphabet")
        self.neofs_contract_dir = os.getenv("NEOFS_CONTRACT_DIR", "./neofs-contract")
        self._init_default_wallet()
        # nodes inside env
        self.storage_nodes = []
        self.inner_ring_nodes = []
        self.s3_gw = None
        self.rest_gw = None
        self.main_chain = None
        self.max_object_size = None

    @property
    def fschain_rpc(self):
        if len(self.inner_ring_nodes) > 0:
            return self.inner_ring_nodes[0].endpoint
        raise ValueError("No Inner Ring nodes configured in this env")

    @property
    def sn_rpc(self):
        if len(self.storage_nodes) > 0:
            return self.storage_nodes[0].endpoint
        raise ValueError("No storage nodes configured in this env")

    @property
    def network_config(self):
        if len(self.inner_ring_nodes) > 0:
            return self.inner_ring_nodes[0].network_config
        raise ValueError("No Inner Ring nodes configured in this env")

    def neofs_adm(self, network_config: Optional[str] = None) -> NeofsAdm:
        if not network_config:
            if len(self.inner_ring_nodes) > 0:
                network_config = self.network_config
            else:
                raise ValueError("Network config need to be specified for neofs-adm commands")
        return NeofsAdm(self.shell, self.neofs_adm_path, network_config)

    def neofs_cli(self, cli_config_path: str) -> NeofsCli:
        return NeofsCli(self.shell, self.neofs_cli_path, cli_config_path)

    def neofs_lens(
        self,
    ) -> NeofsLens:
        return NeofsLens(self.shell, self.neofs_lens_path)

    def neo_go(self) -> NeoGo:
        return NeoGo(self.shell, self.neo_go_path)

    def generate_cli_config(self, wallet: NodeWallet):
        cli_config_path = self._generate_temp_file(os.path.dirname(wallet.path), extension="yml", prefix="cli_config")
        NeoFSEnv.generate_config_file(config_template="cli_cfg.yaml", config_path=cli_config_path, wallet=wallet)
        return cli_config_path

    def generate_neo_go_config(self, wallet: NodeWallet):
        neo_go_config_path = self._generate_temp_file(
            os.path.dirname(wallet.path), extension="yml", prefix="neo_go_config"
        )
        NeoFSEnv.generate_config_file(config_template="neo_go_cfg.yaml", config_path=neo_go_config_path, wallet=wallet)
        return neo_go_config_path

    @allure.step("Deploy inner ring nodes")
    def deploy_inner_ring_nodes(self, count=1, with_main_chain=False, chain_meta_data=False, sn_validator_url=None):
        for _ in range(count):
            new_inner_ring_node = InnerRing(
                self,
                len(self.inner_ring_nodes) + 1,
                chain_meta_data=chain_meta_data,
                sn_validator_url=sn_validator_url,
            )
            new_inner_ring_node.generate_network_config()
            self.inner_ring_nodes.append(new_inner_ring_node)

        alphabet_wallets = self.generate_alphabet_wallets(self.inner_ring_nodes[0].network_config, size=count)

        for ir_node in reversed(self.inner_ring_nodes):
            ir_node.alphabet_wallet = alphabet_wallets.pop()

        for ir_node in self.inner_ring_nodes:
            ir_node.generate_cli_config()

        if with_main_chain:
            self.main_chain = MainChain(self)
            self.main_chain.start()
            self.deploy_neofs_contract()

        for ir_node in self.inner_ring_nodes:
            ir_node.start(wait_until_ready=False, with_main_chain=with_main_chain)

        with allure.step("Wait until all IR nodes are READY"):
            for ir_node_idx, ir_node in enumerate(self.inner_ring_nodes):
                logger.info(f"Wait until IR: {ir_node} is READY")
                try:
                    ir_node._wait_until_ready()
                except Exception as e:
                    allure.attach.file(ir_node.stderr, name=f"ir{ir_node_idx} node stderr", extension="txt")
                    allure.attach.file(ir_node.stdout, name=f"ir{ir_node_idx} node stdout", extension="txt")
                    raise e

    @allure.step("Deploy storage node")
    def deploy_storage_nodes(self, count=1, node_attrs: Optional[dict] = None, writecache=False):
        logger.info(f"Going to deploy {count} storage nodes")
        deploy_threads = []
        for idx in range(count):
            node_attrs_list = None
            if node_attrs:
                node_attrs_list = node_attrs.get(idx, None)
            new_storage_node = StorageNode(
                self,
                len(self.storage_nodes) + 1,
                writecache=writecache,
                node_attrs=node_attrs_list,
            )
            self.storage_nodes.append(new_storage_node)
            deploy_threads.append(threading.Thread(target=new_storage_node.start))
        for t in deploy_threads:
            t.start()
        logger.info("Wait until storage nodes are deployed")
        try:
            self._wait_until_all_storage_nodes_are_ready()
        except Exception as e:
            for sn in self.storage_nodes:
                allure.attach.file(sn.stderr, name=f"sn{sn.sn_number} stderr", extension="txt")
                allure.attach.file(sn.stdout, name=f"sn{sn.sn_number} stdout", extension="txt")
            raise e
        # tick epoch to speed up storage nodes bootstrap
        self.neofs_adm().fschain.force_new_epoch(
            rpc_endpoint=f"http://{self.fschain_rpc}",
            alphabet_wallets=self.alphabet_wallets_dir,
        )
        for t in deploy_threads:
            t.join()

    @retry(wait=wait_fixed(2), stop=stop_after_attempt(60), reraise=True)
    def _wait_until_all_storage_nodes_are_ready(self):
        ready_counter = 0
        for sn in self.storage_nodes:
            neofs_cli = self.neofs_cli(sn.cli_config)
            result = neofs_cli.control.healthcheck(endpoint=sn.control_endpoint)
            if "Health status: READY" in result.stdout:
                ready_counter += 1
        assert ready_counter == len(self.storage_nodes)

    @allure.step("Deploy s3 gateway")
    def deploy_s3_gw(self):
        self.s3_gw = S3_GW(self)
        self.s3_gw.start()
        allure.attach(str(self.s3_gw), "s3_gw", allure.attachment_type.TEXT, ".txt")

    @allure.step("Deploy rest gateway")
    def deploy_rest_gw(self):
        self.rest_gw = REST_GW(self)
        self.rest_gw.start()
        allure.attach(str(self.rest_gw), "rest_gw", allure.attachment_type.TEXT, ".txt")

    @allure.step("Deploy neofs contract")
    def deploy_neofs_contract(self):
        if len(self.inner_ring_nodes) < 1:
            raise RuntimeError(
                "There should be at least a single IR instance configured(not started) to deploy neofs contract"
            )
        neo_go = self.neo_go()
        neo_go.nep17.balance(
            self.main_chain.wallet.address,
            "GAS",
            f"http://{self.main_chain.rpc_address}",
            wallet_config=self.main_chain.neo_go_config,
        )
        neo_go.nep17.transfer(
            "GAS",
            self.default_wallet.address,
            f"http://{self.main_chain.rpc_address}",
            from_address=self.main_chain.wallet.address,
            amount=9000,
            force=True,
            wallet_config=self.main_chain.neo_go_config,
            await_=True,
        )

        for ir_node in self.inner_ring_nodes:
            accounts = wallet_utils.get_accounts_from_wallet(ir_node.alphabet_wallet.path, self.default_password)
            for acc in accounts:
                neo_go.nep17.transfer(
                    "GAS",
                    acc.address,
                    f"http://{self.main_chain.rpc_address}",
                    from_address=self.main_chain.wallet.address,
                    amount=9000,
                    force=True,
                    wallet_config=self.main_chain.neo_go_config,
                    await_=True,
                )

        pub_keys_of_existing_ir_nodes = " ".join(
            wallet_utils.get_last_public_key_from_wallet_with_neogo(
                self.neo_go(),
                ir_node.alphabet_wallet.path,
            ).splitlines()
        )
        result = neo_go.contract.deploy(
            input_file=f"{self.neofs_contract_dir}/neofs/contract.nef",
            manifest=f"{self.neofs_contract_dir}/neofs/manifest.json",
            force=True,
            rpc_endpoint=f"http://{self.main_chain.rpc_address}",
            post_data=f"[ true ffffffffffffffffffffffffffffffffffffffff [ {pub_keys_of_existing_ir_nodes} ] [ InnerRingCandidateFee 10 WithdrawFee 10 ] ]",
            wallet_config=self.default_wallet_neogo_config,
        )
        contract_hash = result.stdout.split("Contract: ")[-1].strip()
        self.main_chain.neofs_contract_hash = contract_hash
        assert self.main_chain.neofs_contract_hash, "Couldn't calculate neofs contract hash"
        result = neo_go.util.convert(contract_hash).stdout
        for line in result.splitlines():
            if "LE ScriptHash to Address" in line:
                self.main_chain.neofs_contract_address = line.split("LE ScriptHash to Address")[-1].strip()
                break
        assert self.main_chain.neofs_contract_address, "Couldn't calculate neofs contract address"

    @allure.step("Generate storage wallet")
    def generate_storage_wallet(
        self,
        prepared_wallet: NodeWallet,
        network_config: Optional[str] = None,
        label: Optional[str] = None,
    ):
        neofs_adm = self.neofs_adm(network_config)

        neofs_adm.fschain.generate_storage_wallet(
            alphabet_wallets=self.alphabet_wallets_dir,
            storage_wallet=prepared_wallet.path,
            initial_gas="10",
            label=label,
        )

        # neo-go requires some attributes to be set
        with open(prepared_wallet.path, "r") as wallet_file:
            wallet_json = json.load(wallet_file)

        wallet_json["name"] = None
        for acc in wallet_json["accounts"]:
            acc["extra"] = None

        with open(prepared_wallet.path, "w") as wallet_file:
            json.dump(wallet_json, wallet_file)
        ###

        prepared_wallet.address = wallet_utils.get_last_address_from_wallet(
            prepared_wallet.path, prepared_wallet.password
        )

    @allure.step("Generate alphabet wallets")
    def generate_alphabet_wallets(
        self, network_config: Optional[str] = None, size: Optional[int] = 1, alphabet_wallets_dir: Optional[str] = None
    ) -> list[NodeWallet]:
        neofs_adm = self.neofs_adm(network_config)

        if not alphabet_wallets_dir:
            alphabet_wallets_dir = self.alphabet_wallets_dir
        neofs_adm.fschain.generate_alphabet(alphabet_wallets=alphabet_wallets_dir, size=size)

        generated_wallets = []

        for generated_wallet in os.listdir(alphabet_wallets_dir):
            # neo3 package requires some attributes to be set
            with open(os.path.join(alphabet_wallets_dir, generated_wallet), "r") as wallet_file:
                wallet_json = json.load(wallet_file)

            wallet_json["name"] = None
            for acc in wallet_json["accounts"]:
                acc["extra"] = None

            with open(os.path.join(alphabet_wallets_dir, generated_wallet), "w") as wallet_file:
                json.dump(wallet_json, wallet_file)

            generated_wallets.append(
                NodeWallet(
                    path=os.path.join(alphabet_wallets_dir, generated_wallet),
                    password=self.default_password,
                    address=wallet_utils.get_last_address_from_wallet(
                        os.path.join(alphabet_wallets_dir, generated_wallet), self.default_password
                    ),
                )
            )
        return generated_wallets

    @allure.step("Kill current neofs env")
    def kill(self):
        if self.rest_gw and self.rest_gw.process:
            self.rest_gw.process.kill()
        if self.s3_gw and self.s3_gw.process:
            self.s3_gw.process.kill()
        if self.main_chain and self.main_chain.process:
            self.main_chain.process.kill()
        for sn in self.storage_nodes:
            if sn.process:
                sn.process.kill()
        for ir in self.inner_ring_nodes:
            if ir.process:
                ir.process.kill()

    def persist(self) -> str:
        persisted_path = self._generate_temp_file(os.path.dirname(self._env_dir), prefix="persisted_env")
        with open(persisted_path, "wb") as fp:
            pickle.dump(self, fp)
        logger.info(f"Persist env at: {persisted_path}")
        return persisted_path

    def log_env_details_to_file(self):
        with open(f"{get_assets_dir_path()}/env_details", "w") as fp:
            env_details = ""

            env_details += f"{self.main_chain}\n"

            for ir_node in self.inner_ring_nodes:
                env_details += f"{ir_node}\n"

            for sn_node in self.storage_nodes:
                env_details += f"{sn_node}\n"

            env_details += f"{self.s3_gw}\n"
            env_details += f"{self.rest_gw}\n"

            allure.attach(env_details, "neofs env details", allure.attachment_type.TEXT, ".txt")
            fp.write(env_details)

    def log_versions_to_allure(self):
        versions = ""
        versions += NeoFSEnv._run_single_command(self.neofs_adm_path, "--version")
        versions += NeoFSEnv._run_single_command(self.neofs_cli_path, "--version")
        versions += NeoFSEnv._run_single_command(self.neo_go_path, "--version")
        versions += NeoFSEnv._run_single_command(self.neofs_ir_path, "--version")
        versions += NeoFSEnv._run_single_command(self.neofs_node_path, "--version")
        versions += NeoFSEnv._run_single_command(self.neofs_s3_authmate_path, "--version")
        versions += NeoFSEnv._run_single_command(self.neofs_s3_gw_path, "--version")
        versions += NeoFSEnv._run_single_command(self.neofs_rest_gw_path, "--version")
        allure.attach(versions, "neofs env versions", allure.attachment_type.TEXT, ".txt")

    @allure.step("Download binaries")
    def download_binaries(self):
        with open(BINARY_DOWNLOADS_LOCK_FILE, "w") as lock_file:
            fcntl.flock(lock_file, fcntl.LOCK_EX)
            try:
                logger.info("Going to download missing binaries and contracts, if needed")
                deploy_threads = []

                binaries = [
                    (self.neofs_adm_path, "neofs_adm"),
                    (self.neofs_cli_path, "neofs_cli"),
                    (self.neofs_lens_path, "neofs_lens"),
                    (self.neo_go_path, "neo_go"),
                    (self.neofs_ir_path, "neofs_ir"),
                    (self.neofs_node_path, "neofs_node"),
                    (self.neofs_s3_authmate_path, "neofs_s3_authmate"),
                    (self.neofs_s3_gw_path, "neofs_s3_gw"),
                    (self.neofs_rest_gw_path, "neofs_rest_gw"),
                    (self.neofs_contract_dir, "neofs_contract"),
                    (self.warp_path, "warp"),
                ]

                for binary in binaries:
                    binary_path, binary_name = binary
                    if not os.path.isfile(binary_path) and not os.path.isdir(binary_path):
                        neofs_binary_params = self.neofs_env_config["binaries"][binary_name]
                        allure_step_name = "Downloading "
                        allure_step_name += f" {neofs_binary_params['repo']}/"
                        allure_step_name += f"{neofs_binary_params['version']}/"
                        allure_step_name += f"{neofs_binary_params['file']}"
                        with allure.step(allure_step_name):
                            deploy_threads.append(
                                threading.Thread(
                                    target=NeoFSEnv.download_binary,
                                    args=(
                                        neofs_binary_params["repo"],
                                        neofs_binary_params["version"],
                                        neofs_binary_params["file"],
                                        binary_path,
                                    ),
                                )
                            )
                    else:
                        logger.info(f"'{binary_path}' already exists, will not be downloaded")

                if len(deploy_threads) > 0:
                    for t in deploy_threads:
                        t.start()
                    logger.info("Wait until all binaries are downloaded")
                    for t in deploy_threads:
                        t.join()
            finally:
                fcntl.flock(lock_file, fcntl.LOCK_UN)

    def _is_binary_compatible(self, expected_platform: str = None, expected_arch: str = None) -> bool:
        if expected_platform is None or expected_arch is None:
            return True
        return expected_platform == sys.platform and expected_arch == platform.machine()

    def _init_default_wallet(self):
        self.default_wallet = NodeWallet(
            path=self._generate_temp_file(self._env_dir, prefix="default_neofs_env_wallet"),
            address="",
            password=self.default_password,
        )
        wallet_utils.init_wallet(self.default_wallet.path, self.default_wallet.password)
        self.default_wallet.address = wallet_utils.get_last_address_from_wallet(
            self.default_wallet.path, self.default_wallet.password
        )
        self.default_wallet_config = self.generate_cli_config(self.default_wallet)
        self.default_wallet_neogo_config = self.generate_neo_go_config(self.default_wallet)

    @classmethod
    def load(cls, persisted_path: str) -> "NeoFSEnv":
        with open(persisted_path, "rb") as fp:
            return pickle.load(fp)

    @classmethod
    def _generate_default_neofs_env_config(cls) -> dict:
        jinja_env = jinja2.Environment()
        config_template = files("neofs_testlib.env.templates").joinpath("neofs_env_config.yaml").read_text()
        jinja_template = jinja_env.from_string(config_template)
        arch = platform.machine()
        if arch == "x86_64":
            config_arch = "linux-amd64"
            warp_binary_name = "warp_Linux_x86_64.tar.gz"
        elif arch == "arm64":
            config_arch = "darwin-arm64"
            warp_binary_name = "warp_Darwin_arm64.tar.gz"
        else:
            raise RuntimeError(f"Unsupported arch: {arch}")
        neofs_env_config = jinja_template.render(arch=config_arch, warp_binary_name=warp_binary_name)
        neofs_env_config = yaml.safe_load(str(neofs_env_config))
        return neofs_env_config

    @classmethod
    @allure.step("Deploy NeoFS Environment")
    def deploy(
        cls,
        neofs_env_config: dict = None,
        with_main_chain=False,
        storage_nodes_count=4,
        inner_ring_nodes_count=1,
        writecache=False,
        with_s3_gw=True,
        with_rest_gw=True,
        request=None,
        chain_meta_data=False,
        sn_validator_url=None,
    ) -> "NeoFSEnv":
        if not neofs_env_config:
            neofs_env_config = cls._generate_default_neofs_env_config()

        neofs_env = NeoFSEnv(neofs_env_config=neofs_env_config)
        neofs_env.download_binaries()
        try:
            neofs_env.deploy_inner_ring_nodes(
                count=inner_ring_nodes_count,
                with_main_chain=with_main_chain,
                chain_meta_data=chain_meta_data,
                sn_validator_url=sn_validator_url,
            )

            if storage_nodes_count:
                node_attrs = {
                    0: ["UN-LOCODE:RU MOW", "Price:22"],
                    1: ["UN-LOCODE:RU LED", "Price:33"],
                    2: ["UN-LOCODE:SE STO", "Price:11"],
                    3: ["UN-LOCODE:FI HEL", "Price:44"],
                }
                adjusted_node_attrs = {k: node_attrs[k] for k in list(node_attrs.keys())[:storage_nodes_count]}
                neofs_env.deploy_storage_nodes(
                    count=storage_nodes_count,
                    node_attrs=adjusted_node_attrs,
                    writecache=writecache,
                )
            if with_main_chain:
                neofs_adm = neofs_env.neofs_adm()
                for sn in neofs_env.storage_nodes:
                    neofs_adm.fschain.refill_gas(
                        rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
                        alphabet_wallets=neofs_env.alphabet_wallets_dir,
                        storage_wallet=sn.wallet.path,
                        gas="10.0",
                    )
                neofs_env.neofs_adm().fschain.set_config(
                    rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
                    alphabet_wallets=neofs_env.alphabet_wallets_dir,
                    post_data="WithdrawFee=5",
                )
            else:
                neofs_env.neofs_adm().fschain.set_config(
                    rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
                    alphabet_wallets=neofs_env.alphabet_wallets_dir,
                    post_data="ContainerFee=0 ContainerAliasFee=0 MaxObjectSize=524288",
                )
            if with_s3_gw:
                neofs_env.deploy_s3_gw()
            if with_rest_gw:
                neofs_env.deploy_rest_gw()
        except Exception as e:
            neofs_env.finalize(request, force_collect_logs=True)
            raise e
        if len(neofs_env.storage_nodes) > 0:
            storage_node = neofs_env.storage_nodes[0]
            net_info = get_netmap_netinfo(
                wallet=storage_node.wallet.path,
                wallet_config=storage_node.cli_config,
                endpoint=storage_node.endpoint,
                shell=neofs_env.shell,
            )
            neofs_env.max_object_size = net_info["maximum_object_size"]
        neofs_env.log_env_details_to_file()
        neofs_env.log_versions_to_allure()
        return neofs_env

    @allure.step("Cleanup neofs env")
    def finalize(self, request, force_collect_logs=False):
        if request.config.getoption("--persist-env"):
            self.persist()
        else:
            if not request.config.getoption("--load-env"):
                self.kill()

        if not request.config.getoption("--persist-env") and not request.config.getoption("--load-env"):
            for ir in self.inner_ring_nodes:
                os.remove(ir.ir_storage_path)

            for sn in self.storage_nodes:
                for shard in sn.shards:
                    os.remove(shard.metabase_path)
                    shutil.rmtree(shard.fstree_path, ignore_errors=True)
                    os.remove(shard.pilorama_path)
                    shutil.rmtree(shard.wc_path, ignore_errors=True)

            if request.session.testsfailed or force_collect_logs:
                shutil.make_archive(os.path.join(get_assets_dir_path(), f"neofs_env_{self._id}"), "zip", self._env_dir)
                allure.attach.file(
                    os.path.join(get_assets_dir_path(), f"neofs_env_{self._id}.zip"),
                    name="neofs env files",
                    extension="zip",
                )

            shutil.rmtree(self._env_dir, ignore_errors=True)

        NeoFSEnv.cleanup_unused_ports()

    @staticmethod
    def generate_config_file(config_template: str, config_path: str, custom=False, **kwargs):
        jinja_env = jinja2.Environment()
        if custom:
            config_template = Path(config_template).read_text()
        else:
            config_template = files("neofs_testlib.env.templates").joinpath(config_template).read_text()
        jinja_template = jinja_env.from_string(config_template)
        rendered_config = jinja_template.render(**kwargs)
        with open(config_path, mode="w") as fp:
            fp.write(rendered_config)

    @staticmethod
    def _run_single_command(binary: str, command: str) -> str:
        result = subprocess.run([binary, command], capture_output=True, text=True)
        return f"{result.stdout}\n{result.stderr}\n"

    @staticmethod
    def get_available_port() -> str:
        with _thread_lock:
            with open(ALLOCATED_PORTS_LOCK_FILE, "w") as lock_file:
                fcntl.flock(lock_file, fcntl.LOCK_EX)
                try:
                    if os.path.exists(ALLOCATED_PORTS_FILE):
                        with open(ALLOCATED_PORTS_FILE, "r") as f:
                            reserved_ports = set(map(int, f.read().splitlines()))
                    else:
                        reserved_ports = set()

                    while True:
                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        s.bind(("", 0))
                        port = s.getsockname()[1]
                        s.close()

                        if port not in reserved_ports:
                            reserved_ports.add(port)
                            break

                    with open(ALLOCATED_PORTS_FILE, "w") as f:
                        for p in reserved_ports:
                            f.write(f"{p}\n")
                finally:
                    fcntl.flock(lock_file, fcntl.LOCK_UN)
        return port

    @staticmethod
    def cleanup_unused_ports():
        with open(ALLOCATED_PORTS_LOCK_FILE, "w") as lock_file:
            fcntl.flock(lock_file, fcntl.LOCK_EX)

            try:
                if os.path.exists(ALLOCATED_PORTS_FILE):
                    with open(ALLOCATED_PORTS_FILE, "r") as f:
                        reserved_ports = set(map(int, f.read().splitlines()))
                else:
                    reserved_ports = set()

                still_used_ports = set()

                for port in reserved_ports:
                    if NeoFSEnv.is_port_in_use(port):
                        still_used_ports.add(port)

                with open(ALLOCATED_PORTS_FILE, "w") as f:
                    for port in still_used_ports:
                        f.write(f"{port}\n")
            finally:
                fcntl.flock(lock_file, fcntl.LOCK_UN)

    @staticmethod
    def is_port_in_use(port: str):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.bind(("", port))
        except OSError:
            return True
        finally:
            s.close()
        return False

    @staticmethod
    def download_binary(repo: str, version: str, file: str, target: str):
        download_url = f"https://github.com/{repo}/releases/download/{version}/{file}"
        resp = requests.get(download_url, timeout=DEFAULT_OBJECT_OPERATION_TIMEOUT)
        if not resp.ok:
            raise AssertionError(
                f"Can not download binary from url: {download_url}: {resp.status_code}/{resp.reason}/{resp.json()}"
            )
        with open(target, mode="wb") as binary_file:
            binary_file.write(resp.content)
        if "contract" in file:
            # unpack contracts file into dir
            tar_file = tarfile.open(target)
            tar_file.extractall()
            tar_file.close()
            os.remove(target)
            logger.info(f"rename: {file.rstrip('.tar.gz')} into {target}")
            os.rename(file.rstrip(".tar.gz"), target)
        elif "warp" in file:
            temp_dir = f"temp_dir_{uuid.uuid4()}"
            with tarfile.open(target) as tar_file:
                tar_file.extractall(
                    path=temp_dir, filter=lambda tarinfo, _: tarinfo if tarinfo.name == "warp" else None
                )
            os.remove(target)
            shutil.move(f"{temp_dir}/warp", target)
            shutil.rmtree(temp_dir, ignore_errors=True)
            os.chmod(target, os.stat(target).st_mode | stat.S_IEXEC)
        else:
            # make binary executable
            current_perm = os.stat(target)
            os.chmod(target, current_perm.st_mode | stat.S_IEXEC)

    def get_binary_version(self, binary_path: str) -> str:
        raw_version_output = self._run_single_command(binary_path, "--version")
        for line in raw_version_output.splitlines():
            if "Version:" in line:
                return line.split("Version:")[1].strip()
        return ""

    def get_object_size(self, object_type: str) -> int:
        if object_type == ObjectType.SIMPLE.value:
            return int(SIMPLE_OBJECT_SIZE) if int(SIMPLE_OBJECT_SIZE) < self.max_object_size else self.max_object_size
        elif object_type == ObjectType.COMPLEX.value:
            return self.max_object_size * int(COMPLEX_OBJECT_CHUNKS_COUNT) + int(COMPLEX_OBJECT_TAIL_SIZE)
        else:
            raise AssertionError(f"Invalid {object_type=}")

    def _generate_temp_file(self, base_dir: str, extension: str = "", prefix: str = "tmp_file") -> str:
        file_path = f"{base_dir}/{prefix}_{''.join(random.choices(string.ascii_lowercase, k=10))}"
        if extension:
            file_path += f".{extension}"
        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.touch()
        return file_path

    def _generate_temp_dir(self, prefix: str = "tmp_dir") -> str:
        dir_path = f"{self._env_dir}/{prefix}_{''.join(random.choices(string.ascii_lowercase, k=10))}"
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        return dir_path


class ResurrectableProcess:
    def __getstate__(self):
        attributes = self.__dict__.copy()
        del attributes["process"]
        return attributes

    def __setstate__(self, state):
        self.__dict__.update(state)
        try:
            if psutil.pid_exists(self.pid):
                self.process = psutil.Process(self.pid)
            else:
                logger.info(f"Process {self.pid} no longer exists.")
                self.process = None
        except Exception as e:
            logger.info(f"Failed to reattach to process {self.pid}: {e}")
            self.process = None


class MainChain(ResurrectableProcess):
    def __init__(self, neofs_env: NeoFSEnv):
        self.neofs_env = neofs_env
        self.main_chain_dir = self.neofs_env._generate_temp_dir("main_chain")
        self.cli_config = self.neofs_env._generate_temp_file(
            self.main_chain_dir, extension="yml", prefix="main_chain_cli_config"
        )
        self.neo_go_config = self.neofs_env._generate_temp_file(
            self.main_chain_dir, extension="yml", prefix="main_chain_neo_go_config"
        )
        self.main_chain_config_path = os.getenv(
            "MAINCHAIN_CONFIG_PATH",
            self.neofs_env._generate_temp_file(self.main_chain_dir, extension="yml", prefix="main_chain_config"),
        )
        self.main_chain_boltdb = self.neofs_env._generate_temp_file(
            self.main_chain_dir, extension="db", prefix="main_chain_bolt_db"
        )
        self.rpc_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.p2p_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.pprof_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.prometheus_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.wallet_dir = self.neofs_env._generate_temp_dir(prefix="mainchain_wallet")
        self.wallet = None
        self.stdout = "Not initialized"
        self.stderr = "Not initialized"
        self.process = None
        self.pid = None
        self.neofs_contract_address = None
        self.neofs_contract_hash = None

    def __str__(self):
        return f"""
            Main Chain:
            - Config path: {self.main_chain_config_path}
            - BoltDB path: {self.main_chain_boltdb}
            - RPC address: {self.rpc_address}
            - P2P address: {self.p2p_address}
            - Pprof address: {self.pprof_address}
            - Prometheus address: {self.prometheus_address}
            - STDOUT: {self.stdout}
            - STDERR: {self.stderr}
        """

    @allure.step("Start Main Chain")
    def start(self, wait_until_ready=True):
        if self.process is not None:
            raise RuntimeError("This main chain instance has already been started")

        self.wallet = self.neofs_env.generate_alphabet_wallets(alphabet_wallets_dir=self.wallet_dir)[0]

        standby_committee = wallet_utils.get_last_public_key_from_wallet_with_neogo(
            self.neofs_env.neo_go(), self.wallet.path
        )

        ir_alphabet_pubkey_from_neogo = wallet_utils.get_last_public_key_from_wallet_with_neogo(
            self.neofs_env.neo_go(), self.neofs_env.inner_ring_nodes[-1].alphabet_wallet.path
        )

        if len(self.neofs_env.inner_ring_nodes) > 1:
            ir_public_keys = ir_alphabet_pubkey_from_neogo.splitlines()
        else:
            ir_public_keys = [ir_alphabet_pubkey_from_neogo]

        logger.info(f"Generating main chain config at: {self.main_chain_config_path}")
        main_chain_config_template = "main_chain.yaml"

        if not os.getenv("MAINCHAIN_CONFIG_PATH", None):
            NeoFSEnv.generate_config_file(
                config_template=main_chain_config_template,
                config_path=self.main_chain_config_path,
                custom=Path(main_chain_config_template).is_file(),
                wallet=self.wallet,
                standby_committee=standby_committee,
                ir_public_keys=ir_public_keys,
                main_chain_boltdb=self.main_chain_boltdb,
                p2p_address=self.p2p_address,
                rpc_address=self.rpc_address,
                sn_addresses=[sn.endpoint for sn in self.neofs_env.storage_nodes],
                pprof_address=self.pprof_address,
                prometheus_address=self.prometheus_address,
            )
        logger.info(f"Generating CLI config at: {self.cli_config}")
        NeoFSEnv.generate_config_file(config_template="cli_cfg.yaml", config_path=self.cli_config, wallet=self.wallet)
        logger.info(f"Generating NEO GO config at: {self.neo_go_config}")
        NeoFSEnv.generate_config_file(
            config_template="neo_go_cfg.yaml", config_path=self.neo_go_config, wallet=self.wallet
        )
        logger.info(f"Launching Main Chain:{self}")
        self._launch_process()
        logger.info(f"Launched Main Chain:{self}")
        if wait_until_ready:
            logger.info("Wait until Main Chain is READY")
            self._wait_until_ready()

    def _launch_process(self):
        self.stdout = self.neofs_env._generate_temp_file(self.main_chain_dir, prefix="main_chain_stdout")
        self.stderr = self.neofs_env._generate_temp_file(self.main_chain_dir, prefix="main_chain_stderr")
        stdout_fp = open(self.stdout, "w")
        stderr_fp = open(self.stderr, "w")
        self.process = subprocess.Popen(
            [self.neofs_env.neo_go_path, "node", "--config-file", self.main_chain_config_path, "--debug"],
            stdout=stdout_fp,
            stderr=stderr_fp,
        )
        self.pid = self.process.pid

    @retry(wait=wait_fixed(10), stop=stop_after_attempt(50), reraise=True)
    def _wait_until_ready(self):
        result = self.neofs_env.neo_go().query.height(rpc_endpoint=f"http://{self.rpc_address}")
        logger.info("WAIT UNTIL MAIN CHAIN IS READY:")
        logger.info(result.stdout)
        logger.info(result.stderr)


class InnerRing(ResurrectableProcess):
    def __init__(
        self,
        neofs_env: NeoFSEnv,
        ir_number: int,
        chain_meta_data=False,
        sn_validator_url=None,
    ):
        self.neofs_env = neofs_env
        self.ir_number = ir_number
        self.inner_ring_dir = self.neofs_env._generate_temp_dir("inner_ring")
        self.network_config = self.neofs_env._generate_temp_file(
            self.inner_ring_dir, extension="yml", prefix="ir_network_config"
        )
        self.cli_config = self.neofs_env._generate_temp_file(
            self.inner_ring_dir, extension="yml", prefix="ir_cli_config"
        )
        self.alphabet_wallet = None
        self.ir_node_config_path = os.getenv(
            f"IR{self.ir_number}_CONFIG_PATH",
            self.neofs_env._generate_temp_file(self.inner_ring_dir, extension="yml", prefix="ir_node_config"),
        )
        self.ir_storage_path = self.neofs_env._generate_temp_file(
            self.inner_ring_dir, extension="db", prefix="ir_storage"
        )
        self.endpoint = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.p2p_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.control_endpoint = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.pprof_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.prometheus_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.ir_state_file = self.neofs_env._generate_temp_file(self.inner_ring_dir, prefix="ir_state_file")
        if (
            parse_version(self.neofs_env.get_binary_version(self.neofs_env.neofs_node_path)) <= parse_version("0.45.2")
            and chain_meta_data
        ):
            pytest.skip("chain_meta_data=True is not supported on 0.45.2 and below")
        self.chain_meta_data = chain_meta_data
        if (
            parse_version(self.neofs_env.get_binary_version(self.neofs_env.neofs_node_path)) <= parse_version("0.46.1")
            and sn_validator_url
        ):
            pytest.skip("sn_validator_url=True is not supported on 0.46.1 and below")
        self.sn_validator_url = sn_validator_url
        self.stdout = "Not initialized"
        self.stderr = "Not initialized"
        self.process = None
        self.pid = None

    def __str__(self):
        return f"""
            Inner Ring:
            - Alphabet wallet: {self.alphabet_wallet}
            - IR Config path: {self.ir_node_config_path}
            - Endpoint: {self.endpoint}
            - Control endpoint: {self.control_endpoint}
            - P2P address: {self.p2p_address}
            - Pprof address: {self.pprof_address}
            - Prometheus address: {self.prometheus_address}
            - IR State file path: {self.ir_state_file}
            - STDOUT: {self.stdout}
            - STDERR: {self.stderr}
        """

    def generate_network_config(self):
        logger.info(f"Generating network config at: {self.network_config}")

        network_config_template = "network.yaml"

        NeoFSEnv.generate_config_file(
            config_template=network_config_template,
            config_path=self.network_config,
            custom=Path(network_config_template).is_file(),
            fschain_endpoint=self.endpoint,
            alphabet_wallets_path=self.neofs_env.alphabet_wallets_dir,
            default_password=self.neofs_env.default_password,
        )

    def generate_cli_config(self):
        logger.info(f"Generating CLI config at: {self.cli_config}")
        NeoFSEnv.generate_config_file(
            config_template="cli_cfg.yaml", config_path=self.cli_config, wallet=self.alphabet_wallet
        )

    @allure.step("Start Inner Ring node")
    def start(
        self,
        wait_until_ready=True,
        with_main_chain=False,
        pub_keys_of_existing_ir_nodes=None,
        seed_node_addresses_of_existing_ir_nodes=None,
        fschain_autodeploy=True,
    ):
        if self.process is not None:
            raise RuntimeError("This inner ring node instance has already been started")
        logger.info(f"Generating IR config at: {self.ir_node_config_path}")
        ir_config_template = "ir.yaml"

        if not pub_keys_of_existing_ir_nodes:
            pub_keys_of_existing_ir_nodes = [
                wallet_utils.get_last_public_key_from_wallet(
                    ir_node.alphabet_wallet.path, ir_node.alphabet_wallet.password
                )
                for ir_node in self.neofs_env.inner_ring_nodes
            ]

        if not seed_node_addresses_of_existing_ir_nodes:
            seed_node_addresses_of_existing_ir_nodes = [
                ir_node.p2p_address for ir_node in self.neofs_env.inner_ring_nodes
            ]

        if not os.getenv(f"IR{self.ir_number}_CONFIG_PATH", None):
            NeoFSEnv.generate_config_file(
                config_template=ir_config_template,
                config_path=self.ir_node_config_path,
                custom=Path(ir_config_template).is_file(),
                wallet=self.alphabet_wallet,
                public_keys=pub_keys_of_existing_ir_nodes,
                ir_storage_path=self.ir_storage_path,
                seed_nodes_addresses=seed_node_addresses_of_existing_ir_nodes,
                rpc_address=self.endpoint,
                p2p_address=self.p2p_address,
                grpc_address=self.control_endpoint,
                ir_state_file=self.ir_state_file,
                peers_min_number=int(
                    len(self.neofs_env.inner_ring_nodes) - (len(self.neofs_env.inner_ring_nodes) - 1) / 3 - 1
                ),
                set_roles_in_genesis=str(False if len(self.neofs_env.inner_ring_nodes) == 1 else True).lower(),
                fschain_autodeploy=fschain_autodeploy,
                control_public_key=wallet_utils.get_last_public_key_from_wallet(
                    self.alphabet_wallet.path, self.alphabet_wallet.password
                ),
                without_mainnet=f"{not with_main_chain}".lower(),
                main_chain_rpc="localhost:1234" if not with_main_chain else self.neofs_env.main_chain.rpc_address,
                neofs_contract_hash="123" if not with_main_chain else self.neofs_env.main_chain.neofs_contract_hash,
                pprof_address=self.pprof_address,
                prometheus_address=self.prometheus_address,
                chain_meta_data=self.chain_meta_data,
                sn_validator_url=self.sn_validator_url,
            )
        logger.info(f"Launching Inner Ring Node:{self}")
        self._launch_process()
        logger.info(f"Launched Inner Ring Node:{self}")
        if wait_until_ready:
            logger.info("Wait until IR is READY")
            self._wait_until_ready()

    def _launch_process(self):
        self.stdout = self.neofs_env._generate_temp_file(self.inner_ring_dir, prefix="ir_stdout")
        self.stderr = self.neofs_env._generate_temp_file(self.inner_ring_dir, prefix="ir_stderr")
        stdout_fp = open(self.stdout, "w")
        stderr_fp = open(self.stderr, "w")
        self.process = subprocess.Popen(
            [self.neofs_env.neofs_ir_path, "--config", self.ir_node_config_path],
            stdout=stdout_fp,
            stderr=stderr_fp,
        )
        self.pid = self.process.pid

    @retry(wait=wait_fixed(12), stop=stop_after_attempt(100), reraise=True)
    def _wait_until_ready(self):
        neofs_cli = self.neofs_env.neofs_cli(self.cli_config)
        result = neofs_cli.control.healthcheck(endpoint=self.control_endpoint, post_data="--ir")
        assert "READY" in result.stdout


class Shard:
    def __init__(self, neofs_env: NeoFSEnv, sn_dir: str):
        self.metabase_path = neofs_env._generate_temp_file(sn_dir, prefix="shard_metabase")
        self.fstree_path = neofs_env._generate_temp_dir(prefix="shards/shard_fstree")
        self.pilorama_path = neofs_env._generate_temp_file(sn_dir, prefix="shard_pilorama")
        self.wc_path = neofs_env._generate_temp_dir(prefix="shards/shard_wc")


class StorageNode(ResurrectableProcess):
    def __init__(
        self,
        neofs_env: NeoFSEnv,
        sn_number: int,
        writecache=False,
        node_attrs: Optional[list] = None,
        attrs: Optional[dict] = None,
    ):
        self.neofs_env = neofs_env
        self.sn_dir = self.neofs_env._generate_temp_dir(prefix=f"sn_{sn_number}")
        self.wallet = NodeWallet(
            path=self.neofs_env._generate_temp_file(self.sn_dir, prefix=f"sn_{sn_number}_wallet"),
            address="",
            password=self.neofs_env.default_password,
        )
        self.cli_config = self.neofs_env._generate_temp_file(
            self.sn_dir, extension="yml", prefix=f"sn_{sn_number}_cli_config"
        )
        self.storage_node_config_path = os.getenv(
            f"SN{sn_number}_CONFIG_PATH",
            self.neofs_env._generate_temp_file(self.sn_dir, extension="yml", prefix=f"sn_{sn_number}_config"),
        )
        self.state_file = self.neofs_env._generate_temp_file(self.sn_dir, prefix=f"sn_{sn_number}_state")
        self.shards = [Shard(neofs_env, self.sn_dir), Shard(neofs_env, self.sn_dir)]
        self.endpoint = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.control_endpoint = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.pprof_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.prometheus_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.metadata_path = self.neofs_env._generate_temp_dir(prefix=f"sn_{sn_number}_metadata")
        self.stdout = "Not initialized"
        self.stderr = "Not initialized"
        self.sn_number = sn_number
        self.process = None
        self.pid = None
        self.attrs = {}
        self.node_attrs = node_attrs
        self.writecache = writecache
        if attrs:
            self.attrs.update(attrs)

    def __str__(self):
        return f"""
            Storage node:
            - Endpoint: {self.endpoint}
            - Control endpoint: {self.control_endpoint}
            - Pprof address: {self.pprof_address}
            - Prometheus address: {self.prometheus_address}
            - Attributes: {self.attrs}
            - STDOUT: {self.stdout}
            - STDERR: {self.stderr}
        """

    def get_config_template(self):
        if parse_version(self.neofs_env.get_binary_version(self.neofs_env.neofs_node_path)) > parse_version("0.45.2"):
            return "sn_post_0_45_2.yaml"
        else:
            return "sn.yaml"

    @allure.step("Start storage node")
    def start(self, fresh=True, wait_until_ready=True):
        if fresh:
            logger.info("Generating wallet for storage node")
            self.neofs_env.generate_storage_wallet(self.wallet, label=f"sn{self.sn_number}")
            logger.info(f"Generating config for storage node at {self.storage_node_config_path}")

            if not os.getenv(f"SN{self.sn_number}_CONFIG_PATH", None):
                sn_config_template = self.get_config_template()

                NeoFSEnv.generate_config_file(
                    config_template=sn_config_template,
                    config_path=self.storage_node_config_path,
                    custom=Path(sn_config_template).is_file(),
                    fschain_endpoint=self.neofs_env.fschain_rpc,
                    shards=self.shards,
                    writecache=self.writecache,
                    wallet=self.wallet,
                    state_file=self.state_file,
                    pprof_address=self.pprof_address,
                    prometheus_address=self.prometheus_address,
                    attrs=self.node_attrs,
                    metadata_path=self.metadata_path,
                )
            logger.info(f"Generating cli config for storage node at: {self.cli_config}")
            NeoFSEnv.generate_config_file(
                config_template="cli_cfg.yaml", config_path=self.cli_config, wallet=self.wallet
            )
        logger.info(f"Launching Storage Node:{self}")
        self._launch_process()
        if wait_until_ready:
            logger.info("Wait until storage node is READY")
            self._wait_until_ready()
        allure.attach(str(self), f"sn_{self.sn_number}", allure.attachment_type.TEXT, ".txt")

    def stop(self):
        with allure.step(f"Stop SN: {self.endpoint}; {self.stderr}"):
            if self.process:
                terminate_process(self.process)
                self.process = None
                self.pid = None
                with allure.step("Wait until storage node is not ready"):
                    self._wait_until_not_ready()
            else:
                AssertionError("Storage node has been already stopped")

    def kill(self):
        with allure.step(f"Kill SN: {self.endpoint}; {self.stderr}"):
            if self.process:
                self.process.kill()
                self.process = None
                self.pid = None
                with allure.step("Wait until storage node is not ready"):
                    self._wait_until_not_ready()
            else:
                AssertionError("Storage node has been already killed")

    @allure.step("Delete storage node data")
    def delete_data(self):
        self.stop()
        for shard in self.shards:
            os.remove(shard.metabase_path)
            os.rmdir(shard.fstree_path)
            os.remove(shard.pilorama_path)
            shutil.rmtree(shard.wc_path, ignore_errors=True)
        os.remove(self.state_file)
        self.shards = [Shard(), Shard()]

        if not os.getenv(f"SN{self.sn_number}_CONFIG_PATH", None):
            sn_config_template = self.get_config_template()

            NeoFSEnv.generate_config_file(
                config_template=sn_config_template,
                config_path=self.storage_node_config_path,
                custom=Path(sn_config_template).is_file(),
                fschain_endpoint=self.neofs_env.fschain_rpc,
                shards=self.shards,
                writecache=self.writecache,
                wallet=self.wallet,
                state_file=self.state_file,
                pprof_address=self.pprof_address,
                prometheus_address=self.prometheus_address,
                attrs=self.node_attrs,
                metadata_path=self.metadata_path,
            )
        time.sleep(1)

    @allure.step("Delete storage node metadata")
    def delete_metadata(self):
        self.stop()
        for shard in self.shards:
            os.remove(shard.metabase_path)
            shard.metabase_path = self.neofs_env._generate_temp_file(self.sn_dir, prefix="shard_metabase")

        if not os.getenv(f"SN{self.sn_number}_CONFIG_PATH", None):
            sn_config_template = self.get_config_template()

            NeoFSEnv.generate_config_file(
                config_template=sn_config_template,
                config_path=self.storage_node_config_path,
                custom=Path(sn_config_template).is_file(),
                fschain_endpoint=self.neofs_env.fschain_rpc,
                shards=self.shards,
                writecache=self.writecache,
                wallet=self.wallet,
                state_file=self.state_file,
                pprof_address=self.pprof_address,
                prometheus_address=self.prometheus_address,
                attrs=self.node_attrs,
                metadata_path=self.metadata_path,
            )
        time.sleep(1)

    @allure.step("Set metabase resync")
    def set_metabase_resync(self, resync_state: bool):
        self.stop()
        neofs_shard_env_variable = "NEOFS_STORAGE_SHARD_{idx}_RESYNC_METABASE"
        if parse_version(self.neofs_env.get_binary_version(self.neofs_env.neofs_node_path)) > parse_version("0.45.2"):
            neofs_shard_env_variable = "NEOFS_STORAGE_SHARDS_{idx}_RESYNC_METABASE"
        for idx, _ in enumerate(self.shards):
            self.attrs.update({neofs_shard_env_variable.format(idx=idx): f"{resync_state}".lower()})
        self.start(fresh=False)

    def _launch_process(self):
        self.stdout = self.neofs_env._generate_temp_file(self.sn_dir, prefix=f"sn_{self.sn_number}_stdout")
        self.stderr = self.neofs_env._generate_temp_file(self.sn_dir, prefix=f"sn_{self.sn_number}_stderr")
        stdout_fp = open(self.stdout, "w")
        stderr_fp = open(self.stderr, "w")
        env_dict = {
            "NEOFS_NODE_WALLET_PATH": self.wallet.path,
            "NEOFS_NODE_WALLET_PASSWORD": self.wallet.password,
            "NEOFS_NODE_ADDRESSES": self.endpoint,
            "NEOFS_GRPC_0_ENDPOINT": self.endpoint,
            "NEOFS_CONTROL_GRPC_ENDPOINT": self.control_endpoint,
        }
        env_dict.update(self.attrs)
        self.process = subprocess.Popen(
            [self.neofs_env.neofs_node_path, "--config", self.storage_node_config_path],
            stdout=stdout_fp,
            stderr=stderr_fp,
            env=env_dict,
        )
        self.pid = self.process.pid

    @retry(wait=wait_fixed(15), stop=stop_after_attempt(30), reraise=True)
    def _wait_until_ready(self):
        neofs_cli = self.neofs_env.neofs_cli(self.cli_config)
        result = neofs_cli.control.healthcheck(endpoint=self.control_endpoint)
        assert "Health status: READY" in result.stdout, "Health is not ready"
        assert "Network status: ONLINE" in result.stdout, "Network is not online"

    @retry(wait=wait_fixed(15), stop=stop_after_attempt(10), reraise=True)
    def _wait_until_not_ready(self):
        neofs_cli = self.neofs_env.neofs_cli(self.cli_config)
        try:
            result = neofs_cli.control.healthcheck(endpoint=self.control_endpoint)
        except Exception as e:
            with allure.step(f"Exception caught: {e}, node is not ready"):
                return
        assert "Health status: READY" not in result.stdout, "Health is ready"
        assert "Network status: ONLINE" not in result.stdout, "Network is online"


class S3_GW(ResurrectableProcess):
    def __init__(self, neofs_env: NeoFSEnv, internal_slicer=False):
        self.neofs_env = neofs_env
        self.s3_gw_dir = self.neofs_env._generate_temp_dir("s3-gw")
        self.config_path = os.getenv(
            "S3_GW_CONFIG_PATH",
            self.neofs_env._generate_temp_file(self.s3_gw_dir, extension="yml", prefix="s3gw_config"),
        )
        self.wallet = NodeWallet(
            path=self.neofs_env._generate_temp_file(self.s3_gw_dir, prefix="s3gw_wallet"),
            address="",
            password=self.neofs_env.default_password,
        )
        self.endpoint = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.pprof_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.prometheus_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.tls_cert_path = self.neofs_env._generate_temp_file(self.s3_gw_dir, prefix="s3gw_tls_cert")
        self.tls_key_path = self.neofs_env._generate_temp_file(self.s3_gw_dir, prefix="s3gw_tls_key")
        self.stdout = "Not initialized"
        self.stderr = "Not initialized"
        self.tls_enabled = True
        self.process = None
        self.pid = None
        self.internal_slicer = internal_slicer

    def __str__(self):
        return f"""
            S3 Gateway:
            - Endpoint: {self.endpoint}
            - Pprof address: {self.pprof_address}
            - Prometheus address: {self.prometheus_address}
            - S3 GW Config path: {self.config_path}
            - STDOUT: {self.stdout}
            - STDERR: {self.stderr}
        """

    def start(self, fresh=True):
        if self.process is not None:
            raise RuntimeError(f"This s3 gw instance has already been started:\n{self}")
        if fresh:
            self.neofs_env.generate_storage_wallet(self.wallet, label="s3")
        logger.info(f"Generating config for s3 gw at {self.config_path}")
        self._generate_config()
        logger.info(f"Launching S3 GW: {self}")
        self._launch_process()
        try:
            self._wait_until_ready()
        except Exception as e:
            allure.attach.file(self.stderr, name="s3 gw stderr", extension="txt")
            allure.attach.file(self.stdout, name="s3 gw stdout", extension="txt")
            allure.attach.file(self.config_path, name="s3 gw config", extension="txt")
            raise e

    @allure.step("Stop s3 gw")
    def stop(self):
        logger.info(f"Stopping s3 gw:{self}")
        self.process.terminate()
        terminate_process(self.process)
        self.process = None
        self.pid = None

    @retry(wait=wait_fixed(10), stop=stop_after_attempt(10), reraise=True)
    def _wait_until_ready(self):
        endpoint = f"https://{self.endpoint}" if self.tls_enabled else f"http://{self.endpoint}"
        resp = requests.get(endpoint, verify=False, timeout=DEFAULT_REST_OPERATION_TIMEOUT)
        assert resp.status_code == 200

    def _generate_config(self):
        if os.getenv("S3_GW_CONFIG_PATH", None):
            return
        tls_crt_template = files("neofs_testlib.env.templates").joinpath("tls.crt").read_text()
        with open(self.tls_cert_path, mode="w") as fp:
            fp.write(tls_crt_template)
        tls_key_template = files("neofs_testlib.env.templates").joinpath("tls.key").read_text()
        with open(self.tls_key_path, mode="w") as fp:
            fp.write(tls_key_template)

        s3_config_template = "s3.yaml"

        S3peer = namedtuple("S3peer", ["address", "priority", "weight"])

        peers = []
        for sn in self.neofs_env.storage_nodes:
            peers.append(S3peer(sn.endpoint, 1, 1))

        NeoFSEnv.generate_config_file(
            config_template=s3_config_template,
            config_path=self.config_path,
            custom=Path(s3_config_template).is_file(),
            address=self.endpoint,
            tls_enabled=str(self.tls_enabled).lower(),
            cert_file_path=self.tls_cert_path,
            key_file_path=self.tls_key_path,
            wallet=self.wallet,
            fschain_endpoint=self.neofs_env.fschain_rpc,
            peers=peers,
            tree_service_endpoint=self.neofs_env.storage_nodes[0].endpoint,
            listen_domain=self.neofs_env.domain,
            s3_gw_version=self.neofs_env.get_binary_version(self.neofs_env.neofs_s3_gw_path),
            pprof_address=self.pprof_address,
            prometheus_address=self.prometheus_address,
            internal_slicer=self.internal_slicer,
        )

    def _launch_process(self):
        self.stdout = self.neofs_env._generate_temp_file(self.s3_gw_dir, prefix="s3gw_stdout")
        self.stderr = self.neofs_env._generate_temp_file(self.s3_gw_dir, prefix="s3gw_stderr")
        stdout_fp = open(self.stdout, "w")
        stderr_fp = open(self.stderr, "w")

        self.process = subprocess.Popen(
            [self.neofs_env.neofs_s3_gw_path, "--config", self.config_path],
            stdout=stdout_fp,
            stderr=stderr_fp,
        )
        self.pid = self.process.pid


class REST_GW(ResurrectableProcess):
    def __init__(self, neofs_env: NeoFSEnv, default_timestamp: bool = False):
        self.neofs_env = neofs_env
        self.rest_gw_dir = self.neofs_env._generate_temp_dir("rest-gw")
        self.config_path = os.getenv(
            "REST_GW_CONFIG_PATH",
            self.neofs_env._generate_temp_file(self.rest_gw_dir, extension="yml", prefix="rest_gw_config"),
        )
        self.wallet = NodeWallet(
            path=self.neofs_env._generate_temp_file(self.rest_gw_dir, prefix="rest_gw_wallet"),
            address="",
            password=self.neofs_env.default_password,
        )
        self.endpoint = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.pprof_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.prometheus_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.default_timestamp = default_timestamp
        self.stdout = "Not initialized"
        self.stderr = "Not initialized"
        self.process = None
        self.pid = None

    def __str__(self):
        return f"""
            REST Gateway:
            - Endpoint: {self.endpoint}
            - Pprof address: {self.pprof_address}
            - Prometheus address: {self.prometheus_address}
            - REST GW Config path: {self.config_path}
            - STDOUT: {self.stdout}
            - STDERR: {self.stderr}
        """

    def start(self, fresh=True):
        if self.process is not None:
            raise RuntimeError(f"This rest gw instance has already been started:\n{self}")
        if fresh:
            self.neofs_env.generate_storage_wallet(self.wallet, label="rest")
        logger.info(f"Generating config for rest gw at {self.config_path}")
        self._generate_config()
        logger.info(f"Launching REST GW: {self}")
        self._launch_process()
        logger.info(f"Launched REST GW: {self}")
        try:
            self._wait_until_ready()
        except Exception as e:
            allure.attach.file(self.stderr, name="rest gw stderr", extension="txt")
            allure.attach.file(self.stdout, name="rest gw stdout", extension="txt")
            allure.attach.file(self.config_path, name="rest gw config", extension="txt")
            raise e

    @allure.step("Stop rest gw")
    def stop(self):
        logger.info(f"Stopping rest gw:{self}")
        self.process.terminate()
        terminate_process(self.process)
        self.process = None
        self.pid = None

    @retry(wait=wait_fixed(10), stop=stop_after_attempt(10), reraise=True)
    def _wait_until_ready(self):
        endpoint = f"http://{self.endpoint}"
        resp = requests.get(endpoint, verify=False, timeout=DEFAULT_REST_OPERATION_TIMEOUT)
        assert resp.status_code == 200

    def _generate_config(self):
        if os.getenv("REST_GW_CONFIG_PATH", None):
            return

        rest_config_template = "rest.yaml"

        NeoFSEnv.generate_config_file(
            config_template=rest_config_template,
            config_path=self.config_path,
            custom=Path(rest_config_template).is_file(),
            address=self.endpoint,
            wallet=self.wallet,
            pprof_address=self.pprof_address,
            metrics_address=self.prometheus_address,
            default_timestamp=self.default_timestamp,
        )

    def _launch_process(self):
        self.stdout = self.neofs_env._generate_temp_file(self.rest_gw_dir, prefix="rest_gw_stdout")
        self.stderr = self.neofs_env._generate_temp_file(self.rest_gw_dir, prefix="rest_gw_stderr")
        stdout_fp = open(self.stdout, "w")
        stderr_fp = open(self.stderr, "w")
        rest_gw_env = {}

        for index, sn in enumerate(self.neofs_env.storage_nodes):
            rest_gw_env[f"REST_GW_POOL_PEERS_{index}_ADDRESS"] = sn.endpoint
            rest_gw_env[f"REST_GW_POOL_PEERS_{index}_WEIGHT"] = "0.2"

        self.process = subprocess.Popen(
            [self.neofs_env.neofs_rest_gw_path, "--config", self.config_path],
            stdout=stdout_fp,
            stderr=stderr_fp,
            env=rest_gw_env,
        )
        self.pid = self.process.pid


class XK6:
    def __init__(self, neofs_env: NeoFSEnv):
        self.neofs_env = neofs_env
        self.xk6_dir = os.getenv("XK6_DIR", "../xk6-neofs")
        self.wallet = NodeWallet(
            path=neofs_env._generate_temp_file(self.xk6_dir, prefix="xk6_wallet"),
            address="",
            password=self.neofs_env.default_password,
        )
        self.neofs_env.generate_storage_wallet(self.wallet, label="xk6")
        self.config = neofs_env.generate_cli_config(self.wallet)

    @allure.step("Run K6 Loader")
    def run(
        self,
        endpoints: list[str],
        out="grpc.json",
        duration=30,
        write_obj_size=8192,
        readers=2,
        writers=2,
        registry_file="registry.bolt",
        scenario="grpc.js",
    ):
        if not os.path.exists(self.xk6_dir):
            raise RuntimeError("Invalid xk6 directory")
        command = (
            f"{self.xk6_dir}/xk6-neofs "
            f"run "
            f"-e DURATION={duration} "
            f"-e WRITE_OBJ_SIZE={write_obj_size} "
            f"-e READERS={readers} "
            f"-e WRITERS={writers} "
            f"-e REGISTRY_FILE={registry_file} "
            f"-e GRPC_ENDPOINTS={endpoints[0]} "
            f"-e PREGEN_JSON={out} "
            f"{self.xk6_dir}/scenarios/{scenario} "
        )
        result = self.neofs_env.shell.exec(command)
        assert not result.return_code, "RC after k6 load is not zero"
        assert "neofs_obj_get_fails" not in result.stdout, "some GET requests failed, see logs"
        assert "neofs_obj_put_fails" not in result.stdout, "some PUT requests failed, see logs"

    @allure.step("Prepare containers and objects for k6 load run")
    def prepare(
        self,
        endpoints: list[str],
        size=1024,
        containers=1,
        out="grpc.json",
        preload_obj=100,
        policy="REP 2 IN X CBF 1 SELECT 2 FROM * AS X",
        workers=2,
    ):
        if not os.path.exists(self.xk6_dir):
            raise RuntimeError("Invalid xk6 directory")

        command = (
            f"{self.xk6_dir}/scenarios/preset/preset_grpc.py "
            f"--size {size}  "
            f"--containers {containers} "
            f"--out {out} "
            f"--endpoint {endpoints[0]} "
            f"--preload_obj {preload_obj} "
            f'--policy "{policy}" '
            f"--wallet {self.wallet.path} "
            f"--config {self.config} "
            f"--workers {workers} "
        )
        result = self.neofs_env.shell.exec(command)
        assert not result.return_code, "RC after k6 prepare script is not zero"
        assert f"Total Containers has been created: {containers}" in result.stdout, (
            "Prepare script didn't create requested containers"
        )
        assert f"Total Objects has been created: {preload_obj}" in result.stdout, (
            "Prepare script didn't create requested objects"
        )

        shutil.copy(out, os.path.join(self.xk6_dir, "scenarios"))
