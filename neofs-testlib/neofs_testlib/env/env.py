import json
import logging
import os
import pickle
import random
import socket
import stat
import string
import subprocess
import threading
import time
from dataclasses import dataclass
from enum import Enum
from importlib.resources import files
from pathlib import Path
from typing import Optional

import allure
import jinja2
import requests
import yaml
from tenacity import retry, stop_after_attempt, wait_fixed

from neofs_testlib.cli import NeofsAdm, NeofsCli
from neofs_testlib.shell import LocalShell
from neofs_testlib.utils import wallet as wallet_utils

logger = logging.getLogger("neofs.testlib.env")


@dataclass
class NodeWallet:
    path: str
    address: str
    password: str


class WalletType(Enum):
    STORAGE = 1
    ALPHABET = 2


class NeoFSEnv:
    _busy_ports = []

    def __init__(self, neofs_env_config: dict = None):
        self.domain = "localhost"
        self.default_password = "password"
        self.shell = LocalShell()
        # utilities
        self.neofs_env_config = neofs_env_config
        self.neofs_adm_path = os.getenv("NEOFS_ADM_BIN", "./neofs-adm")
        self.neofs_cli_path = os.getenv("NEOFS_CLI_BIN", "./neofs-cli")
        self.neo_go_path = os.getenv("NEO_GO_BIN", "./neo-go")
        self.neofs_ir_path = os.getenv("NEOFS_IR_BIN", "./neofs-ir")
        self.neofs_node_path = os.getenv("NEOFS_NODE_BIN", "./neofs-node")
        self.neofs_s3_authmate_path = os.getenv("NEOFS_S3_AUTHMATE_BIN", "./neofs-s3-authmate")
        self.neofs_s3_gw_path = os.getenv("NEOFS_S3_GW_BIN", "./neofs-s3-gw")
        self.neofs_rest_gw_path = os.getenv("NEOFS_REST_GW_BIN", "./neofs-rest-gw")
        self.neofs_http_gw_path = os.getenv("NEOFS_HTTP_GW_BIN", "./neofs-http-gw")
        # nodes inside env
        self.storage_nodes = []
        self.inner_ring_nodes = []
        self.s3_gw = None
        self.rest_gw = None
        self.http_gw = None

    @property
    def morph_rpc(self):
        if len(self.inner_ring_nodes) > 0:
            return self.inner_ring_nodes[0].rpc_address
        raise ValueError("No Inner Ring nodes configured in this env")

    @property
    def sn_rpc(self):
        if len(self.storage_nodes) > 0:
            return self.storage_nodes[0].endpoint
        raise ValueError("No storage nodes configured in this env")

    @property
    def alphabet_wallets_dir(self):
        if len(self.inner_ring_nodes) > 0:
            if self.inner_ring_nodes[0].alphabet_wallet.address == "":
                raise ValueError("Alphabet Wallets has not beet initialized")
            return os.path.dirname(self.inner_ring_nodes[0].alphabet_wallet.path)
        raise ValueError("No Inner Ring nodes configured in this env")

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

    def generate_cli_config(self, wallet: NodeWallet):
        cli_config_path = NeoFSEnv._generate_temp_file(extension="yml", prefix="cli_config")
        NeoFSEnv.generate_config_file(
            config_template="cli_cfg.yaml", config_path=cli_config_path, wallet=wallet
        )
        return cli_config_path

    @allure.step("Deploy inner ring node")
    def deploy_inner_ring_node(self):
        new_inner_ring_node = InnerRing(self)
        new_inner_ring_node.start()
        self.inner_ring_nodes.append(new_inner_ring_node)

    @allure.step("Deploy storage node")
    def deploy_storage_nodes(self, count=1, node_attrs: Optional[dict] = None):
        logger.info(f"Going to deploy {count} storage nodes")
        deploy_threads = []
        for idx in range(count):
            node_attrs_list = None
            if node_attrs:
                node_attrs_list = node_attrs.get(idx, None)
            new_storage_node = StorageNode(self, len(self.storage_nodes) + 1, node_attrs=node_attrs_list)
            self.storage_nodes.append(new_storage_node)
            deploy_threads.append(
                threading.Thread(target=new_storage_node.start)
            )
        for t in deploy_threads:
            t.start()
        logger.info(f"Wait until storage nodes are deployed")
        self._wait_until_all_storage_nodes_are_ready()
        # tick epoch to speed up storage nodes bootstrap
        self.neofs_adm().morph.force_new_epoch(
            rpc_endpoint=f"http://{self.morph_rpc}",
            alphabet_wallets=self.alphabet_wallets_dir,
        )
        for t in deploy_threads:
            t.join()
            
    @retry(wait=wait_fixed(2), stop=stop_after_attempt(60), reraise=True)
    def _wait_until_all_storage_nodes_are_ready(self):
        ready_counter = 0
        for sn in self.storage_nodes:
            neofs_cli = self.neofs_cli(sn.cli_config)
            result = neofs_cli.control.healthcheck(endpoint=sn.control_grpc_endpoint)
            if "Health status: READY" in result.stdout:
                ready_counter += 1
        assert ready_counter == len(self.storage_nodes)
            
    @allure.step("Deploy s3 gateway")
    def deploy_s3_gw(self):
        self.s3_gw = S3_GW(self)
        self.s3_gw.start()
        allure.attach(str(self.s3_gw), "s3_gw", allure.attachment_type.TEXT, ".txt")

    @allure.step("Deploy http gateway")
    def deploy_http_gw(self):
        self.http_gw = HTTP_GW(self)
        self.http_gw.start()
        allure.attach(str(self.http_gw), "http_gw", allure.attachment_type.TEXT, ".txt")

    @allure.step("Deploy rest gateway")
    def deploy_rest_gw(self):
        self.rest_gw = REST_GW(self)
        self.rest_gw.start()
        allure.attach(str(self.rest_gw), "http_gw", allure.attachment_type.TEXT, ".txt")

    @allure.step("Generate wallet")
    def generate_wallet(
        self,
        wallet_type: WalletType,
        prepared_wallet: NodeWallet,
        network_config: Optional[str] = None,
        label: Optional[str] = None,
    ):
        neofs_adm = self.neofs_adm(network_config)

        if wallet_type == WalletType.STORAGE:
            neofs_adm.morph.generate_storage_wallet(
                alphabet_wallets=self.alphabet_wallets_dir,
                storage_wallet=prepared_wallet.path,
                initial_gas="10",
                label=label,
            )
        elif wallet_type == WalletType.ALPHABET:
            neofs_adm.morph.generate_alphabet(alphabet_wallets=prepared_wallet.path, size=1)
            prepared_wallet.path += "/az.json"
        else:
            raise ValueError(f"Unsupported wallet type: {wallet_type}")

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

    @allure.step("Kill current neofs env")
    def kill(self):
        self.rest_gw.process.kill()
        self.http_gw.process.kill()
        self.s3_gw.process.kill()
        for sn in self.storage_nodes:
            sn.process.kill()
        for ir in self.inner_ring_nodes:
            ir.process.kill()

    def persist(self) -> str:
        persisted_path = NeoFSEnv._generate_temp_file(prefix="persisted_env")
        with open(persisted_path, "wb") as fp:
            pickle.dump(self, fp)
        logger.info(f"Persist env at: {persisted_path}")
        return persisted_path
    
    def log_env_details_to_file(self):
        with open("env_details", "w") as fp:
            env_details = ""
            
            for ir_node in self.inner_ring_nodes:
                env_details += f"{ir_node}\n"
                
            for sn_node in self.storage_nodes:
                env_details += f"{sn_node}\n"
                
            env_details += f"{self.s3_gw}\n"
            env_details += f"{self.rest_gw}\n"
            env_details += f"{self.http_gw}\n"
            
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
        versions += NeoFSEnv._run_single_command(self.neofs_http_gw_path, "--version")
        allure.attach(versions, f"neofs env versions", allure.attachment_type.TEXT, ".txt")

    @allure.step("Download binaries")
    def download_binaries(self):
        logger.info(f"Going to download missing binaries, if any")
        deploy_threads = []

        binaries = [
            (self.neofs_adm_path, "neofs_adm"),
            (self.neofs_cli_path, "neofs_cli"),
            (self.neo_go_path, "neo_go"),
            (self.neofs_ir_path, "neofs_ir"),
            (self.neofs_node_path, "neofs_node"),
            (self.neofs_s3_authmate_path, "neofs_s3_authmate"),
            (self.neofs_s3_gw_path, "neofs_s3_gw"),
            (self.neofs_rest_gw_path, "neofs_rest_gw"),
            (self.neofs_http_gw_path, "neofs_http_gw"),
        ]

        for binary in binaries:
            binary_path, binary_name = binary
            if not os.path.isfile(binary_path):
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
                logger.info(f"'{binary_name}' already exists, will not be downloaded")

        if len(deploy_threads) > 0:
            for t in deploy_threads:
                t.start()
            logger.info(f"Wait until all binaries are downloaded")
            for t in deploy_threads:
                t.join()

    @classmethod
    def load(cls, persisted_path: str) -> "NeoFSEnv":
        with open(persisted_path, "rb") as fp:
            return pickle.load(fp)

    @classmethod
    @allure.step("Deploy simple neofs env")
    def simple(cls, neofs_env_config: dict = None) -> "NeoFSEnv":
        if not neofs_env_config:
            neofs_env_config = yaml.safe_load(
                files("neofs_testlib.env.templates").joinpath("neofs_env_config.yaml").read_text()
            )
        neofs_env = NeoFSEnv(neofs_env_config=neofs_env_config)
        neofs_env.download_binaries()
        neofs_env.deploy_inner_ring_node()
        neofs_env.deploy_storage_nodes(
            count=4, 
            node_attrs={
                0: ["UN-LOCODE:RU MOW", "Price:22"],
                1: ["UN-LOCODE:RU LED", "Price:33"],
                2: ["UN-LOCODE:SE STO", "Price:11"],
                3: ["UN-LOCODE:FI HEL", "Price:44"]
            }
        )
        neofs_env.deploy_s3_gw()
        neofs_env.deploy_http_gw()
        neofs_env.deploy_rest_gw()
        neofs_env.log_env_details_to_file()
        neofs_env.log_versions_to_allure()
        return neofs_env

    @staticmethod
    def generate_config_file(config_template: str, config_path: str, custom=False, **kwargs):
        jinja_env = jinja2.Environment()
        if custom:
            config_template = Path(config_template).read_text()
        else:
            config_template = (
                files("neofs_testlib.env.templates").joinpath(config_template).read_text()
            )
        jinja_template = jinja_env.from_string(config_template)
        rendered_config = jinja_template.render(**kwargs)
        with open(config_path, mode="w") as fp:
            fp.write(rendered_config)
      
    @staticmethod      
    def _run_single_command(binary: str, command: str) -> str:
        result = subprocess.run(
            [binary, command],
            capture_output = True,
            text = True
        )
        return f"{result.stdout}\n{result.stderr}\n"

    @classmethod
    def get_available_port(cls) -> str:
        for _ in range(len(cls._busy_ports) + 2):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(("", 0))
            addr = s.getsockname()
            s.close()
            if addr[1] not in cls._busy_ports:
                cls._busy_ports.append(addr[1])
                return addr[1]
        raise AssertionError("Can not find an available port")

    @staticmethod
    def download_binary(repo: str, version: str, file: str, target: str):
        download_url = f"https://github.com/{repo}/releases/download/{version}/{file}"
        resp = requests.get(download_url)
        if not resp.ok:
            raise AssertionError(
                f"Can not download binary from url: {download_url}: {resp.status_code}/{resp.reason}/{resp.json()}"
            )
        with open(target, mode="wb") as binary_file:
            binary_file.write(resp.content)
        # make binary executable
        current_perm = os.stat(target)
        os.chmod(target, current_perm.st_mode | stat.S_IEXEC)

    @staticmethod
    def _generate_temp_file(extension: str = "", prefix: str = "tmp_file") -> str:
        file_path = f"env_files/{prefix}_{''.join(random.choices(string.ascii_lowercase, k=10))}"
        if extension:
            file_path += f".{extension}"
        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.touch()
        return file_path

    @staticmethod
    def _generate_temp_dir(prefix: str = "tmp_dir") -> str:
        dir_path = f"env_files/{prefix}_{''.join(random.choices(string.ascii_lowercase, k=10))}"
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        return dir_path


class InnerRing:
    def __init__(self, neofs_env: NeoFSEnv):
        self.neofs_env = neofs_env
        self.network_config = NeoFSEnv._generate_temp_file(extension="yml", prefix="ir_network_config")
        self.cli_config = NeoFSEnv._generate_temp_file(extension="yml", prefix="ir_cli_config")
        self.alphabet_wallet = NodeWallet(
            path=NeoFSEnv._generate_temp_dir(prefix="ir_alphabet"), 
            address="", 
            password=self.neofs_env.default_password
        )
        self.ir_node_config_path = NeoFSEnv._generate_temp_file(extension="yml", prefix="ir_node_config")
        self.ir_storage_path = NeoFSEnv._generate_temp_file(extension="db", prefix="ir_storage")
        self.seed_nodes_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.rpc_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.p2p_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.grpc_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.ir_state_file = NeoFSEnv._generate_temp_file(prefix="ir_state_file")
        self.stdout = "Not initialized"
        self.stderr = "Not initialized"
        self.process = None

    def __str__(self):
        return f"""
            Inner Ring:
            - Alphabet wallet: {self.alphabet_wallet}
            - IR Config path: {self.ir_node_config_path}
            - Seed nodes address: {self.seed_nodes_address}
            - RPC address: {self.rpc_address}
            - P2P address: {self.p2p_address}
            - GRPC address: {self.grpc_address}
            - IR State file path: {self.ir_state_file}
            - STDOUT: {self.stdout}
            - STDERR: {self.stderr}
        """

    def __getstate__(self):
        attributes = self.__dict__.copy()
        del attributes["process"]
        return attributes

    def start(self):
        if self.process is not None:
            raise RuntimeError(f"This inner ring node instance has already been started")
        logger.info(f"Generating network config at: {self.network_config}")

        network_config_template = "network.yaml"

        NeoFSEnv.generate_config_file(
            config_template=network_config_template,
            config_path=self.network_config,
            custom=Path(network_config_template).is_file(),
            morph_endpoint=self.rpc_address,
            alphabet_wallets_path=self.alphabet_wallet.path,
            default_password=self.neofs_env.default_password,
        )
        logger.info(f"Generating alphabet wallets")
        self.neofs_env.generate_wallet(
            WalletType.ALPHABET, self.alphabet_wallet, network_config=self.network_config
        )
        logger.info(f"Generating IR config at: {self.ir_node_config_path}")

        ir_config_template = "ir.yaml"

        NeoFSEnv.generate_config_file(
            config_template=ir_config_template,
            config_path=self.ir_node_config_path,
            custom=Path(ir_config_template).is_file(),
            wallet=self.alphabet_wallet,
            public_key=wallet_utils.get_last_public_key_from_wallet(
                self.alphabet_wallet.path, self.alphabet_wallet.password
            ),
            ir_storage_path=self.ir_storage_path,
            seed_nodes_address=self.seed_nodes_address,
            rpc_address=self.rpc_address,
            p2p_address=self.p2p_address,
            grpc_address=self.grpc_address,
            ir_state_file=self.ir_state_file,
        )
        logger.info(f"Generating CLI config at: {self.cli_config}")
        NeoFSEnv.generate_config_file(
            config_template="cli_cfg.yaml", config_path=self.cli_config, wallet=self.alphabet_wallet
        )
        logger.info(f"Launching Inner Ring Node:{self}")
        self._launch_process()
        logger.info(f"Wait until IR is READY")
        self._wait_until_ready()

        self.neofs_env.neofs_adm

    def _launch_process(self):
        self.stdout = NeoFSEnv._generate_temp_file(prefix="ir_stdout")
        self.stderr = NeoFSEnv._generate_temp_file(prefix="ir_stderr")
        stdout_fp = open(self.stdout, "w")
        stderr_fp = open(self.stderr, "w")
        self.process = subprocess.Popen(
            [self.neofs_env.neofs_ir_path, "--config", self.ir_node_config_path],
            stdout=stdout_fp,
            stderr=stderr_fp,
        )

    @retry(wait=wait_fixed(10), stop=stop_after_attempt(10), reraise=True)
    def _wait_until_ready(self):
        neofs_cli = self.neofs_env.neofs_cli(self.cli_config)
        result = neofs_cli.control.healthcheck(endpoint=self.grpc_address, post_data="--ir")
        assert "READY" in result.stdout


class Shard:
    def __init__(self):
        self.metabase_path = NeoFSEnv._generate_temp_file(prefix="shard_metabase")
        self.blobovnicza_path = NeoFSEnv._generate_temp_file(prefix="shard_blobovnicza")
        self.fstree_path = NeoFSEnv._generate_temp_dir(prefix="shard_fstree")
        self.pilorama_path = NeoFSEnv._generate_temp_file(prefix="shard_pilorama")
        self.wc_path = NeoFSEnv._generate_temp_file(prefix="shard_wc")


class StorageNode:
    def __init__(
        self, 
        neofs_env: NeoFSEnv, 
        sn_number: int,
        node_attrs: Optional[list] = None, 
        attrs: Optional[dict] = None
    ):
        self.neofs_env = neofs_env
        self.wallet = NodeWallet(
            path=NeoFSEnv._generate_temp_file(prefix=f"sn_{sn_number}_wallet"),
            address="",
            password=self.neofs_env.default_password,
        )
        self.cli_config = NeoFSEnv._generate_temp_file(extension="yml", prefix=f"sn_{sn_number}_cli_config")
        self.storage_node_config_path = NeoFSEnv._generate_temp_file(extension="yml", prefix=f"sn_{sn_number}_config")
        self.state_file = NeoFSEnv._generate_temp_file(prefix=f"sn_{sn_number}_state")
        self.shards = [Shard(), Shard()]
        self.endpoint = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.control_grpc_endpoint = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.stdout = "Not initialized"
        self.stderr = "Not initialized"
        self.sn_number = sn_number
        self.process = None
        self.attrs = {}
        if node_attrs:
            self.attrs.update({f"NEOFS_NODE_ATTRIBUTE_{index}": attr for index, attr in enumerate(node_attrs)})
        if attrs:
            self.attrs.update(attrs)

    def __str__(self):
        return f"""
            Storage node:
            - Endpoint: {self.endpoint}
            - Control gRPC endpoint: {self.control_grpc_endpoint}
            - Attributes: {self.attrs}
            - STDOUT: {self.stdout}
            - STDERR: {self.stderr}
        """

    def __getstate__(self):
        attributes = self.__dict__.copy()
        del attributes["process"]
        return attributes

    @allure.step("Start storage node")
    def start(self, fresh=True):
        if fresh:
            logger.info(f"Generating wallet for storage node")
            self.neofs_env.generate_wallet(WalletType.STORAGE, self.wallet, label=f"sn{self.sn_number}")
            logger.info(f"Generating config for storage node at {self.storage_node_config_path}")

            sn_config_template = "sn.yaml"

            NeoFSEnv.generate_config_file(
                config_template=sn_config_template,
                config_path=self.storage_node_config_path,
                custom=Path(sn_config_template).is_file(),
                morph_endpoint=self.neofs_env.morph_rpc,
                shards=self.shards,
                wallet=self.wallet,
                state_file=self.state_file,
            )
            logger.info(f"Generating cli config for storage node at: {self.cli_config}")
            NeoFSEnv.generate_config_file(
                config_template="cli_cfg.yaml", config_path=self.cli_config, wallet=self.wallet
            )
        logger.info(f"Launching Storage Node:{self}")
        self._launch_process()
        logger.info(f"Wait until storage node is READY")
        self._wait_until_ready()
        allure.attach(str(self), f"sn_{self.sn_number}", allure.attachment_type.TEXT, ".txt")
        
    @allure.step("Stop storage node")
    def stop(self):
        self.process.terminate()
        
    @allure.step("Delete storage node data")
    def delete_data(self):
        self.stop()
        for shard in self.shards:
            os.remove(shard.metabase_path)
            os.remove(shard.blobovnicza_path)
            os.rmdir(shard.fstree_path)
            os.remove(shard.pilorama_path)
            os.remove(shard.wc_path)
        os.remove(self.state_file)
        self.shards = [Shard(), Shard()]

        sn_config_template = "sn.yaml"

        NeoFSEnv.generate_config_file(
            config_template=sn_config_template,
            config_path=self.storage_node_config_path,
            custom=Path(sn_config_template).is_file(),
            morph_endpoint=self.neofs_env.morph_rpc,
            shards=self.shards,
            wallet=self.wallet,
            state_file=self.state_file,
        )
        time.sleep(1)
        
    @allure.step("Delete storage node metadata")
    def delete_metadata(self):
        self.stop()
        for shard in self.shards:
            os.remove(shard.metabase_path)
            shard.metabase_path = NeoFSEnv._generate_temp_file(prefix=f"shard_metabase")

        sn_config_template = "sn.yaml"

        NeoFSEnv.generate_config_file(
            config_template=sn_config_template,
            config_path=self.storage_node_config_path,
            custom=Path(sn_config_template).is_file(),
            morph_endpoint=self.neofs_env.morph_rpc,
            shards=self.shards,
            wallet=self.wallet,
            state_file=self.state_file,
        )
        time.sleep(1)
        
    @allure.step("Set metabase resync")
    def set_metabase_resync(self, resync_state: bool):
        self.stop()
        for idx, _ in enumerate(self.shards):
            self.attrs.update({f"NEOFS_STORAGE_SHARD_{idx}_RESYNC_METABASE": f"{resync_state}".lower()})
        self.start(fresh=False)

    def _launch_process(self):
        self.stdout = NeoFSEnv._generate_temp_file(prefix=f"sn_{self.sn_number}_stdout")
        self.stderr = NeoFSEnv._generate_temp_file(prefix=f"sn_{self.sn_number}_stderr")
        stdout_fp = open(self.stdout, "w")
        stderr_fp = open(self.stderr, "w")
        env_dict = {
            "NEOFS_NODE_WALLET_PATH": self.wallet.path,
            "NEOFS_NODE_WALLET_PASSWORD": self.wallet.password,
            "NEOFS_NODE_ADDRESSES": self.endpoint,
            "NEOFS_GRPC_0_ENDPOINT": self.endpoint,
            "NEOFS_CONTROL_GRPC_ENDPOINT": self.control_grpc_endpoint,
        }
        env_dict.update(self.attrs)
        self.process = subprocess.Popen(
            [self.neofs_env.neofs_node_path, "--config", self.storage_node_config_path],
            stdout=stdout_fp,
            stderr=stderr_fp,
            env=env_dict,
        )

    @retry(wait=wait_fixed(15), stop=stop_after_attempt(30), reraise=True)
    def _wait_until_ready(self):
        neofs_cli = self.neofs_env.neofs_cli(self.cli_config)
        result = neofs_cli.control.healthcheck(endpoint=self.control_grpc_endpoint)
        assert "Health status: READY" in result.stdout, "Health is not ready"
        assert "Network status: ONLINE" in result.stdout, "Network is not online"


class S3_GW:
    def __init__(self, neofs_env: NeoFSEnv):
        self.neofs_env = neofs_env
        self.config_path = NeoFSEnv._generate_temp_file(extension="yml", prefix="s3gw_config")
        self.wallet = NodeWallet(
            path=NeoFSEnv._generate_temp_file(prefix="s3gw_wallet"),
            address="",
            password=self.neofs_env.default_password,
        )
        self.address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.tls_cert_path = NeoFSEnv._generate_temp_file(prefix="s3gw_tls_cert")
        self.tls_key_path = NeoFSEnv._generate_temp_file(prefix="s3gw_tls_key")
        self.stdout = "Not initialized"
        self.stderr = "Not initialized"
        self.process = None

    def __str__(self):
        return f"""
            S3 Gateway:
            - Address: {self.address}
            - S3 GW Config path: {self.config_path}
            - STDOUT: {self.stdout}
            - STDERR: {self.stderr}
        """

    def __getstate__(self):
        attributes = self.__dict__.copy()
        del attributes["process"]
        return attributes

    def start(self):
        if self.process is not None:
            raise RuntimeError(f"This s3 gw instance has already been started:\n{self}")
        self.neofs_env.generate_wallet(WalletType.STORAGE, self.wallet, label=f"s3")
        logger.info(f"Generating config for s3 gw at {self.config_path}")
        self._generate_config()
        logger.info(f"Launching S3 GW: {self}")
        self._launch_process()

    def _generate_config(self):
        tls_crt_template = files("neofs_testlib.env.templates").joinpath("tls.crt").read_text()
        with open(self.tls_cert_path, mode="w") as fp:
            fp.write(tls_crt_template)
        tls_key_template = files("neofs_testlib.env.templates").joinpath("tls.key").read_text()
        with open(self.tls_key_path, mode="w") as fp:
            fp.write(tls_key_template)

        s3_config_template = "s3.yaml"

        NeoFSEnv.generate_config_file(
            config_template=s3_config_template,
            config_path=self.config_path,
            custom=Path(s3_config_template).is_file(),
            address=self.address,
            cert_file_path=self.tls_cert_path,
            key_file_path=self.tls_key_path,
            wallet=self.wallet,
            morph_endpoint=self.neofs_env.morph_rpc,
        )

    def _launch_process(self):
        self.stdout = NeoFSEnv._generate_temp_file(prefix="s3gw_stdout")
        self.stderr = NeoFSEnv._generate_temp_file(prefix="s3gw_stderr")
        stdout_fp = open(self.stdout, "w")
        stderr_fp = open(self.stderr, "w")
        s3_gw_env = {
            "S3_GW_LISTEN_DOMAINS": self.neofs_env.domain,
            "S3_GW_TREE_SERVICE": self.neofs_env.storage_nodes[0].endpoint,
        }

        for index, sn in enumerate(self.neofs_env.storage_nodes):
            s3_gw_env[f"S3_GW_PEERS_{index}_ADDRESS"] = sn.endpoint
            s3_gw_env[f"S3_GW_PEERS_{index}_WEIGHT"] = "0.2"

        self.process = subprocess.Popen(
            [self.neofs_env.neofs_s3_gw_path, "--config", self.config_path],
            stdout=stdout_fp,
            stderr=stderr_fp,
            env=s3_gw_env,
        )


class HTTP_GW:
    def __init__(self, neofs_env: NeoFSEnv):
        self.neofs_env = neofs_env
        self.config_path = NeoFSEnv._generate_temp_file(extension="yml", prefix="http_gw_config")
        self.wallet = NodeWallet(
            path=NeoFSEnv._generate_temp_file(prefix="http_gw_wallet"),
            address="",
            password=self.neofs_env.default_password,
        )
        self.address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.stdout = "Not initialized"
        self.stderr = "Not initialized"
        self.process = None

    def __str__(self):
        return f"""
            HTTP Gateway:
            - Address: {self.address}
            - HTTP GW Config path: {self.config_path}
            - STDOUT: {self.stdout}
            - STDERR: {self.stderr}
        """

    def __getstate__(self):
        attributes = self.__dict__.copy()
        del attributes["process"]
        return attributes

    def start(self):
        if self.process is not None:
            raise RuntimeError(f"This http gw instance has already been started:\n{self}")
        self.neofs_env.generate_wallet(WalletType.STORAGE, self.wallet, label=f"http")
        logger.info(f"Generating config for http gw at {self.config_path}")
        self._generate_config()
        logger.info(f"Launching HTTP GW: {self}")
        self._launch_process()
        logger.info(f"Launched HTTP GW: {self}")

    def _generate_config(self):
        http_config_template = "http.yaml"

        NeoFSEnv.generate_config_file(
            config_template=http_config_template,
            config_path=self.config_path,
            custom=Path(http_config_template).is_file(),
            address=self.address,
            wallet=self.wallet,
        )

    def _launch_process(self):
        self.stdout = NeoFSEnv._generate_temp_file(prefix="http_gw_stdout")
        self.stderr = NeoFSEnv._generate_temp_file(prefix="http_gw_stderr")
        stdout_fp = open(self.stdout, "w")
        stderr_fp = open(self.stderr, "w")
        http_gw_env = {}

        for index, sn in enumerate(self.neofs_env.storage_nodes):
            http_gw_env[f"HTTP_GW_PEERS_{index}_ADDRESS"] = sn.endpoint
            http_gw_env[f"HTTP_GW_PEERS_{index}_WEIGHT"] = "0.2"

        self.process = subprocess.Popen(
            [self.neofs_env.neofs_http_gw_path, "--config", self.config_path],
            stdout=stdout_fp,
            stderr=stderr_fp,
            env=http_gw_env,
        )


class REST_GW:
    def __init__(self, neofs_env: NeoFSEnv):
        self.neofs_env = neofs_env
        self.config_path = NeoFSEnv._generate_temp_file(extension="yml", prefix="rest_gw_config")
        self.wallet = NodeWallet(
            path=NeoFSEnv._generate_temp_file(prefix="rest_gw_wallet"),
            address="",
            password=self.neofs_env.default_password,
        )
        self.address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.pprof_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.metrics_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.stdout = "Not initialized"
        self.stderr = "Not initialized"
        self.process = None

    def __str__(self):
        return f"""
            REST Gateway:
            - Address: {self.address}
            - Pprof address: {self.pprof_address}
            - Metrics address: {self.metrics_address}
            - REST GW Config path: {self.config_path}
            - STDOUT: {self.stdout}
            - STDERR: {self.stderr}
        """

    def __getstate__(self):
        attributes = self.__dict__.copy()
        del attributes["process"]
        return attributes

    def start(self):
        if self.process is not None:
            raise RuntimeError(f"This rest gw instance has already been started:\n{self}")
        self.neofs_env.generate_wallet(WalletType.STORAGE, self.wallet, label=f"rest")
        logger.info(f"Generating config for rest gw at {self.config_path}")
        self._generate_config()
        logger.info(f"Launching REST GW: {self}")
        self._launch_process()
        logger.info(f"Launched REST GW: {self}")

    def _generate_config(self):
        rest_config_template = "rest.yaml"

        NeoFSEnv.generate_config_file(
            config_template=rest_config_template,
            config_path=self.config_path,
            custom=Path(rest_config_template).is_file(),
            address=self.address,
            wallet=self.wallet,
            pprof_address=self.pprof_address,
            metrics_address=self.metrics_address,
        )

    def _launch_process(self):
        self.stdout = NeoFSEnv._generate_temp_file(prefix="rest_gw_stdout")
        self.stderr = NeoFSEnv._generate_temp_file(prefix="rest_gw_stderr")
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
