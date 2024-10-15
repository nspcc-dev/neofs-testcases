import datetime
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
from typing import Optional

import allure
import jinja2
import requests
import yaml
from helpers.common import get_assets_dir_path

from neofs_testlib.cli import NeofsAdm, NeofsCli, NeofsLens, NeoGo
from neofs_testlib.shell import LocalShell
from neofs_testlib.utils import wallet as wallet_utils
from tenacity import retry, stop_after_attempt, wait_fixed

logger = logging.getLogger("neofs.testlib.env")


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


class NeoFSEnv:
    _busy_ports = []

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
    def deploy_inner_ring_nodes(self, count=1, with_main_chain=False):
        for _ in range(count):
            new_inner_ring_node = InnerRing(self)
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
            for ir_node in self.inner_ring_nodes:
                logger.info(f"Wait until IR: {ir_node} is READY")
                try:
                    ir_node._wait_until_ready()
                except Exception as e:
                    allure.attach.file(ir_node.stderr, name="ir node logs", extension="txt")
                    raise e

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
            deploy_threads.append(threading.Thread(target=new_storage_node.start))
        for t in deploy_threads:
            t.start()
        logger.info("Wait until storage nodes are deployed")
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
        neo_go.nep17.transfer(
            "GAS",
            self.default_wallet.address,
            f"http://{self.main_chain.rpc_address}",
            from_address=self.inner_ring_nodes[-1].alphabet_wallet.address,
            amount=9000,
            force=True,
            wallet_config=self.main_chain.neo_go_config,
            await_=True,
        )
        ir_alphabet_pubkey_from_neogo = wallet_utils.get_last_public_key_from_wallet_with_neogo(
            self.neo_go(), self.inner_ring_nodes[-1].alphabet_wallet.path
        )
        result = neo_go.contract.deploy(
            input_file=f"{self.neofs_contract_dir}/neofs/neofs_contract.nef",
            manifest=f"{self.neofs_contract_dir}/neofs/config.json",
            force=True,
            rpc_endpoint=f"http://{self.main_chain.rpc_address}",
            post_data=f"[ true ffffffffffffffffffffffffffffffffffffffff [ {ir_alphabet_pubkey_from_neogo} ] [ InnerRingCandidateFee 10 WithdrawFee 10 ] ]",
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

        neofs_adm.morph.generate_storage_wallet(
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
        self,
        network_config: Optional[str] = None,
        size: Optional[int] = 1,
    ) -> list[NodeWallet]:
        neofs_adm = self.neofs_adm(network_config)

        neofs_adm.morph.generate_alphabet(alphabet_wallets=self.alphabet_wallets_dir, size=size)

        generated_wallets = []

        for generated_wallet in os.listdir(self.alphabet_wallets_dir):
            # neo3 package requires some attributes to be set
            with open(os.path.join(self.alphabet_wallets_dir, generated_wallet), "r") as wallet_file:
                wallet_json = json.load(wallet_file)

            wallet_json["name"] = None
            for acc in wallet_json["accounts"]:
                acc["extra"] = None

            with open(os.path.join(self.alphabet_wallets_dir, generated_wallet), "w") as wallet_file:
                json.dump(wallet_json, wallet_file)

            generated_wallets.append(
                NodeWallet(
                    path=os.path.join(self.alphabet_wallets_dir, generated_wallet),
                    password=self.default_password,
                    address=wallet_utils.get_last_address_from_wallet(
                        os.path.join(self.alphabet_wallets_dir, generated_wallet), self.default_password
                    ),
                )
            )
        return generated_wallets

    @allure.step("Kill current neofs env")
    def kill(self):
        if self.rest_gw:
            self.rest_gw.process.kill()
        if self.s3_gw:
            self.s3_gw.process.kill()
        if self.main_chain:
            self.main_chain.process.kill()
        for sn in self.storage_nodes:
            sn.process.kill()
        for ir in self.inner_ring_nodes:
            ir.process.kill()

    def persist(self) -> str:
        persisted_path = self._generate_temp_file(self._env_dir, prefix="persisted_env")
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
            (self.warp_path, "warp_linux_x86_64"),
            (self.warp_path, "warp_darwin_arm64"),
        ]

        for binary in binaries:
            binary_path, binary_name = binary
            if not os.path.isfile(binary_path) and not os.path.isdir(binary_path):
                neofs_binary_params = self.neofs_env_config["binaries"][binary_name]
                if not self._is_binary_compatible(neofs_binary_params.get("platform"), neofs_binary_params.get("arch")):
                    logger.info(f"Skip '{binary_name}' because of unsupported platform/architecture")
                    continue
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
    @allure.step("Deploy simple neofs env")
    def simple(cls, neofs_env_config: dict = None, with_main_chain=False) -> "NeoFSEnv":
        if not neofs_env_config:
            neofs_env_config = yaml.safe_load(
                files("neofs_testlib.env.templates").joinpath("neofs_env_config.yaml").read_text()
            )
        neofs_env = NeoFSEnv(neofs_env_config=neofs_env_config)
        neofs_env.download_binaries()
        neofs_env.deploy_inner_ring_nodes(with_main_chain=with_main_chain)
        neofs_env.deploy_storage_nodes(
            count=4,
            node_attrs={
                0: ["UN-LOCODE:RU MOW", "Price:22"],
                1: ["UN-LOCODE:RU LED", "Price:33"],
                2: ["UN-LOCODE:SE STO", "Price:11"],
                3: ["UN-LOCODE:FI HEL", "Price:44"],
            },
        )
        if with_main_chain:
            neofs_adm = neofs_env.neofs_adm()
            for sn in neofs_env.storage_nodes:
                neofs_adm.morph.refill_gas(
                    rpc_endpoint=f"http://{neofs_env.morph_rpc}",
                    alphabet_wallets=neofs_env.alphabet_wallets_dir,
                    storage_wallet=sn.wallet.path,
                    gas="10.0",
                )
            neofs_env.neofs_adm().morph.set_config(
                rpc_endpoint=f"http://{neofs_env.morph_rpc}",
                alphabet_wallets=neofs_env.alphabet_wallets_dir,
                post_data="WithdrawFee=5",
            )
        else:
            neofs_env.neofs_adm().morph.set_config(
                rpc_endpoint=f"http://{neofs_env.morph_rpc}",
                alphabet_wallets=neofs_env.alphabet_wallets_dir,
                post_data="ContainerFee=0 ContainerAliasFee=0 MaxObjectSize=524288",
            )
        time.sleep(30)
        neofs_env.deploy_s3_gw()
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
            config_template = files("neofs_testlib.env.templates").joinpath(config_template).read_text()
        jinja_template = jinja_env.from_string(config_template)
        rendered_config = jinja_template.render(**kwargs)
        with open(config_path, mode="w") as fp:
            fp.write(rendered_config)

    @staticmethod
    def _run_single_command(binary: str, command: str) -> str:
        result = subprocess.run([binary, command], capture_output=True, text=True)
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
        if "contract" in file:
            # unpack contracts file into dir
            tar_file = tarfile.open(target)
            tar_file.extractall()
            tar_file.close()
            os.remove(target)
            logger.info(f"rename: {file.rstrip(".tar.gz")} into {target}")
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


class MainChain:
    def __init__(self, neofs_env: NeoFSEnv):
        self.neofs_env = neofs_env
        self.main_chain_dir = self.neofs_env._generate_temp_dir("main_chain")
        self.cli_config = self.neofs_env._generate_temp_file(
            self.main_chain_dir, extension="yml", prefix="main_chain_cli_config"
        )
        self.neo_go_config = self.neofs_env._generate_temp_file(
            self.main_chain_dir, extension="yml", prefix="main_chain_neo_go_config"
        )
        self.main_chain_config_path = self.neofs_env._generate_temp_file(
            self.main_chain_dir, extension="yml", prefix="main_chain_config"
        )
        self.main_chain_boltdb = self.neofs_env._generate_temp_file(
            self.main_chain_dir, extension="db", prefix="main_chain_bolt_db"
        )
        self.rpc_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.p2p_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.pprof_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.prometheus_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.stdout = "Not initialized"
        self.stderr = "Not initialized"
        self.process = None
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

    def __getstate__(self):
        attributes = self.__dict__.copy()
        del attributes["process"]
        return attributes

    @allure.step("Start Main Chain")
    def start(self, wait_until_ready=True):
        if self.process is not None:
            raise RuntimeError("This main chain instance has already been started")

        alphabet_wallet = self.neofs_env.inner_ring_nodes[-1].alphabet_wallet

        ir_alphabet_pubkey_from_neogo = wallet_utils.get_last_public_key_from_wallet_with_neogo(
            self.neofs_env.neo_go(), alphabet_wallet.path
        )

        logger.info(f"Generating main chain config at: {self.main_chain_config_path}")
        main_chain_config_template = "main_chain.yaml"

        NeoFSEnv.generate_config_file(
            config_template=main_chain_config_template,
            config_path=self.main_chain_config_path,
            custom=Path(main_chain_config_template).is_file(),
            wallet=alphabet_wallet,
            public_key=ir_alphabet_pubkey_from_neogo,
            main_chain_boltdb=self.main_chain_boltdb,
            p2p_address=self.p2p_address,
            rpc_address=self.rpc_address,
            sn_addresses=[sn.endpoint for sn in self.neofs_env.storage_nodes],
            pprof_address=self.pprof_address,
            prometheus_address=self.prometheus_address,
        )
        logger.info(f"Generating CLI config at: {self.cli_config}")
        NeoFSEnv.generate_config_file(
            config_template="cli_cfg.yaml", config_path=self.cli_config, wallet=alphabet_wallet
        )
        logger.info(f"Generating NEO GO config at: {self.neo_go_config}")
        NeoFSEnv.generate_config_file(
            config_template="neo_go_cfg.yaml", config_path=self.neo_go_config, wallet=alphabet_wallet
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

    @retry(wait=wait_fixed(10), stop=stop_after_attempt(50), reraise=True)
    def _wait_until_ready(self):
        result = self.neofs_env.neo_go().query.height(rpc_endpoint=f"http://{self.rpc_address}")
        logger.info("WAIT UNTIL MAIN CHAIN IS READY:")
        logger.info(result.stdout)
        logger.info(result.stderr)


class InnerRing:
    def __init__(self, neofs_env: NeoFSEnv):
        self.neofs_env = neofs_env
        self.inner_ring_dir = self.neofs_env._generate_temp_dir("inner_ring")
        self.network_config = self.neofs_env._generate_temp_file(
            self.inner_ring_dir, extension="yml", prefix="ir_network_config"
        )
        self.cli_config = self.neofs_env._generate_temp_file(
            self.inner_ring_dir, extension="yml", prefix="ir_cli_config"
        )
        self.alphabet_wallet = None
        self.ir_node_config_path = self.neofs_env._generate_temp_file(
            self.inner_ring_dir, extension="yml", prefix="ir_node_config"
        )
        self.ir_storage_path = self.neofs_env._generate_temp_file(
            self.inner_ring_dir, extension="db", prefix="ir_storage"
        )
        self.rpc_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.p2p_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.grpc_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.pprof_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.prometheus_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.ir_state_file = self.neofs_env._generate_temp_file(self.inner_ring_dir, prefix="ir_state_file")
        self.stdout = "Not initialized"
        self.stderr = "Not initialized"
        self.process = None

    def __str__(self):
        return f"""
            Inner Ring:
            - Alphabet wallet: {self.alphabet_wallet}
            - IR Config path: {self.ir_node_config_path}
            - RPC address: {self.rpc_address}
            - P2P address: {self.p2p_address}
            - GRPC address: {self.grpc_address}
            - Pprof address: {self.pprof_address}
            - Prometheus address: {self.prometheus_address}
            - IR State file path: {self.ir_state_file}
            - STDOUT: {self.stdout}
            - STDERR: {self.stderr}
        """

    def __getstate__(self):
        attributes = self.__dict__.copy()
        del attributes["process"]
        return attributes

    def generate_network_config(self):
        logger.info(f"Generating network config at: {self.network_config}")

        network_config_template = "network.yaml"

        NeoFSEnv.generate_config_file(
            config_template=network_config_template,
            config_path=self.network_config,
            custom=Path(network_config_template).is_file(),
            morph_endpoint=self.rpc_address,
            alphabet_wallets_path=self.neofs_env.alphabet_wallets_dir,
            default_password=self.neofs_env.default_password,
        )

    def generate_cli_config(self):
        logger.info(f"Generating CLI config at: {self.cli_config}")
        NeoFSEnv.generate_config_file(
            config_template="cli_cfg.yaml", config_path=self.cli_config, wallet=self.alphabet_wallet
        )

    @allure.step("Start Inner Ring node")
    def start(self, wait_until_ready=True, with_main_chain=False):
        if self.process is not None:
            raise RuntimeError("This inner ring node instance has already been started")
        logger.info(f"Generating IR config at: {self.ir_node_config_path}")
        ir_config_template = "ir.yaml"

        pub_keys_of_existing_ir_nodes = [
            wallet_utils.get_last_public_key_from_wallet(ir_node.alphabet_wallet.path, ir_node.alphabet_wallet.password)
            for ir_node in self.neofs_env.inner_ring_nodes
        ]

        seed_node_addresses_of_existing_ir_nodes = [ir_node.p2p_address for ir_node in self.neofs_env.inner_ring_nodes]

        NeoFSEnv.generate_config_file(
            config_template=ir_config_template,
            config_path=self.ir_node_config_path,
            custom=Path(ir_config_template).is_file(),
            wallet=self.alphabet_wallet,
            public_keys=pub_keys_of_existing_ir_nodes,
            ir_storage_path=self.ir_storage_path,
            seed_nodes_addresses=seed_node_addresses_of_existing_ir_nodes,
            rpc_address=self.rpc_address,
            p2p_address=self.p2p_address,
            grpc_address=self.grpc_address,
            ir_state_file=self.ir_state_file,
            peers_min_number=int(
                len(self.neofs_env.inner_ring_nodes) - (len(self.neofs_env.inner_ring_nodes) - 1) / 3 - 1
            ),
            set_roles_in_genesis=str(False if len(self.neofs_env.inner_ring_nodes) == 1 else True).lower(),
            control_public_key=wallet_utils.get_last_public_key_from_wallet(
                self.alphabet_wallet.path, self.alphabet_wallet.password
            ),
            without_mainnet=f"{not with_main_chain}".lower(),
            main_chain_rpc="localhost:1234" if not with_main_chain else self.neofs_env.main_chain.rpc_address,
            neofs_contract_hash="123" if not with_main_chain else self.neofs_env.main_chain.neofs_contract_hash,
            pprof_address=self.pprof_address,
            prometheus_address=self.prometheus_address,
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

    @retry(wait=wait_fixed(10), stop=stop_after_attempt(50), reraise=True)
    def _wait_until_ready(self):
        neofs_cli = self.neofs_env.neofs_cli(self.cli_config)
        result = neofs_cli.control.healthcheck(endpoint=self.grpc_address, post_data="--ir")
        assert "READY" in result.stdout


class Shard:
    def __init__(self, neofs_env: NeoFSEnv, sn_dir: str):
        self.metabase_path = neofs_env._generate_temp_file(sn_dir, prefix="shard_metabase")
        self.blobovnicza_path = neofs_env._generate_temp_file(sn_dir, prefix="shard_blobovnicza")
        self.fstree_path = neofs_env._generate_temp_dir(prefix="shards/shard_fstree")
        self.pilorama_path = neofs_env._generate_temp_file(sn_dir, prefix="shard_pilorama")
        self.wc_path = neofs_env._generate_temp_file(sn_dir, prefix="shard_wc")


class StorageNode:
    def __init__(
        self, neofs_env: NeoFSEnv, sn_number: int, node_attrs: Optional[list] = None, attrs: Optional[dict] = None
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
        self.storage_node_config_path = self.neofs_env._generate_temp_file(
            self.sn_dir, extension="yml", prefix=f"sn_{sn_number}_config"
        )
        self.state_file = self.neofs_env._generate_temp_file(self.sn_dir, prefix=f"sn_{sn_number}_state")
        self.shards = [Shard(neofs_env, self.sn_dir), Shard(neofs_env, self.sn_dir)]
        self.endpoint = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.control_grpc_endpoint = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.pprof_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.prometheus_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
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
            - Pprof address: {self.pprof_address}
            - Prometheus address: {self.prometheus_address}
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
            logger.info("Generating wallet for storage node")
            self.neofs_env.generate_storage_wallet(self.wallet, label=f"sn{self.sn_number}")
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
                pprof_address=self.pprof_address,
                prometheus_address=self.prometheus_address,
            )
            logger.info(f"Generating cli config for storage node at: {self.cli_config}")
            NeoFSEnv.generate_config_file(
                config_template="cli_cfg.yaml", config_path=self.cli_config, wallet=self.wallet
            )
        logger.info(f"Launching Storage Node:{self}")
        self._launch_process()
        logger.info("Wait until storage node is READY")
        self._wait_until_ready()
        allure.attach(str(self), f"sn_{self.sn_number}", allure.attachment_type.TEXT, ".txt")

    @allure.step("Stop storage node")
    def stop(self):
        logger.info(f"Stopping Storage Node:{self}")
        self.process.terminate()
        self.process.wait()

    @allure.step("Kill storage node")
    def kill(self):
        logger.info(f"Killing Storage Node:{self}")
        self.process.kill()

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
            pprof_address=self.pprof_address,
            prometheus_address=self.prometheus_address,
        )
        time.sleep(1)

    @allure.step("Delete storage node metadata")
    def delete_metadata(self):
        self.stop()
        for shard in self.shards:
            os.remove(shard.metabase_path)
            shard.metabase_path = self.neofs_env._generate_temp_file(self.sn_dir, prefix="shard_metabase")

        sn_config_template = "sn.yaml"

        NeoFSEnv.generate_config_file(
            config_template=sn_config_template,
            config_path=self.storage_node_config_path,
            custom=Path(sn_config_template).is_file(),
            morph_endpoint=self.neofs_env.morph_rpc,
            shards=self.shards,
            wallet=self.wallet,
            state_file=self.state_file,
            pprof_address=self.pprof_address,
            prometheus_address=self.prometheus_address,
        )
        time.sleep(1)

    @allure.step("Set metabase resync")
    def set_metabase_resync(self, resync_state: bool):
        self.stop()
        for idx, _ in enumerate(self.shards):
            self.attrs.update({f"NEOFS_STORAGE_SHARD_{idx}_RESYNC_METABASE": f"{resync_state}".lower()})
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

    def _get_version(self) -> str:
        raw_version_output = self.neofs_env._run_single_command(self.neofs_env.neofs_node_path, "--version")
        for line in raw_version_output.splitlines():
            if "Version:" in line:
                return line.split("Version:")[1].strip()
        return ""


class S3_GW:
    def __init__(self, neofs_env: NeoFSEnv):
        self.neofs_env = neofs_env
        self.s3_gw_dir = self.neofs_env._generate_temp_dir("s3-gw")
        self.config_path = self.neofs_env._generate_temp_file(self.s3_gw_dir, extension="yml", prefix="s3gw_config")
        self.wallet = NodeWallet(
            path=self.neofs_env._generate_temp_file(self.s3_gw_dir, prefix="s3gw_wallet"),
            address="",
            password=self.neofs_env.default_password,
        )
        self.address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.pprof_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.prometheus_address = f"{self.neofs_env.domain}:{NeoFSEnv.get_available_port()}"
        self.tls_cert_path = self.neofs_env._generate_temp_file(self.s3_gw_dir, prefix="s3gw_tls_cert")
        self.tls_key_path = self.neofs_env._generate_temp_file(self.s3_gw_dir, prefix="s3gw_tls_key")
        self.stdout = "Not initialized"
        self.stderr = "Not initialized"
        self.tls_enabled = True
        self.process = None

    def __str__(self):
        return f"""
            S3 Gateway:
            - Address: {self.address}
            - S3 GW Config path: {self.config_path}
            - Pprof address: {self.pprof_address}
            - Prometheus address: {self.prometheus_address}
            - STDOUT: {self.stdout}
            - STDERR: {self.stderr}
        """

    def __getstate__(self):
        attributes = self.__dict__.copy()
        del attributes["process"]
        return attributes

    def start(self, fresh=True):
        if self.process is not None:
            raise RuntimeError(f"This s3 gw instance has already been started:\n{self}")
        if fresh:
            self.neofs_env.generate_storage_wallet(self.wallet, label="s3")
        logger.info(f"Generating config for s3 gw at {self.config_path}")
        self._generate_config()
        logger.info(f"Launching S3 GW: {self}")
        self._launch_process()
        self._wait_until_ready()

    @allure.step("Stop s3 gw")
    def stop(self):
        logger.info(f"Stopping s3 gw:{self}")
        self.process.terminate()
        self.process.wait()
        self.process = None

    @retry(wait=wait_fixed(10), stop=stop_after_attempt(2), reraise=True)
    def _wait_until_ready(self):
        endpoint = f"https://{self.address}" if self.tls_enabled else f"http://{self.address}"
        resp = requests.get(endpoint, verify=False)
        assert resp.status_code == 200

    def _generate_config(self):
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
            address=self.address,
            tls_enabled=str(self.tls_enabled).lower(),
            cert_file_path=self.tls_cert_path,
            key_file_path=self.tls_key_path,
            wallet=self.wallet,
            morph_endpoint=self.neofs_env.morph_rpc,
            peers=peers,
            tree_service_endpoint=self.neofs_env.storage_nodes[0].endpoint,
            listen_domain=self.neofs_env.domain,
            s3_gw_version=self._get_version(),
            pprof_address=self.pprof_address,
            prometheus_address=self.prometheus_address,
        )

    def _get_version(self) -> str:
        raw_version_output = self.neofs_env._run_single_command(self.neofs_env.neofs_s3_gw_path, "--version")
        for line in raw_version_output.splitlines():
            if "Version:" in line:
                return line.split("Version:")[1].strip()
        return ""

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


class REST_GW:
    def __init__(self, neofs_env: NeoFSEnv):
        self.neofs_env = neofs_env
        self.rest_gw_dir = self.neofs_env._generate_temp_dir("rest-gw")
        self.config_path = self.neofs_env._generate_temp_file(
            self.rest_gw_dir, extension="yml", prefix="rest_gw_config"
        )
        self.wallet = NodeWallet(
            path=self.neofs_env._generate_temp_file(self.rest_gw_dir, prefix="rest_gw_wallet"),
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
        self.neofs_env.generate_storage_wallet(self.wallet, label="rest")
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
        assert (
            f"Total Containers has been created: {containers}" in result.stdout
        ), "Prepare script didn't create requested containers"
        assert (
            f"Total Objects has been created: {preload_obj}" in result.stdout
        ), "Prepare script didn't create requested objects"

        shutil.copy(out, os.path.join(self.xk6_dir, "scenarios"))
