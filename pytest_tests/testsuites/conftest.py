import logging
import os
import shutil
from datetime import datetime

import allure
import pytest
import wallet
import yaml
from binary_version_helper import get_local_binaries_versions, get_remote_binaries_versions
from common import (
    ASSETS_DIR,
    FREE_STORAGE,
    HOSTING_CONFIG_FILE,
    INFRASTRUCTURE_TYPE,
    MAINNET_WALLET_PATH,
    NEOFS_NETMAP_DICT,
)
from env_properties import save_env_properties
from neofs_testlib.hosting import Hosting
from neofs_testlib.shell import LocalShell, Shell
from payment_neogo import neofs_deposit, transfer_mainnet_gas
from python_keywords.node_management import node_healthcheck

logger = logging.getLogger("NeoLogger")


@pytest.fixture(scope="session")
def client_shell() -> Shell:
    yield LocalShell()


@pytest.fixture(scope="session")
def cloud_infrastructure_check():
    if INFRASTRUCTURE_TYPE != "CLOUD_VM":
        pytest.skip("Test only works on SberCloud infrastructure")
    yield


@pytest.fixture(scope="session")
def hosting() -> Hosting:
    with open(HOSTING_CONFIG_FILE, "r") as file:
        hosting_config = yaml.full_load(file)

    hosting_instance = Hosting()
    hosting_instance.configure(hosting_config)
    yield hosting_instance


@pytest.fixture(scope="session", autouse=True)
@allure.title("Check binary versions")
def check_binary_versions(request, hosting: Hosting, client_shell: Shell):
    local_versions = get_local_binaries_versions(client_shell)
    remote_versions = get_remote_binaries_versions(hosting)

    all_versions = {**local_versions, **remote_versions}
    save_env_properties(request.config, all_versions)


@pytest.fixture(scope="session")
@allure.title("Prepare tmp directory")
def prepare_tmp_dir():
    full_path = f"{os.getcwd()}/{ASSETS_DIR}"
    shutil.rmtree(full_path, ignore_errors=True)
    os.mkdir(full_path)
    yield full_path
    shutil.rmtree(full_path)


@pytest.fixture(scope="session", autouse=True)
@allure.title("Collect logs")
def collect_logs(prepare_tmp_dir, hosting: Hosting):
    start_time = datetime.utcnow()
    yield
    end_time = datetime.utcnow()

    # Dump logs to temp directory (because they might be too large to keep in RAM)
    logs_dir = os.path.join(prepare_tmp_dir, "logs")
    os.makedirs(logs_dir)

    for host in hosting.hosts:
        host.dump_logs(logs_dir, since=start_time, until=end_time)

    # Zip all files and attach to Allure because it is more convenient to download a single
    # zip with all logs rather than mess with individual logs files per service or node
    logs_zip_file_path = shutil.make_archive(logs_dir, "zip", logs_dir)
    allure.attach.file(logs_zip_file_path, name="logs.zip", extension="zip")


@pytest.fixture(scope="session", autouse=True)
@allure.title("Run health check for all storage nodes")
def run_health_check(collect_logs, hosting: Hosting):
    failed_nodes = []
    for node_name in NEOFS_NETMAP_DICT.keys():
        health_check = node_healthcheck(hosting, node_name)
        if health_check.health_status != "READY" or health_check.network_status != "ONLINE":
            failed_nodes.append(node_name)

    if failed_nodes:
        raise AssertionError(f"Nodes {failed_nodes} are not healthy")


@pytest.fixture(scope="session")
@allure.title("Prepare wallet and deposit")
def prepare_wallet_and_deposit(prepare_tmp_dir):
    wallet_path, addr, _ = wallet.init_wallet(ASSETS_DIR)
    logger.info(f"Init wallet: {wallet_path},\naddr: {addr}")
    allure.attach.file(wallet_path, os.path.basename(wallet_path), allure.attachment_type.JSON)

    if not FREE_STORAGE:
        deposit = 30
        transfer_mainnet_gas(wallet_path, deposit + 1, wallet_path=MAINNET_WALLET_PATH)
        neofs_deposit(wallet_path, deposit)

    return wallet_path
