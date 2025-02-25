import datetime
import logging
import os
import shutil
import time
from pathlib import Path

import allure
import pytest
from helpers.common import (
    COMPLEX_OBJECT_CHUNKS_COUNT,
    COMPLEX_OBJECT_TAIL_SIZE,
    SIMPLE_OBJECT_SIZE,
    TEST_FILES_DIR,
    TEST_OBJECTS_DIR,
    get_assets_dir_path,
)
from helpers.file_helper import generate_file
from helpers.neofs_verbs import get_netmap_netinfo
from helpers.wallet_helpers import create_wallet
from neofs_testlib.env.env import NeoFSEnv, NodeWallet
from neofs_testlib.reporter import AllureHandler, get_reporter
from neofs_testlib.shell import Shell

get_reporter().register_handler(AllureHandler())
logger = logging.getLogger("NeoLogger")


def pytest_addoption(parser):
    parser.addoption("--persist-env", action="store_true", default=False, help="persist deployed env")
    parser.addoption("--load-env", action="store", help="load persisted env from file")


@pytest.fixture(scope="session")
def neofs_env(temp_directory, artifacts_directory, request):
    NeoFSEnv.cleanup_unused_ports()
    if request.config.getoption("--load-env"):
        neofs_env = NeoFSEnv.load(request.config.getoption("--load-env"))
    else:
        neofs_env = NeoFSEnv.simple()

    yield neofs_env

    if request.config.getoption("--persist-env"):
        neofs_env.persist()
    else:
        if not request.config.getoption("--load-env"):
            neofs_env.kill()

    if (
        request.session.testsfailed
        and not request.config.getoption("--persist-env")
        and not request.config.getoption("--load-env")
    ):
        for ir in neofs_env.inner_ring_nodes:
            os.remove(ir.ir_storage_path)
        for sn in neofs_env.storage_nodes:
            for shard in sn.shards:
                os.remove(shard.metabase_path)
                os.remove(shard.blobovnicza_path)
                shutil.rmtree(shard.fstree_path, ignore_errors=True)
                os.remove(shard.pilorama_path)
                shutil.rmtree(shard.wc_path, ignore_errors=True)

        shutil.make_archive(
            os.path.join(get_assets_dir_path(), f"neofs_env_{neofs_env._id}"), "zip", neofs_env._env_dir
        )
        shutil.rmtree(neofs_env._env_dir, ignore_errors=True)
        allure.attach.file(
            os.path.join(get_assets_dir_path(), f"neofs_env_{neofs_env._id}.zip"),
            name="neofs env files",
            extension="zip",
        )
    NeoFSEnv.cleanup_unused_ports()


@pytest.fixture(scope="session")
@allure.title("Prepare default wallet and deposit")
def default_wallet(temp_directory):
    return create_wallet()


@pytest.fixture(scope="session")
def client_shell(neofs_env: NeoFSEnv) -> Shell:
    yield neofs_env.shell


@pytest.fixture(scope="session")
def max_object_size(neofs_env: NeoFSEnv, client_shell: Shell) -> int:
    storage_node = neofs_env.storage_nodes[0]
    net_info = get_netmap_netinfo(
        wallet=storage_node.wallet.path,
        wallet_config=storage_node.cli_config,
        endpoint=storage_node.endpoint,
        shell=client_shell,
    )
    yield net_info["maximum_object_size"]


@pytest.fixture(scope="session")
def simple_object_size(max_object_size: int) -> int:
    yield int(SIMPLE_OBJECT_SIZE) if int(SIMPLE_OBJECT_SIZE) < max_object_size else max_object_size


@pytest.fixture(scope="session")
def complex_object_size(max_object_size: int) -> int:
    return max_object_size * int(COMPLEX_OBJECT_CHUNKS_COUNT) + int(COMPLEX_OBJECT_TAIL_SIZE)


@pytest.fixture(scope="session")
@allure.title("Prepare tmp directory")
def temp_directory(request) -> str:
    with allure.step("Prepare tmp directory"):
        full_path = get_assets_dir_path()
        create_dir(full_path)

    yield full_path

    if not request.config.getoption("--persist-env") and not request.config.getoption("--load-env"):
        with allure.step("Remove tmp directory"):
            remove_dir(full_path)


@pytest.fixture(scope="session", autouse=True)
@allure.title("Prepare test files directories")
def artifacts_directory(request, temp_directory: str) -> None:
    dirs = [TEST_FILES_DIR, TEST_OBJECTS_DIR]
    for dir_name in dirs:
        with allure.step(f"Prepare {dir_name} directory"):
            full_path = os.path.join(temp_directory, dir_name)
            create_dir(full_path)

    yield

    if not request.config.getoption("--persist-env") and not request.config.getoption("--load-env"):
        for dir_name in dirs:
            with allure.step(f"Remove {dir_name} directory"):
                remove_dir(full_path)


@pytest.fixture(scope="module")
def owner_wallet(temp_directory) -> NodeWallet:
    """
    Returns wallet which owns containers and objects
    """
    return create_wallet()


@pytest.fixture(scope="module")
def user_wallet(temp_directory) -> NodeWallet:
    """
    Returns wallet which will use objects from owner via static session
    """
    return create_wallet()


@pytest.fixture(scope="module")
def stranger_wallet(temp_directory) -> NodeWallet:
    """
    Returns stranger wallet which should fail to obtain data
    """
    return create_wallet()


@pytest.fixture(scope="module")
def scammer_wallet(temp_directory) -> NodeWallet:
    """
    Returns stranger wallet which should fail to obtain data
    """
    return create_wallet()


@pytest.fixture(scope="module")
def not_owner_wallet(temp_directory) -> NodeWallet:
    """
    Returns stranger wallet which should fail to obtain data
    """
    return create_wallet()


@pytest.fixture(scope="function")
@allure.title("Enable metabase resync on start")
def enable_metabase_resync_on_start(neofs_env: NeoFSEnv):
    for node in neofs_env.storage_nodes:
        node.set_metabase_resync(True)
    yield
    for node in neofs_env.storage_nodes:
        node.set_metabase_resync(False)


@pytest.fixture(scope="module")
def file_path(simple_object_size, artifacts_directory):
    yield generate_file(simple_object_size)


def create_dir(dir_path: str) -> None:
    with allure.step("Create directory"):
        remove_dir(dir_path)
        Path(dir_path).mkdir(parents=True, exist_ok=True)


def remove_dir(dir_path: str) -> None:
    with allure.step("Remove directory"):
        shutil.rmtree(dir_path, ignore_errors=True)


@pytest.fixture
def datadir(tmpdir, request):
    filename = request.module.__file__
    test_dir, _ = os.path.splitext(filename)

    if os.path.isdir(test_dir):
        shutil.copytree(test_dir, str(tmpdir), dirs_exist_ok=True)

    return tmpdir


@pytest.fixture
def neofs_env_with_mainchain(request):
    NeoFSEnv.cleanup_unused_ports()
    if request.config.getoption("--load-env"):
        neofs_env = NeoFSEnv.load(request.config.getoption("--load-env"))
    else:
        neofs_env = NeoFSEnv.simple(with_main_chain=True)

    yield neofs_env

    if request.config.getoption("--persist-env"):
        neofs_env.persist()
    else:
        if not request.config.getoption("--load-env"):
            neofs_env.kill()

    if request.session.testsfailed:
        logs_id = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d-%H-%M-%S")
        logs_path = os.path.join(get_assets_dir_path(), f"neofs_env_with_mainchain_logs_{logs_id}")
        os.makedirs(logs_path, exist_ok=True)

        shutil.copyfile(neofs_env.main_chain.stderr, f"{logs_path}/main_chain_log.txt")
        shutil.copyfile(neofs_env.s3_gw.stderr, f"{logs_path}/s3_gw_log.txt")
        shutil.copyfile(neofs_env.rest_gw.stderr, f"{logs_path}/rest_gw_log.txt")
        for idx, ir in enumerate(neofs_env.inner_ring_nodes):
            shutil.copyfile(ir.stderr, f"{logs_path}/ir_{idx}_log.txt")
        for idx, sn in enumerate(neofs_env.storage_nodes):
            shutil.copyfile(sn.stderr, f"{logs_path}/sn_{idx}_log.txt")

        logs_zip_file_path = shutil.make_archive(
            os.path.join(get_assets_dir_path(), f"neofs_env_with_mainchain_logs_{logs_id}"), "zip", logs_path
        )
        allure.attach.file(logs_zip_file_path, name="neofs env with main chain files", extension="zip")
    logger.info(f"Cleaning up dir {neofs_env}")
    shutil.rmtree(os.path.join(get_assets_dir_path(), neofs_env._env_dir), ignore_errors=True)
    NeoFSEnv.cleanup_unused_ports()


@pytest.fixture
def clear_neofs_env():
    neofs_env = NeoFSEnv(neofs_env_config=NeoFSEnv._generate_default_neofs_env_config())
    yield neofs_env
    neofs_env.kill()


@pytest.fixture(scope="module", autouse=True)
def cleanup_temp_files():
    yield
    for f in os.listdir(os.path.join(get_assets_dir_path(), TEST_FILES_DIR)):
        if f.startswith("temp_file"):
            os.remove(os.path.join(get_assets_dir_path(), TEST_FILES_DIR, f))
