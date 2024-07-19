import logging
import os
import shutil
import time

import allure
import pytest
from helpers.common import (
    ASSETS_DIR,
    COMPLEX_OBJECT_CHUNKS_COUNT,
    COMPLEX_OBJECT_TAIL_SIZE,
    SIMPLE_OBJECT_SIZE,
    TEST_FILES_DIR,
    TEST_OBJECTS_DIR,
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

    if request.session.testsfailed:
        env_files_path = os.path.join(os.getcwd(), neofs_env._env_dir)
        env_files_archived = shutil.make_archive(f"neofs_env_{neofs_env._id}", "zip", env_files_path)
        allure.attach.file(env_files_archived, name="neofs env files", extension="zip")

        temp_files_path = os.path.join(os.getcwd(), ASSETS_DIR)
        temp_files_archived = shutil.make_archive("temp_files", "zip", temp_files_path)
        allure.attach.file(temp_files_archived, name="tests temp files", extension="zip")


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
        full_path = os.path.join(os.getcwd(), ASSETS_DIR)
        create_dir(full_path)

    yield full_path

    if not request.config.getoption("--persist-env"):
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

    if not request.config.getoption("--persist-env"):
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
        os.mkdir(dir_path)


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
def neofs_env_with_mainchain():
    neofs_env = NeoFSEnv.simple(with_main_chain=True)
    yield neofs_env
    neofs_env.kill()
