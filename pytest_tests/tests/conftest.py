import logging
import os
import shutil
from pathlib import Path

import allure
import pytest
from helpers.common import SIMPLE_OBJECT_SIZE, TEST_FILES_DIR, TEST_OBJECTS_DIR, get_assets_dir_path
from helpers.file_helper import generate_file
from helpers.wallet_helpers import create_wallet
from neofs_testlib.env.env import NeoFSEnv, NodeWallet
from neofs_testlib.reporter import AllureHandler, get_reporter
from neofs_testlib.shell import Shell

get_reporter().register_handler(AllureHandler())
logger = logging.getLogger("NeoLogger")


def pytest_addoption(parser):
    parser.addoption("--persist-env", action="store_true", default=False, help="persist deployed env")
    parser.addoption("--load-env", action="store", help="load persisted env from file")


def get_or_create_neofs_env(
    request,
    with_main_chain=False,
    storage_nodes_count=4,
    inner_ring_nodes_count=1,
    writecache=False,
    peapod_required=True,
    with_s3_gw=True,
    with_rest_gw=True,
):
    NeoFSEnv.cleanup_unused_ports()
    if request.config.getoption("--load-env"):
        neofs_env = NeoFSEnv.load(request.config.getoption("--load-env"))
    else:
        neofs_env = NeoFSEnv.deploy(
            with_main_chain=with_main_chain,
            storage_nodes_count=storage_nodes_count,
            inner_ring_nodes_count=inner_ring_nodes_count,
            writecache=writecache,
            peapod_required=peapod_required,
            with_s3_gw=with_s3_gw,
            with_rest_gw=with_rest_gw,
            request=request,
        )
    return neofs_env


@pytest.fixture(scope="session")
def neofs_env(temp_directory, artifacts_directory, request):
    neofs_env = get_or_create_neofs_env(request, with_s3_gw=False, with_rest_gw=False)
    yield neofs_env
    neofs_env.finalize(request)


@pytest.fixture(scope="session")
def neofs_env_s3_gw(temp_directory, artifacts_directory, request):
    neofs_env = get_or_create_neofs_env(request, with_s3_gw=True, with_rest_gw=False)
    yield neofs_env
    neofs_env.finalize(request)


@pytest.fixture(scope="session")
def neofs_env_rest_gw(temp_directory, artifacts_directory, request):
    neofs_env = get_or_create_neofs_env(request, with_s3_gw=False, with_rest_gw=True)
    yield neofs_env
    neofs_env.finalize(request)


@pytest.fixture(scope="function")
def neofs_env_function_scope(temp_directory, artifacts_directory, request):
    neofs_env = get_or_create_neofs_env(request, with_s3_gw=False, with_rest_gw=False)
    yield neofs_env
    neofs_env.finalize(request)


@pytest.fixture()
def neofs_env_with_writecache(temp_directory, artifacts_directory, request):
    neofs_env = get_or_create_neofs_env(
        request, writecache=True, peapod_required=False, with_s3_gw=False, with_rest_gw=False
    )
    yield neofs_env
    neofs_env.finalize(request)


@pytest.fixture()
def neofs_env_single_sn(temp_directory, artifacts_directory, request):
    neofs_env = get_or_create_neofs_env(request, storage_nodes_count=1, with_s3_gw=True, with_rest_gw=True)
    yield neofs_env
    neofs_env.finalize(request)


@pytest.fixture()
def neofs_env_4_ir(temp_directory, artifacts_directory, request):
    neofs_env = get_or_create_neofs_env(
        request,
        storage_nodes_count=4,
        inner_ring_nodes_count=4,
        with_s3_gw=False,
        with_rest_gw=False,
    )
    yield neofs_env
    neofs_env.finalize(request)


@pytest.fixture(scope="module")
def neofs_env_4_ir_4_sn(temp_directory, artifacts_directory, request):
    neofs_env = get_or_create_neofs_env(
        request,
        storage_nodes_count=4,
        inner_ring_nodes_count=4,
        with_s3_gw=False,
        with_rest_gw=False,
    )
    yield neofs_env
    neofs_env.finalize(request)


@pytest.fixture()
def neofs_env_7_ir(temp_directory, artifacts_directory, request):
    neofs_env = get_or_create_neofs_env(
        request,
        storage_nodes_count=4,
        inner_ring_nodes_count=7,
        with_s3_gw=False,
        with_rest_gw=False,
    )
    yield neofs_env
    neofs_env.finalize(request)


@pytest.fixture()
def neofs_env_4_ir_with_mainchain(temp_directory, artifacts_directory, request):
    neofs_env = get_or_create_neofs_env(
        request,
        with_main_chain=True,
        storage_nodes_count=1,
        inner_ring_nodes_count=4,
        with_s3_gw=False,
        with_rest_gw=False,
    )
    yield neofs_env
    neofs_env.finalize(request)


@pytest.fixture()
def neofs_env_7_ir_with_mainchain(temp_directory, artifacts_directory, request):
    neofs_env = get_or_create_neofs_env(
        request,
        with_main_chain=True,
        storage_nodes_count=1,
        inner_ring_nodes_count=7,
        with_s3_gw=False,
        with_rest_gw=False,
    )
    yield neofs_env
    neofs_env.finalize(request)


@pytest.fixture
def neofs_env_with_mainchain(temp_directory, artifacts_directory, request):
    neofs_env = get_or_create_neofs_env(request, with_main_chain=True, with_s3_gw=True, with_rest_gw=True)
    yield neofs_env
    neofs_env.finalize(request)


@pytest.fixture(scope="session")
@allure.title("Prepare default wallet and deposit")
def default_wallet(temp_directory):
    return create_wallet()


@pytest.fixture(scope="session")
def client_shell(neofs_env: NeoFSEnv) -> Shell:
    yield neofs_env.shell


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
def file_path(artifacts_directory):
    yield generate_file(int(SIMPLE_OBJECT_SIZE))


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


@pytest.fixture(scope="module", autouse=True)
def cleanup_temp_files():
    yield
    for f in os.listdir(os.path.join(get_assets_dir_path(), TEST_FILES_DIR)):
        if f.startswith("temp_file"):
            os.remove(os.path.join(get_assets_dir_path(), TEST_FILES_DIR, f))
