import logging
import os
import shutil
from pathlib import Path

import allure
import neofs_env.neofs_epoch as neofs_epoch
import pytest
from helpers.common import (
    SIMPLE_OBJECT_SIZE,
    SN_VALIDATOR_DEFAULT_PORT,
    STORAGE_GC_TIME,
    TEST_FILES_DIR,
    TEST_OBJECTS_DIR,
    get_assets_dir_path,
)
from helpers.file_helper import generate_file
from helpers.node_management import restart_storage_nodes
from helpers.wallet_helpers import create_wallet
from neofs_testlib.env.env import NeoFSEnv, NodeWallet
from neofs_testlib.reporter import AllureHandler, get_reporter
from neofs_testlib.shell import Shell

get_reporter().register_handler(AllureHandler())
logger = logging.getLogger("NeoLogger")

logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("s3transfer").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


def pytest_addoption(parser):
    parser.addoption("--persist-env", action="store_true", default=False, help="persist deployed env")
    parser.addoption("--load-env", action="store", help="load persisted env from file")


def get_or_create_neofs_env(
    request,
    with_main_chain=False,
    storage_nodes_count=4,
    inner_ring_nodes_count=1,
    writecache=False,
    with_s3_gw=True,
    with_rest_gw=True,
    chain_meta_data=True,
    sn_validator_url=None,
    allow_ec=False,
    shards_count=2,
    gc_remover_batch_size=200,
    gc_sleep_interval=STORAGE_GC_TIME,
    replication_cooldown="10s",
    object_batch_size=None,
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
            with_s3_gw=with_s3_gw,
            with_rest_gw=with_rest_gw,
            request=request,
            chain_meta_data=chain_meta_data,
            sn_validator_url=sn_validator_url,
            allow_ec=allow_ec,
            shards_count=shards_count,
            gc_remover_batch_size=gc_remover_batch_size,
            gc_sleep_interval=gc_sleep_interval,
            replication_cooldown=replication_cooldown,
            object_batch_size=object_batch_size,
        )
    return neofs_env


@pytest.fixture(scope="session")
def neofs_env(temp_directory, artifacts_directory, request):
    if hasattr(request, "param"):
        params = request.param
    else:
        params = {}
    neofs_env = get_or_create_neofs_env(
        request,
        with_s3_gw=True,
        with_rest_gw=True,
        chain_meta_data=params.get("chain_meta_data", True),
        allow_ec=params.get("allow_ec", False),
        replication_cooldown=params.get("replication_cooldown", "10s"),
    )
    yield neofs_env
    neofs_env.finalize(request)


@pytest.fixture(scope="function")
def neofs_env_function_scope(temp_directory, artifacts_directory, request):
    neofs_env = get_or_create_neofs_env(request, with_s3_gw=False, with_rest_gw=False)
    yield neofs_env
    neofs_env.finalize(request)


@pytest.fixture()
def neofs_env_with_writecache(temp_directory, artifacts_directory, request):
    neofs_env = get_or_create_neofs_env(request, writecache=True, with_s3_gw=False, with_rest_gw=False)
    yield neofs_env
    neofs_env.finalize(request)


@pytest.fixture()
def neofs_env_slow_policer(temp_directory, artifacts_directory, request):
    neofs_env = get_or_create_neofs_env(
        request,
        with_s3_gw=False,
        with_rest_gw=False,
        allow_ec=True,
        replication_cooldown="1h",
        object_batch_size=1,
    )
    yield neofs_env
    neofs_env.finalize(request)


@pytest.fixture()
def neofs_env_single_sn(temp_directory, artifacts_directory, request):
    neofs_env = get_or_create_neofs_env(request, storage_nodes_count=1, with_s3_gw=True, with_rest_gw=True)
    yield neofs_env
    neofs_env.finalize(request)


@pytest.fixture()
def neofs_env_single_sn_custom_gc(temp_directory, artifacts_directory, request):
    neofs_env = get_or_create_neofs_env(
        request,
        storage_nodes_count=1,
        with_s3_gw=False,
        with_rest_gw=False,
        shards_count=1,
        gc_remover_batch_size=1,
        gc_sleep_interval=STORAGE_GC_TIME,
    )
    yield neofs_env
    neofs_env.finalize(request)


@pytest.fixture()
def neofs_env_ir_only_with_sn_validator(temp_directory, artifacts_directory, request):
    neofs_env = get_or_create_neofs_env(
        request,
        storage_nodes_count=0,
        with_s3_gw=False,
        with_rest_gw=False,
        sn_validator_url=f"http://localhost:{SN_VALIDATOR_DEFAULT_PORT}/verify",
    )
    yield neofs_env
    neofs_env.finalize(request)


@pytest.fixture()
def neofs_env_ir_only(temp_directory, artifacts_directory, request):
    neofs_env = get_or_create_neofs_env(request, storage_nodes_count=0, with_s3_gw=False, with_rest_gw=False)
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


@pytest.fixture(scope="module")
def neofs_env_with_mainchain(temp_directory, artifacts_directory, request):
    neofs_env = get_or_create_neofs_env(request, with_main_chain=True, with_s3_gw=True, with_rest_gw=True)
    GAS = 10**12
    MAX_OBJECT_SIZE = 10**7
    EPOCH_DURATION = 20
    CONTAINER_FEE = GAS
    STORAGE_FEE = GAS
    with allure.step("Set more convenient network config values"):
        neofs_env.neofs_adm().fschain.set_config(
            rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
            alphabet_wallets=neofs_env.alphabet_wallets_dir,
            post_data=f"MaxObjectSize={MAX_OBJECT_SIZE} ContainerFee={CONTAINER_FEE} BasicIncomeRate={STORAGE_FEE} EpochDuration={EPOCH_DURATION}",
        )

        # Temporary workaround for a problem with propagading MaxObjectSize between storage nodes
        restart_storage_nodes(neofs_env.storage_nodes)

        neofs_epoch.tick_epoch_and_wait(neofs_env=neofs_env)
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


@pytest.fixture(scope="function")
def unique_wallet(temp_directory) -> NodeWallet:
    """
    Returns a unique wallet per a single test for general purposes
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
