import logging

import allure
import pytest
from common import COMPLEX_OBJ_SIZE, SIMPLE_OBJ_SIZE
from container import create_container
from epoch import get_epoch, tick_epoch
from file_helper import generate_file, get_file_hash
from grpc_responses import OBJECT_NOT_FOUND
from neofs_testlib.shell import Shell
from pytest import FixtureRequest
from python_keywords.neofs_verbs import get_object, put_object
from utility import wait_for_gc_pass_on_storage_nodes

logger = logging.getLogger("NeoLogger")


@allure.title("Test object life time")
@pytest.mark.sanity
@pytest.mark.grpc_api
@pytest.mark.parametrize(
    "object_size", [SIMPLE_OBJ_SIZE, COMPLEX_OBJ_SIZE], ids=["simple object", "complex object"]
)
def test_object_api_lifetime(
    prepare_wallet_and_deposit: str, client_shell: Shell, request: FixtureRequest, object_size: int
):
    """
    Test object deleted after expiration epoch.
    """
    wallet = prepare_wallet_and_deposit
    cid = create_container(wallet, shell=client_shell)

    allure.dynamic.title(f"Test object life time for {request.node.callspec.id}")

    file_path = generate_file(object_size)
    file_hash = get_file_hash(file_path)
    epoch = get_epoch(shell=client_shell)

    oid = put_object(wallet, file_path, cid, shell=client_shell, expire_at=epoch + 1)
    got_file = get_object(wallet, cid, oid, shell=client_shell)
    assert get_file_hash(got_file) == file_hash

    with allure.step("Tick two epochs"):
        for _ in range(2):
            tick_epoch(shell=client_shell)

    # Wait for GC, because object with expiration is counted as alive until GC removes it
    wait_for_gc_pass_on_storage_nodes()

    with allure.step("Check object deleted because it expires-on epoch"):
        with pytest.raises(Exception, match=OBJECT_NOT_FOUND):
            get_object(wallet, cid, oid, shell=client_shell)
