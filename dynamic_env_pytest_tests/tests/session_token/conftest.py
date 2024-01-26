import pytest
from neofs_testlib.env.env import NodeWallet

from helpers.wallet_helpers import create_wallet


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
