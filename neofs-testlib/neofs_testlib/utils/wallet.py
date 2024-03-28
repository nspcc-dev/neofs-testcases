import json
import logging

from neo3.wallet import account as neo3_account
from neo3.wallet import wallet as neo3_wallet

logger = logging.getLogger("neofs.testlib.utils")


def init_wallet(wallet_path: str, wallet_password: str) -> str:
    """
    Create new wallet and new account.
    Args:
        wallet_path:  The path to the wallet to save wallet.
        wallet_password: The password for new wallet.
    """
    wallet = neo3_wallet.Wallet()
    account = neo3_account.Account.create_new(wallet_password)
    wallet.account_add(account)
    with open(wallet_path, "w") as out:
        json.dump(wallet.to_json(), out)
    logger.info(f"Init new wallet: {wallet_path}, address: {account.address}")
    return account.address


def get_last_address_from_wallet(
    wallet_path: str, wallet_password: str | None = None, wallet_passwords: list[str] | None = None
):
    """
    Extracting the last address from the given wallet.
    Args:
        wallet_path:  The path to the wallet to extract address from.
        wallet_password: The password for the given wallet.
        wallet_passwords: The password list for the given accounts in the wallet
    Returns:
        The address for the wallet.
    """
    if wallet_password is None and wallet_passwords is None:
        raise ValueError("Either wallet_password or wallet_passwords should be specified")

    with open(wallet_path) as wallet_file:
        wallet_json = json.load(wallet_file)
        if wallet_password is not None:
            wallet_passwords = [wallet_password] * len(wallet_json["accounts"])
        wallet = neo3_wallet.Wallet.from_json(wallet_json, passwords=wallet_passwords)
    address = wallet.accounts[-1].address
    logger.info(f"got address: {address}")
    return address


def get_last_public_key_from_wallet(
    wallet_path: str, wallet_password: str | None = None, wallet_passwords: list[str] | None = None
):
    """
    Extracting the last address from the given wallet.
    Args:
        wallet_path:  The path to the wallet to extract address from.
        wallet_password: The password for the given wallet.
        wallet_passwords: The password list for the given accounts in the wallet
    Returns:
        The address for the wallet.
    """
    if wallet_password is None and wallet_passwords is None:
        raise ValueError("Either wallet_password or wallet_passwords should be specified")

    with open(wallet_path) as wallet_file:
        wallet_json = json.load(wallet_file)
        if wallet_password is not None:
            wallet_passwords = [wallet_password] * len(wallet_json["accounts"])
        wallet = neo3_wallet.Wallet.from_json(wallet_json, passwords=wallet_passwords)
    public_key = wallet.accounts[-1].public_key
    logger.info(f"got public_key: {public_key}")
    return public_key
