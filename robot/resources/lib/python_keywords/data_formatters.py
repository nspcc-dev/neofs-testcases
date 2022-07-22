import json
from neo3 import wallet


def dict_to_attrs(attrs: dict) -> str:
    """
    This function takes a dictionary of object's attributes and converts them
    into string. The string is passed to `--attributes` key of neofs-cli.

    Args:
        attrs (dict): object attributes in {"a": "b", "c": "d"} format.

    Returns:
        (str): string in "a=b,c=d" format.
    """
    return ",".join(f"{key}={value}" for key, value in attrs.items())


def pub_key_hex(wallet_path: str, wallet_password=""):
    wallet_content = ''
    with open(wallet_path) as out:
        wallet_content = json.load(out)
    wallet_from_json = wallet.Wallet.from_json(wallet_content, password=wallet_password)
    pub_key_64 = str(wallet_from_json.accounts[0].public_key)
    
    return pub_key_64
