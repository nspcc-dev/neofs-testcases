"""
    A bunch of functions which might rearrange some data or
    change their representation.
"""

from functools import reduce


def dict_to_attrs(attrs: dict):
    """
    This function takes dictionary of object attributes and converts them
    into the string. The string is passed to `--attibutes` key of the
    neofs-cli.

    Args:
        attrs (dict): object attirbutes in {"a": "b", "c": "d"} format.

    Returns:
        (str): string in "a=b,c=d" format.
    """
    return reduce(lambda a, b: f"{a},{b}", map(lambda i: f"{i}={attrs[i]}", attrs))
