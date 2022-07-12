
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
