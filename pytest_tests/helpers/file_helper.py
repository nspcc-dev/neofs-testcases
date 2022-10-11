import hashlib
import logging
import os
import uuid
from typing import Optional

import allure
from common import ASSETS_DIR, SIMPLE_OBJ_SIZE

logger = logging.getLogger("NeoLogger")


def generate_file(size: int = SIMPLE_OBJ_SIZE) -> str:
    """Generates a binary file with the specified size in bytes.

    Args:
        size: Size in bytes, can be declared as 6e+6 for example.

    Returns:
        The path to the generated file.
    """
    file_path = f"{os.getcwd()}/{ASSETS_DIR}/{str(uuid.uuid4())}"
    with open(file_path, "wb") as file:
        file.write(os.urandom(size))
    logger.info(f"File with size {size} bytes has been generated: {file_path}")

    return file_path


@allure.step("Get File Hash")
def get_file_hash(file_path: str, len: Optional[int] = None) -> str:
    """Generates hash for the specified file.

    Args:
        file_path: Path to the file to generate hash for.
        len: How many bytes to read.

    Returns:
        Hash of the file as hex-encoded string.
    """
    file_hash = hashlib.sha256()
    with open(file_path, "rb") as out:
        if len:
            file_hash.update(out.read(len))
        else:
            file_hash.update(out.read())
    return file_hash.hexdigest()


@allure.step("Concatenation set of files to one file")
def concat_files(file_paths: list, resulting_file_path: Optional[str] = None) -> str:
    """Concatenates several files into a single file.

    Args:
        file_paths: Paths to the files to concatenate.
        resulting_file_name: Path to the file where concatenated content should be stored.

    Returns:
        Path to the resulting file.
    """
    if not resulting_file_path:
        resulting_file_path = f"{os.getcwd()}/{ASSETS_DIR}/{str(uuid.uuid4())}"
    with open(resulting_file_path, "wb") as f:
        for file in file_paths:
            with open(file, "rb") as part_file:
                f.write(part_file.read())
    return resulting_file_path
