import hashlib
import logging
import os
import random
import uuid
from typing import Any, Optional

import allure
from helpers.common import TEST_FILES_DIR, get_assets_dir_path

logger = logging.getLogger("NeoLogger")

RANGES_COUNT = 4
RANGE_MIN_LEN = 10
RANGE_MAX_LEN = 500

# Size of a block payloads are compared by when their hashes differ.
DIFF_BLOCK_SIZE = 512


def generate_payload_ranges(file_size: int) -> list[tuple[int, int]]:
    """Generate randomized `(offset, length)` payload ranges for tests.

    Args:
        file_size: Size of the source payload in bytes.

    Returns:
        List of `(offset, length)` tuples; every range satisfies
        `offset >= 0`, `length > 0` and `offset + length <= file_size`.
    """
    assert file_size > 0, "file_size must be positive"

    ranges_to_test: list[tuple[int, int]] = []

    edge_len = min(RANGE_MIN_LEN, file_size)
    ranges_to_test.append((0, edge_len))
    ranges_to_test.append((file_size - edge_len, edge_len))

    if file_size >= RANGES_COUNT:
        file_range_step = file_size // RANGES_COUNT
        for i in range(RANGES_COUNT):
            quarter_start = file_range_step * i
            quarter_end = file_range_step * (i + 1) if i < RANGES_COUNT - 1 else file_size
            quarter_len = quarter_end - quarter_start
            if quarter_len <= 0:
                continue
            range_length = random.randint(RANGE_MIN_LEN, RANGE_MAX_LEN)
            range_start = random.randint(quarter_start, quarter_start + quarter_len - 1)
            ranges_to_test.append((range_start, min(range_length, file_size - range_start)))

    return ranges_to_test


def generate_file(size: int) -> str:
    """Generates a binary file with the specified size in bytes.

    Args:
        size: Size in bytes, can be declared as 6e+6 for example.

    Returns:
        The path to the generated file.
    """
    file_path = os.path.join(get_assets_dir_path(), TEST_FILES_DIR, f"temp_file_{uuid.uuid4()}")
    with open(file_path, "wb") as file:
        file.write(os.urandom(size))
    logger.info(f"File with size {size} bytes has been generated: {file_path}")

    return file_path


def generate_file_with_content(
    size: int,
    file_path: Optional[str] = None,
    content: Optional[str] = None,
) -> str:
    """Creates a new file with specified content.

    Args:
        file_path: Path to the file that should be created. If not specified, then random file
            path will be generated.
        content: Content that should be stored in the file. If not specified, then random binary
            content will be generated.

    Returns:
        Path to the generated file.
    """
    mode = "w+"
    if content is None:
        content = os.urandom(size)
        mode = "wb"

    if not file_path:
        file_path = os.path.join(get_assets_dir_path(), TEST_FILES_DIR, f"temp_file_{uuid.uuid4()}")
    else:
        if not os.path.exists(os.path.dirname(file_path)):
            os.makedirs(os.path.dirname(file_path))

    with open(file_path, mode) as file:
        file.write(content)

    return file_path


@allure.step("Get File Hash")
def get_file_hash(file_path: str, len: Optional[int] = None, offset: Optional[int] = None) -> str:
    """Generates hash for the specified file.

    Args:
        file_path: Path to the file to generate hash for.
        len: How many bytes to read.
        offset: Position to start reading from.

    Returns:
        Hash of the file as hex-encoded string.
    """
    file_hash = hashlib.sha256()
    with open(file_path, "rb") as out:
        if len and not offset:
            file_hash.update(out.read(len))
        elif len and offset:
            out.seek(offset, 0)
            file_hash.update(out.read(len))
        elif offset and not len:
            out.seek(offset, 0)
            file_hash.update(out.read())
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
        resulting_file_path = os.path.join(get_assets_dir_path(), TEST_FILES_DIR, str(uuid.uuid4()))
    with open(resulting_file_path, "wb") as f:
        for file in file_paths:
            with open(file, "rb") as part_file:
                f.write(part_file.read())
    return resulting_file_path


def split_file(file_path: str, parts: int) -> list[str]:
    """Splits specified file into several specified number of parts.

    Each part is saved under name `{original_file}_part_{i}`.

    Args:
        file_path: Path to the file that should be split.
        parts: Number of parts the file should be split into.

    Returns:
        Paths to the part files.
    """
    with open(file_path, "rb") as file:
        content = file.read()

    content_size = len(content)
    chunk_size = int((content_size + parts) / parts)

    part_id = 1
    part_file_paths = []
    for content_offset in range(0, content_size + 1, chunk_size):
        part_file_name = f"{file_path}_part_{part_id}"
        part_file_paths.append(part_file_name)
        with open(part_file_name, "wb") as out_file:
            out_file.write(content[content_offset : content_offset + chunk_size])
        part_id += 1

    return part_file_paths


def get_file_content(
    file_path: str, content_len: Optional[int] = None, mode: str = "r", offset: Optional[int] = None
) -> Any:
    """Returns content of specified file.

    Args:
        file_path: Path to the file.
        content_len: Limit of content length. If None, then entire file content is returned;
            otherwise only the first content_len bytes of the content are returned.
        mode: Mode of opening the file.
        offset: Position to start reading from.

    Returns:
        Content of the specified file.
    """
    with open(file_path, mode) as file:
        if content_len and not offset:
            content = file.read(content_len)
        elif content_len and offset:
            file.seek(offset, 0)
            content = file.read(content_len)
        elif offset and not content_len:
            file.seek(offset, 0)
            content = file.read()
        else:
            content = file.read()

    return content


def _merge_block_indexes(indexes: list[int]) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    for index in indexes:
        if ranges and ranges[-1][1] == index - 1:
            ranges[-1] = (ranges[-1][0], index)
        else:
            ranges.append((index, index))

    return ranges


def get_diff_ranges(expected_path: str, actual_path: str, block_size: int = DIFF_BLOCK_SIZE) -> str:
    expected_size, actual_size = os.path.getsize(expected_path), os.path.getsize(actual_path)

    diff_indexes = []
    blocks_count = 0
    with open(expected_path, "rb") as expected_file, open(actual_path, "rb") as actual_file:
        while True:
            expected_block = expected_file.read(block_size)
            actual_block = actual_file.read(block_size)
            if not expected_block and not actual_block:
                break
            if expected_block != actual_block:
                diff_indexes.append(blocks_count)
            blocks_count += 1

    lines = [
        f"block size: {block_size}",
        f"expected size: {expected_size}",
        f"actual size: {actual_size}",
        f"differing blocks: {len(diff_indexes)} of {blocks_count}",
        "",
        "differing ranges:",
    ]

    if not diff_indexes:
        lines.append("  none")

    last_offset = max(expected_size, actual_size) - 1
    for first, last in _merge_block_indexes(diff_indexes):
        start, end = first * block_size, min((last + 1) * block_size - 1, last_offset)
        lines.append(f"  bytes {start}-{end}, blocks {first}-{last}")

    return "\n".join(lines)
