import os
import uuid

from common import ASSETS_DIR, SIMPLE_OBJ_SIZE


def create_file_with_content(file_path: str = None, content: str = None) -> str:
    mode = 'w+'
    if not content:
        content = os.urandom(SIMPLE_OBJ_SIZE)
        mode = 'wb'

    if not file_path:
        file_path = f"{os.getcwd()}/{ASSETS_DIR}/{str(uuid.uuid4())}"
    else:
        if not os.path.exists(os.path.dirname(file_path)):
            os.makedirs(os.path.dirname(file_path))

    with open(file_path, mode) as out_file:
        out_file.write(content)

    return file_path


def get_file_content(file_path: str) -> str:
    with open(file_path, 'r') as out_file:
        content = out_file.read()

    return content


def split_file(file_path: str, parts: int) -> list[str]:
    files = []
    with open(file_path, 'rb') as in_file:
        data = in_file.read()

    content_size = len(data)

    chunk_size = int((content_size + parts) / parts)
    part_id = 1
    for start_position in range(0, content_size + 1, chunk_size):
        part_file_name = f'{file_path}_part_{part_id}'
        files.append(part_file_name)
        with open(part_file_name, 'wb') as out_file:
            out_file.write(data[start_position:start_position + chunk_size])
        part_id += 1

    return files
