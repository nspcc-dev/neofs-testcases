import os
import uuid
from common import SIMPLE_OBJ_SIZE, ASSETS_DIR


def create_file_with_content(file_path: str = None, content: str = None) -> str:
    if not content:
        content = os.urandom(SIMPLE_OBJ_SIZE)

    if not file_path:
        file_path = f"{os.getcwd()}/{ASSETS_DIR}/{str(uuid.uuid4())}"
    with open(file_path, 'w+') as out_file:
        out_file.write(content)

    return file_path


def get_file_content(file_path: str) -> str:
    with open(file_path, 'r') as out_file:
        content = out_file.read()

    return content
