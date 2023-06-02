import sys
import filecmp
from pathlib import Path
import pytest
import shutil
import tempfile
import difflib

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.openssl_config_fix import modify_openssl_config

path_to_original = Path(__file__).parent.parent / 'data/original_openssl.cnf'
path_to_modified = Path(__file__).parent.parent / 'data/modified_openssl.cnf'


@pytest.fixture
def temp_file():
    with tempfile.NamedTemporaryFile(delete=False) as temp:
        yield temp.name
    Path(temp.name).unlink()


def test_modify_openssl_config(temp_file):
    """Test the function modify_openssl_config."""
    # Create temporary test file based on the original configuration file
    shutil.copy(path_to_original, temp_file)
    # Test modify_openssl_config
    modify_openssl_config(Path(temp_file))
    if not filecmp.cmp(temp_file, path_to_modified):
        with open(temp_file, 'r') as tempfile, open(path_to_modified, 'r') as modified_file:
            diff = difflib.unified_diff(
                tempfile.readlines(),
                modified_file.readlines(),
                fromfile='temp_file',
                tofile='path_to_modified',
            )
        print(''.join(diff))
        assert False
