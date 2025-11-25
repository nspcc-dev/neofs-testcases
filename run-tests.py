import hashlib
import os
import subprocess
import sys

import pytest

total_shards = int(sys.argv[1])
shard_index = int(sys.argv[2])
tests_path = sys.argv[3] if len(sys.argv) > 3 else "pytest_tests/tests"
marks = sys.argv[4] if len(sys.argv) > 4 else ""

result = subprocess.run(["pytest", "--collect-only", "-q", tests_path, "-m", marks], capture_output=True, text=True)
test_items = [
    f"{tests_path.split('/')[0].strip()}/{line.strip()}" for line in result.stdout.splitlines() if "::" in line
]

selected = [
    test for test in test_items if int(hashlib.md5(test.encode()).hexdigest(), 16) % total_shards == shard_index
]

exit_code = pytest.main(
    [
        "--timeout=1500",
        "-q",
        "--show-capture=no",
        f"--alluredir={os.environ.get('GITHUB_WORKSPACE', '.')}/allure-results",
        "--allure-no-capture",
        *selected,
    ]
)

sys.exit(exit_code)
