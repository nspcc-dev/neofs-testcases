"""
Script to download NeoFS API protobuf files and generate Python bindings.
"""

import argparse
import logging
import os
import re
import shutil
import subprocess
import tempfile
import urllib.request
import zipfile
from pathlib import Path
from typing import List


def download_and_extract_archive(version: str, temp_dir: Path) -> Path:
    url = f"https://github.com/nspcc-dev/neofs-api/archive/refs/tags/{version}.zip"
    zip_path = temp_dir / f"neofs-api-{version}.zip"

    logging.info(f"Downloading {url}...")
    urllib.request.urlretrieve(url, zip_path)

    logging.info("Extracting archive...")
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(temp_dir)

    extracted_dir = temp_dir / f"neofs-api-{version.lstrip('v')}"
    if not extracted_dir.exists():
        extracted_dir = temp_dir / f"neofs-api-{version}"

    return extracted_dir


def find_proto_files(proto_dir: Path) -> List[Path]:
    proto_files = []
    for root, _, files in os.walk(proto_dir):
        for file in files:
            if file.endswith(".proto"):
                proto_files.append(Path(root) / file)
    return proto_files


def generate_python_files(proto_files: List[Path], proto_dir: Path, output_dir: Path):
    logging.info(f"Generating Python files for {len(proto_files)} proto files...")

    for proto_file in proto_files:
        relative_path = proto_file.relative_to(proto_dir)
        cmd = ["protoc", f"--proto_path={proto_dir}", f"--python_out={output_dir}", str(relative_path)]

        logging.debug(f"Generating {relative_path}...")
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            logging.error(f"Error generating {relative_path}: {result.stderr}")
            raise RuntimeError(f"protoc failed for {relative_path}")


def fix_import_paths(generated_dir: Path, base_import_path: str = "neofs_testlib.protobuf.generated"):
    logging.info("Fixing import paths...")

    for py_file in generated_dir.rglob("*_pb2.py"):
        with open(py_file, "r") as f:
            content = f.read()

        import_pattern = r"from\s+([a-zA-Z0-9_./]+)\s+import\s+([a-zA-Z0-9_]+_pb2)\s+as\s+([a-zA-Z0-9_.]+)"

        def replace_import(match):
            original_path = match.group(1)
            pb2_module = match.group(2)
            alias = match.group(3)

            normalized_path = original_path.replace("/", ".").replace("\\", ".")
            if normalized_path.endswith(".proto"):
                normalized_path = normalized_path[:-6]

            full_import_path = f"{base_import_path}.{normalized_path}"

            return f"from {full_import_path} import {pb2_module} as {alias}"

        new_content = re.sub(import_pattern, replace_import, content)

        if new_content != content:
            with open(py_file, "w") as f:
                f.write(new_content)
            logging.debug(f"Fixed imports in {py_file.relative_to(generated_dir)}")


def replace_existing_files(generated_dir: Path, target_dir: Path):
    logging.info(f"Replacing existing files in {target_dir}...")

    if target_dir.exists():
        shutil.rmtree(target_dir)

    shutil.copytree(generated_dir, target_dir)
    logging.info(f"Successfully replaced files in {target_dir}")


def main():
    parser = argparse.ArgumentParser(description="Generate Python protobuf bindings for NeoFS API")
    parser.add_argument("version", help="Version tag (e.g., v2.18.0)")
    parser.add_argument(
        "--output-dir",
        default="neofs-testlib/neofs_testlib/protobuf/generated",
        help="Output directory for generated files",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S",
    )

    output_dir = Path(args.output_dir).resolve()

    with tempfile.TemporaryDirectory() as temp_dir_str:
        temp_dir = Path(temp_dir_str)

        try:
            extracted_dir = download_and_extract_archive(args.version, temp_dir)

            proto_files = find_proto_files(extracted_dir)
            if not proto_files:
                raise RuntimeError(f"No .proto files found in {extracted_dir}")

            logging.info(f"Found {len(proto_files)} proto files")

            temp_generated_dir = temp_dir / "generated"
            temp_generated_dir.mkdir()

            generate_python_files(proto_files, extracted_dir, temp_generated_dir)

            fix_import_paths(temp_generated_dir)

            replace_existing_files(temp_generated_dir, output_dir)

            logging.info(f"Successfully generated protobuf files for {args.version}")

        except Exception as e:
            logging.error(f"Error: {e}")
            return 1

    return 0


if __name__ == "__main__":
    exit(main())
