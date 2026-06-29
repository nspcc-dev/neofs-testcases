import logging
import os
import sys
import uuid

import allure
import pytest
from helpers.common import NEOFS_CLI_EXEC, TEST_OBJECTS_DIR, WALLET_CONFIG, get_assets_dir_path
from helpers.complex_object_actions import get_object_chunks
from helpers.container import (
    DEFAULT_PLACEMENT_RULE,
    EC_3_1_PLACEMENT_RULE,
    create_container,
    delete_container,
    generate_ranges_for_ec_object,
)
from helpers.file_helper import (
    generate_file,
    generate_payload_ranges,
    get_file_content,
    get_file_hash,
)
from helpers.grpc_responses import (
    INVALID_LENGTH_SPECIFIER,
    INVALID_OFFSET_SPECIFIER,
    INVALID_RANGE_OVERFLOW,
    INVALID_RANGE_ZERO_LENGTH,
    OUT_OF_RANGE,
)
from helpers.neofs_verbs import (
    get_object_with_range,
    get_range,
    put_object_to_random_node,
)
from neofs_env.neofs_env_test_base import TestNeofsBase
from neofs_testlib.cli import NeofsCli
from neofs_testlib.env.env import NeoFSEnv, NodeWallet
from pytest import FixtureRequest

logger = logging.getLogger("NeoLogger")

SMALL_RANGE_LEN = 10


@pytest.fixture(
    params=[
        pytest.param(DEFAULT_PLACEMENT_RULE, id="regular policy"),
        pytest.param(EC_3_1_PLACEMENT_RULE, id="ec policy"),
    ],
)
def container(request: FixtureRequest, default_wallet: NodeWallet, neofs_env: NeoFSEnv) -> str:
    cid = create_container(default_wallet.path, shell=neofs_env.shell, endpoint=neofs_env.sn_rpc, rule=request.param)
    yield cid
    delete_container(default_wallet.path, cid, shell=neofs_env.shell, endpoint=neofs_env.sn_rpc)


@pytest.fixture
def default_container(default_wallet: NodeWallet, neofs_env: NeoFSEnv) -> str:
    cid = create_container(
        default_wallet.path,
        shell=neofs_env.shell,
        endpoint=neofs_env.sn_rpc,
        rule=DEFAULT_PLACEMENT_RULE,
    )
    yield cid
    delete_container(default_wallet.path, cid, shell=neofs_env.shell, endpoint=neofs_env.sn_rpc)


def _put_object(neofs_env: NeoFSEnv, wallet: NodeWallet, cid: str, file_path: str) -> str:
    return put_object_to_random_node(
        wallet=wallet.path,
        path=file_path,
        cid=cid,
        shell=neofs_env.shell,
        neofs_env=neofs_env,
    )


class TestObjectRangedGet(TestNeofsBase):
    @allure.title("Validate ranged GET for a simple object")
    @pytest.mark.simple
    def test_ranged_get_simple_object(self, request: FixtureRequest, default_wallet: NodeWallet, container: str):
        file_size = self.neofs_env.get_object_size("simple_object_size")
        file_path = generate_file(file_size)
        oid = _put_object(self.neofs_env, default_wallet, container, file_path)

        ranges_to_test = generate_payload_ranges(file_size) + [(0, file_size)]
        logger.info(f"Ranges used in test: {ranges_to_test}")

        for idx, (offset, length) in enumerate(ranges_to_test):
            range_cut = f"{offset}:{length}"
            with allure.step(f"GET payload range {range_cut}"):
                _, content, stdout = get_object_with_range(
                    wallet=default_wallet.path,
                    cid=container,
                    oid=oid,
                    range_cut=range_cut,
                    shell=self.neofs_env.shell,
                    endpoint=self.neofs_env.sn_rpc,
                )
                expected = get_file_content(file_path, content_len=length, mode="rb", offset=offset)
                assert content == expected, f"Expected range content to match {range_cut} slice of file payload"

                if idx == 0:
                    for marker in ("Owner:", "CreatedAt:", "Size:", "Attributes:"):
                        assert marker in stdout, (
                            f"Header marker {marker!r} missing from default ranged GET stdout; stdout:\n{stdout}"
                        )

    @allure.title("Ranged GET returns same content as legacy object range")
    @pytest.mark.simple
    def test_ranged_get_matches_object_range(self, request: FixtureRequest, default_wallet: NodeWallet, container: str):
        file_size = self.neofs_env.get_object_size("simple_object_size")
        file_path = generate_file(file_size)
        oid = _put_object(self.neofs_env, default_wallet, container, file_path)

        ranges_to_test = generate_ranges_for_ec_object(file_size)
        for offset, length in ranges_to_test:
            range_cut = f"{offset}:{length}"
            with allure.step(f"Compare GET --range {range_cut} with object range"):
                _, get_content, _ = get_object_with_range(
                    wallet=default_wallet.path,
                    cid=container,
                    oid=oid,
                    range_cut=range_cut,
                    shell=self.neofs_env.shell,
                    endpoint=self.neofs_env.sn_rpc,
                )
                _, range_content = get_range(
                    wallet=default_wallet.path,
                    cid=container,
                    oid=oid,
                    shell=self.neofs_env.shell,
                    endpoint=self.neofs_env.sn_rpc,
                    range_cut=range_cut,
                )
                assert get_content == range_content, f"GET --range and object range diverged on {range_cut}"

    @allure.title("Ranged GET with zero offset and zero length returns the full payload")
    @pytest.mark.simple
    def test_ranged_get_zero_range_returns_full_payload(self, default_wallet: NodeWallet, default_container: str):
        file_size = self.neofs_env.get_object_size("simple_object_size")
        file_path = generate_file(file_size)
        file_hash = get_file_hash(file_path)
        oid = _put_object(self.neofs_env, default_wallet, default_container, file_path)

        with allure.step("GET payload with --range 0:0"):
            saved_path, _, _ = get_object_with_range(
                wallet=default_wallet.path,
                cid=default_container,
                oid=oid,
                range_cut="0:0",
                shell=self.neofs_env.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
        assert get_file_hash(saved_path) == file_hash, (
            "Full payload retrieved via --range 0:0 differs from the source file"
        )

    @allure.title("Ranged GET works for a complex (split) object")
    @pytest.mark.complex
    def test_ranged_get_complex_object_spans_children(self, default_wallet: NodeWallet, default_container: str):
        """Ranges that span multiple split children must be assembled correctly.

        This mirrors ``test_object_get_range_complex`` from ``test_object_api.py``
        but exercises the new ``object get --range`` code path on the node.
        """
        file_size = self.neofs_env.get_object_size("complex_object_size")
        file_path = generate_file(file_size)
        oid = _put_object(self.neofs_env, default_wallet, default_container, file_path)

        parts = get_object_chunks(default_wallet.path, default_container, oid, self.shell, self.neofs_env)
        assert len(parts) >= 2, "complex object is expected to have at least 2 chunks"

        ranges_to_test = [
            # inside the first child
            (0, parts[0][1] - 1),
            # exactly the second child
            (parts[0][1], parts[1][1]),
            # spans the first two children
            (0, parts[0][1] + parts[1][1] - 1),
            # spans from the first to the last child
            (0, file_size - 1),
        ]
        ranges_to_test.extend(generate_payload_ranges(file_size))
        ranges_to_test.append((0, file_size))
        logger.info(f"Ranges used in test: {ranges_to_test}")

        for offset, length in ranges_to_test:
            range_cut = f"{offset}:{length}"
            with allure.step(f"GET payload range {range_cut} (complex object)"):
                _, content, _ = get_object_with_range(
                    wallet=default_wallet.path,
                    cid=default_container,
                    oid=oid,
                    range_cut=range_cut,
                    shell=self.neofs_env.shell,
                    endpoint=self.neofs_env.sn_rpc,
                )
                expected = get_file_content(file_path, content_len=length, mode="rb", offset=offset)
                assert content == expected, f"Complex object ranged GET returned wrong bytes for {range_cut}"

    @allure.title("GET with --payload-only omits the object header from stdout")
    @pytest.mark.simple
    def test_get_payload_only_omits_header(self, default_wallet: NodeWallet, default_container: str):
        file_size = self.neofs_env.get_object_size("simple_object_size")
        file_path = generate_file(file_size)
        file_hash = get_file_hash(file_path)
        oid = _put_object(self.neofs_env, default_wallet, default_container, file_path)

        header_markers = ("Owner:", "CreatedAt:", "Size:", "Attributes:")

        with allure.step("Ranged GET with --payload-only does not print the header"):
            offset = 0
            length = min(SMALL_RANGE_LEN, file_size)
            range_cut = f"{offset}:{length}"
            _, content, stdout = get_object_with_range(
                wallet=default_wallet.path,
                cid=default_container,
                oid=oid,
                range_cut=range_cut,
                shell=self.neofs_env.shell,
                endpoint=self.neofs_env.sn_rpc,
                payload_only=True,
            )
            expected = get_file_content(file_path, content_len=length, mode="rb", offset=offset)
            assert content == expected, "Ranged payload differs from the expected slice in --payload-only mode"
            for marker in header_markers:
                assert marker not in stdout, (
                    f"Header marker {marker!r} unexpectedly leaked into stdout while "
                    f"--payload-only was set with --range; stdout:\n{stdout}"
                )

        with allure.step("Full GET with --payload-only does not print the header"):
            cli = NeofsCli(self.neofs_env.shell, NEOFS_CLI_EXEC, WALLET_CONFIG)
            out_file = os.path.join(get_assets_dir_path(), TEST_OBJECTS_DIR, str(uuid.uuid4()))
            result = cli.object.get(
                rpc_endpoint=self.neofs_env.sn_rpc,
                wallet=default_wallet.path,
                cid=default_container,
                oid=oid,
                file=out_file,
                no_progress=True,
                payload_only=True,
            )
            assert get_file_hash(out_file) == file_hash, (
                "Full payload retrieved with --payload-only differs from the source file"
            )
            for marker in header_markers:
                assert marker not in result.stdout, (
                    f"Header marker {marker!r} unexpectedly leaked into stdout while "
                    f"--payload-only was set without --range; stdout:\n{result.stdout}"
                )

    @allure.title("Ranged GET negative cases for invalid ranges")
    @pytest.mark.simple
    def test_ranged_get_negatives(self, default_wallet: NodeWallet, default_container: str):
        file_size = self.neofs_env.get_object_size("simple_object_size")
        file_path = generate_file(file_size)
        oid = _put_object(self.neofs_env, default_wallet, default_container, file_path)

        invalid_cases: list[tuple[int, int, str]] = [
            # offset > object size
            (file_size + 1, SMALL_RANGE_LEN, OUT_OF_RANGE),
            # offset + length > object size
            (file_size - SMALL_RANGE_LEN, SMALL_RANGE_LEN * 2, OUT_OF_RANGE),
            # uint64 overflow on offset+length
            (SMALL_RANGE_LEN, sys.maxsize * 2 + 1, INVALID_RANGE_OVERFLOW),
            # zero length with non-zero offset
            (10, 0, INVALID_RANGE_ZERO_LENGTH),
            # negative offset
            (-1, 1, INVALID_OFFSET_SPECIFIER),
            # negative length
            (10, -5, INVALID_LENGTH_SPECIFIER),
        ]

        for offset, length, expected_error in invalid_cases:
            range_cut = f"{offset}:{length}"
            pattern = expected_error.format(range=range_cut) if "{range}" in expected_error else expected_error
            with allure.step(f"GET payload range {range_cut} (expected error)"):
                with pytest.raises(Exception, match=pattern):
                    get_object_with_range(
                        wallet=default_wallet.path,
                        cid=default_container,
                        oid=oid,
                        range_cut=range_cut,
                        shell=self.neofs_env.shell,
                        endpoint=self.neofs_env.sn_rpc,
                    )

    @allure.title("Ranged GET rejects --binary together with --range")
    @pytest.mark.simple
    def test_ranged_get_binary_rejected(self, default_wallet: NodeWallet, default_container: str):
        file_size = self.neofs_env.get_object_size("simple_object_size")
        file_path = generate_file(file_size)
        oid = _put_object(self.neofs_env, default_wallet, default_container, file_path)

        cli = NeofsCli(self.neofs_env.shell, NEOFS_CLI_EXEC, WALLET_CONFIG)
        out_file = os.path.join(get_assets_dir_path(), TEST_OBJECTS_DIR, str(uuid.uuid4()))
        with pytest.raises(Exception, match=r"--binary cannot be used with --range"):
            cli.object.get(
                rpc_endpoint=self.neofs_env.sn_rpc,
                wallet=default_wallet.path,
                cid=default_container,
                oid=oid,
                file=out_file,
                range="0:1",
                binary=True,
                no_progress=True,
            )

    @allure.title("Ranged GET rejects multiple ranges")
    @pytest.mark.simple
    def test_ranged_get_multiple_ranges_rejected(self, default_wallet: NodeWallet, default_container: str):
        file_size = self.neofs_env.get_object_size("simple_object_size")
        file_path = generate_file(file_size)
        oid = _put_object(self.neofs_env, default_wallet, default_container, file_path)

        with pytest.raises(Exception, match=r"at most one range can be specified"):
            get_object_with_range(
                wallet=default_wallet.path,
                cid=default_container,
                oid=oid,
                range_cut="0:1,5:1",
                shell=self.neofs_env.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
