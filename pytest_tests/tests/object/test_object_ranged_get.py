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
    BINARY_WITH_RANGE,
    EXTENDED_RANGE_BAD_FIRST,
    EXTENDED_RANGE_BAD_LAST,
    EXTENDED_RANGE_INVALID_FORM,
    EXTENDED_RANGE_REVERSED_BOUNDS,
    EXTENDED_RANGE_ZERO_SUFFIX,
    INVALID_LENGTH_SPECIFIER,
    INVALID_OFFSET_SPECIFIER,
    INVALID_RANGE_OVERFLOW,
    INVALID_RANGE_ZERO_LENGTH,
    OUT_OF_RANGE,
    RANGE_WITH_EXTENDED_RANGE,
)
from helpers.neofs_verbs import (
    get_object_with_extended_range,
    get_object_with_range,
    get_range,
    put_object_to_random_node,
)
from helpers.utility import parse_version
from neofs_env.neofs_env_test_base import TestNeofsBase
from neofs_testlib.cli import NeofsCli
from neofs_testlib.env.env import NeoFSEnv, NodeWallet
from pytest import FixtureRequest

logger = logging.getLogger("NeoLogger")

SMALL_RANGE_LEN = 10


@pytest.fixture
def skip_if_extended_range_unsupported(neofs_env: NeoFSEnv) -> None:
    node_version = neofs_env.get_binary_version(neofs_env.neofs_node_path)
    if parse_version(node_version) <= parse_version("0.54.0"):
        pytest.skip(f"Extended GET ranges are not supported by neofs-node {node_version} (<= {'0.54.0'})")


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


def _resolve_extended_range(spec: str, size: int) -> tuple[int, int]:
    """Resolve an ``--extended-range`` spec into an ``(offset, length)`` pair.

    Mirrors the node-side resolution semantics so tests can assert byte-exact
    results independently of the server implementation:

    * ``first:last`` - inclusive bounds, ``last`` clamped to the last byte;
    * ``first:`` - from ``first`` to the payload end;
    * ``:length`` - the last ``length`` bytes, clamped to the whole payload.
    """
    first_str, _, last_str = spec.partition(":")
    if first_str and last_str:
        first = int(first_str)
        last = min(int(last_str), size - 1)
        return first, last - first + 1
    if first_str:
        first = int(first_str)
        return first, size - first
    length = min(int(last_str), size)
    return size - length, length


def _expected_extended_slice(file_path: str, spec: str, size: int) -> bytes:
    """Read the exact payload slice an ``--extended-range`` spec must return."""
    offset, length = _resolve_extended_range(spec, size)
    return get_file_content(file_path, content_len=length, mode="rb", offset=offset)


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

    @allure.title("Extended ranged GET (bounds form) returns inclusive byte ranges")
    @pytest.mark.simple
    def test_ranged_get_extended_bounds(
        self, skip_if_extended_range_unsupported, default_wallet: NodeWallet, container: str
    ):
        """End-to-end ``--extended-range first:last`` over regular and EC objects.

        Bounds are inclusive on both ends and ``last`` is clamped to the payload
        tail, so the retrieved bytes must match the corresponding slice of the
        original file for every position, including the boundary/clamped cases.
        """
        file_size = self.neofs_env.get_object_size("simple_object_size")
        file_path = generate_file(file_size)
        oid = _put_object(self.neofs_env, default_wallet, container, file_path)

        specs = [
            f"0:{file_size - 1}",  # whole payload, inclusive
            f"0:{min(SMALL_RANGE_LEN, file_size) - 1}",  # head slice
            f"{file_size - SMALL_RANGE_LEN}:{file_size - 1}",  # tail slice
            f"{file_size // 3}:{2 * file_size // 3}",  # middle slice
            f"{file_size // 2}:{file_size + 100}",  # last clamped past the payload end
        ]
        if file_size > SMALL_RANGE_LEN:
            specs.append(f"{SMALL_RANGE_LEN}:{SMALL_RANGE_LEN}")  # single byte
        logger.info(f"Extended bounds specs used in test: {specs}")

        for idx, spec in enumerate(specs):
            with allure.step(f"GET --extended-range {spec}"):
                _, content, stdout = get_object_with_extended_range(
                    wallet=default_wallet.path,
                    cid=container,
                    oid=oid,
                    extended_range=spec,
                    shell=self.neofs_env.shell,
                    endpoint=self.neofs_env.sn_rpc,
                )
                expected = _expected_extended_slice(file_path, spec, file_size)
                assert content == expected, f"Extended bounds range {spec} returned unexpected bytes"

                if idx == 0:
                    for marker in ("Owner:", "CreatedAt:", "Size:", "Attributes:"):
                        assert marker in stdout, (
                            f"Header marker {marker!r} missing from extended ranged GET stdout; stdout:\n{stdout}"
                        )

    @allure.title("Extended ranged GET (open-ended form) reads from an offset to the payload end")
    @pytest.mark.simple
    def test_ranged_get_extended_from(
        self, skip_if_extended_range_unsupported, default_wallet: NodeWallet, container: str
    ):
        file_size = self.neofs_env.get_object_size("simple_object_size")
        file_path = generate_file(file_size)
        oid = _put_object(self.neofs_env, default_wallet, container, file_path)

        specs = [
            "0:",  # the whole payload
            f"{SMALL_RANGE_LEN}:",
            f"{file_size // 2}:",
            f"{file_size - 1}:",  # only the last byte
        ]
        logger.info(f"Extended open-ended specs used in test: {specs}")

        for spec in specs:
            with allure.step(f"GET --extended-range {spec}"):
                _, content, _ = get_object_with_extended_range(
                    wallet=default_wallet.path,
                    cid=container,
                    oid=oid,
                    extended_range=spec,
                    shell=self.neofs_env.shell,
                    endpoint=self.neofs_env.sn_rpc,
                )
                expected = _expected_extended_slice(file_path, spec, file_size)
                assert content == expected, f"Extended open-ended range {spec} returned unexpected bytes"

    @allure.title("Extended ranged GET (suffix form) reads the last N bytes")
    @pytest.mark.simple
    def test_ranged_get_extended_suffix(
        self, skip_if_extended_range_unsupported, default_wallet: NodeWallet, container: str
    ):
        file_size = self.neofs_env.get_object_size("simple_object_size")
        file_path = generate_file(file_size)
        oid = _put_object(self.neofs_env, default_wallet, container, file_path)

        specs = [
            ":1",  # last byte only
            f":{SMALL_RANGE_LEN}",
            f":{file_size // 2}",
            f":{file_size}",  # exactly the whole payload
            f":{file_size + 100}",  # clamped to the whole payload
        ]
        logger.info(f"Extended suffix specs used in test: {specs}")

        for spec in specs:
            with allure.step(f"GET --extended-range {spec}"):
                _, content, _ = get_object_with_extended_range(
                    wallet=default_wallet.path,
                    cid=container,
                    oid=oid,
                    extended_range=spec,
                    shell=self.neofs_env.shell,
                    endpoint=self.neofs_env.sn_rpc,
                )
                expected = _expected_extended_slice(file_path, spec, file_size)
                assert content == expected, f"Extended suffix range {spec} returned unexpected bytes"

    @allure.title("Extended ranged GET matches equivalent legacy --range results")
    @pytest.mark.simple
    def test_ranged_get_extended_matches_legacy_range(
        self, skip_if_extended_range_unsupported, default_wallet: NodeWallet, container: str
    ):
        """Each extended form must return the same bytes as its ``--range`` twin.

        This cross-checks the new code path against the already-trusted legacy
        ``offset:length`` implementation for bounds, open-ended and suffix forms.
        """
        file_size = self.neofs_env.get_object_size("simple_object_size")
        file_path = generate_file(file_size)
        oid = _put_object(self.neofs_env, default_wallet, container, file_path)

        first = file_size // 4
        last = 3 * file_size // 4
        suffix_len = file_size // 2

        equivalents = [
            (f"{first}:{last}", f"{first}:{last - first + 1}"),  # bounds
            (f"{first}:", f"{first}:{file_size - first}"),  # open-ended
            (f":{suffix_len}", f"{file_size - suffix_len}:{suffix_len}"),  # suffix
        ]

        for ext_spec, legacy_spec in equivalents:
            with allure.step(f"Compare --extended-range {ext_spec} with --range {legacy_spec}"):
                _, ext_content, _ = get_object_with_extended_range(
                    wallet=default_wallet.path,
                    cid=container,
                    oid=oid,
                    extended_range=ext_spec,
                    shell=self.neofs_env.shell,
                    endpoint=self.neofs_env.sn_rpc,
                )
                _, legacy_content, _ = get_object_with_range(
                    wallet=default_wallet.path,
                    cid=container,
                    oid=oid,
                    range_cut=legacy_spec,
                    shell=self.neofs_env.shell,
                    endpoint=self.neofs_env.sn_rpc,
                )
                assert ext_content == legacy_content, f"--extended-range {ext_spec} diverged from --range {legacy_spec}"

    @allure.title("Extended ranged GET works for a complex (split) object")
    @pytest.mark.complex
    def test_ranged_get_extended_complex_object(
        self, skip_if_extended_range_unsupported, default_wallet: NodeWallet, default_container: str
    ):
        """All three extended forms must be assembled correctly across split children."""
        file_size = self.neofs_env.get_object_size("complex_object_size")
        file_path = generate_file(file_size)
        oid = _put_object(self.neofs_env, default_wallet, default_container, file_path)

        parts = get_object_chunks(default_wallet.path, default_container, oid, self.shell, self.neofs_env)
        assert len(parts) >= 2, "complex object is expected to have at least 2 chunks"
        first_child = parts[0][1]
        second_child = parts[1][1]

        specs = [
            f"0:{first_child - 1}",  # exactly the first child
            f"{first_child}:{first_child + second_child - 1}",  # exactly the second child
            f"0:{first_child + second_child - 1}",  # spans the first two children
            f"{first_child - 1}:{first_child + 1}",  # straddles a child boundary
            f"0:{file_size - 1}",  # the whole payload, inclusive
            f"{first_child}:",  # from a child boundary to the end
            f":{second_child + 1}",  # a suffix spanning the last children
        ]
        logger.info(f"Extended specs used in complex test: {specs}")

        for spec in specs:
            with allure.step(f"GET --extended-range {spec} (complex object)"):
                _, content, _ = get_object_with_extended_range(
                    wallet=default_wallet.path,
                    cid=default_container,
                    oid=oid,
                    extended_range=spec,
                    shell=self.neofs_env.shell,
                    endpoint=self.neofs_env.sn_rpc,
                )
                expected = _expected_extended_slice(file_path, spec, file_size)
                assert content == expected, f"Complex object extended range {spec} returned wrong bytes"

    @allure.title("Extended ranged GET honors --payload-only")
    @pytest.mark.simple
    def test_ranged_get_extended_payload_only(
        self, skip_if_extended_range_unsupported, default_wallet: NodeWallet, default_container: str
    ):
        file_size = self.neofs_env.get_object_size("simple_object_size")
        file_path = generate_file(file_size)
        oid = _put_object(self.neofs_env, default_wallet, default_container, file_path)

        spec = f"{file_size // 4}:{3 * file_size // 4}"
        with allure.step(f"GET --extended-range {spec} --payload-only"):
            _, content, stdout = get_object_with_extended_range(
                wallet=default_wallet.path,
                cid=default_container,
                oid=oid,
                extended_range=spec,
                shell=self.neofs_env.shell,
                endpoint=self.neofs_env.sn_rpc,
                payload_only=True,
            )
            expected = _expected_extended_slice(file_path, spec, file_size)
            assert content == expected, "Extended range payload differs from the expected slice in --payload-only mode"
            for marker in ("Owner:", "CreatedAt:", "Size:", "Attributes:"):
                assert marker not in stdout, (
                    f"Header marker {marker!r} leaked into stdout while --payload-only was set with "
                    f"--extended-range; stdout:\n{stdout}"
                )

    @allure.title("Extended ranged GET negative cases")
    @pytest.mark.simple
    def test_ranged_get_extended_negatives(
        self, skip_if_extended_range_unsupported, default_wallet: NodeWallet, default_container: str
    ):
        file_size = self.neofs_env.get_object_size("simple_object_size")
        file_path = generate_file(file_size)
        oid = _put_object(self.neofs_env, default_wallet, default_container, file_path)

        invalid_cases: list[tuple[str, str]] = [
            # reversed bounds (first > last), rejected client-side
            (f"{SMALL_RANGE_LEN}:{SMALL_RANGE_LEN - 1}", EXTENDED_RANGE_REVERSED_BOUNDS),
            # zero-length suffix, rejected client-side
            (":0", EXTENDED_RANGE_ZERO_SUFFIX),
            # bounds with first position at/after the payload end -> out of range
            (f"{file_size}:{file_size + SMALL_RANGE_LEN}", OUT_OF_RANGE),
            # open-ended with first position at the payload end -> out of range
            (f"{file_size}:", OUT_OF_RANGE),
            # malformed value without a colon
            (str(SMALL_RANGE_LEN), EXTENDED_RANGE_INVALID_FORM),
            # malformed value with too many colons
            ("1:2:3", EXTENDED_RANGE_INVALID_FORM),
            # non-numeric first position
            ("abc:5", EXTENDED_RANGE_BAD_FIRST),
            # non-numeric last position/suffix length
            ("5:abc", EXTENDED_RANGE_BAD_LAST),
        ]

        for spec, expected_error in invalid_cases:
            with allure.step(f"GET --extended-range {spec} (expected error)"):
                with pytest.raises(Exception, match=expected_error):
                    get_object_with_extended_range(
                        wallet=default_wallet.path,
                        cid=default_container,
                        oid=oid,
                        extended_range=spec,
                        shell=self.neofs_env.shell,
                        endpoint=self.neofs_env.sn_rpc,
                    )

    @allure.title("Extended ranged GET is mutually exclusive with --range and --binary")
    @pytest.mark.simple
    def test_ranged_get_extended_conflicts(
        self, skip_if_extended_range_unsupported, default_wallet: NodeWallet, default_container: str
    ):
        file_size = self.neofs_env.get_object_size("simple_object_size")
        file_path = generate_file(file_size)
        oid = _put_object(self.neofs_env, default_wallet, default_container, file_path)

        cli = NeofsCli(self.neofs_env.shell, NEOFS_CLI_EXEC, WALLET_CONFIG)

        with allure.step("--range together with --extended-range is rejected"):
            out_file = os.path.join(get_assets_dir_path(), TEST_OBJECTS_DIR, str(uuid.uuid4()))
            with pytest.raises(Exception, match=RANGE_WITH_EXTENDED_RANGE):
                cli.object.get(
                    rpc_endpoint=self.neofs_env.sn_rpc,
                    wallet=default_wallet.path,
                    cid=default_container,
                    oid=oid,
                    file=out_file,
                    range="0:1",
                    extended_range="0:1",
                    no_progress=True,
                )

        with allure.step("--binary together with --extended-range is rejected"):
            out_file = os.path.join(get_assets_dir_path(), TEST_OBJECTS_DIR, str(uuid.uuid4()))
            with pytest.raises(Exception, match=BINARY_WITH_RANGE):
                cli.object.get(
                    rpc_endpoint=self.neofs_env.sn_rpc,
                    wallet=default_wallet.path,
                    cid=default_container,
                    oid=oid,
                    file=out_file,
                    extended_range="0:1",
                    binary=True,
                    no_progress=True,
                )
