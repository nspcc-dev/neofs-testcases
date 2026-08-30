import allure
import pytest
from helpers.file_helper import generate_file, get_file_content
from helpers.s3_helper import object_key_from_file_path, parametrize_clients
from s3 import s3_object
from s3.s3_base import TestNeofsS3Base

SMALL_RANGE_LEN = 10
PARTIAL_CONTENT = 206
INVALID_RANGE_ERROR = r".*(InvalidRange|The requested range is not satisfiable).*"


def pytest_generate_tests(metafunc):
    parametrize_clients(metafunc)


def _resolve_http_range(spec: str, size: int) -> tuple[int, int]:
    assert spec.startswith("bytes="), f"HTTP Range spec must start with bytes=: {spec}"
    first_str, _, last_str = spec.removeprefix("bytes=").partition("-")
    if first_str and last_str:
        first = int(first_str)
        last = min(int(last_str), size - 1)
        return first, last
    if first_str:
        first = int(first_str)
        return first, size - 1
    length = min(int(last_str), size)
    return size - length, size - 1


def _expected_http_slice(file_path: str, spec: str, size: int) -> bytes:
    first, last = _resolve_http_range(spec, size)
    return get_file_content(file_path, content_len=last - first + 1, mode="rb", offset=first)


def _assert_partial_content(response: dict, first: int, last: int, size: int) -> None:
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status is not None:
        assert status == PARTIAL_CONTENT, f"Expected HTTP 206 Partial Content, got {status}; response:\n{response}"

    assert response.get("AcceptRanges") == "bytes", (
        f"Expected Accept-Ranges: bytes, got {response.get('AcceptRanges')!r}; response:\n{response}"
    )

    expected_content_range = f"bytes {first}-{last}/{size}"
    assert response.get("ContentRange") == expected_content_range, (
        f"Expected Content-Range {expected_content_range!r}, got {response.get('ContentRange')!r}; "
        f"response:\n{response}"
    )

    expected_content_length = last - first + 1
    assert int(response.get("ContentLength")) == expected_content_length, (
        f"Expected Content-Length {expected_content_length}, got {response.get('ContentLength')}; response:\n{response}"
    )


def _get_and_check_range(s3_client, bucket: str, object_key: str, file_path: str, spec: str, size: int) -> None:
    content, response = s3_object.get_object_range_s3(s3_client, bucket, object_key, spec)
    first, last = _resolve_http_range(spec, size)
    expected = _expected_http_slice(file_path, spec, size)
    assert content == expected, f"Range {spec} returned unexpected bytes"
    assert len(content) == last - first + 1, f"Range {spec} body length {len(content)} != {last - first + 1}"
    _assert_partial_content(response, first, last, size)


class TestS3Ranges(TestNeofsS3Base):
    @allure.title("S3 ranged GET (bounds form) returns inclusive byte ranges with Partial Content headers")
    @pytest.mark.simple
    def test_s3_range_bounds(self, bucket):
        file_size = self.neofs_env.get_object_size("simple_object_size")
        file_path = generate_file(file_size)
        file_name = object_key_from_file_path(file_path)
        s3_object.put_object_s3(self.s3_client, bucket, file_path)

        specs = [
            f"bytes=0-{file_size - 1}",
            f"bytes=0-{min(SMALL_RANGE_LEN, file_size) - 1}",
            f"bytes={file_size - SMALL_RANGE_LEN}-{file_size - 1}",
            f"bytes={file_size // 3}-{2 * file_size // 3}",
            f"bytes={file_size // 2}-{file_size + 100}",
            "bytes=0-0",
        ]
        if file_size > SMALL_RANGE_LEN:
            specs.append(f"bytes={SMALL_RANGE_LEN}-{SMALL_RANGE_LEN}")

        for spec in specs:
            with allure.step(f"GET Range {spec}"):
                _get_and_check_range(self.s3_client, bucket, file_name, file_path, spec, file_size)

    @allure.title("S3 ranged GET (open-ended form) reads from an offset to the payload end")
    @pytest.mark.simple
    def test_s3_range_open_ended(self, bucket):
        file_size = self.neofs_env.get_object_size("simple_object_size")
        file_path = generate_file(file_size)
        file_name = object_key_from_file_path(file_path)
        s3_object.put_object_s3(self.s3_client, bucket, file_path)

        specs = [
            "bytes=0-",
            f"bytes={SMALL_RANGE_LEN}-",
            f"bytes={file_size // 2}-",
            f"bytes={file_size - 1}-",
        ]

        for spec in specs:
            with allure.step(f"GET Range {spec}"):
                _get_and_check_range(self.s3_client, bucket, file_name, file_path, spec, file_size)

    @allure.title("S3 ranged GET (suffix form) reads the last N bytes")
    @pytest.mark.simple
    def test_s3_range_suffix(self, bucket):
        file_size = self.neofs_env.get_object_size("simple_object_size")
        file_path = generate_file(file_size)
        file_name = object_key_from_file_path(file_path)
        s3_object.put_object_s3(self.s3_client, bucket, file_path)

        specs = [
            "bytes=-1",
            f"bytes=-{SMALL_RANGE_LEN}",
            f"bytes=-{file_size // 2}",
            f"bytes=-{file_size}",
            f"bytes=-{file_size + 100}",
        ]

        for spec in specs:
            with allure.step(f"GET Range {spec}"):
                _get_and_check_range(self.s3_client, bucket, file_name, file_path, spec, file_size)

    @allure.title("S3 HTTP range forms return the same bytes as equivalent inclusive bounds")
    @pytest.mark.simple
    def test_s3_range_forms_equivalent(self, bucket):
        file_size = self.neofs_env.get_object_size("simple_object_size")
        file_path = generate_file(file_size)
        file_name = object_key_from_file_path(file_path)
        s3_object.put_object_s3(self.s3_client, bucket, file_path)

        first = file_size // 4
        last = 3 * file_size // 4
        suffix_len = file_size // 2

        equivalents = [
            (f"bytes={first}-{last}", f"bytes={first}-{last}"),
            (f"bytes={first}-", f"bytes={first}-{file_size - 1}"),
            (f"bytes=-{suffix_len}", f"bytes={file_size - suffix_len}-{file_size - 1}"),  # suffix
        ]

        for spec, bounds_spec in equivalents:
            with allure.step(f"Compare Range {spec} with {bounds_spec}"):
                spec_content, spec_response = s3_object.get_object_range_s3(self.s3_client, bucket, file_name, spec)
                bounds_content, bounds_response = s3_object.get_object_range_s3(
                    self.s3_client, bucket, file_name, bounds_spec
                )
                assert spec_content == bounds_content, f"Range {spec} diverged from {bounds_spec}"
                first_pos, last_pos = _resolve_http_range(bounds_spec, file_size)
                _assert_partial_content(spec_response, first_pos, last_pos, file_size)
                _assert_partial_content(bounds_response, first_pos, last_pos, file_size)

    @allure.title("S3 ranged GET works for a complex (split) object")
    @pytest.mark.complex
    def test_s3_range_complex_object(self, bucket):
        file_size = self.neofs_env.get_object_size("complex_object_size")
        file_path = generate_file(file_size)
        file_name = object_key_from_file_path(file_path)
        s3_object.put_object_s3(self.s3_client, bucket, file_path)

        first_child = self.neofs_env.max_object_size
        second_child = self.neofs_env.max_object_size
        assert file_size > first_child + 1, "complex object is expected to span at least two chunks"

        specs = [
            f"bytes=0-{first_child - 1}",
            f"bytes={first_child}-{first_child + second_child - 1}",
            f"bytes=0-{first_child + second_child - 1}",
            f"bytes={first_child - 1}-{first_child + 1}",
            f"bytes=0-{file_size - 1}",
            f"bytes={first_child}-",
            f"bytes=-{second_child + 1}",
        ]

        for spec in specs:
            with allure.step(f"GET Range {spec} (complex object)"):
                _get_and_check_range(self.s3_client, bucket, file_name, file_path, spec, file_size)

    @allure.title("S3 ranged GET negative cases for unsatisfiable and malformed ranges")
    @pytest.mark.simple
    def test_s3_range_negatives(self, bucket):
        file_size = self.neofs_env.get_object_size("simple_object_size")
        file_path = generate_file(file_size)
        file_name = object_key_from_file_path(file_path)
        s3_object.put_object_s3(self.s3_client, bucket, file_path)

        invalid_cases = [
            f"bytes={SMALL_RANGE_LEN}-{SMALL_RANGE_LEN - 1}",
            "bytes=-0",
            f"bytes={file_size}-{file_size + SMALL_RANGE_LEN}",
            f"bytes={file_size}-",
            f"bytes={file_size + 1}-{file_size + SMALL_RANGE_LEN}",
            "bytes=abc-5",
            "bytes=5-abc",
        ]

        for spec in invalid_cases:
            with allure.step(f"GET Range {spec} (expected InvalidRange)"):
                with pytest.raises(Exception, match=INVALID_RANGE_ERROR):
                    s3_object.get_object_range_s3(self.s3_client, bucket, file_name, spec)
