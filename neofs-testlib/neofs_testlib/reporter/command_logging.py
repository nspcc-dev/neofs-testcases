import os

FULL_LOGS_ENV = "NEOFS_ALLURE_FULL_LOGS"
MAX_ATTACH_CHARS_ENV = "NEOFS_ALLURE_MAX_ATTACH_CHARS"

DEFAULT_MAX_ATTACH_CHARS = 8192

_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})


def full_command_logs_enabled() -> bool:
    return os.environ.get(FULL_LOGS_ENV, "").strip().lower() in _TRUE_VALUES


def should_report_command(is_error: bool) -> bool:
    return is_error or full_command_logs_enabled()


def _max_attach_chars() -> int:
    raw = os.environ.get(MAX_ATTACH_CHARS_ENV)
    if raw is None:
        return DEFAULT_MAX_ATTACH_CHARS
    try:
        return int(raw)
    except ValueError:
        return DEFAULT_MAX_ATTACH_CHARS


def truncate_command_output(body: str) -> str:
    if body is None or full_command_logs_enabled():
        return body

    max_chars = _max_attach_chars()
    if max_chars <= 0 or len(body) <= max_chars:
        return body

    head = max_chars // 2
    tail = max_chars - head
    omitted = len(body) - max_chars
    return (
        f"{body[:head]}\n\n"
        f"... [truncated {omitted} characters, set {FULL_LOGS_ENV}=1 for full output] ...\n\n"
        f"{body[-tail:]}"
    )
