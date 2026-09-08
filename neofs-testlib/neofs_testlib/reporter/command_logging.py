import functools
import os
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager

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


_deferred = threading.local()


def _deferred_stack() -> list[dict]:
    stack = getattr(_deferred, "stack", None)
    if stack is None:
        stack = []
        _deferred.stack = stack
    return stack


@contextmanager
def deferred_command_reporting() -> Iterator[None]:
    stack = _deferred_stack()
    holder: dict = {"pending": []}
    stack.append(holder)
    try:
        yield
    finally:
        stack.pop()
        for report_fn in holder["pending"]:
            report_command(report_fn)


def reset_deferred_attempt() -> None:
    stack = _deferred_stack()
    if stack:
        stack[-1]["pending"] = []


def report_command(report_fn: Callable[[], None]) -> None:
    stack = _deferred_stack()
    if stack:
        stack[-1]["pending"].append(report_fn)
    else:
        report_fn()


def retry(*dargs, **dkwargs):
    import tenacity

    def wrap(func):
        @functools.wraps(func)
        def attempt(*args, **kwargs):
            reset_deferred_attempt()
            return func(*args, **kwargs)

        retried = tenacity.retry(*dargs, **dkwargs)(attempt)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with deferred_command_reporting():
                return retried(*args, **kwargs)

        return wrapper

    # Support bare ``@retry`` usage (without parentheses).
    if len(dargs) == 1 and callable(dargs[0]) and not dkwargs:
        func, dargs = dargs[0], ()
        return wrap(func)
    return wrap


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
