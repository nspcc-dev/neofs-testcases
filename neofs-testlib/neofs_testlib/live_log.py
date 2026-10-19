"""Log levels for local pytest runs.

``ENV`` sits between ``INFO`` and ``WARNING``. Environment summaries — endpoints,
config paths, log files, the persisted env directory — are logged at this level
and painted green on the live console so they stay visible among test output.

``--log-cli-level`` selects how much of a local run is printed:

* ``DEBUG`` — commands and their output, config generation, waits, plus everything below
* ``INFO`` — test progress and environment summaries (the default)
* ``ENV`` — environment summaries, warnings and errors
* ``WARNING`` — warnings and errors
"""

import logging
import sys
from typing import Optional

ENV = 25

_GREEN = "\033[32m"
_RESET = "\033[0m"
_COLOR_WRAPPED = "_neofs_env_color_wrapped"


def install_live_log() -> None:
    """Register the ``ENV`` level so ``--log-cli-level=ENV`` is accepted."""
    logging.addLevelName(ENV, "ENV")
    logging.ENV = ENV


install_live_log()


def log_command_result(logger: logging.Logger, message: str, return_code: Optional[int]) -> None:
    """Log a finished command. Failures stay visible; successful output is debug-only."""
    if return_code not in (None, 0):
        logger.warning(message)
    else:
        logger.debug(message)


def _cli_color_enabled(config) -> bool:
    if not hasattr(config, "option"):
        return False
    color = getattr(config.option, "color", "auto")
    if color == "yes":
        return True
    if color == "no":
        return False
    return sys.stdout.isatty()


def _wrap_env_message(formatter: logging.Formatter) -> None:
    """Paint the message body of ``ENV`` records green, leaving other handlers plain."""
    original_format = formatter.format

    def format(record: logging.LogRecord) -> str:
        if record.levelno != ENV or not isinstance(record.msg, str):
            return original_format(record)

        saved_msg = record.msg
        saved_message = record.__dict__.pop("message", None)
        record.msg = f"{_GREEN}{saved_msg}{_RESET}"
        try:
            return original_format(record)
        finally:
            record.msg = saved_msg
            if saved_message is None:
                record.__dict__.pop("message", None)
            else:
                record.message = saved_message

    formatter.format = format
    setattr(formatter, _COLOR_WRAPPED, True)


def colorize_live_logs(config) -> None:
    """Color environment summaries on pytest's live log handler."""
    install_live_log()
    if not _cli_color_enabled(config):
        return

    plugin = config.pluginmanager.get_plugin("logging-plugin")
    if plugin is None:
        return

    formatter = plugin.log_cli_handler.formatter
    if getattr(formatter, _COLOR_WRAPPED, False):
        return

    if hasattr(formatter, "add_color_level"):
        formatter.add_color_level(ENV, "green", "bold")
    _wrap_env_message(formatter)
