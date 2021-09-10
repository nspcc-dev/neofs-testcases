#!/usr/bin/python3.8

"""
Helper functions to use with `neofs-cli`, `neo-go`
and other CLIs.
"""

import subprocess

from robot.api import logger

ROBOT_AUTO_KEYWORDS = False


def _cmd_run(cmd):
    """
    Runs given shell command <cmd>, in case of success returns its stdout,
    in case of failure returns error message.
    """
    try:
        compl_proc = subprocess.run(cmd, check=True, universal_newlines=True,
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=30,
                    shell=True)
        output = compl_proc.stdout
        logger.info(f"Output: {output}")
        return output
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"Error:\nreturn code: {exc.returncode} "
                f"\nOutput: {exc.output}") from exc
