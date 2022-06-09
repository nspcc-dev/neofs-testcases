#!/usr/bin/python3.8

"""
Helper functions to use with `neofs-cli`, `neo-go`
and other CLIs.
"""

import subprocess

import pexpect
from robot.api import logger

ROBOT_AUTO_KEYWORDS = False


def _cmd_run(cmd, timeout=30):
    """
    Runs given shell command <cmd>, in case of success returns its stdout,
    in case of failure returns error message.
    """
    try:
        logger.info(f"Executing command: {cmd}")
        compl_proc = subprocess.run(cmd, check=True, universal_newlines=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout,
                                    shell=True)
        output = compl_proc.stdout
        logger.info(f"Output: {output}")
        return output
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"Error:\nreturn code: {exc.returncode} "
                           f"\nOutput: {exc.output}") from exc
    except Exception as exc:
        return_code, _ = subprocess.getstatusoutput(cmd)
        logger.info(f"Error:\nreturn code: {return_code}\nOutput: "
                    f"{exc.output.decode('utf-8') if type(exc.output) is bytes else exc.output}")
        raise


def _run_with_passwd(cmd):
    child = pexpect.spawn(cmd)
    child.expect(".*")
    child.sendline('\r')
    child.wait()
    cmd = child.read()
    return cmd.decode()
