#!/usr/bin/python3.8

"""
Helper functions to use with `neofs-cli`, `neo-go`
and other CLIs.
"""

import sys
import subprocess
from datetime import datetime
from textwrap import shorten

import allure
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
        start_time = datetime.now()
        compl_proc = subprocess.run(cmd, check=True, universal_newlines=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout,
                                    shell=True)
        output = compl_proc.stdout
        return_code = compl_proc.returncode
        end_time = datetime.now()
        logger.info(f"Output: {output}")
        _attach_allure_log(cmd, output, return_code, start_time, end_time)

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


def _attach_allure_log(cmd: str, output: str, return_code: int, start_time: datetime, end_time: datetime):
    if 'allure' in sys.modules:
        command_attachment = (
            f"COMMAND: '{cmd}'\n"
            f'OUTPUT:\n {output}\n'
            f'RC: {return_code}\n'
            f'Start / End / Elapsed\t {start_time.time()} / {end_time.time()} / {end_time - start_time}'
        )
        with allure.step(f'COMMAND: {shorten(cmd, width=60, placeholder="...")}'):
            allure.attach(command_attachment, 'Command execution', allure.attachment_type.TEXT)
