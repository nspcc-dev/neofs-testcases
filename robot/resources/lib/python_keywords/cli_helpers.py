#!/usr/bin/python3.9

"""
Helper functions to use with `neofs-cli`, `neo-go` and other CLIs.
"""
import json
import subprocess
import sys
from contextlib import suppress
from datetime import datetime
from textwrap import shorten
from typing import Union

import allure
import pexpect
from robot.api import logger

ROBOT_AUTO_KEYWORDS = False


def _cmd_run(cmd: str, timeout: int = 30) -> str:
    """
    Runs given shell command <cmd>, in case of success returns its stdout,
    in case of failure returns error message.
    """
    try:
        logger.info(f"Executing command: {cmd}")
        start_time = datetime.utcnow()
        compl_proc = subprocess.run(cmd, check=True, universal_newlines=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                    timeout=timeout,
                                    shell=True)
        output = compl_proc.stdout
        return_code = compl_proc.returncode
        end_time = datetime.utcnow()
        logger.info(f"Output: {output}")
        _attach_allure_log(cmd, output, return_code, start_time, end_time)

        return output
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"Error:\nreturn code: {exc.returncode} "
                           f"\nOutput: {exc.output}") from exc
    except OSError as exc:
        raise RuntimeError(f"Output: {exc.strerror}") from exc
    except Exception as exc:
        return_code, _ = subprocess.getstatusoutput(cmd)
        logger.info(f"Error:\nreturn code: {return_code}\nOutput: "
                    f"{exc.output.decode('utf-8') if type(exc.output) is bytes else exc.output}")
        raise


def _run_with_passwd(cmd: str) -> str:
    child = pexpect.spawn(cmd)
    child.delaybeforesend = 1
    child.expect(".*")
    child.sendline('\r')
    if sys.platform == "darwin":
        child.expect(pexpect.EOF)
        cmd = child.before
    else:
        child.wait()
        cmd = child.read()
    return cmd.decode()


def _configure_aws_cli(cmd: str, key_id: str, access_key: str, out_format: str = "json") -> str:
    child = pexpect.spawn(cmd)
    child.delaybeforesend = 1

    child.expect("AWS Access Key ID.*")
    child.sendline(key_id)

    child.expect("AWS Secret Access Key.*")
    child.sendline(access_key)

    child.expect("Default region name.*")
    child.sendline('')

    child.expect("Default output format.*")
    child.sendline(out_format)

    child.wait()
    cmd = child.read()
    # child.expect(pexpect.EOF)
    # cmd = child.before
    return cmd.decode()


def _attach_allure_log(cmd: str, output: str, return_code: int, start_time: datetime,
                       end_time: datetime) -> None:
    if 'robot' not in sys.modules:
        command_attachment = (
            f"COMMAND: '{cmd}'\n"
            f'OUTPUT:\n {output}\n'
            f'RC: {return_code}\n'
            f'Start / End / Elapsed\t {start_time.time()} / {end_time.time()} / {end_time - start_time}'
        )
        with allure.step(f'COMMAND: {shorten(cmd, width=60, placeholder="...")}'):
            allure.attach(command_attachment, 'Command execution', allure.attachment_type.TEXT)


def log_command_execution(cmd: str, output: Union[str, dict]) -> None:
    logger.info(f'{cmd}: {output}')
    if 'robot' not in sys.modules:
        with suppress(Exception):
            json_output = json.dumps(output, indent=4, sort_keys=True)
            output = json_output
        command_attachment = (
            f"COMMAND: '{cmd}'\n"
            f'OUTPUT:\n {output}\n'
        )
        with allure.step(f'COMMAND: {shorten(cmd, width=60, placeholder="...")}'):
            allure.attach(command_attachment, 'Command execution', allure.attachment_type.TEXT)
