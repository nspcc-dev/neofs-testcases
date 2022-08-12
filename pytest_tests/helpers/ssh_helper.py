import logging
import socket
import tempfile
import textwrap
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from functools import wraps
from time import sleep
from typing import ClassVar, Optional

import allure
from paramiko import AutoAddPolicy, SFTPClient, SSHClient, SSHException, ssh_exception, RSAKey
from paramiko.ssh_exception import AuthenticationException


class HostIsNotAvailable(Exception):
    """Raises when host is not reachable."""

    def __init__(self, ip: str = None, exc: Exception = None):
        msg = f'Host is not available{f" by ip: {ip}" if ip else ""}'
        if exc:
            msg = f'{msg}. {exc}'
        super().__init__(msg)


def log_command(func):
    @wraps(func)
    def wrapper(host: 'HostClient', command: str, *args, **kwargs):
        display_length = 60
        short = command.removeprefix("$ProgressPreference='SilentlyContinue'\n")
        short = short[:display_length]
        short += '...' if short != command else ''
        with allure.step(f'SSH: {short}'):
            logging.info(f'Execute command "{command}" on "{host.ip}"')

            start_time = datetime.utcnow()
            cmd_result = func(host, command, *args, **kwargs)
            end_time = datetime.utcnow()

            log_message = f'HOST: {host.ip}\n' \
                          f'COMMAND:\n{textwrap.indent(command, " ")}\n' \
                          f'RC:\n {cmd_result.rc}\n' \
                          f'STDOUT:\n{textwrap.indent(cmd_result.stdout, " ")}\n' \
                          f'STDERR:\n{textwrap.indent(cmd_result.stderr, " ")}\n' \
                          f'Start / End / Elapsed\t {start_time.time()} / {end_time.time()} / {end_time - start_time}'

            logging.info(log_message)
            allure.attach(log_message, 'SSH command', allure.attachment_type.TEXT)
        return cmd_result
    return wrapper


@dataclass
class SSHCommand:
    stdout: str
    stderr: str
    rc: int


class HostClient:
    ssh_client: SSHClient
    SSH_CONNECTION_ATTEMPTS: ClassVar[int] = 3
    CONNECTION_TIMEOUT = 90

    TIMEOUT_RESTORE_CONNECTION = 10, 24

    def __init__(self, ip: str, login: str, password: Optional[str] = None,
                 private_key_path: Optional[str] = None, private_key_passphrase: Optional[str] = None,
                 init_ssh_client=True) -> None:
        self.ip = ip
        self.login = login
        self.password = password
        self.private_key_path = private_key_path
        self.private_key_passphrase = private_key_passphrase
        if init_ssh_client:
            self.create_connection(self.SSH_CONNECTION_ATTEMPTS)

    def exec(self, cmd: str, verify=True, timeout=90) -> SSHCommand:
        cmd_result = self._inner_exec(cmd, timeout)
        if verify:
            assert cmd_result.rc == 0, f'Non zero rc from command: "{cmd}"'
        return cmd_result

    @log_command
    def exec_with_confirmation(self, cmd: str, confirmation: list, verify=True, timeout=90) -> SSHCommand:
        ssh_stdin, ssh_stdout, ssh_stderr = self.ssh_client.exec_command(cmd, timeout=timeout)
        for line in confirmation:
            if not line.endswith('\n'):
                line = f'{line}\n'
            try:
                ssh_stdin.write(line)
            except OSError as err:
                logging.error(f'Got error {err} executing command {cmd}')
        ssh_stdin.close()
        output = SSHCommand(stdout=ssh_stdout.read().decode(errors='ignore'),
                            stderr=ssh_stderr.read().decode(errors='ignore'),
                            rc=ssh_stdout.channel.recv_exit_status())
        if verify:
            debug_info = f'\nSTDOUT: {output.stdout}\nSTDERR: {output.stderr}\nRC: {output.rc}'
            assert output.rc == 0, f'Non zero rc from command: "{cmd}"{debug_info}'
        return output

    @contextmanager
    def as_user(self, user: str, password: str):
        keep_user, keep_password = self.login, self.password
        self.login, self.password = user, password
        self.create_connection()
        yield
        self.login, self.password = keep_user, keep_password
        self.create_connection()

    @contextmanager
    def create_ssh_connection(self) -> 'SSHClient':
        if not self.ssh_client:
            self.create_connection()
        try:
            yield self.ssh_client
        finally:
            self.drop()

    @allure.step('Restore connection')
    def restore_ssh_connection(self):
        retry_time, retry_count = self.TIMEOUT_RESTORE_CONNECTION
        for _ in range(retry_count):
            try:
                self.create_connection()
            except AssertionError:
                logging.warning(f'Host: Cant reach host: {self.ip}.')
                sleep(retry_time)
            else:
                logging.info(f'Host: Cant reach host: {self.ip}.')
                return
        raise AssertionError(f'Host: Cant reach host: {self.ip} after 240 seconds..')

    @allure.step('Copy file {host_path_to_file} to local file {path_to_file}')
    def copy_file_from_host(self, host_path_to_file: str, path_to_file: str):
        with self._sftp_client() as sftp_client:
            sftp_client.get(host_path_to_file, path_to_file)

    def copy_file_to_host(self, path_to_file: str, host_path_to_file: str):
        with allure.step(f'Copy local file {path_to_file} to remote file {host_path_to_file} on host {self.ip}'):
            with self._sftp_client() as sftp_client:
                sftp_client.put(path_to_file, host_path_to_file)

    @allure.step('Save string to remote file {host_path_to_file}')
    def copy_str_to_host_file(self, string: str, host_path_to_file: str):
        with tempfile.NamedTemporaryFile(mode='r+') as temp:
            temp.writelines(string)
            temp.flush()
            with self._sftp_client() as client:
                client.put(temp.name, host_path_to_file)
        self.exec(f'cat {host_path_to_file}', verify=False)

    def create_connection(self, attempts=SSH_CONNECTION_ATTEMPTS):
        exc_err = None
        for attempt in range(attempts):
            self.ssh_client = SSHClient()
            self.ssh_client.set_missing_host_key_policy(AutoAddPolicy())
            try:
                if self.private_key_path:
                    logging.info(
                        f"Trying to connect to host {self.ip} as {self.login} using SSH key "
                        f"{self.private_key_path} (attempt {attempt})"
                    )
                    self.ssh_client.connect(
                        hostname=self.ip,
                        username=self.login,
                        pkey=RSAKey.from_private_key_file(self.private_key_path, self.private_key_passphrase),
                        timeout=self.CONNECTION_TIMEOUT
                    )
                else:
                    logging.info(
                        f"Trying to connect to host {self.ip} as {self.login} using password "
                        f"(attempt {attempt})"
                    )
                    self.ssh_client.connect(
                        hostname=self.ip,
                        username=self.login,
                        password=self.password,
                        timeout=self.CONNECTION_TIMEOUT
                    )
                return True

            except AuthenticationException as auth_err:
                logging.error(f'Host: {self.ip}. {auth_err}')
                self.drop()
                raise auth_err

            except (
                    SSHException,
                    ssh_exception.NoValidConnectionsError,
                    AttributeError,
                    socket.timeout,
                    OSError
            ) as ssh_err:
                exc_err = ssh_err
                self.drop()
                logging.error(f'Host: {self.ip}, connection error. {exc_err}')

        raise HostIsNotAvailable(self.ip, exc_err)

    def drop(self):
        self.ssh_client.close()

    @log_command
    def _inner_exec(self, cmd: str, timeout: int) -> SSHCommand:
        if not self.ssh_client:
            self.create_connection()
        for _ in range(self.SSH_CONNECTION_ATTEMPTS):
            try:
                _, stdout, stderr = self.ssh_client.exec_command(cmd, timeout=timeout)
                return SSHCommand(
                    stdout=stdout.read().decode(errors='ignore'),
                    stderr=stderr.read().decode(errors='ignore'),
                    rc=stdout.channel.recv_exit_status()
                )
            except (
                    SSHException,
                    TimeoutError,
                    ssh_exception.NoValidConnectionsError,
                    ConnectionResetError,
                    AttributeError,
                    socket.timeout,
            ) as ssh_err:
                logging.error(f'Host: {self.ip}, exec command error {ssh_err}')
                self.create_connection()
        raise HostIsNotAvailable(f'Host: {self.ip} is not reachable.')

    @contextmanager
    def _sftp_client(self) -> SFTPClient:
        with self.ssh_client.open_sftp() as sftp:
            yield sftp
