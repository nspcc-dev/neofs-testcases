from typing import Optional

from cli_helpers import _cmd_run


class NeofsCliCommandBase:
    neofs_cli_exec: Optional[str] = None
    timeout: Optional[int] = None
    __base_params: Optional[str] = None
    map_params = {'json_mode': 'json', 'await_mode': 'await', 'hash_type': 'hash'}

    def __init__(self, neofs_cli_exec: str, timeout: int = 30, **base_params):
        self.neofs_cli_exec = neofs_cli_exec
        self.timeout = timeout
        self.__base_params = ' '.join([f'--{param} {value}' for param, value in base_params.items() if value])

    def _format_command(self, command: str, **params) -> str:
        param_str = []
        for param, value in params.items():
            if param in self.map_params.keys():
                param = self.map_params[param]
            param = param.replace('_', '-')
            if not value:
                continue
            if isinstance(value, bool):
                param_str.append(f'--{param}')
            elif isinstance(value, list):
                param_str.append(f'--{param} \'{",".join(value)}\'')
            elif isinstance(value, dict):
                param_str.append(f'--{param} \'{",".join(f"{key}={val}" for key, val in value.items())}\'')
            else:
                value_str = str(value).replace("'", "\\'")
                param_str.append(f"--{param} '{value_str}'")
        param_str = ' '.join(param_str)

        return f'{self.neofs_cli_exec} {self.__base_params} {command or ""} {param_str}'

    def _execute(self, command: Optional[str], **params) -> str:
        return _cmd_run(self._format_command(command, **params), timeout=self.timeout)
