from typing import Optional

from cli_helpers import _cmd_run


class NeofsCliCommand:
    neofs_cli_exec: Optional[str] = None
    timeout: Optional[int] = None
    __base_params: Optional[str] = None
    map_params = {
        "json_mode": "json",
        "await_mode": "await",
        "hash_type": "hash",
        "doc_type": "type",
    }

    def __init__(self, neofs_cli_exec: str, timeout: int, **base_params):
        self.neofs_cli_exec = neofs_cli_exec
        self.timeout = timeout
        self.__base_params = " ".join(
            [f"--{param} {value}" for param, value in base_params.items() if value]
        )

    def _format_command(self, command: str, **params) -> str:
        param_str = []
        for param, value in params.items():
            if param in self.map_params.keys():
                param = self.map_params[param]
            param = param.replace("_", "-")
            if not value:
                continue
            if isinstance(value, bool):
                param_str.append(f"--{param}")
            elif isinstance(value, int):
                param_str.append(f"--{param} {value}")
            elif isinstance(value, list):
                for value_item in value:
                    val_str = str(value_item).replace("'", "\\'")
                    param_str.append(f"--{param} '{val_str}'")
            elif isinstance(value, dict):
                param_str.append(
                    f"--{param} '{','.join(f'{key}={val}' for key, val in value.items())}'"
                )
            else:
                if "'" in str(value):
                    value_str = str(value).replace('"', '\\"')
                    param_str.append(f'--{param} "{value_str}"')
                else:
                    param_str.append(f"--{param} '{value}'")

        param_str = " ".join(param_str)

        return f'{self.neofs_cli_exec} {self.__base_params} {command or ""} {param_str}'

    def _execute(self, command: Optional[str], **params) -> str:
        return _cmd_run(self._format_command(command, **params), timeout=self.timeout)
