from typing import Optional

from common import NEOFS_CLI_EXEC

from .accounting import NeofsCliAccounting
from .cli_command import NeofsCliCommandBase
from .container import NeofsCliContainer
from .object import NeofsCliObject


class NeofsCli:
    neofs_cli_exec_path: Optional[str] = None
    config: Optional[str] = None
    accounting: Optional[NeofsCliAccounting] = None
    container: Optional[NeofsCliContainer] = None
    object: Optional[NeofsCliObject] = None

    def __init__(self, neofs_cli_exec_path: Optional[str] = None, config: Optional[str] = None, timeout: int = 30):
        self.config = config    # config(str):  config file (default is $HOME/.config/neofs-cli/config.yaml)
        self.neofs_cli_exec_path = neofs_cli_exec_path or NEOFS_CLI_EXEC
        self.accounting = NeofsCliAccounting(self.neofs_cli_exec_path, timeout=timeout, config=config)
        self.container = NeofsCliContainer(self.neofs_cli_exec_path, timeout=timeout, config=config)
        self.object = NeofsCliObject(self.neofs_cli_exec_path, timeout=timeout, config=config)

    def version(self) -> str:
        """Application version and NeoFS API compatibility

        Returns:
            str: Command string

        """
        return NeofsCliCommandBase(self.neofs_cli_exec_path, config=self.config)._execute(command=None, version=True)
