from typing import Optional

from common import NEOFS_CLI_EXEC

from .accounting import NeofsCliAccounting
from .acl import NeofsCliACL
from .container import NeofsCliContainer
from .object import NeofsCliObject
from .version import NeofsCliVersion


class NeofsCli:
    neofs_cli_exec_path: Optional[str] = None
    config: Optional[str] = None
    accounting: Optional[NeofsCliAccounting] = None
    acl: Optional[NeofsCliACL] = None
    container: Optional[NeofsCliContainer] = None
    object: Optional[NeofsCliObject] = None
    version: Optional[NeofsCliVersion] = None

    def __init__(
        self,
        neofs_cli_exec_path: Optional[str] = None,
        config: Optional[str] = None,
        timeout: int = 30,
    ):
        self.config = (
            config  # config(str):  config file (default is $HOME/.config/neofs-cli/config.yaml)
        )
        self.neofs_cli_exec_path = neofs_cli_exec_path or NEOFS_CLI_EXEC
        self.accounting = NeofsCliAccounting(
            self.neofs_cli_exec_path, timeout=timeout, config=config
        )
        self.acl = NeofsCliACL(self.neofs_cli_exec_path, timeout=timeout, config=config)
        self.container = NeofsCliContainer(self.neofs_cli_exec_path, timeout=timeout, config=config)
        self.object = NeofsCliObject(self.neofs_cli_exec_path, timeout=timeout, config=config)
        self.version = NeofsCliVersion(self.neofs_cli_exec_path, timeout=timeout, config=config)
