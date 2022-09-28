from typing import Optional

from cli_utils.cli.netmap import NeofsCliNetmap
from common import NEOFS_CLI_EXEC

from .accounting import NeofsCliAccounting
from .acl import NeofsCliACL
from .container import NeofsCliContainer
from .object import NeofsCliObject
from .version import NeofsCliVersion


class NeofsCli:
    accounting: NeofsCliAccounting
    acl: NeofsCliACL
    container: NeofsCliContainer
    netmap: NeofsCliNetmap
    object: NeofsCliObject
    version: NeofsCliVersion

    def __init__(
        self,
        neofs_cli_exec_path: Optional[str] = None,
        config: Optional[str] = None,
        timeout: int = 30,
    ):
        neofs_cli_exec_path = neofs_cli_exec_path or NEOFS_CLI_EXEC
        self.accounting = NeofsCliAccounting(neofs_cli_exec_path, timeout=timeout, config=config)
        self.acl = NeofsCliACL(neofs_cli_exec_path, timeout=timeout, config=config)
        self.container = NeofsCliContainer(neofs_cli_exec_path, timeout=timeout, config=config)
        self.netmap = NeofsCliNetmap(neofs_cli_exec_path, timeout=timeout, config=config)
        self.object = NeofsCliObject(neofs_cli_exec_path, timeout=timeout, config=config)
        self.version = NeofsCliVersion(neofs_cli_exec_path, timeout=timeout, config=config)
