from typing import Optional

from neofs_testlib.cli.neofs_cli.accounting import NeofsCliAccounting
from neofs_testlib.cli.neofs_cli.acl import NeofsCliACL
from neofs_testlib.cli.neofs_cli.bearer import NeofsCliBearer
from neofs_testlib.cli.neofs_cli.container import NeofsCliContainer
from neofs_testlib.cli.neofs_cli.control import NeofsCliControl
from neofs_testlib.cli.neofs_cli.netmap import NeofsCliNetmap
from neofs_testlib.cli.neofs_cli.object import NeofsCliObject
from neofs_testlib.cli.neofs_cli.session import NeofsCliSession
from neofs_testlib.cli.neofs_cli.shards import NeofsCliShards
from neofs_testlib.cli.neofs_cli.storagegroup import NeofsCliStorageGroup
from neofs_testlib.cli.neofs_cli.util import NeofsCliUtil
from neofs_testlib.cli.neofs_cli.version import NeofsCliVersion
from neofs_testlib.shell import Shell


class NeofsCli:
    accounting: Optional[NeofsCliAccounting] = None
    acl: Optional[NeofsCliACL] = None
    bearer: Optional[NeofsCliBearer] = None
    container: Optional[NeofsCliContainer] = None
    netmap: Optional[NeofsCliNetmap] = None
    object: Optional[NeofsCliObject] = None
    session: Optional[NeofsCliSession] = None
    shards: Optional[NeofsCliShards] = None
    storagegroup: Optional[NeofsCliStorageGroup] = None
    util: Optional[NeofsCliUtil] = None
    version: Optional[NeofsCliVersion] = None
    control: Optional[NeofsCliControl] = None

    def __init__(self, shell: Shell, neofs_cli_exec_path: str, config_file: Optional[str] = None):
        self.accounting = NeofsCliAccounting(shell, neofs_cli_exec_path, config=config_file)
        self.acl = NeofsCliACL(shell, neofs_cli_exec_path, config=config_file)
        self.bearer = NeofsCliBearer(shell, neofs_cli_exec_path, config=config_file)
        self.container = NeofsCliContainer(shell, neofs_cli_exec_path, config=config_file)
        self.netmap = NeofsCliNetmap(shell, neofs_cli_exec_path, config=config_file)
        self.object = NeofsCliObject(shell, neofs_cli_exec_path, config=config_file)
        self.session = NeofsCliSession(shell, neofs_cli_exec_path, config=config_file)
        self.shards = NeofsCliShards(shell, neofs_cli_exec_path, config=config_file)
        self.storagegroup = NeofsCliStorageGroup(shell, neofs_cli_exec_path, config=config_file)
        self.util = NeofsCliUtil(shell, neofs_cli_exec_path, config=config_file)
        self.version = NeofsCliVersion(shell, neofs_cli_exec_path, config=config_file)
        self.control = NeofsCliControl(shell, neofs_cli_exec_path, config=config_file)
