from typing import Optional

from neofs_testlib.cli.neofs_adm.config import NeofsAdmConfig
from neofs_testlib.cli.neofs_adm.fschain import NeofsAdmFSChain
from neofs_testlib.cli.neofs_adm.storage_config import NeofsAdmStorageConfig
from neofs_testlib.cli.neofs_adm.subnet import NeofsAdmFSChainSubnet
from neofs_testlib.cli.neofs_adm.version import NeofsAdmVersion
from neofs_testlib.shell import Shell


class NeofsAdm:
    fschain: Optional[NeofsAdmFSChain] = None
    subnet: Optional[NeofsAdmFSChainSubnet] = None
    storage_config: Optional[NeofsAdmStorageConfig] = None
    version: Optional[NeofsAdmVersion] = None

    def __init__(self, shell: Shell, neofs_adm_exec_path: str, config_file: Optional[str] = None):
        self.config = NeofsAdmConfig(shell, neofs_adm_exec_path, config=config_file)
        self.fschain = NeofsAdmFSChain(shell, neofs_adm_exec_path, config=config_file)
        self.subnet = NeofsAdmFSChainSubnet(shell, neofs_adm_exec_path, config=config_file)
        self.storage_config = NeofsAdmStorageConfig(shell, neofs_adm_exec_path, config=config_file)
        self.version = NeofsAdmVersion(shell, neofs_adm_exec_path, config=config_file)
