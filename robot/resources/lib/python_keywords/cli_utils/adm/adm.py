from typing import Optional

from common import NEOFS_ADM_EXEC

from .completion import NeofsAdmCompletion
from .config import NeofsAdmConfig
from .gendoc import NeofsAdmGenDoc
from .morph import NeofsAdmMorph
from .storage_config import NeofsAdmStorageConfig
from .subnet import NeofsAdmMorphSubnet
from .version import NeofsAdmVersion


class NeofsAdm:
    neofs_adm_exec_path: Optional[str] = None
    config_file: Optional[str] = None

    completion: Optional[NeofsAdmCompletion] = None
    config: Optional[NeofsAdmConfig] = None
    gendoc: Optional[NeofsAdmGenDoc] = None
    morph: Optional[NeofsAdmMorph] = None
    subnet: Optional[NeofsAdmMorphSubnet] = None
    storage_config: Optional[NeofsAdmStorageConfig] = None
    version: Optional[NeofsAdmVersion] = None

    def __init__(
        self,
        neofs_adm_exec_path: Optional[str] = None,
        config_file: Optional[str] = None,
        timeout: int = 30,
    ):
        self.config_file = config_file
        self.neofs_adm_exec_path = neofs_adm_exec_path or NEOFS_ADM_EXEC

        self.completion = NeofsAdmCompletion(
            self.neofs_adm_exec_path, timeout=timeout, config=config_file
        )
        self.config = NeofsAdmConfig(self.neofs_adm_exec_path, timeout=timeout, config=config_file)
        self.gendoc = NeofsAdmGenDoc(self.neofs_adm_exec_path, timeout=timeout, config=config_file)
        self.morph = NeofsAdmMorph(self.neofs_adm_exec_path, timeout=timeout, config=config_file)
        self.subnet = NeofsAdmMorphSubnet(
            self.neofs_adm_exec_path, timeout=timeout, config=config_file
        )
        self.storage_config = NeofsAdmStorageConfig(
            self.neofs_adm_exec_path, timeout=timeout, config=config_file
        )
        self.version = NeofsAdmVersion(
            self.neofs_adm_exec_path, timeout=timeout, config=config_file
        )
