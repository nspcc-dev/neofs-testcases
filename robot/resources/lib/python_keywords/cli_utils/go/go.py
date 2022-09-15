from typing import Optional

from common import NEOGO_EXECUTABLE

from .candidate import NeoGoCandidate
from .contract import NeoGoContract
from .db import NeoGoDb
from .nep17 import NeoGoNep17
from .node import NeoGoNode
from .query import NeoGoQuery
from .version import NeoGoVersion
from .wallet import NeoGoWallet


class NeoGo:
    neo_go_exec_path: Optional[str] = None
    config_path: Optional[str] = None
    candidate: Optional[NeoGoCandidate] = None
    contract: Optional[NeoGoContract] = None
    db: Optional[NeoGoDb] = None
    nep17: Optional[NeoGoNep17] = None
    node: Optional[NeoGoNode] = None
    query: Optional[NeoGoQuery] = None
    version: Optional[NeoGoVersion] = None
    wallet: Optional[NeoGoWallet] = None

    def __init__(
        self,
        neo_go_exec_path: Optional[str] = None,
        config_path: Optional[str] = None,
        timeout: int = 30,
    ):
        self.config_path = config_path
        self.neo_go_exec_path = neo_go_exec_path or NEOGO_EXECUTABLE
        self.candidate = NeoGoCandidate(
            self.neo_go_exec_path, timeout=timeout, config_path=config_path
        )
        self.contract = NeoGoContract(
            self.neo_go_exec_path, timeout=timeout, config_path=config_path
        )
        self.db = NeoGoDb(self.neo_go_exec_path, timeout=timeout, config_path=config_path)
        self.nep17 = NeoGoNep17(self.neo_go_exec_path, timeout=timeout, config_path=config_path)
        self.node = NeoGoNode(self.neo_go_exec_path, timeout=timeout, config_path=config_path)
        self.query = NeoGoQuery(self.neo_go_exec_path, timeout=timeout, config_path=config_path)
        self.version = NeoGoVersion(self.neo_go_exec_path, timeout=timeout, config_path=config_path)
        self.wallet = NeoGoWallet(self.neo_go_exec_path, timeout=timeout, config_path=config_path)
