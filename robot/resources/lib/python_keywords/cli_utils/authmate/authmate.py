from typing import Optional

from common import NEOFS_AUTHMATE_EXEC

from .secret import NeofsAuthmateSecret
from .version import NeofsAuthmateVersion


class NeofsAuthmate:
    neofs_authmate_exec_path: Optional[str] = None

    secret: Optional[NeofsAuthmateSecret] = None
    version: Optional[NeofsAuthmateVersion] = None

    def __init__(
        self,
        neofs_authmate_exec_path: Optional[str] = None,
        timeout: int = 60,
    ):
        self.neofs_authmate_exec_path = neofs_authmate_exec_path or NEOFS_AUTHMATE_EXEC

        self.secret = NeofsAuthmateSecret(self.neofs_authmate_exec_path, timeout=timeout)
        self.version = NeofsAuthmateVersion(self.neofs_authmate_exec_path, timeout=timeout)
