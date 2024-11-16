from typing import Optional

from neofs_testlib.cli.neofs_authmate.secret import NeofsAuthmateSecret
from neofs_testlib.cli.neofs_authmate.version import NeofsAuthmateVersion
from neofs_testlib.cli.neofs_authmate.presigned import NeofsAuthmatePresigned
from neofs_testlib.shell import Shell


class NeofsAuthmate:
    secret: Optional[NeofsAuthmateSecret] = None
    version: Optional[NeofsAuthmateVersion] = None
    presigned: Optional[NeofsAuthmatePresigned] = None

    def __init__(self, shell: Shell, neofs_authmate_exec_path: str):
        self.secret = NeofsAuthmateSecret(shell, neofs_authmate_exec_path)
        self.version = NeofsAuthmateVersion(shell, neofs_authmate_exec_path)
        self.presigned = NeofsAuthmatePresigned(shell, neofs_authmate_exec_path)
