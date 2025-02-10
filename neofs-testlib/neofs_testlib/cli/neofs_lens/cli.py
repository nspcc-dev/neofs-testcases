from typing import Optional

from neofs_testlib.cli.neofs_lens.objects import NeofsLensObject
from neofs_testlib.cli.neofs_lens.storage import NeofsLensStorage
from neofs_testlib.cli.neofs_lens.write_cache import NeofsLensWriteCache
from neofs_testlib.shell import Shell


class NeofsLens:
    object: Optional[NeofsLensObject] = None
    write_cache: Optional[NeofsLensWriteCache] = None
    storage: Optional[NeofsLensStorage] = None

    def __init__(self, shell: Shell, neofs_lens_exec_path: str):
        self.object = NeofsLensObject(shell, neofs_lens_exec_path)
        self.write_cache = NeofsLensWriteCache(shell, neofs_lens_exec_path)
        self.storage = NeofsLensStorage(shell, neofs_lens_exec_path)
