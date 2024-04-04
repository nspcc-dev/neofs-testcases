from typing import Optional

from neofs_testlib.cli.neofs_lens.objects import NeofsLensObject
from neofs_testlib.shell import Shell


class NeofsLens:
	object: Optional[NeofsLensObject] = None

	def __init__(self, shell: Shell, neofs_lens_exec_path: str):
		self.object = NeofsLensObject(shell, neofs_lens_exec_path)
