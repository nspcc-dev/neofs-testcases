from typing import Optional

from neofs_testlib.cli.cli_command import CliCommand
from neofs_testlib.shell import CommandResult


class NeofsCliBearer(CliCommand):
    def create(
        self,
        issued_at: int,
        not_valid_before: int,
        owner: str,
        out: str,
        rpc_endpoint: str,
        json: Optional[bool] = False,
        eacl: Optional[str] = None,
        lifetime: Optional[int] = None,
        expire_at: Optional[int] = None,
    ) -> CommandResult:
        """
        Create bearer token

        Args:
            issued_at: Epoch to issue token at.
            not_valid_before: Not valid before epoch.
            owner: Token owner.
            out: File to write token to.
            rpc_endpoint: Remote node address (as 'multiaddr' or '<host>:<port>').
            json: Output token in JSON.
            eacl: Path to the extended ACL table.
            lifetime: Lock lifetime - relative to the current epoch.
            expire_at: Last epoch in the life of the object - absolute value.

        Returns:
            Command's result.
        """
        return self._execute(
            "bearer create",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )
