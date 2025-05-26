from typing import Optional

from neofs_testlib.cli.cli_command import CliCommand
from neofs_testlib.shell import CommandResult


class NeofsCliRequest(CliCommand):
    def create_container(
        self,
        body: str,
        endpoint: str,
        shell_timeout: Optional[int] = None,
    ) -> CommandResult:
        """Request container creation in NeoFS

        Args:
            body: JSON file with container creation request body.
            endpoint: Remote node address ('<host>:<port>').
            shell_timeout: Shell timeout for the command.

        Returns:
            Command's result.

        """
        return self._execute(
            f"request create-container {body} {endpoint}",
            **{param: value for param, value in locals().items() if param not in ["self", "body", "endpoint"]},
        )
