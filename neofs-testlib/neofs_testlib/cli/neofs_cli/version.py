from neofs_testlib.cli.cli_command import CliCommand
from neofs_testlib.shell import CommandResult


class NeofsCliVersion(CliCommand):
    def get(self) -> CommandResult:
        """
        Application version and NeoFS API compatibility.

        Returns:
            Command's result.
        """
        return self._execute("", version=True)
