from neofs_testlib.cli.cli_command import CliCommand
from neofs_testlib.shell import CommandResult


class NeofsAuthmateVersion(CliCommand):
    def get(self) -> CommandResult:
        """Application version

        Returns:
            Command's result.
        """
        return self._execute("", version=True)
