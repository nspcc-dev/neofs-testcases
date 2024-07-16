from neofs_testlib.cli.cli_command import CliCommand
from neofs_testlib.shell import CommandResult


class NeoGoUtil(CliCommand):
    def convert(self, post_data: str) -> CommandResult:
        """Application version.

        Returns:
            Command's result.
        """
        return self._execute(
            "util convert",
            **{param: param_value for param, param_value in locals().items() if param not in ["self"]},
        )
