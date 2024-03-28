from neofs_testlib.cli.cli_command import CliCommand
from neofs_testlib.shell import CommandResult


class NeofsAdmConfig(CliCommand):
    def init(self, path: str = "~/.neofs/adm/config.yml") -> CommandResult:
        """Initialize basic neofs-adm configuration file.

        Args:
            path: Path to config (default ~/.neofs/adm/config.yml).

        Returns:
            Command's result.
        """
        return self._execute(
            "config init",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            },
        )
