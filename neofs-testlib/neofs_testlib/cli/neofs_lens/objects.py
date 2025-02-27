from neofs_testlib.cli.cli_command import CliCommand
from neofs_testlib.shell import CommandResult


class NeofsLensObject(CliCommand):
    def link(
        self,
        file: str,
    ) -> CommandResult:
        """
        Inspect LINK objects from NeoFS.

        Args:
                file: File with object payload.
        Returns:
                Command's result.
        """
        return self._execute(
            "object link",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )
