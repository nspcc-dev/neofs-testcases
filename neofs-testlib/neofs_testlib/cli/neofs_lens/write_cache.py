from neofs_testlib.cli.cli_command import CliCommand
from neofs_testlib.shell import CommandResult


class NeofsLensWriteCache(CliCommand):
    def list(
        self,
        path: str,
    ) -> CommandResult:
        """
        List all objects stored in a write-cache.

        Args:
                path: Path to storage engine component
        Returns:
                Command's result.
        """
        return self._execute(
            "write-cache list",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )

    def get(
        self,
        address: str,
        path: str,
    ) -> CommandResult:
        """
        Get specific object from a write-cache.

        Args:
                address: Object address
                path: Path to storage engine component
        Returns:
                Command's result.
        """
        return self._execute(
            "write-cache get",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )
