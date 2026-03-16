from neofs_testlib.cli.cli_command import CliCommand
from neofs_testlib.shell import CommandResult


class NeofsLensFstree(CliCommand):
    def list(
        self,
        path: str,
    ) -> CommandResult:
        """
        List all objects stored in a fstree (for neofs-node version > 0.51.1).

        Args:
                path: Path to storage engine component
        Returns:
                Command's result.
        """
        return self._execute(
            "fstree list",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )

    def get(
        self,
        address: str,
        path: str,
        out: str = None,
        payload_only: bool = False,
    ) -> CommandResult:
        """
        Get specific object from a fstree (for neofs-node version > 0.51.1).

        Args:
                address: Object address
                path: Path to storage engine component
                out: File to save object payload (optional)
                payload_only: Save only object payload (optional)
        Returns:
                Command's result.
        """
        return self._execute(
            "fstree get",
            **{
                param: value
                for param, value in locals().items()
                if param not in ["self"] and value is not None and value is not False
            },
        )
