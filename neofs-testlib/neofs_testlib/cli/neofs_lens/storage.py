from neofs_testlib.cli.cli_command import CliCommand
from neofs_testlib.shell import CommandResult


class NeofsLensStorage(CliCommand):
    def list(
        self,
        config: str,
    ) -> CommandResult:
        """
        List all objects stored in a blobstor (as registered in metabase).

        Args:
                config: Path to file with storage node config
        Returns:
                Command's result.
        """
        return self._execute(
            "storage list",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )

    def get(
        self,
        address: str,
        config: str,
    ) -> CommandResult:
        """
        Get object from the NeoFS node's storage snapshot

        Args:
                address: Object address
                config: Path to file with storage node config
        Returns:
                Command's result.
        """
        return self._execute(
            "storage get",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )
