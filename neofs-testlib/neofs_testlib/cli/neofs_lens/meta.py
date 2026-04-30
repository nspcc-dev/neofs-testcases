from neofs_testlib.cli.cli_command import CliCommand
from neofs_testlib.shell import CommandResult


class NeofsLensMeta(CliCommand):
    def resync(
        self,
        blobstor: str,
        path: str,
        force: bool = False,
    ) -> CommandResult:
        """
        Reset the metabase and repopulate it by iterating over all objects
        stored in the given blobstor directory.

        Args:
                blobstor: Path to blobstor directory to resync with
                path: Path to storage engine component
                force: Force resync even if metabase shard ID does not match blobstor
        Returns:
                Command's result.
        """
        return self._execute(
            "meta resync",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )

    def list(
        self,
        path: str,
        limit: int = 100,
    ) -> CommandResult:
        """
        List objects in metabase (metabase's List method).

        Args:
                path: Path to metabase directory
        Returns:
                Command's result.
        """
        return self._execute(
            "meta list",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )
