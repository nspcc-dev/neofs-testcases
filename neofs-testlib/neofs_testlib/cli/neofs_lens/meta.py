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

    def list(self, path: str, limit: int) -> CommandResult:
        """
        List objects registered in a metabase.

        Args:
                path: Path to storage engine component
                limit: Maximum number of objects to list
        Returns:
                Command's result.
        """
        return self._execute(
            "meta list",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )

    def get(self, address: str, path: str) -> CommandResult:
        """
        Get an object header from a metabase.

        Args:
                address: Object address
                path: Path to storage engine component
        Returns:
                Command's result.
        """
        return self._execute(
            "meta get",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )

    def status(self, address: str, path: str) -> CommandResult:
        """
        Read metabase indexes and state for an object.

        Args:
                address: Object address
                path: Path to storage engine component
        Returns:
                Command's result.
        """
        return self._execute(
            "meta status",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )

    def last_resync_epoch(self, path: str) -> CommandResult:
        """
        Read the epoch of the last metabase resync.

        Args:
                path: Path to storage engine component
        Returns:
                Command's result.
        """
        return self._execute(
            "meta last-resync-epoch",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )

    def list_garbage(self, path: str, container: str) -> CommandResult:
        """
        List objects marked as garbage in a metabase.

        Args:
                path: Path to storage engine component
                container: Container ID
        Returns:
                Command's result.
        """
        return self._execute(
            "meta list-garbage",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )

    def put(self, path: str, obj: str) -> CommandResult:
        """
        Put an object from a file into a metabase.

        Args:
                path: Path to storage engine component
                obj: Path to a file with a binary object
        Returns:
                Command's result.
        """
        return self._execute(
            "meta put",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )

    def remove(self, path: str, addresses: str) -> CommandResult:
        """
        Remove objects from a metabase.

        Args:
                path: Path to storage engine component
                addresses: Object addresses to remove, separated by comma
        Returns:
                Command's result.
        """
        return self._execute(
            "meta remove",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )
