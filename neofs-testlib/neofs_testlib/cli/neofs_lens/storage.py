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
        out: str = None,
        payload_only: bool = False,
    ) -> CommandResult:
        """
        Get object from the NeoFS node's storage snapshot

        Args:
                address: Object address
                config: Path to file with storage node config
                out: File to save object payload (optional)
                payload_only: Save only object payload (optional)
        Returns:
                Command's result.
        """
        return self._execute(
            "storage get",
            **{
                param: value
                for param, value in locals().items()
                if param not in ["self"] and value is not None and value is not False
            },
        )

    def status(self, address: str, config: str) -> CommandResult:
        """
        Get object status from the NeoFS node's storage snapshot.

        Args:
                address: Object address
                config: Path to file with storage node config
        Returns:
                Command's result.
        """
        return self._execute(
            "storage status",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )

    def sanity(self, config: str) -> CommandResult:
        """
        Check that objects registered in metabase match blobstor.

        Args:
                config: Path to file with storage node config
        Returns:
                Command's result.
        """
        return self._execute(
            "storage sanity",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )

    def flush_write_caches(self, config: str) -> CommandResult:
        """
        Flush write-caches described by the storage node config.

        Args:
                config: Path to file with storage node config
        Returns:
                Command's result.
        """
        return self._execute(
            "storage flush-write-caches",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )
