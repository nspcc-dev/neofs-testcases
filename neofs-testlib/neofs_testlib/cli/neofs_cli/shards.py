from typing import Optional, List

from neofs_testlib.cli.cli_command import CliCommand
from neofs_testlib.shell import CommandResult


class NeofsCliShards(CliCommand):
    def flush_cache(
        self,
        endpoint: str,
        wallet: str,
        shards_id: Optional[list[str]],
        address: Optional[str] = None,
        all_shards: bool = False,
    ) -> CommandResult:
        """
        Flush objects from the write-cache to the main storage.

        Args:
            address: Address of wallet account.
            shards_id: List of shard IDs in base58 encoding.
            all_shards: Process all shards.
            endpoint: Remote node address (as 'multiaddr' or '<host>:<port>').
            wallet: WIF (NEP-2) string or path to the wallet or binary key.

        Returns:
            Command's result.
        """
        return self._execute(
            f"control shards flush-cache",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )

    def set_mode(
        self,
        endpoint: str,
        wallet: str,
        mode: str,
        shards_id: Optional[list[str]],
        address: Optional[str] = None,
        all_shards: bool = False,
        clear_errors: bool = False,
    ) -> CommandResult:
        """
        Set work mode of the shard.

        Args:
            address: Address of wallet account.
            shards_id: List of shard IDs in base58 encoding.
            mode: New shard mode ('degraded-read-only', 'read-only', 'read-write').
            all_shards: Process all shards.
            clear_errors: Set shard error count to 0.
            endpoint: Remote node address (as 'multiaddr' or '<host>:<port>').
            wallet: WIF (NEP-2) string or path to the wallet or binary key.

        Returns:
            Command's result.
        """
        return self._execute(
            f"control shards set-mode",
            **{
                param: value
                for param, value in locals().items()
                if param not in ["self"]
            },
        )

    def dump(
        self,
        endpoint: str,
        wallet: str,
        shard_id: str,
        path: str,
        address: Optional[str] = None,
        no_errors: bool = False,
    ) -> CommandResult:
        """
        Dump objects from shard to a file.

        Args:
            address: Address of wallet account.
            no_errors: Skip invalid/unreadable objects.
            shard_id: Shard ID in base58 encoding.
            path: File to write objects to.
            endpoint: Remote node address (as 'multiaddr' or '<host>:<port>').
            wallet: WIF (NEP-2) string or path to the wallet or binary key.

        Returns:
            Command's result.
        """
        return self._execute(
            f"control shards dump",
            **{
                param: value
                for param, value in locals().items()
                if param not in ["self"]
            },
        )

    def list(
        self,
        endpoint: str,
        wallet: str,
        address: Optional[str] = None,
        json_mode: bool = False,
    ) -> CommandResult:
        """
        List shards of the storage node.

        Args:
            address: Address of wallet account.
            json_mode: Print shard info as a JSON array.
            endpoint: Remote node address (as 'multiaddr' or '<host>:<port>').
            wallet: WIF (NEP-2) string or path to the wallet or binary key.

        Returns:
            Command's result.
        """
        return self._execute(
            f"control shards list",
            **{
                param: value
                for param, value in locals().items()
                if param not in ["self"]
            },
        )

    def evacuate(
            self,
            endpoint: str,
            wallet: str,
            shards_id: List[str],
            all_shards: bool = False,
            no_errors: bool = False,
            address: Optional[str] = None,
            timeout: Optional[str] = None,
    ) -> CommandResult:
        """
        Evacuate objects from shard to other shards.

        Args:
            endpoint: Remote node address (as 'multiaddr' or '<host>:<port>').
            wallet: WIF (NEP-2) string or path to the wallet or binary key.
            shards_id: List of shard IDs in base58 encoding.
            all_shards: Process all shards.
            no_errors: Skip invalid/unreadable objects.
            address: Address of wallet account.
            timeout: Timeout for the operation (default 15s).

        Returns:
            Command's result.
        """

        return self._execute(
            f"control shards evacuate",
            **{
                param: value
                for param, value in locals().items()
                if param not in ["self"]
            },
        )

    def restore(
            self,
            endpoint: str,
            wallet: str,
            shard_id: str,
            path: str,
            no_errors: bool = False,
            address: Optional[str] = None,
            timeout: Optional[str] = None,
    ) -> CommandResult:
        """
        Restore objects from shard to a file.

        Args:
            endpoint: Remote node address (as 'multiaddr' or '<host>:<port>').
            wallet: WIF (NEP-2) string or path to the wallet or binary key.
            shard_id: Shard ID in base58 encoding.
            path: File to read objects from
            no_errors: Skip invalid/unreadable objects.
            address: Address of wallet account.
            timeout: Timeout for the operation (default 15s).

        Returns:
            Command's result.
        """

        return self._execute(
            f"control shards restore",
            **{
                param: value
                for param, value in locals().items()
                if param not in ["self"]
            },
        )
