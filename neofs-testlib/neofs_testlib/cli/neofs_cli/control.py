from typing import Optional

from neofs_testlib.cli.cli_command import CliCommand
from neofs_testlib.shell import CommandResult


class NeofsCliControl(CliCommand):
    def healthcheck(
        self,
        endpoint: str,
        post_data="",
    ) -> CommandResult:
        """
        Get current epoch number.

        Args:
            address: Address of wallet account.
            generate_key: Generate new private key.
            rpc_endpoint: Remote node address (as 'multiaddr' or '<host>:<port>').
            ttl: TTL value in request meta header (default 2).
            wallet: Path to the wallet or binary key.
            xhdr: Dict with request X-Headers.

        Returns:
            Command's result.
        """
        return self._execute(
            "control healthcheck",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )

    def object_status(self, address: str, endpoint: str, object: str, wallet: str) -> CommandResult:
        """
        Get object status.

        Args:
            address: Address of wallet account
            endpoint: Remote node control address (as 'multiaddr' or '<host>:<port>')
            object: Object address
            wallet: Path to the wallet

        Returns:
            Command's result.
        """
        return self._execute(
            "control object status",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )
