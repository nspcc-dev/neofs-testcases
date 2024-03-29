from typing import Optional

from neofs_testlib.cli.cli_command import CliCommand
from neofs_testlib.shell import CommandResult


class NeofsCliAccounting(CliCommand):
    def balance(
        self,
        wallet: Optional[str] = None,
        rpc_endpoint: Optional[str] = None,
        address: Optional[str] = None,
        owner: Optional[str] = None,
    ) -> CommandResult:
        """Get internal balance of NeoFS account

        Args:
            address: Address of wallet account.
            owner: Owner of balance account (omit to use owner from private key).
            rpc_endpoint: Remote node address (as 'multiaddr' or '<host>:<port>').
            wallet: WIF (NEP-2) string or path to the wallet or binary key.

        Returns:
            Command's result.

        """
        return self._execute(
            "accounting balance",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )
