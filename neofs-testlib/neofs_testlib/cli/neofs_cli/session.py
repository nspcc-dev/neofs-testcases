from typing import Optional

from neofs_testlib.cli.cli_command import CliCommand
from neofs_testlib.shell import CommandResult


class NeofsCliSession(CliCommand):
    def create(
        self,
        rpc_endpoint: str,
        wallet: str,
        wallet_password: str,
        out: str,
        lifetime: Optional[int] = None,
        expire_at: Optional[int] = None,
        address: Optional[str] = None,
        json: Optional[bool] = False,
        shell_timeout: Optional[int] = None,
    ) -> CommandResult:
        """
        Create session token.

        Args:
            address: Address of wallet account.
            out: File to write session token to.
            lifetime: Number of epochs for token to stay valid - relative to the current epoch.
            expire_at: Last epoch in the life of the token - absolute value.
            json: Output token in JSON.
            wallet: WIF (NEP-2) string or path to the wallet or binary key.
            wallet_password: Wallet password.
            rpc_endpoint: Remote node address (as 'multiaddr' or '<host>:<port>').
            shell_timeout: Shell timeout for the command.

        Returns:
            Command's result.
        """
        return self._execute_with_password(
            "session create",
            wallet_password,
            **{param: value for param, value in locals().items() if param not in ["self", "wallet_password"]},
        )

    def create_v2(
        self,
        wallet: str,
        out: str,
        rpc_endpoint: str,
        lifetime: Optional[int] = None,
        expire_at: Optional[int] = None,
        address: Optional[str] = None,
        json: Optional[bool] = False,
        subject: Optional[list[str]] = None,
        subject_nns: Optional[list[str]] = None,
        context: Optional[list[str]] = None,
        origin: Optional[str] = None,
        final: Optional[bool] = False,
        force: Optional[bool] = False,
        shell_timeout: Optional[int] = None,
    ) -> CommandResult:
        """
        Create V2 session token with subjects and multiple contexts.

        V2 tokens support:
        - Multiple subjects (accounts authorized to use the token)
        - Multiple contexts (container + object operations)
        - Server-side session key storage via SessionCreate RPC
        - Token delegation chains via --origin flag

        Args:
            wallet: Path to the wallet.
            out: File to write session token to.
            rpc_endpoint: Remote node address (as 'multiaddr' or '<host>:<port>').
            lifetime: Duration in seconds for token to stay valid (default 36000).
            expire_at: Expiration time in seconds for token to stay valid.
            address: Address of wallet account.
            json: Output token in JSON.
            subject: Subject user IDs (can be specified multiple times).
            subject_nns: Subject NNS names (can be specified multiple times).
            context: Context spec (repeatable): containerID:verbs[:objectID1,objectID2,...].
            origin: Path to origin token file for token delegation chain.
            final: Set the final flag in the token, disallowing further delegation.
            force: Skip token validation (use with caution).
            shell_timeout: Shell timeout for the command.

        Returns:
            Command's result.
        """
        return self._execute(
            "session create-v2",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )
