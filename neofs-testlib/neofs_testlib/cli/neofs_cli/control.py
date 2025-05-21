from typing import Optional

from neofs_testlib.cli.cli_command import CliCommand
from neofs_testlib.shell import CommandResult


class NeofsCliControl(CliCommand):
    def healthcheck(
        self,
        endpoint: str,
        post_data="",
        shell_timeout: Optional[int] = None,
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
            shell_timeout: Shell timeout for the command.

        Returns:
            Command's result.
        """
        return self._execute(
            "control healthcheck",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )

    def object_status(
        self, address: str, endpoint: str, object: str, wallet: str, shell_timeout: Optional[int] = None
    ) -> CommandResult:
        """
        Get object status.

        Args:
            address: Address of wallet account
            endpoint: Remote node control address (as 'multiaddr' or '<host>:<port>')
            object: Object address
            wallet: Path to the wallet
            shell_timeout: Shell timeout for the command.

        Returns:
            Command's result.
        """
        return self._execute(
            "control object status",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )

    def notary_list(
        self, address: str, endpoint: str, wallet: str, shell_timeout: Optional[int] = None
    ) -> CommandResult:
        """
        Get list of all notary requests in network.

        Args:
            address: Address of wallet account
            endpoint: Remote node control address (as 'multiaddr' or '<host>:<port>')
            wallet: Path to the wallet
            shell_timeout: Shell timeout for the command.

        Returns:
            Command's result.
        """
        return self._execute(
            "control notary list",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )

    def notary_request(
        self, address: str, endpoint: str, wallet: str, method: str, post_data="", shell_timeout: Optional[int] = None
    ) -> CommandResult:
        """
        Create and send a notary request with one of the following methods:
        - newEpoch, transaction for creating of new NeoFS epoch event in FS chain, no args
        - setConfig, transaction to add/update global config value in the NeoFS network, 1 arg in the form key=val
        - removeNode, transaction to move nodes to the Offline state in the candidates list, 1 arg is the public key of the node

        Args:
            address: Address of wallet account
            endpoint: Remote node control address (as 'multiaddr' or '<host>:<port>')
            wallet: Path to the wallet
            method: Requested method
            post_data: Requested method argument
            shell_timeout: Shell timeout for the command.

        Returns:
            Command's result.
        """
        return self._execute(
            "control notary request",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )

    def notary_sign(
        self, address: str, endpoint: str, wallet: str, hash: str, shell_timeout: Optional[int] = None
    ) -> CommandResult:
        """
        Sign notary request by its hash

        Args:
            address: Address of wallet account
            endpoint: Remote node control address (as 'multiaddr' or '<host>:<port>')
            wallet: Path to the wallet
            hash: hash of the notary request
            shell_timeout: Shell timeout for the command.

        Returns:
            Command's result.
        """
        return self._execute(
            "control notary sign",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )

    def drop_objects(
        self, endpoint: str, wallet: str, objects: str, address: str = None, shell_timeout: Optional[int] = None
    ) -> CommandResult:
        """
        Drop objects from the node's local storage

        Args:
            address: Address of wallet account
            endpoint: Remote node control address (as 'multiaddr' or '<host>:<port>')
            wallet: Path to the wallet
            objects: List of object addresses to be removed in string format
            shell_timeout: Shell timeout for the command.

        Returns:
            Command's result.
        """
        return self._execute(
            "control drop-objects",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )
