from typing import Optional

from cli_utils.cli_command import NeofsCliCommand


class NeofsCliNetmap(NeofsCliCommand):
    def epoch(
        self,
        rpc_endpoint: str,
        wallet: str,
        address: Optional[str] = None,
        generate_key: bool = False,
        ttl: Optional[int] = None,
        xhdr: Optional[dict] = None,
    ) -> str:
        """
        Get current epoch number.

        Args:
            address:        address of wallet account
            generate_key:   generate new private key
            rpc_endpoint:   remote node address (as 'multiaddr' or '<host>:<port>')
            ttl:            TTL value in request meta header (default 2)
            wallet:         path to the wallet or binary key
            xhdr:           request X-Headers in form of Key=Value

        Returns:
            str: Raw command output
        """
        return self._execute(
            "netmap epoch",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )

    def netinfo(
        self,
        rpc_endpoint: str,
        wallet: str,
        address: Optional[str] = None,
        generate_key: bool = False,
        ttl: Optional[int] = None,
        xhdr: Optional[dict] = None,
    ) -> str:
        """
        Get information about NeoFS network.

        Args:
            address:        address of wallet account
            generate_key:   generate new private key
            rpc_endpoint:   remote node address (as 'multiaddr' or '<host>:<port>')
            ttl:            TTL value in request meta header (default 2)
            wallet:         path to the wallet or binary key
            xhdr:           request X-Headers in form of Key=Value

        Returns:
            str: Raw command output
        """
        return self._execute(
            "netmap netinfo",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )

    def nodeinfo(
        self,
        rpc_endpoint: str,
        wallet: str,
        address: Optional[str] = None,
        generate_key: bool = False,
        json: bool = False,
        ttl: Optional[int] = None,
        xhdr: Optional[dict] = None,
    ) -> str:
        """
        Get target node info.

        Args:
            address:        address of wallet account
            generate_key:   generate new private key
            json:           print node info in JSON format
            rpc_endpoint:   remote node address (as 'multiaddr' or '<host>:<port>')
            ttl:            TTL value in request meta header (default 2)
            wallet:         path to the wallet or binary key
            xhdr:           request X-Headers in form of Key=Value

        Returns:
            str: Raw command output
        """
        return self._execute(
            "netmap nodeinfo",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )

    def snapshot(
        self,
        rpc_endpoint: str,
        wallet: str,
        address: Optional[str] = None,
        generate_key: bool = False,
        ttl: Optional[int] = None,
        xhdr: Optional[dict] = None,
    ) -> str:
        """
        Request current local snapshot of the network map.

        Args:
            address:        address of wallet account
            generate_key:   generate new private key
            rpc_endpoint:   remote node address (as 'multiaddr' or '<host>:<port>')
            ttl:            TTL value in request meta header (default 2)
            wallet:         path to the wallet or binary key
            xhdr:           request X-Headers in form of Key=Value

        Returns:
            str: Raw command output
        """
        return self._execute(
            "netmap snapshot",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )
