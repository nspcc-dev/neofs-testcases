from typing import Optional

from cli_utils.cli_command import NeofsCliCommand


class NeofsAdmMorphSubnet(NeofsCliCommand):
    def create(self, rpc_endpoint: str, address: str, wallet: str, notary: bool = False) -> str:
        """Create NeoFS subnet.

        Args:
            address (str):       Address in the wallet, optional
            notary (bool):       Flag to create subnet in notary environment
            rpc_endpoint (str):  N3 RPC node endpoint
            wallet (str):        Path to file with wallet


        Returns:
            str: Command string

        """
        return self._execute(
            'morph subnet create',
            **{param: param_value for param, param_value in locals().items() if param not in ['self']}
        )

    def get(self, rpc_endpoint: str, subnet: str) -> str:
        """Read information about the NeoFS subnet.

        Args:
            rpc_endpoint (str):  N3 RPC node endpoint
            subnet (str):        ID of the subnet to read


        Returns:
            str: Command string

        """
        return self._execute(
            'morph subnet get',
            **{param: param_value for param, param_value in locals().items() if param not in ['self']}
        )

    def remove(self, rpc_endpoint: str, wallet: str, subnet: str, address: Optional[str] = None) -> str:
        """Remove NeoFS subnet.

        Args:
            address (str):       Address in the wallet, optional
            rpc_endpoint (str):  N3 RPC node endpoint
            subnet (str):        ID of the subnet to read
            wallet (str):        Path to file with wallet


        Returns:
            str: Command string

        """
        return self._execute(
            'morph subnet remove',
            **{param: param_value for param, param_value in locals().items() if param not in ['self']}
        )

    def admin_add(self, rpc_endpoint: str, wallet: str, admin: str, subnet: str, client: Optional[str] = None,
                  group: Optional[str] = None, address: Optional[str] = None) -> str:
        """Add admin to the NeoFS subnet.

        Args:
            address (str):       Address in the wallet, optional
            admin (str):         Hex-encoded public key of the admin
            client (str):        Add client admin instead of node one
            group (str):         Client group ID in text format (needed with --client only)
            rpc_endpoint (str):  N3 RPC node endpoint
            subnet (str):        ID of the subnet to read
            wallet (str):        Path to file with wallet


        Returns:
            str: Command string

        """
        return self._execute(
            'morph subnet admin add',
            **{param: param_value for param, param_value in locals().items() if param not in ['self']}
        )

    def admin_remove(self, rpc_endpoint: str, wallet: str, admin: str, subnet: str, client: Optional[str] = None,
                     address: Optional[str] = None) -> str:
        """Remove admin of the NeoFS subnet.

        Args:
            address (str):       Address in the wallet, optional
            admin (str):         Hex-encoded public key of the admin
            client (str):        Remove client admin instead of node one
            rpc_endpoint (str):  N3 RPC node endpoint
            subnet (str):        ID of the subnet to read
            wallet (str):        Path to file with wallet


        Returns:
            str: Command string

        """
        return self._execute(
            'morph subnet admin remove',
            **{param: param_value for param, param_value in locals().items() if param not in ['self']}
        )

    def client_add(self, rpc_endpoint: str, wallet: str, subnet: str, client: Optional[str] = None,
                  group: Optional[str] = None, address: Optional[str] = None) -> str:
        """Add client to the NeoFS subnet.

        Args:
            address (str):       Address in the wallet, optional
            client (str):        Add client admin instead of node one
            group (str):         Client group ID in text format (needed with --client only)
            rpc_endpoint (str):  N3 RPC node endpoint
            subnet (str):        ID of the subnet to read
            wallet (str):        Path to file with wallet


        Returns:
            str: Command string

        """
        return self._execute(
            'morph subnet client add',
            **{param: param_value for param, param_value in locals().items() if param not in ['self']}
        )

    def client_remove(self, rpc_endpoint: str, wallet: str, client: str, group: str, subnet: str,
                      address: Optional[str] = None) -> str:
        """Remove client of the NeoFS subnet.

        Args:
            address (str):       Address in the wallet, optional
            client (str):        Remove client admin instead of node one
            group (str):         ID of the client group to work with
            rpc_endpoint (str):  N3 RPC node endpoint
            subnet (str):        ID of the subnet to read
            wallet (str):        Path to file with wallet


        Returns:
            str: Command string

        """
        return self._execute(
            'morph subnet client remove',
            **{param: param_value for param, param_value in locals().items() if param not in ['self']}
        )

    def node_add(self, rpc_endpoint: str, wallet: str, node: str, subnet: str) -> str:
        """Add node to the NeoFS subnet.

        Args:
            node (str):          Hex-encoded public key of the node
            rpc_endpoint (str):  N3 RPC node endpoint
            subnet (str):        ID of the subnet to read
            wallet (str):        Path to file with wallet


        Returns:
            str: Command string

        """
        return self._execute(
            'morph subnet node add',
            **{param: param_value for param, param_value in locals().items() if param not in ['self']}
        )

    def node_remove(self, rpc_endpoint: str, wallet: str, node: str, subnet: str) -> str:
        """Remove node from the NeoFS subnet.

        Args:
            node (str):          Hex-encoded public key of the node
            rpc_endpoint (str):  N3 RPC node endpoint
            subnet (str):        ID of the subnet to read
            wallet (str):        Path to file with wallet


        Returns:
            str: Command string

        """
        return self._execute(
            'morph subnet node remove',
            **{param: param_value for param, param_value in locals().items() if param not in ['self']}
        )
