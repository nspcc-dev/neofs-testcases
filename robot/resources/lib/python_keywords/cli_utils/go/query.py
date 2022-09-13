from typing import Optional

from cli_utils.cli_command import NeofsCliCommand


class NeoGoQuery(NeofsCliCommand):
    def candidates(
        self,
        rpc_endpoint: str,
        timeout: int = 10,
    ) -> str:
        """Get candidates and votes

        Args:
            rpc_endpoint (str):  RPC node address
            timeout (int):       Timeout for the operation (default: 10s)

        Returns:
            str: Command string

        """
        return self._execute(
            "query candidates",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            },
        )

    def committee(
        self,
        rpc_endpoint: str,
        timeout: int = 10,
    ) -> str:
        """Get committee list

        Args:
            rpc_endpoint (str):  RPC node address
            timeout (int):       Timeout for the operation (default: 10s)

        Returns:
            str: Command string

        """
        return self._execute(
            "query committee",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            },
        )

    def height(
        self,
        rpc_endpoint: str,
        timeout: int = 10,
    ) -> str:
        """Get node height

        Args:
            rpc_endpoint (str):  RPC node address
            timeout (int):       Timeout for the operation (default: 10s)

        Returns:
            str: Command string

        """
        return self._execute(
            "query height",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            },
        )

    def tx(
        self,
        tx_hash: str,
        rpc_endpoint: str,
        timeout: int = 10,
    ) -> str:
        """Query transaction status

        Args:
            tx_hash (str):       Hash of transaction
            rpc_endpoint (str):  RPC node address
            timeout (int):       Timeout for the operation (default: 10s)

        Returns:
            str: Command string

        """
        return self._execute(
            f"query tx {tx_hash}",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self", "hash"]
            },
        )

    def voter(
        self,
        rpc_endpoint: str,
        timeout: int = 10,
    ) -> str:
        """Print NEO holder account state

        Args:
            rpc_endpoint (str):  RPC node address
            timeout (int):       Timeout for the operation (default: 10s)

        Returns:
            str: Command string

        """
        return self._execute(
            "query voter",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            },
        )
