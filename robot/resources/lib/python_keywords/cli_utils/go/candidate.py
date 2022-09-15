from typing import Optional

from cli_utils.cli_command import NeofsCliCommand


class NeoGoCandidate(NeofsCliCommand):

    def register(
            self,
            address: str,
            rpc_endpoint: str,
            wallet: Optional[str] = None,
            wallet_config: Optional[str] = None,
            gas: Optional[float] = None,
            timeout: int = 10,
    ) -> str:
        """ register as a new candidate

        Args:
            address (str):       Address to register
            wallet (str):        Target location of the wallet file ('-' to read from stdin);
                                 conflicts with --wallet-config flag.
            wallet_config (str): Target location of the wallet config file;
                                 conflicts with --wallet flag.
            gas (float):         network fee to add to the transaction (prioritizing it)
            rpc_endpoint (str):  RPC node address
            timeout (int):       Timeout for the operation (default: 10s)


        Returns:
            str: Command string

        """
        assert bool(wallet) ^ bool(wallet_config), self.WALLET_SOURCE_ERROR_MSG

        return self._execute(
            "wallet candidate register",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )

    def unregister(
        self,
        address: str,
        rpc_endpoint: str,
        wallet: Optional[str] = None,
        wallet_config: Optional[str] = None,
        gas: Optional[float] = None,
        timeout: int = 10,
    ) -> str:
        """ unregister self as a candidate

        Args:
            address (str):       Address to unregister
            wallet (str):        Target location of the wallet file ('-' to read from stdin);
                                 conflicts with --wallet-config flag.
            wallet_config (str): Target location of the wallet config file;
                                 conflicts with --wallet flag.
            gas (float):         network fee to add to the transaction (prioritizing it)
            rpc_endpoint (str):  RPC node address
            timeout (int):       Timeout for the operation (default: 10s)


        Returns:
            str: Command string

        """
        assert bool(wallet) ^ bool(wallet_config), self.WALLET_SOURCE_ERROR_MSG

        return self._execute(
            "wallet candidate unregister",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )

    def vote(
        self,
        candidate: str,
        rpc_endpoint: str,
        wallet: Optional[str] = None,
        wallet_config: Optional[str] = None,
        gas: Optional[float] = None,
        timeout: int = 10,
    ) -> str:
        """ Votes for a validator by calling "vote" method of a NEO native
            contract. Do not provide candidate argument to perform unvoting.


        Args:
            candidate (str):     Public key of candidate to vote for
            wallet (str):        Target location of the wallet file ('-' to read from stdin);
                                 conflicts with --wallet-config flag.
            wallet_config (str): Target location of the wallet config file;
                                 conflicts with --wallet flag.
            gas (float):         network fee to add to the transaction (prioritizing it)
            rpc_endpoint (str):  RPC node address
            timeout (int):       Timeout for the operation (default: 10s)


        Returns:
            str: Command string

        """
        assert bool(wallet) ^ bool(wallet_config), self.WALLET_SOURCE_ERROR_MSG

        return self._execute(
            "wallet candidate vote",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )
