from typing import List, Optional

from cli_utils.cli_command import NeofsCliCommand


class NeoGoNep17(NeofsCliCommand):
    def balance(
        self,
        address: str,
        token: str,
        rpc_endpoint: str,
        wallet: Optional[str] = None,
        wallet_config: Optional[str] = None,
        timeout: int = 10,
    ) -> str:
        """Get address balance

        Args:
            address (str):       Address to use
            wallet (str):        Target location of the wallet file ('-' to read from stdin);
                                 conflicts with --wallet-config flag.
            wallet_config (str): Target location of the wallet config file;
                                 conflicts with --wallet flag.
            token (str):         Token to use (hash or name (for NEO/GAS or imported tokens))
            rpc_endpoint (str):  RPC node address
            timeout (int):       Timeout for the operation (default: 10s)

        Returns:
            str: Command string

        """
        assert bool(wallet) ^ bool(wallet_config), self.WALLET_SOURCE_ERROR_MSG

        return self._execute(
            "wallet nep17 balance",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )

    def import_token(
        self,
        address: str,
        wallet: Optional[str] = None,
        wallet_config: Optional[str] = None,
        token: Optional[str] = None,
        rpc_endpoint: Optional[str] = None,
        timeout: int = 10,
    ) -> str:
        """import NEP-17 token to a wallet

        Args:
            address (str):       Token contract address or hash in LE
            wallet (str):        Target location of the wallet file ('-' to read from stdin);
                                 conflicts with --wallet-config flag.
            wallet_config (str): Target location of the wallet config file;
                                 conflicts with --wallet flag.
            token (str):         Token to use (hash or name (for NEO/GAS or imported tokens))
            rpc_endpoint (str):  RPC node address
            timeout (int):       Timeout for the operation (default: 10s)

        Returns:
            str: Command string

        """
        assert bool(wallet) ^ bool(wallet_config), self.WALLET_SOURCE_ERROR_MSG

        return self._execute(
            "wallet nep17 import",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )

    def info(
        self,
        token: str,
        wallet: Optional[str] = None,
        wallet_config: Optional[str] = None,
    ) -> str:
        """print imported NEP-17 token info

        Args:
            wallet (str):        Target location of the wallet file ('-' to read from stdin);
                                 conflicts with --wallet-config flag.
            wallet_config (str): Target location of the wallet config file;
                                 conflicts with --wallet flag.
            token (str):         Token to use (hash or name (for NEO/GAS or imported tokens))

        Returns:
            str: Command string

        """
        assert bool(wallet) ^ bool(wallet_config), self.WALLET_SOURCE_ERROR_MSG

        return self._execute(
            "wallet nep17 info",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )

    def remove(
        self,
        token: str,
        wallet: Optional[str] = None,
        wallet_config: Optional[str] = None,
        force: bool = False,
    ) -> str:
        """remove NEP-17 token from the wallet

        Args:
            wallet (str):        Target location of the wallet file ('-' to read from stdin);
                                 conflicts with --wallet-config flag.
            wallet_config (str): Target location of the wallet config file;
                                 conflicts with --wallet flag.
            token (str):         Token to use (hash or name (for NEO/GAS or imported tokens))
            force (bool):        Do not ask for a confirmation

        Returns:
            str: Command string

        """
        return self._execute(
            "wallet nep17 remove",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )

    def transfer(
        self,
        token: str,
        to_address: str,
        sysgas: float,
        rpc_endpoint: str,
        wallet: Optional[str] = None,
        wallet_config: Optional[str] = None,
        out: Optional[str] = None,
        from_address: Optional[str] = None,
        force: bool = False,
        gas: Optional[float] = None,
        amount: float = 0,
        timeout: int = 10,
    ) -> str:
        """Transfers specified NEP-17 token amount with optional 'data' parameter and cosigners
        list attached to the transfer. See 'contract testinvokefunction' documentation
        for the details about 'data' parameter and cosigners syntax. If no 'data' is
        given then default nil value will be used. If no cosigners are given then the
        sender with CalledByEntry scope will be used as the only signer.

        Args:
            wallet (str):        Target location of the wallet file ('-' to read from stdin);
                                 conflicts with --wallet-config flag.
            wallet_config (str): Target location of the wallet config file;
                                 conflicts with --wallet flag.
            out (str):           file to put JSON transaction to
            from_address (str):  Address to send an asset from
            to_address (str):    Address to send an asset to
            token (str):         Token to use (hash or name (for NEO/GAS or imported tokens))
            force (bool):        Do not ask for a confirmation
            gas (float):         network fee to add to the transaction (prioritizing it)
            sysgas (float):      system fee to add to transaction (compensating for execution)
            force (bool):        Do not ask for a confirmation
            amount (float)       Amount of asset to send
            rpc_endpoint (str):  RPC node address
            timeout (int):       Timeout for the operation (default: 10s)


        Returns:
            str: Command string

        """
        assert bool(wallet) ^ bool(wallet_config), self.WALLET_SOURCE_ERROR_MSG

        return self._execute(
            "wallet nep17 transfer",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )

    def multitransfer(
        self,
        token: str,
        to_address: List[str],
        sysgas: float,
        rpc_endpoint: str,
        wallet: Optional[str] = None,
        wallet_config: Optional[str] = None,
        out: Optional[str] = None,
        from_address: Optional[str] = None,
        force: bool = False,
        gas: Optional[float] = None,
        amount: float = 0,

        timeout: int = 10,
    ) -> str:
        """transfer NEP-17 tokens to multiple recipients

        Args:
            wallet (str):        Target location of the wallet file ('-' to read from stdin);
                                 conflicts with --wallet-config flag.
            wallet_config (str): Target location of the wallet config file;
                                 conflicts with --wallet flag.
            out (str):           file to put JSON transaction to
            from_address (str):  Address to send an asset from
            to_address (str):    Address to send an asset to
            token (str):         Token to use (hash or name (for NEO/GAS or imported tokens))
            force (bool):        Do not ask for a confirmation
            gas (float):         network fee to add to the transaction (prioritizing it)
            sysgas (float):      system fee to add to transaction (compensating for execution)
            force (bool):        Do not ask for a confirmation
            amount (float)       Amount of asset to send
            rpc_endpoint (str):  RPC node address
            timeout (int):       Timeout for the operation (default: 10s)


        Returns:
            str: Command string

        """
        assert bool(wallet) ^ bool(wallet_config), self.WALLET_SOURCE_ERROR_MSG

        return self._execute(
            "wallet nep17 multitransfer",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )
