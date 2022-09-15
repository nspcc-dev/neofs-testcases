from typing import Optional

from cli_utils.cli_command import NeofsCliCommand


class NeoGoWallet(NeofsCliCommand):
    def claim(
        self,
        address: str,
        rpc_endpoint: str,
        wallet: Optional[str] = None,
        wallet_config: Optional[str] = None,
        timeout: int = 10,
    ) -> str:
        """claim GAS

        Args:
            address (str):       Address to claim GAS for
            wallet (str):        Target location of the wallet file ('-' to read from stdin);
                                 conflicts with --wallet-config flag.
            wallet_config (str): Target location of the wallet config file;
                                 conflicts with --wallet flag.
            rpc_endpoint (str):  RPC node address
            timeout (int):       Timeout for the operation (default: 10s)

        Returns:
            str: Command string

        """
        assert bool(wallet) ^ bool(wallet_config), self.WALLET_SOURCE_ERROR_MSG

        return self._execute(
            "wallet claim",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )

    def init(
        self,
        wallet: Optional[str] = None,
        wallet_config: Optional[str] = None,
        account: bool = False,
    ) -> str:
        """create a new wallet

        Args:
            wallet (str):        Target location of the wallet file ('-' to read from stdin);
                                 conflicts with --wallet-config flag.
            wallet_config (str): Target location of the wallet config file;
                                 conflicts with --wallet flag.
            account (bool):      Create a new account

        Returns:
            str: Command string

        """
        assert bool(wallet) ^ bool(wallet_config), self.WALLET_SOURCE_ERROR_MSG

        return self._execute(
            "wallet init",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )

    def convert(
        self,
        out: str,
        wallet: Optional[str] = None,
        wallet_config: Optional[str] = None,
    ) -> str:
        """convert addresses from existing NEO2 NEP6-wallet to NEO3 format

        Args:
            wallet (str):        Target location of the wallet file ('-' to read from stdin);
                                 conflicts with --wallet-config flag.
            wallet_config (str): Target location of the wallet config file;
                                 conflicts with --wallet flag.
            out (str):           where to write converted wallet

        Returns:
            str: Command string

        """
        assert bool(wallet) ^ bool(wallet_config), self.WALLET_SOURCE_ERROR_MSG

        return self._execute(
            "wallet convert",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )

    def create(
        self,
        wallet: Optional[str] = None,
        wallet_config: Optional[str] = None,
    ) -> str:
        """add an account to the existing wallet

        Args:
            wallet (str):        Target location of the wallet file ('-' to read from stdin);
                                 conflicts with --wallet-config flag.
            wallet_config (str): Target location of the wallet config file;
                                 conflicts with --wallet flag.

        Returns:
            str: Command string

        """
        assert bool(wallet) ^ bool(wallet_config), self.WALLET_SOURCE_ERROR_MSG

        return self._execute(
            "wallet create",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )

    def dump(
        self,
        wallet: Optional[str] = None,
        wallet_config: Optional[str] = None,
        decrypt: bool = False,
    ) -> str:
        """check and dump an existing NEO wallet

        Args:
            wallet (str):        Target location of the wallet file ('-' to read from stdin);
                                 conflicts with --wallet-config flag.
            wallet_config (str): Target location of the wallet config file;
                                 conflicts with --wallet flag.
            decrypt (bool):      Decrypt encrypted keys.

        Returns:
            str: Command string

        """
        assert bool(wallet) ^ bool(wallet_config), self.WALLET_SOURCE_ERROR_MSG

        return self._execute(
            "wallet dump",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )

    def dump_keys(
        self,
        address: str,
        wallet: Optional[str] = None,
        wallet_config: Optional[str] = None,
    ) -> str:
        """check and dump an existing NEO wallet

        Args:
            wallet (str):        Target location of the wallet file ('-' to read from stdin);
                                 conflicts with --wallet-config flag.
            wallet_config (str): Target location of the wallet config file;
                                 conflicts with --wallet flag.
            address (str):       address to print public keys for

        Returns:
            str: Command string

        """
        assert bool(wallet) ^ bool(wallet_config), self.WALLET_SOURCE_ERROR_MSG

        return self._execute(
            "wallet dump-keys",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )

    def export(
        self,
        wallet: Optional[str] = None,
        wallet_config: Optional[str] = None,
        decrypt: bool = False,
    ) -> str:
        """export keys for address

        Args:
            wallet (str):        Target location of the wallet file ('-' to read from stdin);
                                 conflicts with --wallet-config flag.
            wallet_config (str): Target location of the wallet config file;
                                 conflicts with --wallet flag.
            decrypt (bool):      Decrypt encrypted keys.

        Returns:
            str: Command string

        """
        assert bool(wallet) ^ bool(wallet_config), self.WALLET_SOURCE_ERROR_MSG

        return self._execute(
            "wallet export",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )

    def import_wif(
        self,
        wif: str,
        name: str,
        contract: str,
        wallet: Optional[str] = None,
        wallet_config: Optional[str] = None,
    ) -> str:
        """import WIF of a standard signature contract

        Args:
            wallet (str):        Target location of the wallet file ('-' to read from stdin);
                                 conflicts with --wallet-config flag.
            wallet_config (str): Target location of the wallet config file;
                                 conflicts with --wallet flag.
            wif (str):           WIF to import
            name (str):          Optional account name
            contract (str):      Verification script for custom contracts

        Returns:
            str: Command string

        """
        assert bool(wallet) ^ bool(wallet_config), self.WALLET_SOURCE_ERROR_MSG

        return self._execute(
            "wallet import",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )

    def import_multisig(
        self,
        wif: str,
        name: Optional[str] = None,
        min_number: int = 0,
        wallet: Optional[str] = None,
        wallet_config: Optional[str] = None,
    ) -> str:
        """import multisig contract

        Args:
            wallet (str):        Target location of the wallet file ('-' to read from stdin);
                                 conflicts with --wallet-config flag.
            wallet_config (str): Target location of the wallet config file;
                                 conflicts with --wallet flag.
            wif (str):           WIF to import
            name (str):          Optional account name
            min_number (int):    Minimal number of signatures (default: 0)

        Returns:
            str: Command string

        """
        assert bool(wallet) ^ bool(wallet_config), self.WALLET_SOURCE_ERROR_MSG

        return self._execute(
            "wallet import-multisig",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )

    def import_deployed(
        self,
        wif: str,
        rpc_endpoint: str,
        name: Optional[str] = None,
        wallet: Optional[str] = None,
        wallet_config: Optional[str] = None,
        contract: Optional[str] = None,

        timeout: int = 10,
    ) -> str:
        """import multisig contract

        Args:
            wallet (str):        Target location of the wallet file ('-' to read from stdin);
                                 conflicts with --wallet-config flag.
            wallet_config (str): Target location of the wallet config file;
                                 conflicts with --wallet flag.
            wif (str):           WIF to import
            name (str):          Optional account name
            contract (str):      Contract hash or address
            rpc_endpoint (str):  RPC node address
            timeout (int):       Timeout for the operation (default: 10s)

        Returns:
            str: Command string

        """
        assert bool(wallet) ^ bool(wallet_config), self.WALLET_SOURCE_ERROR_MSG

        return self._execute(
            "wallet import-deployed",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )

    def remove(
        self,
        address: str,
        wallet: Optional[str] = None,
        wallet_config: Optional[str] = None,
        force: bool = False,
    ) -> str:
        """check and dump an existing NEO wallet

        Args:
            wallet (str):        Target location of the wallet file ('-' to read from stdin);
                                 conflicts with --wallet-config flag.
            wallet_config (str): Target location of the wallet config file;
                                 conflicts with --wallet flag.
            address (str):       Account address or hash in LE form to be removed
            force (bool):        Do not ask for a confirmation

        Returns:
            str: Command string

        """
        assert bool(wallet) ^ bool(wallet_config), self.WALLET_SOURCE_ERROR_MSG

        return self._execute(
            "wallet remove",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )

    def sign(
        self,
        input_file: str,
        address: str,
        rpc_endpoint: str,
        wallet: Optional[str] = None,
        wallet_config: Optional[str] = None,
        out: Optional[str] = None,
        timeout: int = 10,
    ) -> str:
        """import multisig contract

        Args:
            wallet (str):        Target location of the wallet file ('-' to read from stdin);
                                 conflicts with --wallet-config flag.
            wallet_config (str): Target location of the wallet config file;
                                 conflicts with --wallet flag.
            out (str):           file to put JSON transaction to
            input_file (str):    file with JSON transaction
            address (str):       Address to use
            rpc_endpoint (str):  RPC node address
            timeout (int):       Timeout for the operation (default: 10s)

        Returns:
            str: Command string

        """
        assert bool(wallet) ^ bool(wallet_config), self.WALLET_SOURCE_ERROR_MSG

        return self._execute(
            "wallet sign",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )
