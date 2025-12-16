import json
from typing import Optional

from neofs_testlib.cli.cli_command import CliCommand
from neofs_testlib.shell import CommandResult


class NeoGoNep11(CliCommand):
    def parse_nep11_balance(self, balance_output: str) -> dict[str, str | list[str]]:
        """
        Parse NEP11 balance output to extract account address, container ID, and token IDs.

        Example input (single token):
        Account NUQmLxj6oSEDe6NSRfNhVNYvgo1iXj7fpX
        FSCNTR: NeoFS Container (a19a485ddb30299afce01b30dac40441674a179a)
                Token: 114cee9afc69033665a6b8f35ed713170f41de8d7d25cfb57f3adef7ff31e920
                        Amount: 1
                        Updated: 98

        Example input (multiple tokens):
        Account NdUk5PX3epanz6jVeziyxe82t5pizSyT5Z
        FSCNTR: NeoFS Container (ea78939538bc61d7e67e745c78edfc8d6e58a65a)
                Token: 207b35ecd8881318fc9089181af21ec6e0f09b4255ae997b5a2c9d58727556f6
                        Amount: 1
                        Updated: 115
                Token: af71eb90a226970580598846bd63cc4ca6b7a9d39b0a9c42b0bc7baf7834a803
                        Amount: 1
                        Updated: 115

        Returns:
            dict with 'account_address', 'contract_hash', and 'token_ids' (list of all tokens) keys
        """
        lines = balance_output.strip().split("\n")
        account_address = None
        contract_hash = None
        token_ids = []

        for line in lines:
            line = line.strip()
            if line.startswith("Account "):
                account_address = line.split("Account ")[1].strip()
            elif "NeoFS Container" in line:
                # Extract contract hash from format: FSCNTR: NeoFS Container (14c9fcc666362438fb008faf4fb4309f90b4e92f)
                if "(" in line and ")" in line:
                    contract_hash = line.split("(")[1].split(")")[0].strip()
            elif line.startswith("Token: "):
                token_ids.append(line.split("Token: ")[1].strip())

        return {
            "account_address": account_address,
            "contract_hash": contract_hash,
            "token_ids": token_ids,
        }

    def parse_nep11_properties(self, properties_output: str) -> dict[str, str]:
        """
        Example:
        {"Timestamp":"1765651523","name":"HEFDijvo5WUHBTsQTxZDKSydiwa1ttp6oZwPR7fgKZMb"}

        Returns:
            dict with 'Timestamp', 'name' keys
        """

        properties = json.loads(properties_output.strip())
        timestamp = properties.get("Timestamp", "")
        name = properties.get("name", "")

        return {"timestamp": timestamp, "name": name}

    def balance(
        self,
        rpc_endpoint: str,
        wallet: Optional[str] = None,
        wallet_config: Optional[str] = None,
        timeout: int = 10,
    ) -> CommandResult:
        """Get address balance.

        Args:
            rpc_endpoint: RPC node address.
            wallet: Target location of the wallet file ('-' to read from stdin);
                conflicts with --wallet-config flag.
            wallet_config: Target location of the wallet config file; conflicts with --wallet flag.
            timeout: Timeout for the operation (default: 10s).

        Returns:
            Command's result.
        """
        assert bool(wallet) ^ bool(wallet_config), self.WALLET_SOURCE_ERROR_MSG
        exec_param = {param: param_value for param, param_value in locals().items() if param not in ["self"]}
        exec_param["timeout"] = f"{timeout}s"
        return self.parse_nep11_balance(
            self._execute(
                "wallet nep11 balance",
                **exec_param,
            ).stdout
        )

    def transfer(
        self,
        token: str,
        to_address: str,
        rpc_endpoint: str,
        sysgas: Optional[float] = None,
        wallet: Optional[str] = None,
        wallet_config: Optional[str] = None,
        out: Optional[str] = None,
        from_address: Optional[str] = None,
        force: bool = False,
        gas: Optional[float] = None,
        amount: float = 0,
        timeout: int = 10,
        id: str = "",
        await_: bool = False,
        signer: str = "",
    ) -> CommandResult:
        """Transfers specified NEP-11 token amount.

        Transfers specified NEP-11 token with optional cosigners list attached to
        the transfer. Amount should be specified for divisible NEP-11
        tokens and omitted for non-divisible NEP-11 tokens. See
        'contract testinvokefunction' documentation for the details
        about cosigners syntax. If no cosigners are given then the
        sender with CalledByEntry scope will be used as the only
        signer. If --await flag is set then the command will wait
        for the transaction to be included in a block.

        Args:
            token: Token to use (hash or name (for NEO/GAS or imported tokens)).
            to_address: Address to send an asset to.
            from_address: Address to send an asset from.
            rpc_endpoint: RPC node address.
            wallet: Target location of the wallet file ('-' to read from stdin);
                conflicts with --wallet-config flag.
            wallet_config: Target location of the wallet config file; conflicts with --wallet flag.
            out: File to put JSON transaction to.
            force: Do not ask for a confirmation.
            gas: Network fee to add to the transaction (prioritizing it).
            sysgas: System fee to add to transaction (compensating for execution).
            force: Do not ask for a confirmation.
            amount: Amount of asset to send.
            id: Hex-encoded token ID
            timeout: Timeout for the operation (default: 10s).

        Returns:
            Command's result.
        """
        assert bool(wallet) ^ bool(wallet_config), self.WALLET_SOURCE_ERROR_MSG
        exec_param = {
            param: param_value
            for param, param_value in locals().items()
            if param not in ["self", "wallet_password", "await_", "from_address", "to_address", "signer"]
        }
        exec_param["timeout"] = f"{timeout}s"
        exec_param["await"] = await_
        exec_param["from"] = from_address
        exec_param["to"] = to_address
        exec_param["post_data"] = f"-- {signer}:CalledByEntry" if signer else ""

        return self._execute(
            "wallet nep11 transfer",
            **exec_param,
        )

    def properties(
        self,
        token: str,
        id: str,
        rpc_endpoint: str,
        timeout: int = 10,
    ) -> CommandResult:
        """Print properties of NEP-11 token

        Args:
            token: Token contract address or hash in LE
            id: Hex-encoded token ID
            rpc_endpoint: RPC node address
            timeout: Timeout for the operation (default: 10s).

        Returns:
            Command's result.
        """
        exec_param = {param: param_value for param, param_value in locals().items() if param not in ["self"]}
        exec_param["timeout"] = f"{timeout}s"
        return self.parse_nep11_properties(
            self._execute(
                "wallet nep11 properties",
                **exec_param,
            ).stdout
        )

    def owner_of(
        self,
        token: str,
        id: str,
        rpc_endpoint: str,
        timeout: int = 10,
    ) -> CommandResult:
        """Print owner of non-divisible NEP-11 token with the specified ID

        Args:
            token: Token contract address or hash in LE
            id: Hex-encoded token ID
            rpc_endpoint: RPC node address
            timeout: Timeout for the operation (default: 10s).

        Returns:
            Command's result.
        """
        exec_param = {param: param_value for param, param_value in locals().items() if param not in ["self"]}
        exec_param["timeout"] = f"{timeout}s"
        return self._execute(
            "wallet nep11 ownerOf",
            **exec_param,
        ).stdout.strip()

    def tokens_of(
        self,
        token: str,
        address: str,
        rpc_endpoint: str,
        timeout: int = 10,
    ) -> CommandResult:
        """Print list of tokens IDs for the specified NFT owner (100 will be printed at max)

        Args:
            token: Token contract address or hash in LE
            address: NFT owner address or hash in LE
            rpc_endpoint: RPC node address
            timeout: Timeout for the operation (default: 10s).

        Returns:
            Command's result.
        """
        exec_param = {param: param_value for param, param_value in locals().items() if param not in ["self"]}
        exec_param["timeout"] = f"{timeout}s"
        return self._execute(
            "wallet nep11 tokensOf",
            **exec_param,
        ).stdout.strip()

    def tokens(
        self,
        token: str,
        rpc_endpoint: str,
        timeout: int = 10,
    ) -> CommandResult:
        """Print list of tokens IDs for the specified NFT owner (100 will be printed at max)

        Args:
            token: Token contract address or hash in LE
            rpc_endpoint: RPC node address
            timeout: Timeout for the operation (default: 10s).

        Returns:
            Command's result.
        """
        exec_param = {param: param_value for param, param_value in locals().items() if param not in ["self"]}
        exec_param["timeout"] = f"{timeout}s"
        return self._execute(
            "wallet nep11 tokens",
            **exec_param,
        ).stdout.strip()
