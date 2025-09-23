from typing import Optional

from neofs_testlib.cli.cli_command import CliCommand
from neofs_testlib.shell import CommandResult


class NeofsAdmFSChain(CliCommand):
    def deposit_notary(
        self,
        rpc_endpoint: str,
        account: str,
        gas: str,
        storage_wallet: Optional[str] = None,
        till: Optional[str] = None,
    ) -> CommandResult:
        """Deposit GAS for notary service.

        Args:
            account: Wallet account address.
            gas: Amount of GAS to deposit.
            rpc_endpoint: N3 RPC node endpoint.
            storage_wallet: Path to storage node wallet.
            till: Notary deposit duration in blocks.

        Returns:
            Command's result.
        """
        return self._execute(
            "fschain deposit-notary",
            **{param: param_value for param, param_value in locals().items() if param not in ["self"]},
        )

    def dump_balances(
        self,
        rpc_endpoint: str,
        alphabet: Optional[str] = None,
        proxy: Optional[str] = None,
        script_hash: Optional[str] = None,
        storage: Optional[str] = None,
    ) -> CommandResult:
        """Dump GAS balances.

        Args:
            alphabet: Dump balances of alphabet contracts.
            proxy: Dump balances of the proxy contract.
            rpc_endpoint: N3 RPC node endpoint.
            script_hash: Use script-hash format for addresses.
            storage: Dump balances of storage nodes from the current netmap.

        Returns:
            Command's result.
        """
        return self._execute(
            "fschain dump-balances",
            **{param: param_value for param, param_value in locals().items() if param not in ["self"]},
        )

    def dump_config(self, rpc_endpoint: str) -> CommandResult:
        """Section for FS chain network configuration commands.

        Args:
            rpc_endpoint: N3 RPC node endpoint

        Returns:
            Command's result.
        """
        return self._execute(
            "fschain dump-config",
            **{param: param_value for param, param_value in locals().items() if param not in ["self"]},
        )

    def dump_containers(
        self,
        rpc_endpoint: str,
        cid: Optional[str] = None,
        container_contract: Optional[str] = None,
        dump: str = "./testlib_dump_container",
    ) -> CommandResult:
        """Dump NeoFS containers to file.

        Args:
            cid: Containers to dump.
            container_contract: Container contract hash (for networks without NNS).
            dump: File where to save dumped containers (default: ./testlib_dump_container).
            rpc_endpoint: N3 RPC node endpoint.

        Returns:
            Command's result.
        """
        return self._execute(
            "fschain dump-containers",
            **{param: param_value for param, param_value in locals().items() if param not in ["self"]},
        )

    def dump_hashes(self, rpc_endpoint: str, domain: str) -> CommandResult:
        """Dump deployed contract hashes.

        Args:
            rpc_endpoint: N3 RPC node endpoint.
            domain: Custom zone for NNS.

        Returns:
            Command's result.
        """
        return self._execute(
            "fschain dump-hashes",
            **{param: param_value for param, param_value in locals().items() if param not in ["self"]},
        )

    def dump_names(self, rpc_endpoint: str, domain: Optional[str] = None) -> CommandResult:
        """Dump known registred NNS names and expirations

        Args:
            rpc_endpoint: N3 RPC node endpoint.
            domain: Filter by domain

        Returns:
            Command's result.
        """
        return self._execute(
            "fschain dump-names",
            **{param: param_value for param, param_value in locals().items() if param not in ["self"]},
        )

    def force_new_epoch(
        self, rpc_endpoint: Optional[str] = None, alphabet_wallets: Optional[str] = None
    ) -> CommandResult:
        """Create new NeoFS epoch event in the side chain.

        Args:
            alphabet: Path to alphabet wallets dir.
            rpc_endpoint: N3 RPC node endpoint.

        Returns:
            Command's result.
        """
        return self._execute(
            "fschain force-new-epoch",
            **{param: param_value for param, param_value in locals().items() if param not in ["self"]},
        )

    def generate_alphabet(
        self,
        alphabet_wallets: str,
        size: int = 7,
    ) -> CommandResult:
        """Generate alphabet wallets for consensus nodes of the FS chain network.

        Args:
            alphabet_wallets: Path to alphabet wallets dir.
            size: Amount of alphabet wallets to generate (default 7).

        Returns:
            Command's result.
        """
        return self._execute(
            "fschain generate-alphabet",
            **{param: param_value for param, param_value in locals().items() if param not in ["self"]},
        )

    def generate_storage_wallet(
        self,
        alphabet_wallets: str,
        storage_wallet: str,
        label: str,
        initial_gas: Optional[str] = None,
    ) -> CommandResult:
        """Generate storage node wallet for the FS chain network.

        Args:
            alphabet_wallets: Path to alphabet wallets dir.
            initial_gas: Initial amount of GAS to transfer.
            storage_wallet: Path to new storage node wallet.
            label: Wallet label.

        Returns:
            Command's result.
        """
        return self._execute(
            "fschain generate-storage-wallet",
            **{param: param_value for param, param_value in locals().items() if param not in ["self"]},
        )

    def init(
        self,
        rpc_endpoint: str,
        alphabet_wallets: str,
        contracts: str,
        protocol: str,
        container_alias_fee: int = 500,
        container_fee: int = 1000,
        epoch_duration: int = 240,
        homomorphic_disabled: bool = False,
        local_dump: Optional[str] = None,
        max_object_size: int = 67108864,
    ) -> CommandResult:
        """Section for FS chain network configuration commands.

        Args:
            alphabet_wallets: Path to alphabet wallets dir.
            container_alias_fee: Container alias fee (default 500).
            container_fee: Container registration fee (default 1000).
            contracts: Path to archive with compiled NeoFS contracts
                (default fetched from latest github release).
            epoch_duration: Amount of side chain blocks in one NeoFS epoch (default 240).
            homomorphic_disabled: Disable object homomorphic hashing.
            local_dump: Path to the blocks dump file.
            max_object_size: Max single object size in bytes (default 67108864).
            protocol: Path to the consensus node configuration.
            rpc_endpoint: N3 RPC node endpoint.

        Returns:
            Command's result.
        """
        return self._execute(
            "fschain init",
            **{param: param_value for param, param_value in locals().items() if param not in ["self"]},
        )

    def refill_gas(
        self,
        rpc_endpoint: str,
        alphabet_wallets: str,
        storage_wallet: str,
        gas: Optional[str] = None,
        wallet_address: Optional[str] = None,
    ) -> CommandResult:
        """Refill GAS of storage node's wallet in the FS chain network

        Args:
            alphabet_wallets: Path to alphabet wallets dir.
            gas: Additional amount of GAS to transfer.
            rpc_endpoint: N3 RPC node endpoint.
            storage_wallet: Path to new storage node wallet.

        Returns:
            Command's result.
        """
        return self._execute(
            "fschain refill-gas",
            **{param: param_value for param, param_value in locals().items() if param not in ["self"]},
        )

    def restore_containers(
        self,
        rpc_endpoint: str,
        alphabet_wallets: str,
        cid: str,
        dump: str,
    ) -> CommandResult:
        """Restore NeoFS containers from file.

        Args:
            alphabet_wallets: Path to alphabet wallets dir.
            cid: Containers to restore.
            dump: File to restore containers from.
            rpc_endpoint: N3 RPC node endpoint.

        Returns:
            Command's result.
        """
        return self._execute(
            "fschain restore-containers",
            **{param: param_value for param, param_value in locals().items() if param not in ["self"]},
        )

    def set_policy(
        self,
        rpc_endpoint: str,
        alphabet_wallets: str,
        exec_fee_factor: Optional[int] = None,
        storage_price: Optional[int] = None,
        fee_per_byte: Optional[int] = None,
    ) -> CommandResult:
        """Set global policy values.

        Args:
            alphabet_wallets: Path to alphabet wallets dir.
            exec_fee_factor: ExecFeeFactor=<n1>.
            storage_price: StoragePrice=<n2>.
            fee_per_byte: FeePerByte=<n3>.
            rpc_endpoint: N3 RPC node endpoint.

        Returns:
            Command's result.
        """
        non_param_attribute = ""
        if exec_fee_factor:
            non_param_attribute += f"ExecFeeFactor={exec_fee_factor} "
        if storage_price:
            non_param_attribute += f"StoragePrice={storage_price} "
        if fee_per_byte:
            non_param_attribute += f"FeePerByte={fee_per_byte} "
        return self._execute(
            f"fschain restore-containers {non_param_attribute}",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self", "exec_fee_factor", "storage_price", "fee_per_byte"]
            },
        )

    def update_contracts(
        self,
        rpc_endpoint: str,
        alphabet_wallets: str,
        contracts: Optional[str] = None,
    ) -> CommandResult:
        """Update NeoFS contracts.

        Args:
            alphabet_wallets: Path to alphabet wallets dir.
            contracts: Path to archive with compiled NeoFS contracts
                (default fetched from latest github release).
            rpc_endpoint: N3 RPC node endpoint.

        Returns:
            Command's result.
        """
        return self._execute(
            "fschain update-contracts",
            **{param: param_value for param, param_value in locals().items() if param not in ["self"]},
        )

    def set_config(self, rpc_endpoint: str, alphabet_wallets: str, post_data: str) -> CommandResult:
        """Set NeoFS config settings.

        Args:
            rpc_endpoint: N3 RPC node endpoint.
            alphabet_wallets: Path to alphabet wallets dir.
            post_data: Config key=value setting, e.g. HomomorphicHashingDisabled=true

        Returns:
            Command's result.
        """
        return self._execute(
            "fschain set-config",
            **{param: param_value for param, param_value in locals().items() if param not in ["self"]},
        )

    def deploy(
        self,
        rpc_endpoint: str,
        alphabet_wallets: str,
        domain: str,
        contract: str,
        post_data: str,
        update=False,
    ) -> CommandResult:
        """Deploy additional smart-contracts

        Args:
            rpc_endpoint: N3 RPC node endpoint.
            alphabet_wallets: Path to alphabet wallets dir.
            domain: Custom zone for NNS.
            contract: Path to the contract directory.
            post_data: Arguments passed to a deploying smart contract.
            update: Update an existing contract.

        Returns:
            Command's result.
        """
        return self._execute(
            "fschain deploy",
            **{param: param_value for param, param_value in locals().items() if param not in ["self"]},
        )

    def mint_balance(
        self,
        alphabet_wallets: str,
        amount: str,
        deposit_tx: str,
        rpc_endpoint: str,
        wallet_address: str,
    ) -> CommandResult:
        """Mint new NEOFS tokens in the FS chain network

        Args:
            alphabet_wallets: Path to alphabet wallets dir
            amount: Amount of NEOFS token to issue (fixed12, GAS * 10000)
            deposit_tx: Deposit transaction hash
            rpc_endpoint: N3 RPC node endpoint
            wallet_address: Address of recipient

        Returns:
            Command's result.
        """
        return self._execute(
            "fschain mint-balance",
            **{param: param_value for param, param_value in locals().items() if param not in ["self"]},
        )

    def netmap_candidates(
        self,
        rpc_endpoint: str,
    ) -> CommandResult:
        """List netmap candidates nodes

        Args:
            rpc_endpoint: N3 RPC node endpoint

        Returns:
            Command's result.
        """
        return self._execute(
            "fschain netmap-candidates",
            **{param: param_value for param, param_value in locals().items() if param not in ["self"]},
        )

    def estimations(self, rpc_endpoint: str, cid: str, epoch: str) -> CommandResult:
        """Set NeoFS config settings.

        Args:
            rpc_endpoint: N3 RPC node endpoint.
            cid: Inspected container, base58 encoded.
            epoch: Epoch for estimations, 0 for current, negative for relative epochs

        Returns:
            Command's result.
        """
        return self._execute(
            "fschain estimations",
            **{param: param_value for param, param_value in locals().items() if param not in ["self"]},
        )

    def verified_nodes_domain_access_list(self, rpc_endpoint: str, domain: str) -> CommandResult:
        """List Neo addresses of the storage nodes that have access to use the specified verified domain.

        Args:
            rpc-endpoint: FS chain RPC endpoint
            domain: Verified domain of the storage nodes. Must be a valid NeoFS NNS domain (e.g. 'nodes.some-org.neofs')

        Returns:
            Command's result.
        """
        return self._execute(
            "fschain verified-nodes-domain access-list",
            **{param: param_value for param, param_value in locals().items() if param not in ["self"]},
        )

    def verified_nodes_domain_set_access_list(
        self,
        rpc_endpoint: str,
        domain: str,
        account: Optional[str] = None,
        neo_addresses: Optional[list[str]] = None,
        public_keys: Optional[list[str]] = None,
        wallet: Optional[str] = None,
        wallet_password: Optional[str] = None,
    ) -> CommandResult:
        """List Neo addresses of the storage nodes that have access to use the specified verified domain.

        Args:
            rpc-endpoint: FS chain RPC endpoint
            domain: Verified domain of the storage nodes. Must be a valid NeoFS NNS domain (e.g. 'nodes.some-org.neofs')
            account: Optional Neo address of the wallet account for signing transactions. If omitted, default change address from the wallet is used
            neo-addresses: Neo addresses resolved from public keys of the storage nodes
            public-keys: HEX-encoded public keys of the storage nodes
            wallet: Path to the Neo wallet file
            wallet_password: Password from the Neo wallet file
        Returns:
            Command's result.
        """
        return self._execute_with_password(
            "fschain verified-nodes-domain set-access-list",
            wallet_password,
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self", "wallet_password"]
            },
        )

    def container_quota(
        self, rpc_endpoint: str, cid: str, wallet: str, wallet_password: str, post_data: str = None, soft: bool = False
    ) -> CommandResult:
        """Manage container space quota values, if <value> is missing, prints already set values.

        Args:
            rpc-endpoint: N3 RPC node endpoint
            cid: Inspected container, base58 encoded
            wallet: Wallet that signs transaction (must have user's key)
            wallet_password: Wallet password
            post_data: Quota value
            soft: Set soft quota limit (omit if hard limit is required)

        Returns:
            Command's result.
        """
        if post_data:
            return self._execute_with_password(
                "fschain quota container",
                wallet_password,
                **{
                    param: param_value
                    for param, param_value in locals().items()
                    if param not in ["self", "wallet_password"]
                },
            )
        else:
            return self._execute(
                "fschain quota container",
                **{
                    param: param_value
                    for param, param_value in locals().items()
                    if param not in ["self", "wallet_password"]
                },
            )

    def user_quota(
        self, rpc_endpoint: str, account: str, wallet: str, wallet_password: str, post_data: str, soft: bool = False
    ) -> CommandResult:
        """Manage user space quota values, if <value> is missing, prints already set values

        Args:
            rpc-endpoint: N3 RPC node endpoint
            account: Inspected user account, base58 encoded
            wallet: Wallet that signs transaction (must have user's key)
            wallet_password: Wallet password
            post_data: Quota value
            soft: Set soft quota limit (omit if hard limit is required)

        Returns:
            Command's result.
        """
        if post_data:
            return self._execute_with_password(
                "fschain quota user",
                wallet_password,
                **{
                    param: param_value
                    for param, param_value in locals().items()
                    if param not in ["self", "wallet_password"]
                },
            )
        else:
            return self._execute(
                "fschain quota user",
                **{
                    param: param_value
                    for param, param_value in locals().items()
                    if param not in ["self", "wallet_password"]
                },
            )
