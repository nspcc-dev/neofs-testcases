from typing import Optional

from cli_utils.cli_command import NeofsCliCommand


class NeofsAdmMorph(NeofsCliCommand):
    def deposit_notary(
        self,
        rpc_endpoint: str,
        account: str,
        gas: str,
        storage_wallet: Optional[str] = None,
        till: Optional[str] = None,
    ) -> str:
        """Deposit GAS for notary service.

        Args:
            account (str):         wallet account address
            gas (str):             amount of GAS to deposit
            rpc_endpoint (str):    N3 RPC node endpoint
            storage_wallet (str):  path to storage node wallet
            till (str):            notary deposit duration in blocks


        Returns:
            str: Command string

        """
        return self._execute(
            "morph deposit-notary",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            },
        )

    def dump_balances(
        self,
        rpc_endpoint: str,
        alphabet: Optional[str] = None,
        proxy: Optional[str] = None,
        script_hash: Optional[str] = None,
        storage: Optional[str] = None,
    ) -> str:
        """Dump GAS balances

        Args:
            alphabet (str):      dump balances of alphabet contracts
            proxy (str):         dump balances of the proxy contract
            rpc_endpoint (str):  N3 RPC node endpoint
            script_hash (str):   use script-hash format for addresses
            storage (str):       dump balances of storage nodes from the current netmap


        Returns:
            str: Command string

        """
        return self._execute(
            "morph dump-balances",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            },
        )

    def dump_config(self, rpc_endpoint: str) -> str:
        """Section for morph network configuration commands.

        Args:
            rpc_endpoint (str):  N3 RPC node endpoint


        Returns:
            str: Command string

        """
        return self._execute(
            "morph dump-config",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            },
        )

    def dump_containers(
        self,
        rpc_endpoint: str,
        cid: Optional[str] = None,
        container_contract: Optional[str] = None,
        dump: str = './testlib_dump_container',
    ) -> str:
        """Dump NeoFS containers to file.

        Args:
            cid (str):                 containers to dump
            container_contract (str):  container contract hash (for networks without NNS)
            dump (str):                file where to save dumped containers
                                       (default: ./testlib_dump_container)
            rpc_endpoint (str):        N3 RPC node endpoint


        Returns:
            str: Command string

        """
        return self._execute(
            "morph dump-containers",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            },
        )

    def dump_hashes(self, rpc_endpoint: str) -> str:
        """Dump deployed contract hashes.

        Args:
            rpc_endpoint (str):        N3 RPC node endpoint


        Returns:
            str: Command string

        """
        return self._execute(
            "morph dump-hashes",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            },
        )

    def force_new_epoch(
        self, rpc_endpoint: Optional[str] = None, alphabet: Optional[str] = None
    ) -> str:
        """Create new NeoFS epoch event in the side chain

        Args:
            alphabet (str):      path to alphabet wallets dir
            rpc_endpoint (str):  N3 RPC node endpoint


        Returns:
            str: Command string

        """
        return self._execute(
            "morph force-new-epoch",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            },
        )

    def generate_alphabet(self, rpc_endpoint: str, alphabet_wallets: str, size: int = 7) -> str:
        """Generate alphabet wallets for consensus nodes of the morph network

        Args:
            alphabet_wallets (str):  path to alphabet wallets dir
            size (int):              amount of alphabet wallets to generate (default 7)
            rpc_endpoint (str):      N3 RPC node endpoint


        Returns:
            str: Command string

        """
        return self._execute(
            "morph generate-alphabet",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            },
        )

    def generate_storage_wallet(
        self,
        rpc_endpoint: str,
        alphabet_wallets: str,
        storage_wallet: str,
        initial_gas: Optional[str] = None,
    ) -> str:
        """Generate storage node wallet for the morph network

        Args:
            alphabet_wallets (str):  path to alphabet wallets dir
            initial_gas (str):       initial amount of GAS to transfer
            rpc_endpoint (str):      N3 RPC node endpoint
            storage_wallet (str):    path to new storage node wallet


        Returns:
            str: Command string

        """
        return self._execute(
            "morph generate-storage-wallet",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            },
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
    ) -> str:
        """Section for morph network configuration commands.

        Args:
            alphabet_wallets (str):        path to alphabet wallets dir
            container_alias_fee (int):     container alias fee (default 500)
            container_fee (int):           container registration fee (default 1000)
            contracts (str):               path to archive with compiled NeoFS contracts
                                           (default fetched from latest github release)
            epoch_duration (int):          amount of side chain blocks in one NeoFS epoch
                                           (default 240)
            homomorphic_disabled: (bool):  disable object homomorphic hashing
            local_dump (str):              path to the blocks dump file
            max_object_size (int):         max single object size in bytes (default 67108864)
            protocol (str):                path to the consensus node configuration
            rpc_endpoint (str):            N3 RPC node endpoint


        Returns:
            str: Command string

        """
        return self._execute(
            "morph init",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            },
        )

    def refill_gas(
        self,
        rpc_endpoint: str,
        alphabet_wallets: str,
        storage_wallet: str,
        gas: Optional[str] = None,
    ) -> str:
        """Refill GAS of storage node's wallet in the morph network

        Args:
            alphabet_wallets (str):  path to alphabet wallets dir
            gas (str):               additional amount of GAS to transfer
            rpc_endpoint (str):      N3 RPC node endpoint
            storage_wallet (str):    path to new storage node wallet


        Returns:
            str: Command string

        """
        return self._execute(
            "morph refill-gas",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            },
        )

    def restore_containers(
        self, rpc_endpoint: str, alphabet_wallets: str, cid: str, dump: str
    ) -> str:
        """Restore NeoFS containers from file.

        Args:
            alphabet_wallets (str):  path to alphabet wallets dir
            cid (str):               containers to restore
            dump (str):              file to restore containers from
            rpc_endpoint (str):      N3 RPC node endpoint


        Returns:
            str: Command string

        """
        return self._execute(
            "morph restore-containers",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            },
        )

    def set_policy(
        self,
        rpc_endpoint: str,
        alphabet_wallets: str,
        exec_fee_factor: Optional[int] = None,
        storage_price: Optional[int] = None,
        fee_per_byte: Optional[int] = None,
    ) -> str:
        """Set global policy values

        Args:
            alphabet_wallets (str):  path to alphabet wallets dir
            exec_fee_factor (int):   ExecFeeFactor=<n1>
            storage_price (int):     StoragePrice=<n2>
            fee_per_byte (int):      FeePerByte=<n3>
            rpc_endpoint (str):      N3 RPC node endpoint


        Returns:
            str: Command string

        """
        non_param_attribute = ""
        if exec_fee_factor:
            non_param_attribute += f"ExecFeeFactor={exec_fee_factor} "
        if storage_price:
            non_param_attribute += f"StoragePrice={storage_price} "
        if fee_per_byte:
            non_param_attribute += f"FeePerByte={fee_per_byte} "
        return self._execute(
            f"morph restore-containers {non_param_attribute}",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self", "exec_fee_factor", "storage_price", "fee_per_byte"]
            },
        )

    def update_contracts(
        self, rpc_endpoint: str, alphabet_wallets: str, contracts: Optional[str] = None
    ) -> str:
        """Update NeoFS contracts.

        Args:
            alphabet_wallets (str):  path to alphabet wallets dir
            contracts (str):         path to archive with compiled NeoFS contracts
                                     (default fetched from latest github release)
            rpc_endpoint (str):      N3 RPC node endpoint


        Returns:
            str: Command string

        """
        return self._execute(
            "morph update-contracts",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            },
        )
