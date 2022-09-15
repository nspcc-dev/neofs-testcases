from typing import Optional

from cli_utils.cli_command import NeofsCliCommand


class NeoGoContract(NeofsCliCommand):
    def compile(
        self,
        input_file: str,
        out: str,
        manifest: str,
        config: str,
        no_standards: bool = False,
        no_events: bool = False,
        no_permissions: bool = False,
        bindings: Optional[str] = None,
    ) -> str:
        """compile a smart contract to a .nef file

        Args:
            input_file (str):      Input file for the smart contract to be compiled
            out (str):             Output of the compiled contract
            manifest (str):        Emit contract manifest (*.manifest.json) file into separate
                                   file using configuration input file (*.yml)
            config (str):          Configuration input file (*.yml)
            no_standards (bool):   do not check compliance with supported standards
            no_events (bool):      do not check emitted events with the manifest
            no_permissions (bool): do not check if invoked contracts are allowed in manifest
            bindings (str):        output file for smart-contract bindings configuration

        Returns:
            str: Command string

        """
        return self._execute(
            "contract compile",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            },
        )

    def deploy(
        self,
        address: str,
        input_file: str,
        sysgas: float,
        manifest: str,
        rpc_endpoint: str,
        wallet: Optional[str] = None,
        wallet_config: Optional[str] = None,
        gas: Optional[float] = None,
        out: Optional[str] = None,
        force: bool = False,
        timeout: int = 10,

    ) -> str:
        """deploy a smart contract (.nef with description)

        Args:
            wallet (str):        wallet to use to get the key for transaction signing;
                                 conflicts with wallet_config
            wallet_config (str): path to wallet config to use to get the key for transaction
                                 signing; conflicts with wallet
            address (str):       address to use as transaction signee (and gas source)
            gas (float):         network fee to add to the transaction (prioritizing it)
            sysgas (float):      system fee to add to transaction (compensating for execution)
            out (str):           file to put JSON transaction to
            force (bool):        Do not ask for a confirmation
            rpc_endpoint (str):  RPC node address
            timeout (int):       Timeout for the operation (default: 10s)
            input_file (str):    Input file for the smart contract (*.nef)
            manifest (str):      Emit contract manifest (*.manifest.json) file into separate
                                 file using configuration input file (*.yml)

        Returns:
            str: Command string

        """
        assert bool(wallet) ^ bool(wallet_config), self.WALLET_SOURCE_ERROR_MSG

        return self._execute(
            "contract deploy",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            },
        )

    def generate_wrapper(
        self,
        out: str,
        hash: str,
        config: Optional[str] = None,
        manifest: Optional[str] = None,
    ) -> str:
        """generate wrapper to use in other contracts

        Args:
            config (str):    Configuration file to use
            manifest (str):  Read contract manifest (*.manifest.json) file
            out (str):       Output of the compiled contract
            hash (str):      Smart-contract hash

        Returns:
            str: Command string

        """
        return self._execute(
            "contract generate-wrapper",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            },
        )

    def invokefunction(
        self,
        address: str,
        scripthash: str,
        wallet: Optional[str] = None,
        method: Optional[str] = None,
        arguments: Optional[str] = None,
        multisig_hash: Optional[str] = None,
        wallet_config: Optional[str] = None,
        gas: Optional[float] = None,
        sysgas: Optional[float] = None,
        out: Optional[str] = None,
        force: bool = False,
        rpc_endpoint: Optional[str] = None,
        timeout: int = 10,
    ) -> str:
        """Executes given (as a script hash) deployed script with the given method,
            arguments and signers. Sender is included in the list of signers by default
            with None witness scope. If you'd like to change default sender's scope,
            specify it via signers parameter. See testinvokefunction documentation for
            the details about parameters. It differs from testinvokefunction in that this
            command sends an invocation transaction to the network.

        Args:
            scripthash (str):    Function hash
            method (str):        Call method
            arguments (str):     Method arguments
            multisig_hash (str): Multisig hash
            wallet (str):        wallet to use to get the key for transaction signing;
                                 conflicts with wallet_config
            wallet_config (str): path to wallet config to use to get the key for transaction
                                 signing; conflicts with wallet
            address (str):       address to use as transaction signee (and gas source)
            gas (float):         network fee to add to the transaction (prioritizing it)
            sysgas (float):      system fee to add to transaction (compensating for execution)
            out (str):           file to put JSON transaction to
            force (bool):        force-push the transaction in case of bad VM state after
                                 test script invocation
            rpc_endpoint (str):  RPC node address
            timeout (int):       Timeout for the operation (default: 10s)

        Returns:
            str: Command string

        """
        multisig_hash = f"-- {multisig_hash}" or ""
        return self._execute(
            "contract invokefunction "
            f"{scripthash} {method or ''} {arguments or ''} {multisig_hash}",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self", "scripthash", "method", "arguments", "multisig_hash"]
            },
        )

    def testinvokefunction(
        self,
        scripthash: str,
        wallet: Optional[str] = None,
        method: Optional[str] = None,
        arguments: Optional[str] = None,
        multisig_hash: Optional[str] = None,
        rpc_endpoint: Optional[str] = None,
        timeout: int = 10,
    ) -> str:
        """Executes given (as a script hash) deployed script with the given method,
           arguments and signers (sender is not included by default). If no method is given
           "" is passed to the script, if no arguments are given, an empty array is
           passed, if no signers are given no array is passed. If signers are specified,
           the first one of them is treated as a sender. All of the given arguments are
           encapsulated into array before invoking the script. The script thus should
           follow the regular convention of smart contract arguments (method string and
           an array of other arguments).

           See more information and samples in `neo-go contract testinvokefunction --help`

        Args:
            scripthash (str):    Function hash
            method (str):        Call method
            arguments (str):     Method arguments
            multisig_hash (str): Multisig hash
            rpc_endpoint (str):  RPC node address
            timeout (int):       Timeout for the operation (default: 10s)

        Returns:
            str: Command string

        """
        multisig_hash = f"-- {multisig_hash}" or ""
        return self._execute(
            "contract testinvokefunction "
            f"{scripthash} {method or ''} {arguments or ''} {multisig_hash}",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self", "scripthash", "method", "arguments", "multisig_hash"]
            },
        )

    def testinvokescript(
        self,
        input_file: str,
        rpc_endpoint: Optional[str] = None,
        timeout: int = 10,
    ) -> str:
        """Executes given compiled AVM instructions in NEF format with the given set of
           signers not included sender by default. See testinvokefunction documentation
           for the details about parameters.


        Args:
            input_file (str):    Input location of the .nef file that needs to be invoked
                                 conflicts with wallet_config
            rpc_endpoint (str):  RPC node address
            timeout (int):       Timeout for the operation (default: 10s)

        Returns:
            str: Command string

        """
        return self._execute(
            f"contract testinvokescript",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            },
        )

    def init(
        self,
        name: str,
        skip_details: bool = False,
    ) -> str:
        """initialize a new smart-contract in a directory with boiler plate code

        Args:
            name (str):          name of the smart-contract to be initialized
            skip_details (bool): skip filling in the projects and contract details

        Returns:
            str: Command string

        """
        return self._execute(
            "contract init",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            },
        )

    def inspect(
        self,
        input_file: Optional[str] = None,
        compile: Optional[str] = None,
    ) -> str:
        """creates a user readable dump of the program instructions

        Args:
            input_file (str): input file of the program (either .go or .nef)
            compile (str):    compile input file (it should be go code then)

        Returns:
            str: Command string

        """
        return self._execute(
            "contract inspect",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            },
        )

    def calc_hash(
        self,
        input_file: str,
        manifest: str,
        sender: Optional[str] = None,
    ) -> str:
        """calculates hash of a contract after deployment

        Args:
            input_file (str): path to NEF file
            sender (str):     sender script hash or address
            manifest (str):   path to manifest file

        Returns:
            str: Command string

        """
        return self._execute(
            "contract calc-hash",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            },
        )

    def add_group(
        self,
        manifest: str,
        address: str,
        wallet: Optional[str] = None,
        wallet_config: Optional[str] = None,
        sender: Optional[str] = None,
        nef: Optional[str] = None,
    ) -> str:
        """adds group to the manifest

        Args:
            wallet (str):        wallet to use to get the key for transaction signing;
                                 conflicts with wallet_config
            wallet_config (str): path to wallet config to use to get the key for transaction
                                 signing; conflicts with wallet
            sender (str):        deploy transaction sender
            address (str):       account to sign group with
            nef (str):           path to the NEF file
            manifest (str):      path to the manifest


        Returns:
            str: Command string

        """
        return self._execute(
            "contract manifest add-group",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            },
        )
