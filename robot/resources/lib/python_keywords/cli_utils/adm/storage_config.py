from cli_utils.cli_command import NeofsCliCommand


class NeofsAdmStorageConfig(NeofsCliCommand):
    def set(self, account: str, wallet: str) -> str:
        """Initialize basic neofs-adm configuration file.

        Args:
            account (str):  wallet account
            wallet (str):   path to wallet


        Returns:
            str: Command string

        """
        return self._execute(
            "storage-config",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )
