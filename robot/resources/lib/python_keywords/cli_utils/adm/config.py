from cli_utils.cli_command import NeofsCliCommand


class NeofsAdmConfig(NeofsCliCommand):
    def init(self, path: str = "~/.neofs/adm/config.yml") -> str:
        """Initialize basic neofs-adm configuration file.

        Args:
            path (str):  path to config (default ~/.neofs/adm/config.yml)


        Returns:
            str: Command string

        """
        return self._execute(
            "config init",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )
