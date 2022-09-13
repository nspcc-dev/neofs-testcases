from cli_utils.cli_command import NeofsCliCommand


class NeoGoVersion(NeofsCliCommand):
    def get(self) -> str:
        """Application version

        Returns:
            str: Command string

        """
        return self._execute("", version=True)
