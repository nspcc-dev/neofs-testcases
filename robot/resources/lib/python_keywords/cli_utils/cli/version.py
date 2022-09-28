from cli_utils.cli_command import NeofsCliCommand


class NeofsCliVersion(NeofsCliCommand):
    def get(self) -> str:
        """
        Application version and NeoFS API compatibility.

        Returns:
            str: Command string
        """
        return self._execute("", version=True)
