from cli_utils.cli_command import NeofsCliCommand

from .completion_type import CompletionType


class NeofsAdmCompletion(NeofsCliCommand):
    def get(self, completion_type: CompletionType = CompletionType.FISH) -> str:
        """To load completions:
            Bash:
              $ source <(neofs-adm completion bash)

            Zsh:
              If shell completion is not already enabled in your environment you will need
              to enable it.  You can execute the following once:
              $ echo "autoload -U compinit; compinit" >> ~/.zshrc

              You will need to start a new shell for this setup to take effect.

            Fish:
              $ neofs-adm completion fish | source

        Args:
            completion_type (CompletionType):  Select completion type (default: Fish)


        Returns:
            str: Command string

        """
        return self._execute("completion " + completion_type.value)
