from typing import Optional

from cli_utils.cli_command import NeofsCliCommand


class NeofsAdmGenDoc(NeofsCliCommand):
    def get(
        self, doc_file: str, depth: int = 1, doc_type: str = "md", extension: Optional[str] = None
    ) -> str:
        """Generate documentation for this command. If the template is not provided,
            builtin cobra generator is used and each subcommand is placed in
            a separate file in the same directory.

            The last optional argument specifies the template to use with text/template.
            In this case there is a number of helper functions which can be used:
              replace STR FROM TO -- same as strings.ReplaceAll
              join ARRAY SEPARATOR -- same as strings.Join
              split STR SEPARATOR -- same as strings.Split
              fullUse CMD -- slice of all command names starting from the parent
              listFlags CMD -- list of command flags

        Args:
            depth (int):      if template is specified, unify all commands starting from depth in a single file.
                              Default = 1.
            doc_file (str):   file where to save generated documentation
            extension (str):  if the template is specified, string to append to the output file names
            doc_type (str):   type for the documentation ('md' or 'man') (default "md")

        Returns:
            str: Command string

        """
        return self._execute(
            f"gendoc {doc_file}",
            **{
                param: value
                for param, value in locals().items()
                if param not in ["self", "doc_file"]
            },
        )
