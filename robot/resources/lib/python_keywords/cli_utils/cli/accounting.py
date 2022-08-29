from typing import Optional

from cli_utils.cli_command import NeofsCliCommand


class NeofsCliAccounting(NeofsCliCommand):
    def balance(self, wallet: str, rpc_endpoint: str, address: Optional[str] = None,
                owner: Optional[str] = None) -> str:
        """Get internal balance of NeoFS account

        Args:
            address:            address of wallet account
            owner:              owner of balance account (omit to use owner from private key)
            rpc_endpoint:       remote node address (as 'multiaddr' or '<host>:<port>')
            wallet:             WIF (NEP-2) string or path to the wallet or binary key


        Returns:
            str: Command string

        """
        return self._execute(
            'accounting balance',
            **{param: param_value for param, param_value in locals().items() if param not in ['self']}
        )
