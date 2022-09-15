from typing import Optional

from cli_utils.cli_command import NeofsCliCommand


class NeofsAuthmateSecret(NeofsCliCommand):
    def obtain(
            self,
            wallet: str,
            peer: str,
            gate_wallet: str,
            access_key_id: str,
            address: Optional[str] = None,
            gate_address: Optional[str] = None,
    ) -> str:
        """Obtain a secret from NeoFS network

        Args:
            wallet (str):         path to the wallet
            address (str):        address of wallet account
            peer (str):           address of neofs peer to connect to
            gate_wallet (str):    path to the wallet
            gate_address (str):   address of wallet account
            access_key_id (str):  access key id for s3

        Returns:
            str: Command string

        """
        return self._execute(
            "obtain-secret",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )

    def issue(
        self,
        wallet: str,
        peer: str,
        bearer_rules: str,
        gate_public_key: str,
        address: Optional[str] = None,
        container_id: Optional[str] = None,
        container_friendly_name: Optional[str] = None,
        container_placement_policy: Optional[str] = None,
        session_tokens: Optional[str] = None,
        lifetime: Optional[str] = None,
        container_policy: Optional[str] = None,
        aws_cli_credentials: Optional[str] = None,
    ) -> str:
        """Obtain a secret from NeoFS network

        Args:
            wallet (str):                      path to the wallet
            address (str):                     address of wallet account
            peer (str):                        address of a neofs peer to connect to
            bearer_rules (str):                rules for bearer token as plain json string
            gate_public_key (str):             public 256r1 key of a gate (use flags repeatedly for
                                               multiple gates)
            container_id (str):                auth container id to put the secret into
            container_friendly_name (str):     friendly name of auth container to put the
                                               secret into
            container_placement_policy (str):  placement policy of auth container to put the
                                               secret into
                                               (default: "REP 2 IN X CBF 3 SELECT 2 FROM * AS X")
            session_tokens (str):              create session tokens with rules, if the rules are
                                               set as 'none', no session tokens will be created
            lifetime (str):                    Lifetime of tokens. For example 50h30m
                                               (note: max time unit is an hour so to set a day you
                                               should use 24h). It will be ceil rounded to the
                                               nearest amount of epoch. (default: 720h0m0s)
            container_policy (str):            mapping AWS storage class to NeoFS storage policy as
                                               plain json string or path to json file
            aws_cli_credentials (str):         path to the aws cli credential file


        Returns:
            str: Command string

        """
        return self._execute(
            "issue-secret",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )
