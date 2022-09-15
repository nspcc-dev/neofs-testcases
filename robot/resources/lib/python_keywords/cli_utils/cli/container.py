from typing import Optional

from cli_utils.cli_command import NeofsCliCommand


class NeofsCliContainer(NeofsCliCommand):
    def create(
        self,
        rpc_endpoint: str,
        wallet: str,
        policy: str,
        address: Optional[str] = None,
        attributes: Optional[dict] = None,
        basic_acl: Optional[str] = None,
        await_mode: bool = False,
        disable_timestamp: bool = False,
        name: Optional[str] = None,
        nonce: Optional[str] = None,
        session: Optional[str] = None,
        subnet: Optional[str] = None,
        ttl: Optional[int] = None,
        xhdr: Optional[dict] = None,
    ) -> str:
        """Create a new container and register it in the NeoFS.
        It will be stored in the sidechain when the Inner Ring accepts it.

        Args:
            address:            address of wallet account
            attributes:         comma separated pairs of container attributes in form of Key1=Value1,Key2=Value2
            await_mode:         block execution until container is persisted
            basic_acl:          hex encoded basic ACL value or keywords like 'public-read-write', 'private',
                                'eacl-public-read' (default "private")
            disable_timestamp:  disable timestamp container attribute
            name:               container name attribute
            nonce:              UUIDv4 nonce value for container
            policy:             QL-encoded or JSON-encoded placement policy or path to file with it
            rpc_endpoint:       remote node address (as 'multiaddr' or '<host>:<port>')
            session:            path to a JSON-encoded container session token
            subnet:             string representation of container subnetwork
            ttl:                TTL value in request meta header (default 2)
            wallet:             WIF (NEP-2) string or path to the wallet or binary key
            xhdr:               Request X-Headers in form of Key=Value

        Returns:
            str: Command string

        """
        return self._execute(
            "container create",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )

    def delete(
        self,
        rpc_endpoint: str,
        wallet: str,
        cid: str,
        address: Optional[str] = None,
        await_mode: bool = False,
        session: Optional[str] = None,
        ttl: Optional[int] = None,
        xhdr: Optional[dict] = None,
        force: bool = False,
    ) -> str:
        """Delete an existing container.
        Only the owner of the container has permission to remove the container.

        Args:
            address:           address of wallet account
            await_mode:        block execution until container is removed
            cid:               container ID
            force:             do not check whether container contains locks and remove immediately
            rpc_endpoint:      remote node address (as 'multiaddr' or '<host>:<port>')
            session:           path to a JSON-encoded container session token
            ttl:               TTL value in request meta header (default 2)
            wallet:            WIF (NEP-2) string or path to the wallet or binary key
            xhdr:              Request X-Headers in form of Key=Value


        Returns:
            str: Command string
        """

        return self._execute(
            "container delete",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )

    def get(
        self,
        rpc_endpoint: str,
        wallet: str,
        cid: str,
        address: Optional[str] = None,
        await_mode: bool = False,
        to: Optional[str] = None,
        json_mode: bool = False,
        ttl: Optional[int] = None,
        xhdr: Optional[dict] = None,
    ) -> str:
        """Get container field info

        Args:
            address:           address of wallet account
            await_mode:        block execution until container is removed
            cid:               container ID
            json_mode:         print or dump container in JSON format
            rpc_endpoint:      remote node address (as 'multiaddr' or '<host>:<port>')
            to:                path to dump encoded container
            ttl:               TTL value in request meta header (default 2)
            wallet:            WIF (NEP-2) string or path to the wallet or binary key
            xhdr:              Request X-Headers in form of Key=Value

        Returns:
            str: Command string

        """

        return self._execute(
            "container get",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )

    def get_eacl(
        self,
        rpc_endpoint: str,
        wallet: str,
        cid: str,
        address: Optional[str] = None,
        await_mode: bool = False,
        to: Optional[str] = None,
        session: Optional[str] = None,
        ttl: Optional[int] = None,
        xhdr: Optional[dict] = None,
    ) -> str:
        """Get extended ACL talbe of container

        Args:
            address:           address of wallet account
            await_mode:        block execution until container is removed
            cid:               container ID
            rpc_endpoint:      remote node address (as 'multiaddr' or '<host>:<port>')
            to:                path to dump encoded container
            session:           path to a JSON-encoded container session token
            ttl:               TTL value in request meta header (default 2)
            wallet:            WIF (NEP-2) string or path to the wallet or binary key
            xhdr:              Request X-Headers in form of Key=Value

        Returns:
            str: Command string

        """
        return self._execute(
            "container get-eacl",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )

    def list(
        self,
        rpc_endpoint: str,
        wallet: str,
        address: Optional[str] = None,
        owner: Optional[str] = None,
        ttl: Optional[int] = None,
        xhdr: Optional[dict] = None,
        **params
    ) -> str:
        """List all created containers

        Args:
            address:           address of wallet account
            owner:             owner of containers (omit to use owner from private key)
            rpc_endpoint:      remote node address (as 'multiaddr' or '<host>:<port>')
            ttl:               TTL value in request meta header (default 2)
            wallet:            WIF (NEP-2) string or path to the wallet or binary key
            xhdr:              Request X-Headers in form of Key=Value

        Returns:
            str: Command string

        """
        return self._execute(
            "container list",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )

    def list_objects(
        self,
        rpc_endpoint: str,
        wallet: str,
        cid: str,
        address: Optional[str] = None,
        ttl: Optional[int] = None,
        xhdr: Optional[dict] = None,
    ) -> str:
        """List existing objects in container

        Args:
            address:           address of wallet account
            cid:               container ID
            rpc_endpoint:      remote node address (as 'multiaddr' or '<host>:<port>')
            ttl:               TTL value in request meta header (default 2)
            wallet:            WIF (NEP-2) string or path to the wallet or binary key
            xhdr:              Request X-Headers in form of Key=Value

        Returns:
            str: Command string

        """

        return self._execute(
            "container list-objects",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )

    def set_eacl(
        self,
        rpc_endpoint: str,
        wallet: str,
        cid: str,
        table: str,
        address: Optional[str] = None,
        await_mode: bool = False,
        session: Optional[str] = None,
        ttl: Optional[int] = None,
        xhdr: Optional[dict] = None,
    ) -> str:
        """Set a new extended ACL table for the container.
        Container ID in the EACL table will be substituted with the ID from the CLI.

        Args:
            address:           address of wallet account
            await_mode:        block execution until container is removed
            cid:               container ID
            rpc_endpoint:      remote node address (as 'multiaddr' or '<host>:<port>')
            session:           path to a JSON-encoded container session token
            table:             path to file with JSON or binary encoded EACL table
            ttl:               TTL value in request meta header (default 2)
            wallet:            WIF (NEP-2) string or path to the wallet or binary key
            xhdr:              Request X-Headers in form of Key=Value

        Returns:
            str: Command string

        """
        return self._execute(
            "container set-eacl",
            **{
                param: param_value
                for param, param_value in locals().items()
                if param not in ["self"]
            }
        )
