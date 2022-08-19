from typing import Optional

from .cli_command import NeofsCliCommandBase


class NeofsCliObject(NeofsCliCommandBase):
    def delete(self, rpc_endpoint: str, wallet: str, cid: str, oid: str, address: Optional[str] = None,
               bearer: Optional[str] = None, session: Optional[str] = None, ttl: Optional[int] = None,
               xhdr: Optional[list] = None, **params) -> str:
        """Delete object from NeoFS

        Args:
            address:        address of wallet account
            bearer:         File with signed JSON or binary encoded bearer token
            cid:            Container ID
            oid:            Object ID
            rpc_endpoint:   remote node address (as 'multiaddr' or '<host>:<port>')
            session:        path to a JSON-encoded container session token
            ttl:            TTL value in request meta header (default 2)
            wallet:         WIF (NEP-2) string or path to the wallet or binary key
            xhdr:           Request X-Headers in form of Key=Value

        Returns:
            str: Command string

        """
        return self._execute(
            'object delete',
            **{param: param_value for param, param_value in locals().items() if param not in ['self', 'params']}
        )

    def get(self, rpc_endpoint: str, wallet: str, cid: str, oid: str, address: Optional[str] = None,
            bearer: Optional[str] = None, file: Optional[str] = None,
            header: Optional[str] = None, no_progress: bool = False, raw: bool = False,
            session: Optional[str] = None, ttl: Optional[int] = None, xhdr: Optional[list] = None, **params) -> str:
        """Get object from NeoFS

        Args:
            address:        address of wallet account
            bearer:         File with signed JSON or binary encoded bearer token
            cid:            Container ID
            file:           File to write object payload to. Default: stdout.
            header:         File to write header to. Default: stdout.
            no_progress:    Do not show progress bar
            oid:            Object ID
            raw:            Set raw request option
            rpc_endpoint:   remote node address (as 'multiaddr' or '<host>:<port>')
            session:        path to a JSON-encoded container session token
            ttl:            TTL value in request meta header (default 2)
            wallet:         WIF (NEP-2) string or path to the wallet or binary key
            xhdr:           Request X-Headers in form of Key=Value

        Returns:
            str: Command string

        """
        return self._execute(
            'object get',
            **{param: param_value for param, param_value in locals().items() if param not in ['self', 'params']}
        )

    def hash(self, rpc_endpoint: str, wallet: str, cid: str, oid: str, address: Optional[str] = None,
             bearer: Optional[str] = None, range: Optional[str] = None, salt: Optional[str] = None,
             ttl: Optional[int] = None, hash_type: Optional[str] = None, xhdr: Optional[list] = None,
             **params) -> str:
        """Get object hash

        Args:
            address:        address of wallet account
            bearer:         File with signed JSON or binary encoded bearer token
            cid:            Container ID
            oid:            Object ID
            range:          Range to take hash from in the form offset1:length1,...
            rpc_endpoint:   remote node address (as 'multiaddr' or '<host>:<port>')
            salt:           Salt in hex format
            ttl:            TTL value in request meta header (default 2)
            hash_type:      Hash type. Either 'sha256' or 'tz' (default "sha256")
            wallet:         WIF (NEP-2) string or path to the wallet or binary key
            xhdr:           Request X-Headers in form of Key=Value

        Returns:
            str: Command string

        """
        return self._execute(
            'object hash',
            **{param: param_value for param, param_value in locals().items() if param not in ['self', 'params']}
        )

    def head(self, rpc_endpoint: str, wallet: str, cid: str, oid: str, address: Optional[str] = None,
             bearer: Optional[str] = None, file: Optional[str] = None,
             json_mode: bool = False, main_only: bool = False, proto: bool = False, raw: bool = False,
             session: Optional[str] = None, ttl: Optional[int] = None, xhdr: Optional[list] = None, **params) -> str:
        """Get object header

                Args:
                    address:        address of wallet account
                    bearer:         File with signed JSON or binary encoded bearer token
                    cid:            Container ID
                    file:           File to write object payload to. Default: stdout.
                    json_mode:      Marshal output in JSON
                    main_only:      Return only main fields
                    oid:            Object ID
                    proto:          Marshal output in Protobuf
                    raw:            Set raw request option
                    rpc_endpoint:   remote node address (as 'multiaddr' or '<host>:<port>')
                    session:        path to a JSON-encoded container session token
                    ttl:            TTL value in request meta header (default 2)
                    wallet:         WIF (NEP-2) string or path to the wallet or binary key
                    xhdr:           Request X-Headers in form of Key=Value


                Returns:
                    str: Command string

                """
        return self._execute(
            'object head',
            **{param: param_value for param, param_value in locals().items() if param not in ['self', 'params']}
        )

    def lock(self, rpc_endpoint: str, wallet: str, cid: str, oid: str, lifetime: int, address: Optional[str] = None,
             bearer: Optional[str] = None, session: Optional[str] = None,
             ttl: Optional[int] = None, xhdr: Optional[list] = None, **params) -> str:
        """Lock object in container

                Args:
                    address:        address of wallet account
                    bearer:         File with signed JSON or binary encoded bearer token
                    cid:            Container ID
                    oid:            Object ID
                    lifetime:       Object lifetime
                    rpc_endpoint:   remote node address (as 'multiaddr' or '<host>:<port>')
                    session:        path to a JSON-encoded container session token
                    ttl:            TTL value in request meta header (default 2)
                    wallet:         WIF (NEP-2) string or path to the wallet or binary key
                    xhdr:           Request X-Headers in form of Key=Value


                Returns:
                    str: Command string

                """
        return self._execute(
            'object lock',
            **{param: param_value for param, param_value in locals().items() if param not in ['self', 'params']}
        )

    def put(self, rpc_endpoint: str, wallet: str, cid: str, file: str, address: Optional[str] = None,
            attributes: Optional[dict] = None, bearer: Optional[str] = None, disable_filename: bool = False,
            disable_timestamp: bool = False, expire_at: Optional[int] = None, no_progress: bool = False,
            notify: Optional[str] = None, session: Optional[str] = None, ttl: Optional[int] = None,
            xhdr: Optional[list] = None, **params) -> str:
        """Put object to NeoFS

        Args:
            address:            address of wallet account
            attributes:         User attributes in form of Key1=Value1,Key2=Value2
            bearer:             File with signed JSON or binary encoded bearer token
            cid:                Container ID
            disable_filename:   Do not set well-known filename attribute
            disable_timestamp:  Do not set well-known timestamp attribute
            expire_at:          Last epoch in the life of the object
            file:               File with object payload
            no_progress:        Do not show progress bar
            notify:             Object notification in the form of *epoch*:*topic*; '-' topic means using default
            rpc_endpoint:       remote node address (as 'multiaddr' or '<host>:<port>')
            session:            path to a JSON-encoded container session token
            ttl:                TTL value in request meta header (default 2)
            wallet:             WIF (NEP-2) string or path to the wallet or binary key
            xhdr:               Request X-Headers in form of Key=Value

        Returns:
            str: Command string

        """
        return self._execute(
            'object put',
            **{param: param_value for param, param_value in locals().items() if param not in ['self', 'params']}
        )

    def range(self, rpc_endpoint: str, wallet: str, cid: str, oid: str, range: str, address: Optional[str] = None,
              bearer: Optional[str] = None, file: Optional[str] = None, json_mode: bool = False, raw: bool = False,
              session: Optional[str] = None, ttl: Optional[int] = None, xhdr: Optional[list] = None, **params) -> str:
        """Get payload range data of an object

                Args:
                    address:        address of wallet account
                    bearer:         File with signed JSON or binary encoded bearer token
                    cid:            Container ID
                    file:           File to write object payload to. Default: stdout.
                    json_mode:      Marshal output in JSON
                    oid:            Object ID
                    range:          Range to take data from in the form offset:length
                    raw:            Set raw request option
                    rpc_endpoint:   remote node address (as 'multiaddr' or '<host>:<port>')
                    session:        path to a JSON-encoded container session token
                    ttl:            TTL value in request meta header (default 2)
                    wallet:         WIF (NEP-2) string or path to the wallet or binary key
                    xhdr:           Request X-Headers in form of Key=Value


                Returns:
                    str: Command string

                """
        return self._execute(
            'object range',
            **{param: param_value for param, param_value in locals().items() if param not in ['self', 'params']}
        )

    def search(self, rpc_endpoint: str, wallet: str, cid: str, address: Optional[str] = None,
               bearer: Optional[str] = None,  filters: Optional[list] = None, oid: Optional[str] = None,
               phy: bool = False, root: bool = False, session: Optional[str] = None, ttl: Optional[int] = None,
               xhdr: Optional[list] = None, **params) -> str:
        """Search object

                Args:
                    address:        address of wallet account
                    bearer:         File with signed JSON or binary encoded bearer token
                    cid:            Container ID
                    filters:        Repeated filter expressions or files with protobuf JSON
                    oid:            Object ID
                    phy:            Search physically stored objects
                    root:           Search for user objects
                    rpc_endpoint:   remote node address (as 'multiaddr' or '<host>:<port>')
                    session:        path to a JSON-encoded container session token
                    ttl:            TTL value in request meta header (default 2)
                    wallet:         WIF (NEP-2) string or path to the wallet or binary key
                    xhdr:           Request X-Headers in form of Key=Value


                Returns:
                    str: Command string

                """
        return self._execute(
            'object search',
            **{param: param_value for param, param_value in locals().items() if param not in ['self', 'params']}
        )
