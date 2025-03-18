from typing import Optional

from neofs_testlib.cli.cli_command import CliCommand
from neofs_testlib.shell import CommandResult


class NeofsCliObject(CliCommand):
    def delete(
        self,
        rpc_endpoint: str,
        wallet: str,
        cid: str,
        oid: str,
        address: Optional[str] = None,
        bearer: Optional[str] = None,
        session: Optional[str] = None,
        ttl: Optional[int] = None,
        xhdr: Optional[dict] = None,
        timeout: Optional[str] = None,
        shell_timeout: Optional[int] = None,
    ) -> CommandResult:
        """
        Delete object from NeoFS.

        Args:
            address: Address of wallet account.
            bearer: File with signed JSON or binary encoded bearer token.
            cid: Container ID.
            oid: Object ID.
            rpc_endpoint: Remote node address (as 'multiaddr' or '<host>:<port>').
            session: Filepath to a JSON- or binary-encoded token of the object DELETE session.
            ttl: TTL value in request meta header (default 2).
            wallet: WIF (NEP-2) string or path to the wallet or binary key.
            xhdr: Dict with request X-Headers.
            timeout: Timeout for the operation (default 15s).
            shell_timeout: Shell timeout for the command.

        Returns:
            Command's result.
        """
        return self._execute(
            "object delete",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )

    def get(
        self,
        rpc_endpoint: str,
        wallet: str,
        cid: str,
        oid: str,
        address: Optional[str] = None,
        bearer: Optional[str] = None,
        file: Optional[str] = None,
        header: Optional[str] = None,
        no_progress: bool = False,
        raw: bool = False,
        session: Optional[str] = None,
        ttl: Optional[int] = None,
        xhdr: Optional[dict] = None,
        timeout: Optional[str] = None,
        shell_timeout: Optional[int] = None,
    ) -> CommandResult:
        """
        Get object from NeoFS.

        Args:
            address: Address of wallet account.
            bearer: File with signed JSON or binary encoded bearer token.
            cid: Container ID.
            file: File to write object payload to. Default: stdout.
            header: File to write header to. Default: stdout.
            no_progress: Do not show progress bar.
            oid: Object ID.
            raw: Set raw request option.
            rpc_endpoint: Remote node address (as 'multiaddr' or '<host>:<port>').
            session: Filepath to a JSON- or binary-encoded token of the object GET session.
            ttl: TTL value in request meta header (default 2).
            wallet: WIF (NEP-2) string or path to the wallet or binary key.
            xhdr: Dict with request X-Headers.
            timeout: Timeout for the operation (default 15s).
            shell_timeout: Shell timeout for the command.

        Returns:
            Command's result.
        """
        return self._execute(
            "object get",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )

    def hash(
        self,
        rpc_endpoint: str,
        wallet: str,
        cid: str,
        oid: str,
        address: Optional[str] = None,
        bearer: Optional[str] = None,
        range: Optional[str] = None,
        salt: Optional[str] = None,
        ttl: Optional[int] = None,
        session: Optional[str] = None,
        hash_type: Optional[str] = None,
        xhdr: Optional[dict] = None,
        timeout: Optional[str] = None,
        shell_timeout: Optional[int] = None,
    ) -> CommandResult:
        """
        Get object hash.

        Args:
            address: Address of wallet account.
            bearer: File with signed JSON or binary encoded bearer token.
            cid: Container ID.
            oid: Object ID.
            range: Range to take hash from in the form offset1:length1,...
            rpc_endpoint: Remote node address (as 'multiaddr' or '<host>:<port>').
            salt: Salt in hex format.
            ttl: TTL value in request meta header (default 2).
            session: Filepath to a JSON- or binary-encoded token of the object RANGEHASH session.
            hash_type: Hash type. Either 'sha256' or 'tz' (default "sha256").
            wallet: WIF (NEP-2) string or path to the wallet or binary key.
            xhdr: Dict with request X-Headers.
            timeout: Timeout for the operation (default 15s).
            shell_timeout: Shell timeout for the command.

        Returns:
            Command's result.
        """
        return self._execute(
            "object hash",
            **{param: value for param, value in locals().items() if param not in ["self", "params"]},
        )

    def head(
        self,
        rpc_endpoint: str,
        wallet: str,
        cid: str,
        oid: str,
        address: Optional[str] = None,
        bearer: Optional[str] = None,
        file: Optional[str] = None,
        json_mode: bool = False,
        main_only: bool = False,
        proto: bool = False,
        raw: bool = False,
        session: Optional[str] = None,
        ttl: Optional[int] = None,
        xhdr: Optional[dict] = None,
        timeout: Optional[str] = None,
        shell_timeout: Optional[int] = None,
    ) -> CommandResult:
        """
        Get object header.

        Args:
            address: Address of wallet account.
            bearer: File with signed JSON or binary encoded bearer token.
            cid: Container ID.
            file: File to write object payload to. Default: stdout.
            json_mode: Marshal output in JSON.
            main_only: Return only main fields.
            oid: Object ID.
            proto: Marshal output in Protobuf.
            raw: Set raw request option.
            rpc_endpoint: Remote node address (as 'multiaddr' or '<host>:<port>').
            session: Filepath to a JSON- or binary-encoded token of the object HEAD session.
            ttl: TTL value in request meta header (default 2).
            wallet: WIF (NEP-2) string or path to the wallet or binary key.
            xhdr: Dict with request X-Headers.
            timeout: Timeout for the operation (default 15s).
            shell_timeout: Shell timeout for the command.

        Returns:
            Command's result.
        """
        return self._execute(
            "object head",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )

    def lock(
        self,
        rpc_endpoint: str,
        wallet: str,
        cid: str,
        oid: str,
        lifetime: Optional[int] = None,
        expire_at: Optional[int] = None,
        address: Optional[str] = None,
        bearer: Optional[str] = None,
        session: Optional[str] = None,
        ttl: Optional[int] = None,
        xhdr: Optional[dict] = None,
        timeout: Optional[str] = None,
        shell_timeout: Optional[int] = None,
    ) -> CommandResult:
        """
        Lock object in container.

        Args:
            address: Address of wallet account.
            bearer: File with signed JSON or binary encoded bearer token.
            cid: Container ID.
            oid: Object ID.
            lifetime: Lock lifetime - relative to the current epoch.
            expire_at: Last epoch in the life of the object - absolute value.
            rpc_endpoint: Remote node address (as 'multiaddr' or '<host>:<port>').
            session: Filepath to a JSON- or binary-encoded token of the object PUT session.
            ttl: TTL value in request meta header (default 2).
            wallet: WIF (NEP-2) string or path to the wallet or binary key.
            xhdr: Dict with request X-Headers.
            timeout: Timeout for the operation (default 15s).
            shell_timeout: Shell timeout for the command.

        Returns:
            Command's result.
        """
        return self._execute(
            "object lock",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )

    def put(
        self,
        rpc_endpoint: str,
        wallet: str,
        cid: str,
        file: str,
        address: Optional[str] = None,
        attributes: Optional[dict] = None,
        bearer: Optional[str] = None,
        disable_filename: bool = False,
        disable_timestamp: bool = False,
        lifetime: Optional[int] = None,
        expire_at: Optional[int] = None,
        no_progress: bool = False,
        notify: Optional[str] = None,
        session: Optional[str] = None,
        ttl: Optional[int] = None,
        xhdr: Optional[dict] = None,
        timeout: Optional[str] = None,
        shell_timeout: Optional[int] = None,
    ) -> CommandResult:
        """
        Put object to NeoFS.

        Args:
            address: Address of wallet account.
            attributes: User attributes in form of Key1=Value1,Key2=Value2.
            bearer: File with signed JSON or binary encoded bearer token.
            cid: Container ID.
            disable_filename: Do not set well-known filename attribute.
            disable_timestamp: Do not set well-known timestamp attribute.
            lifetime: Lock lifetime - relative to the current epoch.
            expire_at: Last epoch in the life of the object - absolute value.
            file: File with object payload.
            no_progress: Do not show progress bar.
            notify: Object notification in the form of *epoch*:*topic*; '-'
                                topic means using default.
            rpc_endpoint: Remote node address (as 'multiaddr' or '<host>:<port>').
            session: Filepath to a JSON- or binary-encoded token of the object PUT session.
            ttl: TTL value in request meta header (default 2).
            wallet: WIF (NEP-2) string or path to the wallet or binary key.
            xhdr: Dict with request X-Headers.
            timeout: Timeout for the operation (default 15s).
            shell_timeout: Shell timeout for the command.

        Returns:
            Command's result.
        """
        return self._execute(
            "object put",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )

    def range(
        self,
        rpc_endpoint: str,
        wallet: str,
        cid: str,
        oid: str,
        range: str,
        address: Optional[str] = None,
        bearer: Optional[str] = None,
        file: Optional[str] = None,
        json_mode: bool = False,
        raw: bool = False,
        session: Optional[str] = None,
        ttl: Optional[int] = None,
        xhdr: Optional[dict] = None,
        timeout: Optional[str] = None,
        shell_timeout: Optional[int] = None,
    ) -> CommandResult:
        """
        Get payload range data of an object.

        Args:
            address: Address of wallet account.
            bearer: File with signed JSON or binary encoded bearer token.
            cid: Container ID.
            file: File to write object payload to. Default: stdout.
            json_mode: Marshal output in JSON.
            oid: Object ID.
            range: Range to take data from in the form offset:length.
            raw: Set raw request option.
            rpc_endpoint: Remote node address (as 'multiaddr' or '<host>:<port>').
            session: Filepath to a JSON- or binary-encoded token of the object RANGE session.
            ttl: TTL value in request meta header (default 2).
            wallet: WIF (NEP-2) string or path to the wallet or binary key.
            xhdr: Dict with request X-Headers.
            timeout: Timeout for the operation (default 15s).
            shell_timeout: Shell timeout for the command.

        Returns:
            Command's result.
        """
        return self._execute(
            "object range",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )

    def search(
        self,
        rpc_endpoint: str,
        wallet: str,
        cid: str,
        address: Optional[str] = None,
        bearer: Optional[str] = None,
        filters: Optional[list] = None,
        oid: Optional[str] = None,
        phy: bool = False,
        root: bool = False,
        session: Optional[str] = None,
        ttl: Optional[int] = None,
        xhdr: Optional[dict] = None,
        timeout: Optional[str] = None,
        shell_timeout: Optional[int] = None,
    ) -> CommandResult:
        """
        Search object.

        Args:
            address: Address of wallet account.
            bearer: File with signed JSON or binary encoded bearer token.
            cid: Container ID.
            filters: Repeated filter expressions or files with protobuf JSON.
            oid: Object ID.
            phy: Search physically stored objects.
            root: Search for user objects.
            rpc_endpoint: Remote node address (as 'multiaddr' or '<host>:<port>').
            session: Filepath to a JSON- or binary-encoded token of the object SEARCH session.
            ttl: TTL value in request meta header (default 2).
            wallet: WIF (NEP-2) string or path to the wallet or binary key.
            xhdr: Dict with request X-Headers.
            timeout: Timeout for the operation (default 15s).
            shell_timeout: Shell timeout for the command.

        Returns:
            Command's result.
        """
        return self._execute(
            "object search",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )

    def searchv2(
        self,
        rpc_endpoint: str,
        wallet: str,
        cid: str,
        filters: Optional[list] = None,
        attributes: Optional[list] = None,
        count: Optional[int] = None,
        cursor: Optional[str] = None,
        address: Optional[str] = None,
        bearer: Optional[str] = None,
        oid: Optional[str] = None,
        phy: bool = False,
        root: bool = False,
        session: Optional[str] = None,
        ttl: Optional[int] = None,
        xhdr: Optional[dict] = None,
        timeout: Optional[str] = None,
        shell_timeout: Optional[int] = None,
    ) -> CommandResult:
        """
        Search object.

        Args:
            address: Address of wallet account.
            bearer: File with signed JSON or binary encoded bearer token.
            cid: Container ID.
            filters: Repeated filter expressions or files with protobuf JSON.
            attributes: Additional attributes to display for suitable objects
            count: Max number of resulting items. Must not exceed 1000
            cursor: Cursor to continue previous search
            oid: Object ID.
            phy: Search physically stored objects.
            root: Search for user objects.
            rpc_endpoint: Remote node address (as 'multiaddr' or '<host>:<port>').
            session: Filepath to a JSON- or binary-encoded token of the object SEARCH session.
            ttl: TTL value in request meta header (default 2).
            wallet: WIF (NEP-2) string or path to the wallet or binary key.
            xhdr: Dict with request X-Headers.
            timeout: Timeout for the operation (default 15s).
            shell_timeout: Shell timeout for the command.

        Returns:
            Command's result.
        """
        return self._execute(
            "object searchv2",
            **{param: value for param, value in locals().items() if param not in ["self"]},
        )
