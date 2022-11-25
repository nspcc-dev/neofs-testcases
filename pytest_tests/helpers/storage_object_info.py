import logging
from dataclasses import dataclass

logger = logging.getLogger("NeoLogger")


@dataclass
class ObjectRef:
    cid: str
    oid: str


@dataclass
class LockObjectInfo(ObjectRef):
    lifetime: int = None
    expire_at: int = None


@dataclass
class StorageObjectInfo(ObjectRef):
    size: str = None
    wallet_file_path: str = None
    file_path: str = None
    file_hash: str = None
    attributes: list[dict[str, str]] = None
    tombstone: str = None
    locks: list[LockObjectInfo] = None
