from dataclasses import dataclass
from typing import Optional


@dataclass
class ObjectRef:
    cid: str
    oid: str


@dataclass
class LockObjectInfo(ObjectRef):
    lifetime: Optional[int] = None
    expire_at: Optional[int] = None


@dataclass
class StorageObjectInfo(ObjectRef):
    size: Optional[int] = None
    wallet_file_path: Optional[str] = None
    file_path: Optional[str] = None
    file_hash: Optional[str] = None
    attributes: Optional[list[dict[str, str]]] = None
    tombstone: Optional[str] = None
    locks: Optional[list[LockObjectInfo]] = None
