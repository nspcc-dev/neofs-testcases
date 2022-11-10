import logging
from dataclasses import dataclass
from time import sleep, time

import allure
import pytest
from common import NEOFS_NETMAP, STORAGE_NODE_SERVICE_NAME_REGEX
from epoch import tick_epoch
from grpc_responses import OBJECT_ALREADY_REMOVED
from neofs_testlib.hosting import Hosting
from neofs_testlib.shell import Shell
from python_keywords.neofs_verbs import delete_object, get_object, head_object
from tombstone import verify_head_tombstone

logger = logging.getLogger("NeoLogger")


@dataclass
class StorageObjectInfo:
    size: str = None
    cid: str = None
    wallet: str = None
    file_path: str = None
    file_hash: str = None
    attributes: list[dict[str, str]] = None
    oid: str = None
    tombstone: str = None
