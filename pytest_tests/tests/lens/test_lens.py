import json
import os
import shutil
import tempfile
import uuid
from contextlib import contextmanager

import allure
import base58
import pytest
from conftest import get_or_create_neofs_env
from helpers.common import SIMPLE_OBJECT_SIZE
from helpers.container import create_container
from helpers.file_helper import get_file_hash
from helpers.grpc_responses import OBJECT_ALREADY_REMOVED, OBJECT_IS_LOCKED, OBJECT_NOT_FOUND
from helpers.neofs_verbs import delete_object, generate_and_put_object, get_object, lock_object
from helpers.node_management import drop_object
from neofs_testlib.cli.neofs_lens.cli import NeofsLens
from neofs_testlib.env.env import NeoFSEnv, NodeWallet, StorageNode

META_LIST_LIMIT = 10000


@pytest.fixture(scope="module")
def lens_env(temp_directory, request):
    neofs_env = get_or_create_neofs_env(
        request,
        storage_nodes_count=1,
        with_s3_gw=False,
        with_rest_gw=False,
        shards_count=1,
    )
    yield neofs_env
    neofs_env.finalize(request)


@pytest.fixture(scope="module")
def lens_container(lens_env: NeoFSEnv, default_wallet: NodeWallet) -> str:
    return create_container(
        default_wallet.path,
        shell=lens_env.shell,
        endpoint=lens_env.sn_rpc,
        rule="REP 1",
    )


def _header_value(raw: str, key: str) -> str:
    prefix = f"{key}: "
    for line in raw.splitlines():
        if line.startswith(prefix):
            return line[len(prefix) :].strip()
    raise AssertionError(f"{key} not found in:\n{raw}")


def _assert_regular_header(raw: str, cid: str, oid: str, size: int) -> None:
    assert _header_value(raw, "Type") == "REGULAR"
    assert _header_value(raw, "CID") == cid
    assert _header_value(raw, "ID") == oid
    assert int(_header_value(raw, "PayloadSize")) == size


def _fstree_depth(fstree_path: str) -> int:
    with open(os.path.join(fstree_path, ".fstree.json"), encoding="utf-8") as descriptor_file:
        descriptor = json.load(descriptor_file)
    return int(descriptor["depth"])


def _object_file(fstree_path: str, cid: str, oid: str) -> str:
    flat = f"{oid}.{cid}"
    depth = _fstree_depth(fstree_path)
    path = os.path.join(fstree_path, *list(flat[:depth]), flat[depth:])
    assert os.path.isfile(path), f"Object file for {cid}/{oid} was not found at {path}"
    return path


def _shard_id(fstree_path: str) -> str:
    with open(os.path.join(fstree_path, ".fstree.json"), encoding="utf-8") as descriptor_file:
        return json.load(descriptor_file)["shard_id"]


@contextmanager
def _stopped(node: StorageNode):
    node.stop()
    try:
        yield
    finally:
        if node.process is None:
            node.start(fresh=False)


def _meta_listing(lens: NeofsLens, metabase_path: str) -> str:
    try:
        return lens.meta.list(str(metabase_path), limit=META_LIST_LIMIT).stdout
    except Exception as exc:
        if "end of object listing" in str(exc):
            return ""
        raise


def _reset_metabase(metabase_path: str) -> None:
    os.remove(metabase_path)
    with open(metabase_path, "wb"):
        pass


def _random_shard_id() -> str:
    raw = uuid.uuid4().bytes
    while raw[:1] == b"\x00":
        raw = uuid.uuid4().bytes
    return base58.b58encode(raw).decode()


def _corrupt(path: str, corruption: str) -> str:
    assert os.stat(path).st_nlink == 1, f"{path} is a hard link; corrupting it would change other objects"
    if corruption == "shred":
        original = open(path, "rb").read()
        shredded = os.urandom(len(original))
        assert shredded != original
        with open(path, "wb") as object_file:
            object_file.write(shredded)
        return path
    if corruption == "empty":
        with open(path, "wb"):
            pass
        assert os.path.getsize(path) == 0
        return path
    if corruption == "rename":
        renamed = os.path.join(os.path.dirname(path), "lalala")
        os.replace(path, renamed)
        assert os.path.isfile(renamed)
        assert not os.path.exists(path)
        return renamed
    raise AssertionError(f"Unknown corruption {corruption}")


@pytest.mark.simple
@allure.title("fstree list and get return a stored object")
def test_fstree_list_and_get(lens_env: NeoFSEnv, default_wallet: NodeWallet, lens_container: str):
    size = int(SIMPLE_OBJECT_SIZE)
    oid, file_path = generate_and_put_object(default_wallet.path, lens_container, lens_env.shell, lens_env.sn_rpc, size)
    address = f"{lens_container}/{oid}"
    node = lens_env.storage_nodes[0]
    lens = lens_env.neofs_lens()

    with _stopped(node):
        fstree_path = node.shards[0].fstree_path
        with allure.step("List objects in fstree"):
            listed = lens.fstree.list(fstree_path).stdout
            assert address in {line.strip() for line in listed.splitlines()}

        with allure.step("Get object header"):
            header = lens.fstree.get(address, fstree_path).stdout
            _assert_regular_header(header, lens_container, oid, size)

            with allure.step("Save object to a file"):
                saved_path = tempfile.NamedTemporaryFile(delete=False).name
                saved = lens.fstree.get(address, fstree_path, out=saved_path, payload_only=True).stdout
                assert "Saved payload" in saved
                payload = open(file_path, "rb").read()
                written = open(saved_path, "rb").read()
                assert payload == written or payload in written
                os.remove(saved_path)


@pytest.mark.simple
@allure.title("fstree remove deletes an object file")
def test_fstree_remove(lens_env: NeoFSEnv, default_wallet: NodeWallet, lens_container: str):
    oid, _ = generate_and_put_object(
        default_wallet.path, lens_container, lens_env.shell, lens_env.sn_rpc, int(SIMPLE_OBJECT_SIZE)
    )
    address = f"{lens_container}/{oid}"
    node = lens_env.storage_nodes[0]
    lens = lens_env.neofs_lens()
    shard = node.shards[0]

    with _stopped(node):
        with allure.step("Remove object from fstree"):
            removed = lens.fstree.remove(shard.fstree_path, address).stdout
            assert "Removed:" in removed
            assert address in removed

        with allure.step("Object is no longer listed or readable"):
            assert address not in {line.strip() for line in lens.fstree.list(shard.fstree_path).stdout.splitlines()}
            with pytest.raises(Exception, match="could not fetch object"):
                lens.fstree.get(address, shard.fstree_path)

        with allure.step("Resync metabase so it no longer references the removed file"):
            resynced = lens.meta.resync(shard.fstree_path, str(shard.metabase_path)).stdout
            assert "Metabase resync completed successfully" in resynced
            assert address not in {line.strip() for line in _meta_listing(lens, str(shard.metabase_path)).splitlines()}

    with pytest.raises(Exception, match=OBJECT_NOT_FOUND):
        get_object(default_wallet.path, lens_container, oid, lens_env.shell, lens_env.sn_rpc)


@pytest.mark.simple
@allure.title("fstree cleanup-tmp removes interrupted write files")
def test_fstree_cleanup_tmp(lens_env: NeoFSEnv, default_wallet: NodeWallet, lens_container: str):
    oid, _ = generate_and_put_object(
        default_wallet.path, lens_container, lens_env.shell, lens_env.sn_rpc, int(SIMPLE_OBJECT_SIZE)
    )
    address = f"{lens_container}/{oid}"
    node = lens_env.storage_nodes[0]
    lens = lens_env.neofs_lens()
    fstree_path = node.shards[0].fstree_path

    with _stopped(node):
        leftover = os.path.join(fstree_path, "interrupted#1")
        kept = os.path.join(fstree_path, "keep-me")
        with open(leftover, "wb") as tmp_file:
            tmp_file.write(b"tmp")
        with open(kept, "wb") as kept_file:
            kept_file.write(b"keep")

        with allure.step("Clean temporary files"):
            cleaned = lens.fstree.cleanup_tmp(fstree_path).stdout
            assert "Cleaning up tmp files" in cleaned

        assert not os.path.exists(leftover)
        assert os.path.isfile(kept)
        assert address in {line.strip() for line in lens.fstree.list(fstree_path).stdout.splitlines()}
        os.remove(kept)


@pytest.mark.simple
@allure.title("meta list, get and status describe a stored object")
def test_meta_list_get_and_status(lens_env: NeoFSEnv, default_wallet: NodeWallet, lens_container: str):
    size = int(SIMPLE_OBJECT_SIZE)
    oid, _ = generate_and_put_object(default_wallet.path, lens_container, lens_env.shell, lens_env.sn_rpc, size)
    address = f"{lens_container}/{oid}"
    node = lens_env.storage_nodes[0]
    lens = lens_env.neofs_lens()
    shard = node.shards[0]

    with _stopped(node):
        metabase = str(shard.metabase_path)
        with allure.step("List metabase"):
            listing = lens.meta.list(metabase, limit=META_LIST_LIMIT).stdout
            assert f"{address}, Type: REGULAR" in listing

        with allure.step("Get object from metabase"):
            header = lens.meta.get(address, metabase).stdout
            _assert_regular_header(header, lens_container, oid, size)

        with allure.step("Read object status and shard id"):
            status = lens.meta.status(address, metabase).stdout
            assert "AVAILABLE" in status
            assert "LOCKED" not in status
            assert "IN GRAVEYARD" not in status
            epoch = lens.meta.last_resync_epoch(metabase).stdout.strip()
            assert epoch.isdigit()


@pytest.mark.simple
@allure.title("meta remove drops an index and meta put restores it")
def test_meta_remove_and_put(lens_env: NeoFSEnv, default_wallet: NodeWallet, lens_container: str):
    size = int(SIMPLE_OBJECT_SIZE)
    oid, _ = generate_and_put_object(default_wallet.path, lens_container, lens_env.shell, lens_env.sn_rpc, size)
    address = f"{lens_container}/{oid}"
    node = lens_env.storage_nodes[0]
    lens = lens_env.neofs_lens()
    shard = node.shards[0]

    with _stopped(node):
        metabase = str(shard.metabase_path)
        object_file = tempfile.NamedTemporaryFile(delete=False).name
        lens.fstree.get(address, shard.fstree_path, out=object_file)

        with allure.step("Remove object from metabase"):
            removed = lens.meta.remove(metabase, address).stdout
            assert oid in removed
            assert address not in {line.strip() for line in _meta_listing(lens, metabase).splitlines()}
            with pytest.raises(Exception, match="could not get object"):
                lens.meta.get(address, metabase)

        with allure.step("Put the object file back into metabase"):
            stored = lens.meta.put(metabase, object_file).stdout
            assert "Object successfully stored" in stored
            assert oid in stored
            assert lens_container in stored
            header = lens.meta.get(address, metabase).stdout
            _assert_regular_header(header, lens_container, oid, size)

        with allure.step("Resync so later checks see blobstor headers"):
            lens.meta.resync(shard.fstree_path, metabase)
        os.remove(object_file)

    downloaded = get_object(default_wallet.path, lens_container, oid, lens_env.shell, lens_env.sn_rpc)
    assert os.path.getsize(downloaded) == size


@pytest.mark.simple
@allure.title("storage list, get, status and sanity inspect node data")
def test_storage_list_get_status_and_sanity(lens_env: NeoFSEnv, default_wallet: NodeWallet, lens_container: str):
    size = int(SIMPLE_OBJECT_SIZE)
    oid, file_path = generate_and_put_object(default_wallet.path, lens_container, lens_env.shell, lens_env.sn_rpc, size)
    address = f"{lens_container}/{oid}"
    node = lens_env.storage_nodes[0]
    lens = lens_env.neofs_lens()
    shard = node.shards[0]
    config_path = node.storage_node_config_path

    with _stopped(node):
        with allure.step("List and get object via storage"):
            assert address in {line.strip() for line in lens.storage.list(config_path).stdout.splitlines()}
            header = lens.storage.get(address, config_path).stdout
            _assert_regular_header(header, lens_container, oid, size)

            payload_path = tempfile.NamedTemporaryFile(delete=False).name
            lens.storage.get(address, config_path, out=payload_path, payload_only=True)
            assert get_file_hash(payload_path) == get_file_hash(file_path)
            os.remove(payload_path)

        with allure.step("Read storage status"):
            status = lens.storage.status(address, config_path).stdout
            assert "fstree" in status
            assert "AVAILABLE" in status
            assert str(shard.fstree_path) in status
            assert str(shard.metabase_path) in status
            assert _shard_id(shard.fstree_path) in status

        with allure.step("Run storage sanity"):
            sanity = lens.storage.sanity(config_path).stdout
            assert f"Checking {_shard_id(shard.fstree_path)} shard" in sanity
            assert "Checked objects in" in sanity or "objects checked in" in sanity

        with allure.step("Flush is rejected when write-cache is disabled"):
            with pytest.raises(Exception, match="write-cache is disabled"):
                lens.storage.flush_write_caches(config_path)


@pytest.mark.simple
@allure.title("meta resync rebuilds a wiped metabase from fstree")
def test_meta_resync_repopulates_metabase(lens_env: NeoFSEnv, default_wallet: NodeWallet, lens_container: str):
    size = int(SIMPLE_OBJECT_SIZE)
    oid, file_path = generate_and_put_object(default_wallet.path, lens_container, lens_env.shell, lens_env.sn_rpc, size)
    address = f"{lens_container}/{oid}"
    node = lens_env.storage_nodes[0]
    lens = lens_env.neofs_lens()
    shard = node.shards[0]

    with _stopped(node):
        metabase = str(shard.metabase_path)
        with allure.step("Replace metabase with an empty file and resync"):
            _reset_metabase(metabase)
            resynced = lens.meta.resync(shard.fstree_path, metabase).stdout
            assert "Metabase resync completed successfully" in resynced

        assert f"{address}, Type: REGULAR" in lens.meta.list(metabase, limit=META_LIST_LIMIT).stdout
        _assert_regular_header(lens.meta.get(address, metabase).stdout, lens_container, oid, size)
        assert lens.meta.last_resync_epoch(metabase).stdout.strip() == "0"

    downloaded = get_object(default_wallet.path, lens_container, oid, lens_env.shell, lens_env.sn_rpc)
    assert get_file_hash(downloaded) == get_file_hash(file_path)


@pytest.mark.simple
@allure.title("meta resync refuses a foreign shard id unless force is set")
def test_meta_resync_force_on_shard_id_mismatch(lens_env: NeoFSEnv, default_wallet: NodeWallet, lens_container: str):
    oid, _ = generate_and_put_object(
        default_wallet.path, lens_container, lens_env.shell, lens_env.sn_rpc, int(SIMPLE_OBJECT_SIZE)
    )
    address = f"{lens_container}/{oid}"
    node = lens_env.storage_nodes[0]
    lens = lens_env.neofs_lens()
    shard = node.shards[0]
    descriptor_path = os.path.join(shard.fstree_path, ".fstree.json")

    with _stopped(node):
        original = open(descriptor_path, encoding="utf-8").read()
        try:
            with allure.step("Point fstree at a different shard id"):
                descriptor = json.loads(original)
                descriptor["shard_id"] = _random_shard_id()
                with open(descriptor_path, "w", encoding="utf-8") as descriptor_file:
                    json.dump(descriptor, descriptor_file)

            with allure.step("Resync without force is rejected"):
                with pytest.raises(Exception, match="does not match blobstor"):
                    lens.meta.resync(shard.fstree_path, str(shard.metabase_path))

            with allure.step("Resync with force accepts the fstree shard id"):
                forced = lens.meta.resync(shard.fstree_path, str(shard.metabase_path), force=True).stdout
                assert "Metabase resync completed successfully" in forced
                assert f"{address}, Type: REGULAR" in _meta_listing(lens, str(shard.metabase_path))
        finally:
            with open(descriptor_path, "w", encoding="utf-8") as descriptor_file:
                descriptor_file.write(original)
            lens.meta.resync(shard.fstree_path, str(shard.metabase_path), force=True)


@pytest.mark.simple
@pytest.mark.parametrize(
    "corruption",
    [
        pytest.param("shred", id="shredded file"),
        pytest.param("empty", id="empty file"),
        pytest.param("rename", id="renamed to lalala"),
    ],
)
@allure.title("meta resync skips broken fstree data and keeps the other objects")
def test_meta_resync_broken_fstree_data(
    lens_env: NeoFSEnv,
    default_wallet: NodeWallet,
    lens_container: str,
    corruption: str,
):
    object_size = 192 * 1024
    broken_oid, _ = generate_and_put_object(
        default_wallet.path, lens_container, lens_env.shell, lens_env.sn_rpc, object_size
    )
    kept_oid, kept_file = generate_and_put_object(
        default_wallet.path, lens_container, lens_env.shell, lens_env.sn_rpc, object_size
    )
    broken_address = f"{lens_container}/{broken_oid}"
    kept_address = f"{lens_container}/{kept_oid}"
    node = lens_env.storage_nodes[0]
    lens = lens_env.neofs_lens()
    shard = node.shards[0]

    with _stopped(node):
        broken_path = _object_file(shard.fstree_path, lens_container, broken_oid)
        with allure.step(f"Corrupt fstree file ({corruption})"):
            _corrupt(broken_path, corruption)

        with allure.step("Resync metabase from the damaged fstree"):
            resynced = lens.meta.resync(shard.fstree_path, str(shard.metabase_path)).stdout
            assert "Metabase resync completed successfully" in resynced
            warnings = [line.strip() for line in resynced.splitlines() if line.strip().startswith("warn:")]
            listed = {line.strip() for line in lens.fstree.list(shard.fstree_path).stdout.splitlines()}
            if corruption == "rename":
                assert not any(broken_address in line for line in warnings)
                assert broken_address not in listed
            else:
                assert any(broken_address in line for line in warnings)
                assert broken_address in listed
                if corruption == "shred":
                    with pytest.raises(Exception, match="could not fetch object"):
                        lens.fstree.get(broken_address, shard.fstree_path)
                else:
                    fetched = lens.fstree.get(broken_address, shard.fstree_path).stdout
                    assert _header_value(fetched, "ID") != broken_oid

        listing = lens.meta.list(str(shard.metabase_path), limit=META_LIST_LIMIT).stdout
        assert f"{kept_address}, Type: REGULAR" in listing
        assert broken_address not in listing
        _assert_regular_header(
            lens.meta.get(kept_address, str(shard.metabase_path)).stdout,
            lens_container,
            kept_oid,
            object_size,
        )

    downloaded = get_object(default_wallet.path, lens_container, kept_oid, lens_env.shell, lens_env.sn_rpc)
    assert get_file_hash(downloaded) == get_file_hash(kept_file)
    with pytest.raises(Exception, match=OBJECT_NOT_FOUND):
        get_object(default_wallet.path, lens_container, broken_oid, lens_env.shell, lens_env.sn_rpc)


@pytest.mark.simple
@allure.title("meta resync settles a lock and a tombstone for the same object")
def test_meta_resync_lock_and_tombstone(lens_env: NeoFSEnv, default_wallet: NodeWallet, lens_container: str):
    oid, _ = generate_and_put_object(
        default_wallet.path, lens_container, lens_env.shell, lens_env.sn_rpc, int(SIMPLE_OBJECT_SIZE)
    )
    address = f"{lens_container}/{oid}"
    node = lens_env.storage_nodes[0]
    lens = lens_env.neofs_lens()
    shard = node.shards[0]

    saved_lock = tempfile.NamedTemporaryFile(delete=False).name
    outcome = None
    try:
        with allure.step("Lock the object and save the lock file from fstree"):
            lock_oid = lock_object(
                default_wallet.path,
                lens_container,
                oid,
                lens_env.shell,
                lens_env.sn_rpc,
                lifetime=100,
            )
            lock_address = f"{lens_container}/{lock_oid}"
            node.stop()
            lock_path = _object_file(shard.fstree_path, lens_container, lock_oid)
            shutil.copy2(lock_path, saved_lock)
            header = lens.fstree.get(lock_address, shard.fstree_path).stdout
            assert _header_value(header, "Type") == "LOCK"

        with allure.step("Drop the lock and delete the object, leaving a tombstone"):
            node.start(fresh=False)
            drop_object(node, lens_container, lock_oid)
            tomb_oid = delete_object(
                default_wallet.path,
                lens_container,
                oid,
                lens_env.shell,
                lens_env.sn_rpc,
            )
            tomb_address = f"{lens_container}/{tomb_oid}"
            node.stop()

        with allure.step("Put the lock file back next to the tombstone"):
            os.makedirs(os.path.dirname(lock_path), exist_ok=True)
            shutil.copy2(saved_lock, lock_path)
            assert os.path.isfile(_object_file(shard.fstree_path, lens_container, tomb_oid))
            listed = {line.strip() for line in lens.fstree.list(shard.fstree_path).stdout.splitlines()}
            assert lock_address in listed
            assert tomb_address in listed
            assert _header_value(lens.fstree.get(lock_address, shard.fstree_path).stdout, "Type") == "LOCK"
            assert _header_value(lens.fstree.get(tomb_address, shard.fstree_path).stdout, "Type") == "TOMBSTONE"

        with allure.step("Resync metabase"):
            resynced = lens.meta.resync(shard.fstree_path, str(shard.metabase_path)).stdout
            assert "Metabase resync completed successfully" in resynced

        with allure.step("Metabase keeps either the lock or the tombstone"):
            metabase = str(shard.metabase_path)
            listing = lens.meta.list(metabase, limit=META_LIST_LIMIT).stdout
            status = lens.meta.status(address, metabase).stdout
            garbage = [
                line.strip()
                for line in lens.meta.list_garbage(metabase, container=lens_container).stdout.splitlines()
                if line.strip()
            ]
            locked = "LOCKED" in status and "IN GRAVEYARD" not in status
            removed = "IN GRAVEYARD" in status and "LOCKED" not in status
            assert locked or removed, status
            if locked:
                outcome = "locked"
                assert f"{address}, Type: REGULAR" in listing
                assert f"{lock_address}, Type: LOCK" in listing
                assert tomb_address not in listing
                assert address not in garbage
            else:
                outcome = "removed"
                assert f"{tomb_address}, Type: TOMBSTONE" in listing
                assert lock_address not in listing
                assert address not in listing
                assert address in garbage
    finally:
        os.remove(saved_lock)
        if node.process is None:
            node.start(fresh=False)

    assert outcome in {"locked", "removed"}
    if outcome == "locked":
        get_object(default_wallet.path, lens_container, oid, lens_env.shell, lens_env.sn_rpc)
        with pytest.raises(Exception, match=OBJECT_IS_LOCKED):
            delete_object(default_wallet.path, lens_container, oid, lens_env.shell, lens_env.sn_rpc)
    else:
        with pytest.raises(Exception, match=f"{OBJECT_ALREADY_REMOVED}|{OBJECT_NOT_FOUND}"):
            get_object(default_wallet.path, lens_container, oid, lens_env.shell, lens_env.sn_rpc)
