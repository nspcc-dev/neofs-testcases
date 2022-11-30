import logging

import allure
import pytest
from common import COMPLEX_OBJ_SIZE, SIMPLE_OBJ_SIZE
from epoch import ensure_fresh_epoch, tick_epoch
from file_helper import generate_file
from grpc_responses import MALFORMED_REQUEST, OBJECT_ACCESS_DENIED, OBJECT_NOT_FOUND
from neofs_testlib.shell import Shell
from pytest import FixtureRequest
from python_keywords.container import create_container
from python_keywords.neofs_verbs import (
    delete_object,
    get_netmap_netinfo,
    get_object,
    get_range,
    get_range_hash,
    head_object,
    put_object,
    search_object,
)
from wallet import WalletFile

from helpers.storage_object_info import StorageObjectInfo
from steps.session_token import (
    INVALID_SIGNATURE,
    UNRELATED_CONTAINER,
    UNRELATED_KEY,
    UNRELATED_OBJECT,
    WRONG_VERB,
    Lifetime,
    ObjectVerb,
    generate_object_session_token,
    get_object_signed_token,
    sign_session_token,
)
from steps.storage_object import delete_objects

logger = logging.getLogger("NeoLogger")

RANGE_OFFSET_FOR_COMPLEX_OBJECT = 200


@pytest.fixture(scope="module")
def storage_containers(owner_wallet: WalletFile, client_shell: Shell) -> list[str]:
    # Separate containers for complex/simple objects to avoid side-effects
    cid = create_container(owner_wallet.path, shell=client_shell)
    other_cid = create_container(owner_wallet.path, shell=client_shell)
    yield [cid, other_cid]


@pytest.fixture(
    params=[SIMPLE_OBJ_SIZE, COMPLEX_OBJ_SIZE],
    ids=["simple object", "complex object"],
    # Scope session to upload/delete each files set only once
    scope="module",
)
def storage_objects(
    owner_wallet: WalletFile,
    client_shell: Shell,
    storage_containers: list[str],
    request: FixtureRequest,
) -> list[StorageObjectInfo]:

    file_path = generate_file(request.param)
    storage_objects = []

    with allure.step("Put objects"):
        # upload couple objects
        for _ in range(3):
            storage_object_id = put_object(
                wallet=owner_wallet.path,
                path=file_path,
                cid=storage_containers[0],
                shell=client_shell,
            )

            storage_object = StorageObjectInfo(storage_containers[0], storage_object_id)
            storage_object.size = request.param
            storage_object.wallet_file_path = owner_wallet.path
            storage_object.file_path = file_path
            storage_objects.append(storage_object)

    yield storage_objects

    # Teardown after all tests done with current param
    delete_objects(storage_objects, client_shell)


@allure.step("Get ranges for test")
def get_ranges(storage_object: StorageObjectInfo, shell: Shell) -> list[str]:
    """
    Returns ranges to test range/hash methods via static session
    """
    object_size = storage_object.size

    if object_size == COMPLEX_OBJ_SIZE:
        net_info = get_netmap_netinfo(storage_object.wallet_file_path, shell)
        max_object_size = net_info["maximum_object_size"]
        # make sure to test multiple parts of complex object
        assert object_size >= max_object_size + RANGE_OFFSET_FOR_COMPLEX_OBJECT
        return [
            "0:10",
            f"{object_size-10}:10",
            f"{max_object_size - RANGE_OFFSET_FOR_COMPLEX_OBJECT}:"
            f"{RANGE_OFFSET_FOR_COMPLEX_OBJECT * 2}",
        ]
    else:
        return ["0:10", f"{object_size-10}:10"]


@pytest.fixture(scope="module")
def static_sessions(
    owner_wallet: WalletFile,
    user_wallet: WalletFile,
    storage_containers: list[str],
    storage_objects: list[StorageObjectInfo],
    client_shell: Shell,
    prepare_tmp_dir: str,
) -> dict[ObjectVerb, str]:
    """
    Returns dict with static session token file paths for all verbs with default lifetime with
    valid container and first two objects
    """
    return {
        verb: get_object_signed_token(
            owner_wallet,
            user_wallet,
            storage_containers[0],
            storage_objects[0:2],
            verb,
            client_shell,
            prepare_tmp_dir,
        )
        for verb in ObjectVerb
    }


@allure.title("Validate static session with read operations")
@pytest.mark.static_session
@pytest.mark.parametrize(
    "method_under_test,verb",
    [
        (head_object, ObjectVerb.HEAD),
        (get_object, ObjectVerb.GET),
    ],
)
def test_static_session_read(
    user_wallet: WalletFile,
    client_shell: Shell,
    storage_objects: list[StorageObjectInfo],
    static_sessions: dict[ObjectVerb, str],
    method_under_test,
    verb: ObjectVerb,
    request: FixtureRequest,
):
    """
    Validate static session with read operations
    """
    allure.dynamic.title(
        f"Validate static session with read operations for {request.node.callspec.id}"
    )

    for storage_object in storage_objects[0:2]:
        method_under_test(
            user_wallet.path,
            storage_object.cid,
            storage_object.oid,
            client_shell,
            session=static_sessions[verb],
        )


@allure.title("Validate static session with range operations")
@pytest.mark.static_session
@pytest.mark.parametrize(
    "method_under_test,verb",
    [(get_range, ObjectVerb.RANGE), (get_range_hash, ObjectVerb.RANGEHASH)],
)
def test_static_session_range(
    user_wallet: WalletFile,
    client_shell: Shell,
    storage_objects: list[StorageObjectInfo],
    static_sessions: dict[ObjectVerb, str],
    method_under_test,
    verb: ObjectVerb,
    request: FixtureRequest,
):
    """
    Validate static session with range operations
    """
    allure.dynamic.title(
        f"Validate static session with range operations for {request.node.callspec.id}"
    )
    storage_object = storage_objects[0]
    ranges_to_test = get_ranges(storage_object, client_shell)

    for range_to_test in ranges_to_test:
        with allure.step(f"Check range {range_to_test}"):
            method_under_test(
                user_wallet.path,
                storage_object.cid,
                storage_object.oid,
                shell=client_shell,
                session=static_sessions[verb],
                range_cut=range_to_test,
            )


@allure.title("Validate static session with search operation")
@pytest.mark.static_session
@pytest.mark.xfail
# (see https://github.com/nspcc-dev/neofs-node/issues/2030)
def test_static_session_search(
    user_wallet: WalletFile,
    client_shell: Shell,
    storage_objects: list[StorageObjectInfo],
    static_sessions: dict[ObjectVerb, str],
    request: FixtureRequest,
):
    """
    Validate static session with search operations
    """
    allure.dynamic.title(f"Validate static session with search for {request.node.callspec.id}")

    cid = storage_objects[0].cid
    expected_object_ids = [storage_object.oid for storage_object in storage_objects[0:2]]
    actual_object_ids = search_object(
        user_wallet.path, cid, client_shell, session=static_sessions[ObjectVerb.SEARCH], root=True
    )
    assert expected_object_ids == actual_object_ids


@allure.title("Validate static session with object id not in session")
@pytest.mark.static_session
def test_static_session_unrelated_object(
    user_wallet: WalletFile,
    client_shell: Shell,
    storage_objects: list[StorageObjectInfo],
    static_sessions: dict[ObjectVerb, str],
    request: FixtureRequest,
):
    """
    Validate static session with object id not in session
    """
    allure.dynamic.title(
        f"Validate static session with object id not in session for {request.node.callspec.id}"
    )
    with pytest.raises(Exception, match=UNRELATED_OBJECT):
        head_object(
            user_wallet.path,
            storage_objects[2].cid,
            storage_objects[2].oid,
            client_shell,
            session=static_sessions[ObjectVerb.HEAD],
        )


@allure.title("Validate static session with user id not in session")
@pytest.mark.static_session
def test_static_session_head_unrelated_user(
    stranger_wallet: WalletFile,
    client_shell: Shell,
    storage_objects: list[StorageObjectInfo],
    static_sessions: dict[ObjectVerb, str],
    request: FixtureRequest,
):
    """
    Validate static session with user id not in session
    """
    allure.dynamic.title(
        f"Validate static session with user id not in session for {request.node.callspec.id}"
    )
    storage_object = storage_objects[0]

    with pytest.raises(Exception, match=UNRELATED_KEY):
        head_object(
            stranger_wallet.path,
            storage_object.cid,
            storage_object.oid,
            client_shell,
            session=static_sessions[ObjectVerb.HEAD],
        )


@allure.title("Validate static session with wrong verb in session")
@pytest.mark.static_session
def test_static_session_head_wrong_verb(
    user_wallet: WalletFile,
    client_shell: Shell,
    storage_objects: list[StorageObjectInfo],
    static_sessions: dict[ObjectVerb, str],
    request: FixtureRequest,
):
    """
    Validate static session with wrong verb in session
    """
    allure.dynamic.title(
        f"Validate static session with wrong verb in session for {request.node.callspec.id}"
    )
    storage_object = storage_objects[0]

    with pytest.raises(Exception, match=WRONG_VERB):
        get_object(
            user_wallet.path,
            storage_object.cid,
            storage_object.oid,
            client_shell,
            session=static_sessions[ObjectVerb.HEAD],
        )


@allure.title("Validate static session with container id not in session")
@pytest.mark.static_session
def test_static_session_unrelated_container(
    owner_wallet: WalletFile,
    user_wallet: WalletFile,
    client_shell: Shell,
    storage_containers: list[str],
    storage_objects: list[StorageObjectInfo],
    static_sessions: dict[ObjectVerb, str],
    request: FixtureRequest,
):
    """
    Validate static session with container id not in session
    """
    allure.dynamic.title(
        f"Validate static session with container id not in session for {request.node.callspec.id}"
    )
    storage_object = storage_objects[0]

    with pytest.raises(Exception, match=UNRELATED_CONTAINER):
        get_object(
            user_wallet.path,
            storage_containers[1],
            storage_object.oid,
            client_shell,
            session=static_sessions[ObjectVerb.GET],
        )


@allure.title("Validate static session which signed by another wallet")
@pytest.mark.static_session
def test_static_session_signed_by_other(
    owner_wallet: WalletFile,
    user_wallet: WalletFile,
    stranger_wallet: WalletFile,
    client_shell: Shell,
    storage_containers: list[str],
    storage_objects: list[StorageObjectInfo],
    prepare_tmp_dir: str,
    request: FixtureRequest,
):
    """
    Validate static session which signed by another wallet
    """
    allure.dynamic.title(
        f"Validate static session which signed by another wallet for {request.node.callspec.id}"
    )
    storage_object = storage_objects[0]

    session_token_file = generate_object_session_token(
        owner_wallet,
        user_wallet,
        [storage_object.oid],
        storage_containers[0],
        ObjectVerb.HEAD,
        prepare_tmp_dir,
    )
    signed_token_file = sign_session_token(client_shell, session_token_file, stranger_wallet)
    with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
        head_object(
            user_wallet.path,
            storage_object.cid,
            storage_object.oid,
            client_shell,
            session=signed_token_file,
        )


@allure.title("Validate static session which signed for another container")
@pytest.mark.static_session
def test_static_session_signed_for_other_container(
    owner_wallet: WalletFile,
    user_wallet: WalletFile,
    client_shell: Shell,
    storage_containers: list[str],
    storage_objects: list[StorageObjectInfo],
    prepare_tmp_dir: str,
    request: FixtureRequest,
):
    """
    Validate static session which signed for another container
    """
    allure.dynamic.title(
        f"Validate static session which signed for another container for {request.node.callspec.id}"
    )
    storage_object = storage_objects[0]
    container = storage_containers[1]

    session_token_file = generate_object_session_token(
        owner_wallet, user_wallet, [storage_object.oid], container, ObjectVerb.HEAD, prepare_tmp_dir
    )
    signed_token_file = sign_session_token(client_shell, session_token_file, owner_wallet)
    with pytest.raises(Exception, match=OBJECT_NOT_FOUND):
        head_object(
            user_wallet.path, container, storage_object.oid, client_shell, session=signed_token_file
        )


@allure.title("Validate static session which wasn't signed")
@pytest.mark.static_session
def test_static_session_without_sign(
    owner_wallet: WalletFile,
    user_wallet: WalletFile,
    client_shell: Shell,
    storage_containers: list[str],
    storage_objects: list[StorageObjectInfo],
    prepare_tmp_dir: str,
    request: FixtureRequest,
):
    """
    Validate static session which wasn't signed
    """
    allure.dynamic.title(
        f"Validate static session which wasn't signed for {request.node.callspec.id}"
    )
    storage_object = storage_objects[0]

    session_token_file = generate_object_session_token(
        owner_wallet,
        user_wallet,
        [storage_object.oid],
        storage_containers[0],
        ObjectVerb.HEAD,
        prepare_tmp_dir,
    )
    with pytest.raises(Exception, match=INVALID_SIGNATURE):
        head_object(
            user_wallet.path,
            storage_object.cid,
            storage_object.oid,
            client_shell,
            session=session_token_file,
        )


@allure.title("Validate static session which expires at next epoch")
@pytest.mark.static_session
def test_static_session_expiration_at_next(
    owner_wallet: WalletFile,
    user_wallet: WalletFile,
    client_shell: Shell,
    storage_containers: list[str],
    storage_objects: list[StorageObjectInfo],
    prepare_tmp_dir: str,
    request: FixtureRequest,
):
    """
    Validate static session which expires at next epoch
    """
    allure.dynamic.title(
        f"Validate static session which expires at next epoch for {request.node.callspec.id}"
    )
    epoch = ensure_fresh_epoch(client_shell)

    container = storage_containers[0]
    object_id = storage_objects[0].oid
    expiration = Lifetime(epoch + 1, epoch, epoch)

    token_expire_at_next_epoch = get_object_signed_token(
        owner_wallet,
        user_wallet,
        container,
        storage_objects,
        ObjectVerb.HEAD,
        client_shell,
        prepare_tmp_dir,
        expiration,
    )

    head_object(
        user_wallet.path, container, object_id, client_shell, session=token_expire_at_next_epoch
    )

    tick_epoch(client_shell)

    with pytest.raises(Exception, match=MALFORMED_REQUEST):
        head_object(
            user_wallet.path, container, object_id, client_shell, session=token_expire_at_next_epoch
        )


@allure.title("Validate static session which is valid starting from next epoch")
@pytest.mark.static_session
def test_static_session_start_at_next(
    owner_wallet: WalletFile,
    user_wallet: WalletFile,
    client_shell: Shell,
    storage_containers: list[str],
    storage_objects: list[StorageObjectInfo],
    prepare_tmp_dir: str,
    request: FixtureRequest,
):
    """
    Validate static session which is valid starting from next epoch
    """
    allure.dynamic.title(
        "Validate static session which is valid starting from next epoch "
        f"for {request.node.callspec.id}"
    )
    epoch = ensure_fresh_epoch(client_shell)

    container = storage_containers[0]
    object_id = storage_objects[0].oid
    expiration = Lifetime(epoch + 2, epoch + 1, epoch)

    token_start_at_next_epoch = get_object_signed_token(
        owner_wallet,
        user_wallet,
        container,
        storage_objects,
        ObjectVerb.HEAD,
        client_shell,
        prepare_tmp_dir,
        expiration,
    )

    with pytest.raises(Exception, match=MALFORMED_REQUEST):
        head_object(
            user_wallet.path, container, object_id, client_shell, session=token_start_at_next_epoch
        )

    tick_epoch(client_shell)
    head_object(
        user_wallet.path, container, object_id, client_shell, session=token_start_at_next_epoch
    )

    tick_epoch(client_shell)
    with pytest.raises(Exception, match=MALFORMED_REQUEST):
        head_object(
            user_wallet.path, container, object_id, client_shell, session=token_start_at_next_epoch
        )


@allure.title("Validate static session which is already expired")
@pytest.mark.static_session
def test_static_session_already_expired(
    owner_wallet: WalletFile,
    user_wallet: WalletFile,
    client_shell: Shell,
    storage_containers: list[str],
    storage_objects: list[StorageObjectInfo],
    prepare_tmp_dir: str,
    request: FixtureRequest,
):
    """
    Validate static session which is already expired
    """
    allure.dynamic.title(
        f"Validate static session which is already expired for {request.node.callspec.id}"
    )
    epoch = ensure_fresh_epoch(client_shell)

    container = storage_containers[0]
    object_id = storage_objects[0].oid
    expiration = Lifetime(epoch - 1, epoch - 2, epoch - 2)

    token_already_expired = get_object_signed_token(
        owner_wallet,
        user_wallet,
        container,
        storage_objects,
        ObjectVerb.HEAD,
        client_shell,
        prepare_tmp_dir,
        expiration,
    )

    with pytest.raises(Exception, match=MALFORMED_REQUEST):
        head_object(
            user_wallet.path, container, object_id, client_shell, session=token_already_expired
        )


@allure.title("Delete verb should be restricted for static session")
def test_static_session_delete_verb(
    user_wallet: WalletFile,
    client_shell: Shell,
    storage_containers: list[str],
    storage_objects: list[StorageObjectInfo],
    static_sessions: dict[ObjectVerb, str],
    request: FixtureRequest,
):
    """
    Delete verb should be restricted for static session
    """
    allure.dynamic.title(
        f"Delete verb should be restricted for static session for {request.node.callspec.id}"
    )
    storage_object = storage_objects[0]
    with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
        delete_object(
            user_wallet.path,
            storage_object.cid,
            storage_object.oid,
            client_shell,
            session=static_sessions[ObjectVerb.DELETE],
        )


@allure.title("Put verb should be restricted for static session")
def test_static_session_put_verb(
    user_wallet: WalletFile,
    client_shell: Shell,
    storage_objects: list[StorageObjectInfo],
    static_sessions: dict[ObjectVerb, str],
    request: FixtureRequest,
):
    """
    Put verb should be restricted for static session
    """
    allure.dynamic.title(
        f"Put verb should be restricted for static session for {request.node.callspec.id}"
    )
    storage_object = storage_objects[0]
    with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
        put_object(
            user_wallet.path,
            storage_object.file_path,
            storage_object.cid,
            client_shell,
            session=static_sessions[ObjectVerb.PUT],
        )


@allure.title("Validate static session which is issued in future epoch")
@pytest.mark.static_session
def test_static_session_invalid_issued_epoch(
    owner_wallet: WalletFile,
    user_wallet: WalletFile,
    client_shell: Shell,
    storage_containers: list[str],
    storage_objects: list[StorageObjectInfo],
    prepare_tmp_dir: str,
    request: FixtureRequest,
):
    """
    Validate static session which is issued in future epoch
    """
    allure.dynamic.title(
        f"Validate static session which is issued in future epoch for {request.node.callspec.id}"
    )
    epoch = ensure_fresh_epoch(client_shell)

    container = storage_containers[0]
    object_id = storage_objects[0].oid
    expiration = Lifetime(epoch + 10, 0, epoch + 1)

    token_invalid_issue_time = get_object_signed_token(
        owner_wallet,
        user_wallet,
        container,
        storage_objects,
        ObjectVerb.HEAD,
        client_shell,
        prepare_tmp_dir,
        expiration,
    )

    with pytest.raises(Exception, match=MALFORMED_REQUEST):
        head_object(
            user_wallet.path, container, object_id, client_shell, session=token_invalid_issue_time
        )
