import allure
from helpers.container import create_container
from helpers.file_helper import generate_file
from helpers.grpc_utils import put_object
from helpers.neofs_verbs import put_object as put_object_via_cli
from helpers.wellknown_acl import ALLOW_ALL_OPERATIONS_EXCEPT_DELETE
from neofs_testlib.env.env import NeoFSEnv, NodeWallet
from neofs_testlib.protobuf.generated.object import types_pb2 as object_types_pb2
from neofs_testlib.protobuf.generated.session import types_pb2 as session_types_pb2


def test_put_storage_group_object_no_longer_supported(default_wallet: NodeWallet, neofs_env: NeoFSEnv):
    with allure.step("Create an empty container for a SG object"):
        cid = create_container(
            default_wallet.path,
            shell=neofs_env.shell,
            endpoint=neofs_env.sn_rpc,
            rule="REP 1",
        )
    with allure.step("Try to put a storage group object via grpc"):
        response = put_object(object_types_pb2.ObjectType.STORAGE_GROUP, default_wallet, neofs_env, b"payload", cid)
        assert response.meta_header.status.message == "strorage group type is no longer supported"


def test_put_tombstone_object_without_delete_permission(
    default_wallet: NodeWallet, not_owner_wallet: NodeWallet, neofs_env: NeoFSEnv
):
    with allure.step("Create container and put an object to associate with a tombstone"):
        cid = create_container(
            default_wallet.path,
            shell=neofs_env.shell,
            endpoint=neofs_env.sn_rpc,
            rule="REP 1",
            basic_acl=ALLOW_ALL_OPERATIONS_EXCEPT_DELETE,
        )

        file_path = generate_file(neofs_env.get_object_size("simple_object_size"))
        oid = put_object_via_cli(default_wallet.path, file_path, cid, neofs_env.shell, neofs_env.sn_rpc)

    with allure.step("Try to put a tombstone object via grpc with a wallet that doesn't have DELETE permission"):
        response = put_object(
            object_types_pb2.ObjectType.TOMBSTONE,
            not_owner_wallet,
            neofs_env,
            b"payload",
            cid,
            oid,
            object_context_verb=session_types_pb2.ObjectSessionContext.DELETE,
        )
        assert response.meta_header.status.message == "access to object operation denied"

    with allure.step("Try to put a tombstone object via grpc with a wallet that doesn't have DELETE permission"):
        response = put_object(
            object_types_pb2.ObjectType.TOMBSTONE,
            not_owner_wallet,
            neofs_env,
            b"payload",
            cid,
            oid,
            object_context_verb=session_types_pb2.ObjectSessionContext.PUT,
        )
        assert response.meta_header.status.message == "malformed request: session token verb is invalid"


def test_put_object_without_homo_hash(default_wallet: NodeWallet, neofs_env_single_sn: NeoFSEnv):
    with allure.step("Disable homomorphic hash"):
        ir_node = neofs_env_single_sn.inner_ring_nodes[0]
        neofsadm = neofs_env_single_sn.neofs_adm()
        neofsadm.fschain.set_config(
            rpc_endpoint=f"http://{ir_node.endpoint}",
            alphabet_wallets=neofs_env_single_sn.alphabet_wallets_dir,
            post_data="HomomorphicHashingDisabled=true",
        )

    with allure.step("Create container and put an object"):
        cid = create_container(
            default_wallet.path,
            shell=neofs_env_single_sn.shell,
            endpoint=neofs_env_single_sn.sn_rpc,
            rule="REP 1",
        )

    with allure.step("Try to put an object via grpc without homo hash"):
        response = put_object(
            object_types_pb2.ObjectType.REGULAR, default_wallet, neofs_env_single_sn, b"payload", cid, homo_hash=False
        )
        assert response.meta_header.status.message != "missing homomorphic payload checksum"

    with allure.step("Enable homomorphic hash"):
        ir_node = neofs_env_single_sn.inner_ring_nodes[0]
        neofsadm = neofs_env_single_sn.neofs_adm()
        neofsadm.fschain.set_config(
            rpc_endpoint=f"http://{ir_node.endpoint}",
            alphabet_wallets=neofs_env_single_sn.alphabet_wallets_dir,
            post_data="HomomorphicHashingDisabled=false",
        )

    with allure.step("Create container and put an object"):
        cid = create_container(
            default_wallet.path,
            shell=neofs_env_single_sn.shell,
            endpoint=neofs_env_single_sn.sn_rpc,
            rule="REP 1",
        )

    with allure.step("Try to put an object via grpc without homo hash"):
        response = put_object(
            object_types_pb2.ObjectType.REGULAR, default_wallet, neofs_env_single_sn, b"payload", cid, homo_hash=False
        )
        assert response.meta_header.status.message == "missing homomorphic payload checksum"
