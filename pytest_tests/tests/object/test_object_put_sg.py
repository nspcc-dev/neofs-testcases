import hashlib

import allure
import grpc
import pytest
from helpers.container import create_container, delete_container
from helpers.grpc_utils import create_object_components, create_put_request, get_wallet_keys
from neo3.core import cryptography
from neofs_testlib.env.env import NeoFSEnv, NodeWallet
from neofs_testlib.protobuf.generated.object import service_pb2 as object_service_pb2
from neofs_testlib.protobuf.generated.refs import types_pb2 as refs_types_pb2


@pytest.fixture
def container(default_wallet: NodeWallet, neofs_env: NeoFSEnv) -> str:
    cid = create_container(default_wallet.path, shell=neofs_env.shell, endpoint=neofs_env.sn_rpc)
    yield cid
    delete_container(default_wallet.path, cid, shell=neofs_env.shell, endpoint=neofs_env.sn_rpc)


def test_put_storage_group_object(default_wallet: NodeWallet, container: str, neofs_env: NeoFSEnv):
    with allure.step("Create put request for a SG object"):
        test_payload = b"test object payload data"

        object_id, version, header = create_object_components(
            container, default_wallet, "4dLQLM8bVRn112vnBFwwQms7e7mtaDXr4XGJAraDtkGX", test_payload
        )
        public_key, private_key = get_wallet_keys(default_wallet)

        signature = refs_types_pb2.Signature()
        signature.key = public_key
        signature.sign = cryptography.sign(test_payload, private_key, hash_func=hashlib.sha512)
        signature.scheme = refs_types_pb2.SignatureScheme.ECDSA_SHA512

        init_request = create_put_request(
            object_id, signature, header, version, test_payload, public_key, private_key, is_init=True
        )
        allure.attach(str(init_request), "Init Request", allure.attachment_type.TEXT)

        chunk_request = create_put_request(
            None, None, None, version, test_payload, public_key, private_key, is_init=False
        )

        allure.attach(str(chunk_request), "Chunk Request", allure.attachment_type.TEXT)

    with allure.step("Send put request via grpc to a storage node"):
        channel = grpc.insecure_channel(neofs_env.storage_nodes[0].endpoint)

        def request_stream():
            yield init_request
            yield chunk_request

        try:
            response = channel.stream_unary(
                "/neo.fs.v2.object.ObjectService/Put",
                request_serializer=object_service_pb2.PutRequest.SerializeToString,
                response_deserializer=object_service_pb2.PutResponse.FromString,
            )(request_stream())
            allure.attach(str(response), "grpc response", allure.attachment_type.TEXT)
            assert response.meta_header.status.message == "strorage group type is no longer supported", (
                "Invalid status code for storage group object"
            )
        except grpc.RpcError as e:
            raise AssertionError(f"gRPC error: {e}")
        finally:
            channel.close()
