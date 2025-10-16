import allure
import pytest
from helpers.container import create_container
from helpers.file_helper import generate_file
from helpers.neofs_verbs import (
    delete_object,
    head_object,
    put_object,
)
from neofs_testlib.env.env import NeoFSEnv, NodeWallet


@pytest.mark.simple
@allure.title("Verify that an object can be revived")
def test_object_revival(neofs_env_single_sn_custom_gc: NeoFSEnv, default_wallet: NodeWallet):
    neofs_env = neofs_env_single_sn_custom_gc

    wallet = default_wallet
    cid = create_container(wallet.path, neofs_env.shell, neofs_env.sn_rpc, rule="REP 1")

    created_objects = []
    for _ in range(2):
        file_path = generate_file(neofs_env.get_object_size("simple_object_size"))
        oid = put_object(default_wallet.path, file_path, cid, neofs_env.shell, neofs_env.sn_rpc)
        head_object(default_wallet.path, cid, oid, neofs_env.shell, neofs_env.sn_rpc)
        created_objects.append(oid)

    for oid in created_objects:
        delete_object(default_wallet.path, cid, oid, shell=neofs_env.shell, endpoint=neofs_env.sn_rpc)

    sn = neofs_env.storage_nodes[0]
    still_alive_objects = []
    for oid in created_objects:
        response = neofs_env.neofs_cli(sn.cli_config).control.object_status(
            address=sn.wallet.address,
            endpoint=sn.control_endpoint,
            object=f"{cid}/{oid}",
            wallet=sn.wallet.path,
        )
        if "AVAILABLE" in response.stdout:
            still_alive_objects.append(oid)

    assert still_alive_objects, "No objects are available for revival"

    potential_revivals = []
    for oid in still_alive_objects:
        response = neofs_env.neofs_cli(sn.cli_config).control.object_revive(
            address=sn.wallet.address,
            endpoint=sn.control_endpoint,
            object=f"{cid}/{oid}",
            wallet=sn.wallet.path,
        )
        if "successful revival" in response.stdout:
            potential_revivals.append(oid)

    assert potential_revivals, "No objects are available for revival"

    successful_revivals = []
    for oid in potential_revivals:
        response = neofs_env.neofs_cli(sn.cli_config).control.object_status(
            address=sn.wallet.address,
            endpoint=sn.control_endpoint,
            object=f"{cid}/{oid}",
            wallet=sn.wallet.path,
        )
        if "AVAILABLE" in response.stdout:
            head_object(default_wallet.path, cid, oid, neofs_env.shell, neofs_env.sn_rpc)
            successful_revivals.append(oid)

    assert successful_revivals, "No objects were successfully revived"
