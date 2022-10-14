"""
    This module contains keywords for work with Storage Groups.
    It contains wrappers for `neofs-cli storagegroup` verbs.
"""
import logging

import allure
from cli_helpers import _cmd_run
from common import COMPLEX_OBJ_SIZE, NEOFS_CLI_EXEC, NEOFS_ENDPOINT, SIMPLE_OBJ_SIZE, WALLET_CONFIG
from complex_object_actions import get_link_object
from neofs_testlib.shell import Shell
from neofs_verbs import head_object

logger = logging.getLogger("NeoLogger")


@allure.step("Put Storagegroup")
def put_storagegroup(
    wallet: str,
    cid: str,
    objects: list,
    bearer_token: str = "",
    wallet_config: str = WALLET_CONFIG,
    lifetime: str = "10",
):
    """
    Wrapper for `neofs-cli storagegroup put`. Before the SG is created,
    neofs-cli performs HEAD on `objects`, so this verb must be allowed
    for `wallet` in `cid`.
    Args:
        wallet (str): path to wallet on whose behalf the SG is created
        cid (str): ID of Container to put SG to
        objects (list): list of Object IDs to include into the SG
        bearer_token (optional, str): path to Bearer token file
        wallet_config (optional, str): path to neofs-cli config file
    Returns:
        (str): Object ID of created Storage Group
    """
    cmd = (
        f"{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} "
        f"--wallet {wallet} --config {wallet_config} "
        f"storagegroup put --cid {cid} --lifetime {lifetime} "
        f'--members {",".join(objects)} '
        f'{"--bearer " + bearer_token if bearer_token else ""}'
    )
    output = _cmd_run(cmd)
    oid = output.split("\n")[1].split(": ")[1]
    return oid


@allure.step("List Storagegroup")
def list_storagegroup(
    wallet: str, cid: str, bearer_token: str = "", wallet_config: str = WALLET_CONFIG
):
    """
    Wrapper for `neofs-cli storagegroup list`.  This operation
    requires SEARCH allowed for `wallet` in `cid`.
    Args:
        wallet (str): path to wallet on whose behalf the SGs are
                    listed in the container
        cid (str): ID of Container to list
        bearer_token (optional, str): path to Bearer token file
        wallet_config (optional, str): path to neofs-cli config file
    Returns:
        (list): Object IDs of found Storage Groups
    """
    cmd = (
        f"{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} "
        f"--wallet {wallet} --config {wallet_config} storagegroup list "
        f'--cid {cid} {"--bearer " + bearer_token if bearer_token else ""}'
    )
    output = _cmd_run(cmd)
    # throwing off the first string of output
    found_objects = output.split("\n")[1:]
    return found_objects


@allure.step("Get Storagegroup")
def get_storagegroup(
    wallet: str,
    cid: str,
    oid: str,
    bearer_token: str = "",
    wallet_config: str = WALLET_CONFIG,
):
    """
    Wrapper for `neofs-cli storagegroup get`.
    Args:
        wallet (str): path to wallet on whose behalf the SG is got
        cid (str): ID of Container where SG is stored
        oid (str): ID of the Storage Group
        bearer_token (optional, str): path to Bearer token file
        wallet_config (optional, str): path to neofs-cli config file
    Returns:
        (dict): detailed information on the Storage Group
    """

    cmd = (
        f"{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} "
        f"--wallet {wallet} --config {wallet_config} "
        f"storagegroup get --cid {cid} --id {oid} "
        f'{"--bearer " + bearer_token if bearer_token else ""}'
    )
    output = _cmd_run(cmd)

    # TODO: temporary solution for parsing output. Needs to be replaced with
    # JSON parsing when https://github.com/nspcc-dev/neofs-node/issues/1355
    # is done.
    strings = output.strip().split("\n")
    # first three strings go to `data`;
    # skip the 'Members:' string;
    # the rest of strings go to `members`
    data, members = strings[:3], strings[3:]
    sg_dict = {}
    for i in data:
        key, val = i.split(": ")
        sg_dict[key] = val
    sg_dict["Members"] = []
    for member in members[1:]:
        sg_dict["Members"].append(member.strip())
    return sg_dict


@allure.step("Delete Storagegroup")
def delete_storagegroup(
    wallet: str,
    cid: str,
    oid: str,
    bearer_token: str = "",
    wallet_config: str = WALLET_CONFIG,
):
    """
    Wrapper for `neofs-cli storagegroup delete`.
    Args:
        wallet (str): path to wallet on whose behalf the SG is deleted
        cid (str): ID of Container where SG is stored
        oid (str): ID of the Storage Group
        bearer_token (optional, str): path to Bearer token file
        wallet_config (optional, str): path to neofs-cli config file
    Returns:
        (str): Tombstone ID of the deleted Storage Group
    """

    cmd = (
        f"{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} "
        f"--wallet {wallet} --config {wallet_config} "
        f"storagegroup delete --cid {cid} --id {oid} "
        f'{"--bearer " + bearer_token if bearer_token else ""}'
    )
    output = _cmd_run(cmd)
    tombstone_id = output.strip().split("\n")[1].split(": ")[1]
    return tombstone_id


@allure.step("Verify list operation over Storagegroup")
def verify_list_storage_group(
    wallet: str,
    cid: str,
    storagegroup: str,
    bearer: str = None,
    wallet_config: str = WALLET_CONFIG,
):
    storage_groups = list_storagegroup(
        wallet, cid, bearer_token=bearer, wallet_config=wallet_config
    )
    assert storagegroup in storage_groups


@allure.step("Verify get operation over Storagegroup")
def verify_get_storage_group(
    wallet: str,
    cid: str,
    storagegroup: str,
    obj_list: list,
    object_size: int,
    shell: Shell,
    bearer: str = None,
    wallet_config: str = WALLET_CONFIG,
):
    obj_parts = []
    if object_size == COMPLEX_OBJ_SIZE:
        for obj in obj_list:
            link_oid = get_link_object(
                wallet, cid, obj, shell=shell, bearer_token=bearer, wallet_config=wallet_config
            )
            obj_head = head_object(
                wallet,
                cid,
                link_oid,
                is_raw=True,
                bearer_token=bearer,
                wallet_config=wallet_config,
            )
            obj_parts = obj_head["header"]["split"]["children"]

    obj_num = len(obj_list)
    storagegroup_data = get_storagegroup(
        wallet, cid, storagegroup, bearer_token=bearer, wallet_config=wallet_config
    )
    if object_size == SIMPLE_OBJ_SIZE:
        exp_size = SIMPLE_OBJ_SIZE * obj_num
        assert int(storagegroup_data["Group size"]) == exp_size
        assert storagegroup_data["Members"] == obj_list
    else:
        exp_size = COMPLEX_OBJ_SIZE * obj_num
        assert int(storagegroup_data["Group size"]) == exp_size
        assert storagegroup_data["Members"] == obj_parts
