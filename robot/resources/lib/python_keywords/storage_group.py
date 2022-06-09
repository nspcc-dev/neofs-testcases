#!/usr/bin/python3

"""
    This module contains keywords for work with Storage Groups.
    It contains wrappers for `neofs-cli storagegroup` verbs.
"""

from robot.api.deco import keyword

from cli_helpers import _cmd_run
from common import NEOFS_CLI_EXEC, NEOFS_ENDPOINT, WALLET_PASS

ROBOT_AUTO_KEYWORDS = False


@keyword('Put Storagegroup')
def put_storagegroup(wallet: str, cid: str, objects: list, bearer_token: str = ""):
    """
        Wrapper for `neofs-cli storagegroup put`. Before the SG is created,
        neofs-cli performs HEAD on `objects`, so this verb must be allowed
        for `wallet` in `cid`.
        Args:
            wallet (str): path to wallet on whose behalf the SG is created
            cid (str): ID of Container to put SG to
            objects (list): list of Object IDs to include into the SG
            bearer_token (optional, str): path to Bearer token file
        Returns:
            (str): Object ID of created Storage Group
    """
    cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} '
        f'--wallet {wallet} --config {WALLET_PASS} '
        f'storagegroup put --cid {cid} '
        f'--members {",".join(objects)} '
        f'{"--bearer " + bearer_token if bearer_token else ""}'
    )
    output = _cmd_run(cmd)
    oid = output.split('\n')[1].split(': ')[1]
    return oid


@keyword('List Storagegroup')
def list_storagegroup(wallet: str, cid: str, bearer_token: str = ""):
    """
        Wrapper for `neofs-cli storagegroup list`.  This operation
        requires SEARCH allowed for `wallet` in `cid`.
        Args:
            wallet (str): path to wallet on whose behalf the SGs are
                        listed in the container
            cid (str): ID of Container to list
            bearer_token (optional, str): path to Bearer token file
        Returns:
            (list): Object IDs of found Storage Groups
    """
    cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} '
        f'--wallet {wallet} --config {WALLET_PASS} storagegroup list '
        f'--cid {cid} {"--bearer " + bearer_token if bearer_token else ""}'
    )
    output = _cmd_run(cmd)
    # throwing off the first string of output
    found_objects = output.split('\n')[1:]
    return found_objects


@keyword('Get Storagegroup')
def get_storagegroup(wallet: str, cid: str, oid: str, bearer_token: str = ''):
    """
        Wrapper for `neofs-cli storagegroup get`.
        Args:
            wallet (str): path to wallet on whose behalf the SG is got
            cid (str): ID of Container where SG is stored
            oid (str): ID of the Storage Group
            bearer_token (optional, str): path to Bearer token file
        Returns:
            (dict): detailed information on the Storage Group
    """

    cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} '
        f'--wallet {wallet} --config {WALLET_PASS} '
        f'storagegroup get --cid {cid} --id {oid} '
        f'{"--bearer " + bearer_token if bearer_token else ""}'
    )
    output = _cmd_run(cmd)

    # TODO: temporary solution for parsing output. Needs to be replaced with
    # JSON parsing when https://github.com/nspcc-dev/neofs-node/issues/1355
    # is done.
    strings = output.strip().split('\n')
    # first three strings go to `data`;
    # skip the 'Members:' string;
    # the rest of strings go to `members`
    data, members = strings[:3], strings[3:]
    sg_dict = {}
    for i in data:
        key, val = i.split(': ')
        sg_dict[key] = val
    sg_dict['Members'] = []
    for member in members[1:]:
        sg_dict['Members'].append(member.strip())
    return sg_dict


@keyword('Delete Storagegroup')
def delete_storagegroup(wallet: str, cid: str, oid: str, bearer_token: str = ""):
    """
        Wrapper for `neofs-cli storagegroup delete`.
        Args:
            wallet (str): path to wallet on whose behalf the SG is deleted
            cid (str): ID of Container where SG is stored
            oid (str): ID of the Storage Group
            bearer_token (optional, str): path to Bearer token file
        Returns:
            (str): Tombstone ID of the deleted Storage Group
    """

    cmd = (
        f'{NEOFS_CLI_EXEC} --rpc-endpoint {NEOFS_ENDPOINT} '
        f'--wallet {wallet} --config {WALLET_PASS} '
        f'storagegroup delete --cid {cid} --id {oid} '
        f'{"--bearer " + bearer_token if bearer_token else ""}'
    )
    output = _cmd_run(cmd)
    tombstone_id = output.strip().split('\n')[1].split(': ')[1]
    return tombstone_id
