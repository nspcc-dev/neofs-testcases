#!/usr/bin/python3

import logging
import os
import requests

from robot.api.deco import keyword
from robot.api import logger
import robot.errors
from robot.libraries.BuiltIn import BuiltIn


ROBOT_AUTO_KEYWORDS = False

if os.getenv('ROBOT_PROFILE') == 'selectel_smoke':
    from selectelcdn_smoke_vars import (NEOGO_CLI_PREFIX, NEO_MAINNET_ENDPOINT,
    NEOFS_NEO_API_ENDPOINT, NEOFS_ENDPOINT, HTTP_GATE)
else:
    from neofs_int_vars import (NEOGO_CLI_PREFIX, NEO_MAINNET_ENDPOINT,
    NEOFS_NEO_API_ENDPOINT, NEOFS_ENDPOINT, HTTP_GATE)


@keyword('Get via HTTP Gate')
def get_via_http_gate(cid: str, oid: str):
    """
    This function gets given object from HTTP gate
    :param cid:      CID to get object from
    :param oid:      object OID
    """
    resp = requests.get(f'{HTTP_GATE}/get/{cid}/{oid}')
    if not resp.ok:
        logger.info(f"""Failed to get object via HTTP gate:
                request: {resp.request.path_url},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}""")
        return

    filename = os.path.curdir + f"/{cid}_{oid}"
    with open(filename, "w+") as f:
        f.write(resp.text)
    return filename
