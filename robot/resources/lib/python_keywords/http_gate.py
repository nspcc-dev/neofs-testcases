#!/usr/bin/python3

import shutil

import requests
from robot.api import logger
from robot.api.deco import keyword
from robot.libraries.BuiltIn import BuiltIn

from common import HTTP_GATE

ROBOT_AUTO_KEYWORDS = False
ASSETS_DIR = BuiltIn().get_variable_value("${ASSETS_DIR}")


@keyword('Get via HTTP Gate')
def get_via_http_gate(cid: str, oid: str):
    """
    This function gets given object from HTTP gate
    :param cid:      CID to get object from
    :param oid:      object OID
    """
    request = f'{HTTP_GATE}/get/{cid}/{oid}'
    resp = requests.get(request, stream=True)

    if not resp.ok:
        raise Exception(f"""Failed to get object via HTTP gate:
                request: {resp.request.path_url},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}""")

    logger.info(f'Request: {request}')
    filename = f"{ASSETS_DIR}/{cid}_{oid}"
    with open(filename, "wb") as get_file:
        shutil.copyfileobj(resp.raw, get_file)
    return filename
