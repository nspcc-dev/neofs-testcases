#!/usr/bin/python3.9

import contract
from robot.api import logger
from robot.api.deco import keyword

from common import IR_WALLET_PATH, IR_WALLET_PASS, MORPH_ENDPOINT

ROBOT_AUTO_KEYWORDS = False


@keyword('Get Epoch')
def get_epoch():
    epoch = int(contract.testinvoke_contract(
        contract.get_netmap_contract_hash(MORPH_ENDPOINT),
        "epoch",
        MORPH_ENDPOINT)
    )
    logger.info(f"Got epoch {epoch}")
    return epoch


@keyword('Tick Epoch')
def tick_epoch():
    cur_epoch = get_epoch()
    return contract.invoke_contract_multisig(
        contract.get_netmap_contract_hash(MORPH_ENDPOINT),
        f"newEpoch int:{cur_epoch+1}",
        IR_WALLET_PATH, IR_WALLET_PASS, MORPH_ENDPOINT)
