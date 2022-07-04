#!/usr/bin/python3.9


import contract
from robot.api import logger
from robot.api.deco import keyword
from robot.libraries.BuiltIn import BuiltIn

IR_WALLET_PATH = BuiltIn().get_variable_value("${IR_WALLET_PATH}")
IR_WALLET_PASS = BuiltIn().get_variable_value("${IR_WALLET_PASS}")
SIDECHAIN_EP = BuiltIn().get_variable_value("${MORPH_ENDPOINT}")


@keyword('Get Epoch')
def get_epoch():
    epoch = int(contract.testinvoke_contract(
        contract.get_netmap_contract_hash(SIDECHAIN_EP),
        'epoch',
        SIDECHAIN_EP)
    )
    logger.info(f"Got epoch {epoch}")
    return epoch


@keyword('Tick Epoch')
def tick_epoch():
    cur_epoch = get_epoch()
    return contract.invoke_contract_multisig(
        contract.get_netmap_contract_hash(SIDECHAIN_EP),
        f"newEpoch int:{cur_epoch+1}",
        IR_WALLET_PATH, IR_WALLET_PASS, SIDECHAIN_EP)
