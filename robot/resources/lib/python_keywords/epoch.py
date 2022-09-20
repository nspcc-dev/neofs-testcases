#!/usr/bin/python3.9
import logging

import allure
import contract
import wrappers
from common import (
    IR_WALLET_PASS,
    IR_WALLET_PATH,
    MORPH_ENDPOINT,
    NEOFS_ADM_CONFIG_PATH,
    NEOFS_ADM_EXEC,
)

logger = logging.getLogger("NeoLogger")
ROBOT_AUTO_KEYWORDS = False


@allure.step("Get Epoch")
def get_epoch():
    epoch = int(
        contract.testinvoke_contract(
            contract.get_netmap_contract_hash(MORPH_ENDPOINT), "epoch", MORPH_ENDPOINT
        )
    )
    logger.info(f"Got epoch {epoch}")
    return epoch


@allure.step("Tick Epoch")
def tick_epoch():
    if NEOFS_ADM_EXEC and NEOFS_ADM_CONFIG_PATH:
        # If neofs-adm is available, then we tick epoch with it (to be consistent with UAT tests)
        cmd = f"{NEOFS_ADM_EXEC} morph force-new-epoch -c {NEOFS_ADM_CONFIG_PATH}"
        logger.info(f"Executing shell command: {cmd}")
        out = ""
        err = ""
        try:
            out = wrappers.run_sh(cmd)
            logger.info(f"Command completed with output: {out}")
        except Exception as exc:
            logger.error(exc)
            err = str(exc)
            raise RuntimeError("Failed to tick epoch") from exc
        finally:
            allure.attach(
                (f"COMMAND: {cmd}\n" f"OUTPUT:\n {out}\n" f"ERROR: {err}\n"),
                "Tick Epoch",
                allure.attachment_type.TEXT,
            )
        return

    # Otherwise we tick epoch using transaction
    cur_epoch = get_epoch()
    return contract.invoke_contract_multisig(
        contract.get_netmap_contract_hash(MORPH_ENDPOINT),
        f"newEpoch int:{cur_epoch + 1}",
        IR_WALLET_PATH,
        IR_WALLET_PASS,
        MORPH_ENDPOINT,
    )
