import json
import logging
from time import sleep

import allure
from common import (
    IR_WALLET_PASS,
    IR_WALLET_PATH,
    MAINNET_BLOCK_TIME,
    MORPH_ENDPOINT,
    NEOFS_ADM_CONFIG_PATH,
    NEOFS_ADM_EXEC,
    NEOGO_EXECUTABLE,
)
from neofs_testlib.cli import NeofsAdm, NeoGo
from neofs_testlib.shell import Shell
from neofs_testlib.utils.wallet import get_last_address_from_wallet
from payment_neogo import get_contract_hash
from utility import parse_time

logger = logging.getLogger("NeoLogger")


@allure.step("Ensure fresh epoch")
def ensure_fresh_epoch(shell: Shell) -> int:
    # ensure new fresh epoch to avoid epoch switch during test session
    current_epoch = get_epoch(shell)
    tick_epoch(shell)
    epoch = get_epoch(shell)
    assert epoch > current_epoch, "Epoch wasn't ticked"
    return epoch


@allure.step("Get Epoch")
def get_epoch(shell: Shell):
    neogo = NeoGo(shell=shell, neo_go_exec_path=NEOGO_EXECUTABLE)
    out = neogo.contract.testinvokefunction(
        scripthash=get_contract_hash("netmap.neofs", shell=shell),
        method="epoch",
        rpc_endpoint=MORPH_ENDPOINT,
    )
    return int(json.loads(out.stdout.replace("\n", ""))["stack"][0]["value"])


@allure.step("Tick Epoch")
def tick_epoch(shell: Shell):
    if NEOFS_ADM_EXEC and NEOFS_ADM_CONFIG_PATH:
        # If neofs-adm is available, then we tick epoch with it (to be consistent with UAT tests)
        neofsadm = NeofsAdm(
            shell=shell, neofs_adm_exec_path=NEOFS_ADM_EXEC, config_file=NEOFS_ADM_CONFIG_PATH
        )
        neofsadm.morph.force_new_epoch()
        return

    # Otherwise we tick epoch using transaction
    cur_epoch = get_epoch(shell)

    ir_address = get_last_address_from_wallet(IR_WALLET_PATH, IR_WALLET_PASS)

    neogo = NeoGo(shell, neo_go_exec_path=NEOGO_EXECUTABLE)
    neogo.contract.invokefunction(
        wallet=IR_WALLET_PATH,
        wallet_password=IR_WALLET_PASS,
        scripthash=get_contract_hash("netmap.neofs", shell=shell),
        method="newEpoch",
        arguments=f"int:{cur_epoch + 1}",
        multisig_hash=f"{ir_address}:Global",
        address=ir_address,
        rpc_endpoint=MORPH_ENDPOINT,
        force=True,
        gas=1,
    )
    sleep(parse_time(MAINNET_BLOCK_TIME))
