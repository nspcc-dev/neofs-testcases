import json
import logging
from time import sleep

import allure
from cluster import Cluster
from common import MAINNET_BLOCK_TIME, NEOFS_ADM_CONFIG_PATH, NEOFS_ADM_EXEC, NEOGO_EXECUTABLE
from neofs_testlib.cli import NeofsAdm, NeoGo
from neofs_testlib.shell import Shell
from neofs_testlib.utils.wallet import get_last_address_from_wallet
from payment_neogo import get_contract_hash
from utility import parse_time

logger = logging.getLogger("NeoLogger")


@allure.step("Ensure fresh epoch")
def ensure_fresh_epoch(shell: Shell, cluster: Cluster) -> int:
    # ensure new fresh epoch to avoid epoch switch during test session
    current_epoch = get_epoch(shell, cluster)
    tick_epoch(shell, cluster)
    epoch = get_epoch(shell, cluster)
    assert epoch > current_epoch, "Epoch wasn't ticked"
    return epoch


@allure.step("Get Epoch")
def get_epoch(shell: Shell, cluster: Cluster):
    morph_chain = cluster.morph_chain_nodes[0]
    morph_endpoint = morph_chain.get_endpoint()

    neogo = NeoGo(shell=shell, neo_go_exec_path=NEOGO_EXECUTABLE)
    out = neogo.contract.testinvokefunction(
        scripthash=get_contract_hash(morph_chain, "netmap.neofs", shell=shell),
        method="epoch",
        rpc_endpoint=morph_endpoint,
    )
    return int(json.loads(out.stdout.replace("\n", ""))["stack"][0]["value"])


@allure.step("Tick Epoch")
def tick_epoch(shell: Shell, cluster: Cluster):

    if NEOFS_ADM_EXEC and NEOFS_ADM_CONFIG_PATH:
        # If neofs-adm is available, then we tick epoch with it (to be consistent with UAT tests)
        neofsadm = NeofsAdm(
            shell=shell, neofs_adm_exec_path=NEOFS_ADM_EXEC, config_file=NEOFS_ADM_CONFIG_PATH
        )
        neofsadm.morph.force_new_epoch()
        return

    # Use first node by default

    # Otherwise we tick epoch using transaction
    cur_epoch = get_epoch(shell, cluster)

    ir_node = cluster.ir_nodes[0]
    # In case if no local_wallet_path is provided, we use wallet_path
    ir_wallet_path = ir_node.get_wallet_path()
    ir_wallet_pass = ir_node.get_wallet_password()
    ir_address = get_last_address_from_wallet(ir_wallet_path, ir_wallet_pass)

    morph_chain = cluster.morph_chain_nodes[0]
    morph_endpoint = morph_chain.get_endpoint()

    neogo = NeoGo(shell, neo_go_exec_path=NEOGO_EXECUTABLE)
    neogo.contract.invokefunction(
        wallet=ir_wallet_path,
        wallet_password=ir_wallet_pass,
        scripthash=get_contract_hash(morph_chain, "netmap.neofs", shell=shell),
        method="newEpoch",
        arguments=f"int:{cur_epoch + 1}",
        multisig_hash=f"{ir_address}:Global",
        address=ir_address,
        rpc_endpoint=morph_endpoint,
        force=True,
        gas=1,
    )
    sleep(parse_time(MAINNET_BLOCK_TIME))
