import json
import logging

import allure
from helpers.test_control import wait_for_success
from helpers.utility import parse_node_height
from neofs_testlib.cli import NeoGo
from neofs_testlib.env.env import InnerRing, NeoFSEnv
from neofs_testlib.utils import converters
from neofs_testlib.utils import wallet as wallet_utils

logger = logging.getLogger("NeoLogger")

DESIGNATION_CONTRACT_HASH = "0x49cf4e5378ffcd4dec034fd98a174c5491e395e2"


@wait_for_success(60, 5)
def new_pub_key_in_alphabet_pub_keys(neofs_env: NeoFSEnv, neo_go: NeoGo, expected_key: str) -> list[str]:
    current_height, _ = parse_node_height(neo_go.query.height(f"http://{neofs_env.main_chain.rpc_address}").stdout)

    current_alphabet_keys = json.loads(
        neo_go.contract.testinvokefunction(
            DESIGNATION_CONTRACT_HASH,
            rpc_endpoint=f"http://{neofs_env.main_chain.rpc_address}",
            method="getDesignatedByRole",
            arguments=f"16 {int(current_height)}",
        ).stdout
    )

    current_alphabet_keys = [
        converters.process_b64_bytearray(key["value"]).decode("utf-8")
        for key in current_alphabet_keys["stack"][0]["value"]
    ]

    logger.info(f"{current_alphabet_keys=}")

    assert len(current_alphabet_keys) == 4, "invalid number of alphabet keys"
    assert expected_key in current_alphabet_keys, "new inner ring pub key not in alphabet keys"

    fschain_alpabet_keys = neo_go.query.committee(f"http://{neofs_env.fschain_rpc}").stdout.strip().split("\n")
    assert len(fschain_alpabet_keys) == 4, "invalid number of alphabet keys in fschain"
    assert expected_key in fschain_alpabet_keys, "new inner ring pub key not in alphabet keys in fschain"


def test_replace_ir_node_from_main_chain(clear_neofs_env: NeoFSEnv):
    neofs_env = clear_neofs_env
    with allure.step("Deploy neofs with 4 ir nodes and main chain"):
        neofs_env.download_binaries()
        neofs_env.deploy_inner_ring_nodes(count=4, with_main_chain=True)
        neofs_env.deploy_storage_nodes(
            count=1,
            node_attrs={
                0: ["UN-LOCODE:RU MOW", "Price:22"],
                1: ["UN-LOCODE:RU LED", "Price:33"],
                2: ["UN-LOCODE:SE STO", "Price:11"],
                3: ["UN-LOCODE:FI HEL", "Price:44"],
            },
        )
        neofs_env.log_env_details_to_file()
        neofs_env.log_versions_to_allure()

        neofs_adm = neofs_env.neofs_adm()
        for sn in neofs_env.storage_nodes:
            neofs_adm.fschain.refill_gas(
                rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
                alphabet_wallets=neofs_env.alphabet_wallets_dir,
                storage_wallet=sn.wallet.path,
                gas="10.0",
            )
        neofs_env.neofs_adm().fschain.set_config(
            rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
            alphabet_wallets=neofs_env.alphabet_wallets_dir,
            post_data="WithdrawFee=5",
        )

    with allure.step("Deploy new inner ring node"):
        new_inner_ring_node = InnerRing(neofs_env)
        new_inner_ring_node.generate_network_config()
        new_inner_ring_node.alphabet_wallet = neofs_env.generate_alphabet_wallets(
            neofs_env.inner_ring_nodes[0].network_config,
            alphabet_wallets_dir=neofs_env._generate_temp_dir(prefix="extra_ir_wallet"),
        )[0]
        new_inner_ring_node.generate_cli_config()

        neofs_adm.fschain.refill_gas(
            rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
            alphabet_wallets=neofs_env.alphabet_wallets_dir,
            storage_wallet=new_inner_ring_node.alphabet_wallet.path,
            gas="200.0",
        )

        new_inner_ring_node.start(wait_until_ready=True, with_main_chain=True, fschain_autodeploy=False)

    with allure.step("Replace existing inner ring node with a new one"):
        neo_go = neofs_env.neo_go()

        new_ir_accounts = wallet_utils.get_accounts_from_wallet(
            new_inner_ring_node.alphabet_wallet.path, neofs_env.default_password
        )
        new_ir_neogo_config = neofs_env.generate_neo_go_config(new_inner_ring_node.alphabet_wallet)
        neofs_adm.fschain.refill_gas(
            rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
            alphabet_wallets=neofs_env.alphabet_wallets_dir,
            storage_wallet=new_inner_ring_node.alphabet_wallet.path,
            gas="1000.0",
            wallet_address=new_ir_accounts[0].address,
        )

        neo_go.nep17.balance(
            new_ir_accounts[0].address,
            "GAS",
            f"http://{neofs_env.fschain_rpc}",
            wallet_config=new_ir_neogo_config,
        )

        neo_go.candidate.register(
            address=new_ir_accounts[0].address,
            rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
            wallet_config=new_ir_neogo_config,
            force=True,
        )

        existing_inner_ring_pub_keys = wallet_utils.get_last_public_key_from_wallet_with_neogo(
            neo_go, neofs_env.inner_ring_nodes[-1].alphabet_wallet.path
        ).splitlines()[:3]

        new_inner_ring_pub_key = str(
            wallet_utils.get_last_public_key_from_wallet(
                new_inner_ring_node.alphabet_wallet.path, new_inner_ring_node.alphabet_wallet.password
            )
        ).strip()

        neo_go.contract.invokefunction(
            DESIGNATION_CONTRACT_HASH,
            address=neofs_env.main_chain.wallet.address,
            rpc_endpoint=f"http://{neofs_env.main_chain.rpc_address}",
            wallet_config=neofs_env.main_chain.neo_go_config,
            method="designateAsRole",
            arguments=f"16 [ {new_inner_ring_pub_key} {' '.join(existing_inner_ring_pub_keys)} ]",
            multisig_hash=f"{neofs_env.main_chain.wallet.address}:CalledByEntry",
            force=True,
        )

        new_pub_key_in_alphabet_pub_keys(neofs_env, neo_go, new_inner_ring_pub_key)
