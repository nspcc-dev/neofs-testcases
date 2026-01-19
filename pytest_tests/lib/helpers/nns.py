"""
This module contains helper functions for NNS (Neo Name Service) operations.
"""

import logging
from typing import Dict, Optional

import allure
from neofs_testlib.env.env import NeoFSEnv, NodeWallet

logger = logging.getLogger("NeoLogger")


@allure.step("Register NNS domain and add record")
def register_nns_domain_with_record(
    neofs_env: NeoFSEnv,
    wallet: NodeWallet,
    domain: str,
    contracts_hashes: Dict[str, str],
    owner_address: Optional[str] = None,
    neo_address: Optional[str] = None,
) -> None:
    """
    Register NNS domain for a wallet and add a Neo address record.

    Args:
        neofs_env: NeoFS environment instance
        wallet: Wallet to register the domain for
        domain: Domain name to register (e.g., "example.neofs")
        contracts_hashes: Dictionary containing contract hashes, must include "nns" key
        owner_address: Optional owner address for domain registration (defaults to wallet.address)
        neo_address: Optional Neo address for addNeoRecord (defaults to wallet.address)

    Returns:
        None
    """
    neo_go_wallet_config = neofs_env.generate_neo_go_config(wallet)

    # Use provided addresses or default to wallet address
    register_owner = owner_address if owner_address else wallet.address
    record_address = neo_address if neo_address else wallet.address

    with allure.step(f"Register NNS domain {domain}"):
        neofs_env.neo_go().contract.invokefunction(
            contracts_hashes["nns"],
            rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
            wallet_config=neo_go_wallet_config,
            method="register",
            arguments=f"{domain} {register_owner} ops@nspcc.ru 3600 600 315360000 3600",
            multisig_hash=f"{wallet.address}:CalledByEntry",
            force=True,
        )

    with allure.step(f"Add Neo address record for domain {domain}"):
        neofs_env.neo_go().contract.invokefunction(
            contracts_hashes["nns"],
            rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
            wallet_config=neo_go_wallet_config,
            method="addNeoRecord",
            arguments=f"{domain} hash160:{record_address}",
            multisig_hash=f"{wallet.address}:CalledByEntry",
            force=True,
        )


@allure.step("Get contract hashes")
def get_contract_hashes(neofs_env: NeoFSEnv) -> Dict[str, str]:
    """
    Get contract hashes from NeoFS environment.

    Args:
        neofs_env: NeoFS environment instance

    Returns:
        Dictionary mapping contract names to their hashes
    """
    neofs_adm = neofs_env.neofs_adm()
    dump_output = neofs_adm.fschain.dump_hashes(
        rpc_endpoint=f"http://{neofs_env.fschain_rpc}",
    ).stdout

    contracts_hashes = {}
    for line in dump_output.strip().split("\n"):
        parts = line.split()
        if len(parts) >= 3:
            contract_name = parts[0]
            contract_hash = parts[2]
            contracts_hashes[contract_name] = contract_hash

    return contracts_hashes
