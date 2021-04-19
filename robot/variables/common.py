#!/usr/bin/python3
import os

ROOT='../..'

RESOURCES="%s/resources/lib" % ROOT
CERT="%s/../../ca" % ROOT
# path from repo root is required for object put and get
# in case when test is run from root in docker
ABSOLUTE_FILE_PATH="/robot/testsuites/integration"
# Price of the contract Deposit/Withdraw execution:
MAINNET_WALLET_PATH = "wallets/wallet.json"
NEOFS_CONTRACT_DEPOSIT_GAS_FEE = 0.1679897
NEOFS_CONTRACT_WITHDRAW_GAS_FEE = 0.0382514
NEOFS_CREATE_CONTAINER_GAS_FEE = -1e-08
NEOFS_EPOCH_TIMEOUT = os.getenv("NEOFS_IR_TIMERS_EPOCH", "300s")
BASENET_BLOCK_TIME = os.getenv('chain_SecondsPerBlock', "15s")
BASENET_WAIT_TIME = "1min"
MORPH_BLOCK_TIME = os.getenv("morph_SecondsPerBlock", '1s')
NEOFS_CONTRACT_CACHE_TIMEOUT = "30s"
NEOFS_IR_WIF = "KxyjQ8eUa4FHt3Gvioyt1Wz29cTUrE4eTqX3yFSk1YFCsPL8uNsY"
NEOFS_SN_WIF = "Kwk6k2eC3L3QuPvD8aiaNyoSXgQ2YL1bwS5CP1oKoA9waeAze97s"
DEF_WALLET_ADDR = "NVUzCUvrbuWadAm6xBoyZ2U7nCmS9QBZtb"
SIMPLE_OBJ_SIZE = 1024
COMPLEX_OBJ_SIZE = 70000000