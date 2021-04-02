#!/usr/bin/python3
import os

ROOT='../..'

RESOURCES="%s/resources/lib" % ROOT
CERT="%s/../../ca" % ROOT
# path from repo root is required for object put and get
# in case when test is run from root in docker
ABSOLUTE_FILE_PATH="/robot/testsuites/integration"
# Price of the contract Deposit/Withdraw execution:
NEOFS_CONTRACT_DEPOSIT_GAS_FEE = 0.1679897
NEOFS_CONTRACT_WITHDRAW_GAS_FEE = 0.0382514
NEOFS_EPOCH_TIMEOUT = "5min"
NEOFS_IR_WIF = "KxyjQ8eUa4FHt3Gvioyt1Wz29cTUrE4eTqX3yFSk1YFCsPL8uNsY"
NEOFS_SN_WIF = "Kwk6k2eC3L3QuPvD8aiaNyoSXgQ2YL1bwS5CP1oKoA9waeAze97s"
DEF_WALLET_ADDR = "NVUzCUvrbuWadAm6xBoyZ2U7nCmS9QBZtb"
SIMPLE_OBJ_SIZE = 1024
COMPLEX_OBJ_SIZE = 70000000
