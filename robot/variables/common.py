#!/usr/bin/python3
import os

ROOT='../..'

RESOURCES="%s/resources/lib" % ROOT
CERT="%s/../../ca" % ROOT
# path from repo root is required for object put and get
# in case when test is run from root in docker
ABSOLUTE_FILE_PATH="/robot/testsuites/integration"

# Price of the contract Deposit execution: 0.1493182 GAS
NEOFS_CONTRACT_DEPOSIT_GAS_FEE = 0.1493182
NEOFS_CONTRACT_WITHDRAW_GAS_FEE = 0.0331791
TEMP_DIR = "TemporaryDir/"