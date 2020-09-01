#!/usr/bin/python3
import os

ROOT='../..'

RESOURCES="%s/resources/lib" % ROOT
CERT="%s/../../ca" % ROOT
# path from repo root is required for object put and get
# in case when test is run from root in docker
ABSOLUTE_FILE_PATH="/robot/testsuites/integration"

JF_TOKEN = os.getenv('JF_TOKEN') 
REG_USR = os.getenv('REG_USR') 
REG_PWD = os.getenv('REG_PWD') 
NEOFS_ENDPOINT = "s01.fs.localtest.nspcc.ru:8080"
NEOFS_NEO_API_ENDPOINT = "https://fs.localtest.nspcc.ru/neo_rpc/"

MORPH_BLOCK_TIMEOUT = "10sec"
NEOFS_EPOCH_TIMEOUT = "30sec"