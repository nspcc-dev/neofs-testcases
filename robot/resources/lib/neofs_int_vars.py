#!/usr/bin/python3
import os

NEOFS_ENDPOINT = "s01.neofs.devenv:8080"
NEOGO_CLI_PREFIX = "docker exec -it main_chain neo-go"
NEO_MAINNET_ENDPOINT = "http://main_chain.neofs.devenv:30333"

NEOFS_NEO_API_ENDPOINT = 'http://morph_chain.neofs.devenv:30333'
HTTP_GATE = 'http://http.neofs.devenv'
S3_GATE = 'https://s3.neofs.devenv:8080'
NEOFS_NETMAP = ['s01.neofs.devenv:8080', 's02.neofs.devenv:8080','s03.neofs.devenv:8080','s04.neofs.devenv:8080']

GAS_HASH = '0xa6a6c15dcdc9b997dac448b6926522d22efeedfb'
NEOFS_CONTRACT = "e11db12b0df3b3c05e6ed5f85e5cf53236e9dbeb"