#!/usr/bin/python3
import os

NEOFS_ENDPOINT = "s01.neofs.devenv:8080"
NEOGO_CLI_PREFIX = "docker exec -it main_chain neo-go"
NEO_MAINNET_ENDPOINT = "http://main_chain.neofs.devenv:30333"

NEOFS_NEO_API_ENDPOINT = 'http://morph_chain.neofs.devenv:30333'
HTTP_GATE = 'http://http.neofs.devenv'
S3_GATE = 'https://s3.neofs.devenv:8080'
NEOFS_NETMAP = ['s01.neofs.devenv:8080', 's02.neofs.devenv:8080','s03.neofs.devenv:8080','s04.neofs.devenv:8080']

GAS_HASH = '0xb5df804bbadefea726afb5d3f4e8a6f6d32d2a20'