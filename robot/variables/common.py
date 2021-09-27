import os

ROOT='../..'

RESOURCES="%s/resources/lib" % ROOT
CERT="%s/../../ca" % ROOT

# Common NeoFS variables can be declared from neofs-dev-env env variables.
# High priority is accepted for those envs.

NEOFS_EPOCH_TIMEOUT = (os.getenv("NEOFS_EPOCH_TIMEOUT") if os.getenv("NEOFS_EPOCH_TIMEOUT")
                  else os.getenv("NEOFS_IR_TIMERS_EPOCH", "300s"))

SIMPLE_OBJ_SIZE = os.getenv("SIMPLE_OBJ_SIZE", "1000")
COMPLEX_OBJ_SIZE = os.getenv("COMPLEX_OBJ_SIZE", "2000")

MAINNET_BLOCK_TIME = os.getenv('MAINNET_BLOCK_TIME', "15s")
MAINNET_TIMEOUT = os.getenv('MAINNET_TIMEOUT', "1min")
MORPH_BLOCK_TIME = os.getenv("MORPH_BLOCK_TIME", '1s')
NEOFS_CONTRACT_CACHE_TIMEOUT = os.getenv("NEOFS_CONTRACT_CACHE_TIMEOUT", "30s")

NEOFS_IR_WIF = os.getenv("NEOFS_IR_WIF", "KxyjQ8eUa4FHt3Gvioyt1Wz29cTUrE4eTqX3yFSk1YFCsPL8uNsY")
NEOFS_SN_WIF = os.getenv("NEOFS_SN_WIF", "Kwk6k2eC3L3QuPvD8aiaNyoSXgQ2YL1bwS5CP1oKoA9waeAze97s")
MAINNET_WALLET_WIF = os.getenv("MAINNET_WALLET_WIF", "KxDgvEKzgSBPPfuVfw67oPQBSjidEiqTHURKSDL1R7yGaGYAeYnr")

NEOFS_ENDPOINT = os.getenv("NEOFS_ENDPOINT", "s01.neofs.devenv:8080")
NEOGO_CLI_EXEC = os.getenv("NEOGO_EXECUTABLE", "neo-go")

NEO_MAINNET_ENDPOINT = os.getenv("NEO_MAINNET_ENDPOINT", 'http://main_chain.neofs.devenv:30333')
NEOFS_NEO_API_ENDPOINT = os.getenv("NEOFS_NEO_API_ENDPOINT", 'http://morph_chain.neofs.devenv:30333')
HTTP_GATE = os.getenv("HTTP_GATE", 'http://http.neofs.devenv')
S3_GATE = os.getenv("S3_GATE", 'https://s3.neofs.devenv:8080')
NEOFS_NETMAP = os.getenv("NEOFS_NETMAP", ['s01.neofs.devenv:8080', 's02.neofs.devenv:8080','s03.neofs.devenv:8080','s04.neofs.devenv:8080'])
GAS_HASH = '0xd2a4cff31913016155e38e474a2c06d08be276cf'

NEOFS_CONTRACT = (os.getenv("NEOFS_CONTRACT") if os.getenv("NEOFS_CONTRACT")
             else os.getenv("NEOFS_IR_CONTRACTS_NEOFS", '008b43d3de8741b896015f79ac0fbfa4055b4574'))

COMMON_PLACEMENT_RULE = "REP 2 IN X CBF 1 SELECT 4 FROM * AS X"

ASSETS_DIR = os.getenv("ASSETS_DIR", "TemporaryDir/")

MORPH_MAGIC = os.environ["MORPH_MAGIC"]
GATE_PUB_KEY = '0313b1ac3a8076e155a7e797b24f0b650cccad5941ea59d7cfd51a024a8b2a06bf'
