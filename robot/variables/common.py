import os

ROOT='../..'

RESOURCES="%s/resources/lib" % ROOT
CERT="%s/../../ca" % ROOT
KEYWORDS="%s/../../../neofs-keywords/" % ROOT

# path from repo root is required for object put and get
# in case when test is run from root in docker
ABSOLUTE_FILE_PATH="/robot/testsuites/integration"

# Common NeoFS variables can be declared from neofs-dev-env env variables.
# High priority is accepted for those envs.

NEOFS_EPOCH_TIMEOUT = (os.getenv("NEOFS_EPOCH_TIMEOUT") if os.getenv("NEOFS_EPOCH_TIMEOUT")
                  else os.getenv("NEOFS_IR_TIMERS_EPOCH", "300s"))

SIMPLE_OBJ_SIZE = os.getenv("SIMPLE_OBJ_SIZE", "1024")
COMPLEX_OBJ_SIZE = os.getenv("COMPLEX_OBJ_SIZE", "70000000")

MAINNET_BLOCK_TIME = os.getenv('MAINNET_BLOCK_TIME', "15s")
MAINNET_TIMEOUT = os.getenv('MAINNET_TIMEOUT', "1min")
MORPH_BLOCK_TIME = os.getenv("MORPH_BLOCK_TIME", '1s')
NEOFS_CONTRACT_CACHE_TIMEOUT = os.getenv("NEOFS_CONTRACT_CACHE_TIMEOUT", "30s")

NEOFS_IR_WIF = os.getenv("NEOFS_IR_WIF", "KxyjQ8eUa4FHt3Gvioyt1Wz29cTUrE4eTqX3yFSk1YFCsPL8uNsY")
NEOFS_SN_WIF = os.getenv("NEOFS_SN_WIF", "Kwk6k2eC3L3QuPvD8aiaNyoSXgQ2YL1bwS5CP1oKoA9waeAze97s")
MAINNET_WALLET_WIF = os.getenv("MAINNET_WALLET_WIF", "KxDgvEKzgSBPPfuVfw67oPQBSjidEiqTHURKSDL1R7yGaGYAeYnr")

NEOFS_ENDPOINT = os.getenv("NEOFS_ENDPOINT", "s01.neofs.devenv:8080")
NEOGO_CLI_EXEC = os.getenv("NEOGO_CLI_EXEC", "neo-go")

NEO_MAINNET_ENDPOINT = (os.getenv("NEO_MAINNET_ENDPOINT") if os.getenv("NEO_MAINNET_ENDPOINT")
                   else os.getenv("NEOFS_IR_MAINNET_ENDPOINT_CLIENT", 'http://main_chain.neofs.devenv:30333'))

NEOFS_NEO_API_ENDPOINT = (os.getenv("NEOFS_NEO_API_ENDPOINT") if os.getenv("NEOFS_NEO_API_ENDPOINT")
                     else os.getenv("NEOFS_IR_MORPH_ENDPOINT_CLIENT", 'http://morph_chain.neofs.devenv:30333'))

HTTP_GATE = os.getenv("HTTP_GATE", 'http://http.neofs.devenv')
S3_GATE = os.getenv("S3_GATE", 'https://s3.neofs.devenv:8080')
NEOFS_NETMAP = os.getenv("NEOFS_NETMAP", ['s01.neofs.devenv:8080', 's02.neofs.devenv:8080','s03.neofs.devenv:8080','s04.neofs.devenv:8080'])
GAS_HASH = os.getenv("GAS_HASH", '0xd2a4cff31913016155e38e474a2c06d08be276cf')

NEOFS_CONTRACT = (os.getenv("NEOFS_CONTRACT") if os.getenv("NEOFS_CONTRACT")
             else os.getenv("NEOFS_IR_CONTRACTS_NEOFS", 'cfe89912c457754b7eb1f89781dc74bb3e0070bf'))

COMMON_PLACEMENT_RULE = "REP 2 IN X CBF 1 SELECT 2 FROM * AS X"

TEMP_DIR = os.getenv("TEMP_DIR", "TemporaryDir/")
