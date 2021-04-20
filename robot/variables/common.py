import os

ROOT='../..'

RESOURCES="%s/resources/lib" % ROOT
CERT="%s/../../ca" % ROOT

# path from repo root is required for object put and get
# in case when test is run from root in docker
ABSOLUTE_FILE_PATH="/robot/testsuites/integration"
# Price of the contract Deposit/Withdraw execution:
MAINNET_WALLET_PATH = os.getenv("MAINNET_WALLET_PATH", "wallets/wallet.json")
NEOFS_CONTRACT_DEPOSIT_GAS_FEE = os.getenv("NEOFS_CONTRACT_DEPOSIT_GAS_FEE", "0.1679897")
NEOFS_CONTRACT_WITHDRAW_GAS_FEE = os.getenv("NEOFS_CONTRACT_WITHDRAW_GAS_FEE", "0.0382514")
NEOFS_CREATE_CONTAINER_GAS_FEE = os.getenv("NEOFS_CREATE_CONTAINER_GAS_FEE", "-1e-08")
# NEOFS_EPOCH_TIMEOUT can be declared from neofs-dev-env env variables as NEOFS_IR_TIMERS_EPOCH 
# (high priority is accepted for env as NEOFS_EPOCH_TIMEOUT)
NEOFS_EPOCH_TIMEOUT = (os.getenv("NEOFS_EPOCH_TIMEOUT") if os.getenv("NEOFS_EPOCH_TIMEOUT") 
                       else os.getenv("NEOFS_IR_TIMERS_EPOCH", "300s"))
BASENET_BLOCK_TIME = os.getenv('BASENET_BLOCK_TIME', "15s")
BASENET_WAIT_TIME = os.getenv("BASENET_WAIT_TIME", "1min")
MORPH_BLOCK_TIME = os.getenv("MORPH_BLOCK_TIME", '1s')
NEOFS_CONTRACT_CACHE_TIMEOUT = os.getenv("NEOFS_CONTRACT_CACHE_TIMEOUT", "30s")
NEOFS_IR_WIF = os.getenv("NEOFS_IR_WIF", "KxyjQ8eUa4FHt3Gvioyt1Wz29cTUrE4eTqX3yFSk1YFCsPL8uNsY")
NEOFS_SN_WIF = os.getenv("NEOFS_SN_WIF", "Kwk6k2eC3L3QuPvD8aiaNyoSXgQ2YL1bwS5CP1oKoA9waeAze97s")
DEF_WALLET_ADDR = os.getenv("DEF_WALLET_ADDR", "NVUzCUvrbuWadAm6xBoyZ2U7nCmS9QBZtb")
SIMPLE_OBJ_SIZE = os.getenv("SIMPLE_OBJ_SIZE", "1024")
COMPLEX_OBJ_SIZE = os.getenv("COMPLEX_OBJ_SIZE", "70000000")

NEOFS_ENDPOINT = os.getenv("NEOFS_ENDPOINT", "s01.neofs.devenv:8080")
NEOGO_CLI_PREFIX = os.getenv("NEOGO_CLI_PREFIX", "docker exec -it main_chain neo-go")
# NEO_MAINNET_ENDPOINT can be declared from neofs-dev-env env variables as NEOFS_IR_MAINNET_ENDPOINT_CLIENT 
# (high priority is accepted for env as NEO_MAINNET_ENDPOINT)
NEO_MAINNET_ENDPOINT = (os.getenv("NEO_MAINNET_ENDPOINT") if os.getenv("NEO_MAINNET_ENDPOINT")
                        else os.getenv("NEOFS_IR_MAINNET_ENDPOINT_CLIENT", 'http://main_chain.neofs.devenv:30333'))

# NEOFS_NEO_API_ENDPOINT can be declared from neofs-dev-env env variables as NEOFS_IR_MORPH_ENDPOINT_CLIENT 
# (high priority is accepted for env as NEOFS_NEO_API_ENDPOINT)
NEOFS_NEO_API_ENDPOINT = (os.getenv("NEOFS_NEO_API_ENDPOINT") if os.getenv("NEOFS_NEO_API_ENDPOINT")
                          else os.getenv("NEOFS_IR_MORPH_ENDPOINT_CLIENT", 'http://morph_chain.neofs.devenv:30333'))
HTTP_GATE = os.getenv("HTTP_GATE", 'http://http.neofs.devenv')
S3_GATE = os.getenv("S3_GATE", 'https://s3.neofs.devenv:8080')
NEOFS_NETMAP = os.getenv("NEOFS_NETMAP", ['s01.neofs.devenv:8080', 's02.neofs.devenv:8080','s03.neofs.devenv:8080','s04.neofs.devenv:8080'])

GAS_HASH = os.getenv("GAS_HASH", '0xd2a4cff31913016155e38e474a2c06d08be276cf')

# NEOFS_CONTRACT can be declared from neofs-dev-env env variables as NEOFS_IR_CONTRACTS_NEOFS 
# (high priority is accepted for env as NEOFS_CONTRACT)
NEOFS_CONTRACT = (os.getenv("NEOFS_CONTRACT") if os.getenv("NEOFS_CONTRACT") 
                  else os.getenv("NEOFS_IR_CONTRACTS_NEOFS", '1e6d8b8e1a7c976649dc630062d8b281cb9c2615'))
TEMP_DIR = os.getenv("TEMP_DIR", "TemporaryDir/")