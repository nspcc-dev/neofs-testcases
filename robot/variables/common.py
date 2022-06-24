import os

ROOT = '../..'
CERT = "%s/../../ca" % ROOT

# Common NeoFS variables can be declared from neofs-dev-env env variables.
# High priority is accepted for those envs.

CONTAINER_WAIT_INTERVAL = "1m"

NEOFS_EPOCH_TIMEOUT = (os.getenv("NEOFS_EPOCH_TIMEOUT") if os.getenv("NEOFS_EPOCH_TIMEOUT")
                       else os.getenv("NEOFS_IR_TIMERS_EPOCH", "300s"))

SIMPLE_OBJ_SIZE = 1000
COMPLEX_OBJ_SIZE = 2000

MAINNET_BLOCK_TIME = os.getenv('MAINNET_BLOCK_TIME', "1s")
MAINNET_TIMEOUT = os.getenv('MAINNET_TIMEOUT', "1min")
MORPH_BLOCK_TIME = os.getenv("MORPH_BLOCK_TIME", '1s')
NEOFS_CONTRACT_CACHE_TIMEOUT = os.getenv("NEOFS_CONTRACT_CACHE_TIMEOUT", "30s")

# TODO: change to NEOFS_STORAGE_DEFAULT_GC_REMOVER_SLEEP_INTERVAL

SHARD_0_GC_SLEEP = os.getenv("NEOFS_STORAGE_SHARD_0_GC_REMOVER_SLEEP_INTERVAL", "1m")

NEOFS_ENDPOINT = os.getenv("NEOFS_ENDPOINT", "s01.neofs.devenv:8080")
NEOGO_CLI_EXEC = os.getenv("NEOGO_EXECUTABLE", "neo-go")

NEO_MAINNET_ENDPOINT = os.getenv("NEO_MAINNET_ENDPOINT", 'http://main-chain.neofs.devenv:30333')
MORPH_ENDPOINT = os.getenv("MORPH_ENDPOINT", 'http://morph-chain.neofs.devenv:30333')
HTTP_GATE = os.getenv("HTTP_GATE", 'http://http.neofs.devenv')
S3_GATE = os.getenv("S3_GATE", 'https://s3.neofs.devenv:8080')
GAS_HASH = '0xd2a4cff31913016155e38e474a2c06d08be276cf'

NEOFS_CONTRACT = os.getenv("NEOFS_IR_CONTRACTS_NEOFS")

COMMON_PLACEMENT_RULE = "REP 2 IN X CBF 1 SELECT 4 FROM * AS X"

ASSETS_DIR = os.getenv("ASSETS_DIR", "TemporaryDir/")

MORPH_MAGIC = os.getenv("MORPH_MAGIC")
GATE_PUB_KEY = '0313b1ac3a8076e155a7e797b24f0b650cccad5941ea59d7cfd51a024a8b2a06bf'

STORAGE_NODE_1 = os.getenv('DATA_NODE_1', 's01.neofs.devenv:8080')
STORAGE_NODE_2 = os.getenv('DATA_NODE_2', 's02.neofs.devenv:8080')
STORAGE_NODE_3 = os.getenv('DATA_NODE_3', 's03.neofs.devenv:8080')
STORAGE_NODE_4 = os.getenv('DATA_NODE_4', 's04.neofs.devenv:8080')

DEVENV_SERVICES_PATH = f"{os.getenv('DEVENV_PATH')}/services"
NEOFS_NETMAP_DICT = {'s01': {'rpc': STORAGE_NODE_1,
                             'control': 's01.neofs.devenv:8081',
                             'wallet_path':f"{DEVENV_SERVICES_PATH}/storage/wallet01.json",
                             'UN-LOCODE': 'RU MOW'},
                     's02': {'rpc': STORAGE_NODE_2,
                             'control': 's02.neofs.devenv:8081',
                             'wallet_path': f"{DEVENV_SERVICES_PATH}/storage/wallet02.json",
                             'UN-LOCODE': 'RU LED'},
                     's03': {'rpc': STORAGE_NODE_3,
                             'control': 's03.neofs.devenv:8081',
                             'wallet_path': f"{DEVENV_SERVICES_PATH}/storage/wallet03.json",
                             'UN-LOCODE': 'SE STO'},
                     's04': {'rpc': STORAGE_NODE_4,
                             'control': 's04.neofs.devenv:8081',
                             'wallet_path': f"{DEVENV_SERVICES_PATH}/storage/wallet04.json",
                             'UN-LOCODE': 'FI HEL'}
                     }
NEOFS_NETMAP = [i['rpc'] for i in NEOFS_NETMAP_DICT.values()]
NEOGO_EXECUTABLE = os.getenv('NEOGO_EXECUTABLE', 'neo-go')
NEOFS_CLI_EXEC = os.getenv('NEOFS_CLI_EXEC', 'neofs-cli')

WALLET_CONFIG = f"{os.getcwd()}/neofs_cli_configs/empty_passwd.yml"
MAINNET_WALLET_PATH = f"{DEVENV_SERVICES_PATH}/chain/node-wallet.json"
MAINNET_WALLET_CONFIG = f"{os.getcwd()}/neofs_cli_configs/mainnet_wallet_passwd.yml"
MAINNET_SINGLE_ADDR = 'NfgHwwTi3wHAS8aFAN243C5vGbkYDpqLHP'
MAINNET_WALLET_PASS = 'one'
IR_WALLET_PATH = f"{DEVENV_SERVICES_PATH}/ir/wallet01.json"
IR_WALLET_CONFIG = f"{os.getcwd()}/neofs_cli_configs/ir_wallet_passwd.yml"
IR_WALLET_PASS = 'one'
STORAGE_WALLET_PATH = f"{DEVENV_SERVICES_PATH}/storage/wallet01.json"
