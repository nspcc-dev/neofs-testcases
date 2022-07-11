import os

# Common NeoFS variables can be declared from neofs-dev-env env variables.
# High priority is accepted for those envs.

CONTAINER_WAIT_INTERVAL = "1m"

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

ASSETS_DIR = os.getenv("ASSETS_DIR", "TemporaryDir/")
DEVENV_PATH = os.getenv("DEVENV_PATH", "../neofs-dev-env")

MORPH_MAGIC = os.getenv("MORPH_MAGIC")

STORAGE_RPC_ENDPOINT_1 = os.getenv("STORAGE_RPC_ENDPOINT_1", "s01.neofs.devenv:8080")
STORAGE_RPC_ENDPOINT_2 = os.getenv("STORAGE_RPC_ENDPOINT_2", "s02.neofs.devenv:8080")
STORAGE_RPC_ENDPOINT_3 = os.getenv("STORAGE_RPC_ENDPOINT_3", "s03.neofs.devenv:8080")
STORAGE_RPC_ENDPOINT_4 = os.getenv("STORAGE_RPC_ENDPOINT_4", "s04.neofs.devenv:8080")

STORAGE_CONTROL_ENDPOINT_1 = os.getenv("STORAGE_CONTROL_ENDPOINT_1", "s01.neofs.devenv:8081")
STORAGE_CONTROL_ENDPOINT_2 = os.getenv("STORAGE_CONTROL_ENDPOINT_2", "s02.neofs.devenv:8081")
STORAGE_CONTROL_ENDPOINT_3 = os.getenv("STORAGE_CONTROL_ENDPOINT_3", "s03.neofs.devenv:8081")
STORAGE_CONTROL_ENDPOINT_4 = os.getenv("STORAGE_CONTROL_ENDPOINT_4", "s04.neofs.devenv:8081")

STORAGE_WALLET_PATH_1 = os.getenv("STORAGE_WALLET_PATH_1", f"{DEVENV_PATH}/services/storage/wallet01.json")
STORAGE_WALLET_PATH_2 = os.getenv("STORAGE_WALLET_PATH_2", f"{DEVENV_PATH}/services/storage/wallet02.json")
STORAGE_WALLET_PATH_3 = os.getenv("STORAGE_WALLET_PATH_3", f"{DEVENV_PATH}/services/storage/wallet03.json")
STORAGE_WALLET_PATH_4 = os.getenv("STORAGE_WALLET_PATH_4", f"{DEVENV_PATH}/services/storage/wallet04.json")
STORAGE_WALLET_PATH = STORAGE_WALLET_PATH_1

NEOFS_NETMAP_DICT = {
    's01': {
        'rpc': STORAGE_RPC_ENDPOINT_1,
        'control': STORAGE_CONTROL_ENDPOINT_1,
        'wallet_path': STORAGE_WALLET_PATH_1,
        'UN-LOCODE': 'RU MOW'
    },
    's02': {
        'rpc': STORAGE_RPC_ENDPOINT_2,
        'control': STORAGE_CONTROL_ENDPOINT_2,
        'wallet_path': STORAGE_WALLET_PATH_2,
        'UN-LOCODE': 'RU LED'
    },
    's03': {
        'rpc': STORAGE_RPC_ENDPOINT_3,
        'control': STORAGE_CONTROL_ENDPOINT_3,
        'wallet_path': STORAGE_WALLET_PATH_3,
        'UN-LOCODE': 'SE STO'
    },
    's04': {
        'rpc': STORAGE_RPC_ENDPOINT_4,
        'control': STORAGE_CONTROL_ENDPOINT_4,
        'wallet_path': STORAGE_WALLET_PATH_4,
        'UN-LOCODE': 'FI HEL'
    },
}
NEOFS_NETMAP = [i['rpc'] for i in NEOFS_NETMAP_DICT.values()]
NEOGO_EXECUTABLE = os.getenv('NEOGO_EXECUTABLE', 'neo-go')
NEOFS_CLI_EXEC = os.getenv('NEOFS_CLI_EXEC', 'neofs-cli')

WALLET_CONFIG = f"{os.getcwd()}/neofs_cli_configs/empty_passwd.yml"
MAINNET_WALLET_PATH = os.getenv("MAINNET_WALLET_PATH", f"{DEVENV_PATH}/services/chain/node-wallet.json")
MAINNET_WALLET_CONFIG = f"{os.getcwd()}/neofs_cli_configs/one_wallet_password.yml"
MAINNET_SINGLE_ADDR = 'NfgHwwTi3wHAS8aFAN243C5vGbkYDpqLHP'
MAINNET_WALLET_PASS = 'one'
IR_WALLET_PATH = os.getenv("IR_WALLET_PATH", f"{DEVENV_PATH}/services/ir/wallet01.json")
IR_WALLET_CONFIG = f"{os.getcwd()}/neofs_cli_configs/one_wallet_password.yml"
IR_WALLET_PASS = 'one'
S3_GATE_WALLET_PATH = f"{DEVENV_PATH}/services/s3_gate/wallet.json"
S3_GATE_WALLET_PASS = 's3'

STORAGE_NODE_USER = os.getenv('STORAGE_NODE_USER', 'root')
STORAGE_NODE_PWD = os.getenv('STORAGE_NODE_PWD')
STORAGE_NODE_BIN_PATH = os.getenv('STORAGE_NODE_BIN_PATH', '/opt/dev-env/vendor/neofs-cli')
STORAGE_NODE_CONFIG_PATH = os.getenv('STORAGE_NODE_CONFIG_PATH', '/opt/dev-env/services/storage/cli-cfg.yml')
STORAGE_NODE_PRIVATE_CONTROL_ENDPOINT = os.getenv('STORAGE_NODE_PRIVATE_CONTROL_ENDPOINT', 'localhost:8091')

FREE_STORAGE = os.getenv('FREE_STORAGE', "false").lower() == "true"
