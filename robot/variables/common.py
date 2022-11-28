import os

import yaml

CONTAINER_WAIT_INTERVAL = "1m"

# TODO: Get object size data from a node config
SIMPLE_OBJ_SIZE = int(os.getenv("SIMPLE_OBJ_SIZE", "1000"))
COMPLEX_OBJ_SIZE = int(os.getenv("COMPLEX_OBJ_SIZE", "2000"))

MAINNET_BLOCK_TIME = os.getenv("MAINNET_BLOCK_TIME", "1s")
MAINNET_TIMEOUT = os.getenv("MAINNET_TIMEOUT", "1min")
MORPH_BLOCK_TIME = os.getenv("MORPH_BLOCK_TIME", "1s")
NEOFS_CONTRACT_CACHE_TIMEOUT = os.getenv("NEOFS_CONTRACT_CACHE_TIMEOUT", "30s")

# Time interval that allows a GC pass on storage node (this includes GC sleep interval
# of 1min plus 15 seconds for GC pass itself)
STORAGE_GC_TIME = os.getenv("STORAGE_GC_TIME", "75s")

# TODO: we should use hosting instead of these endpoints
NEOFS_ENDPOINT = os.getenv("NEOFS_ENDPOINT", "s01.neofs.devenv:8080")
NEO_MAINNET_ENDPOINT = os.getenv("NEO_MAINNET_ENDPOINT", "http://main-chain.neofs.devenv:30333")
MORPH_ENDPOINT = os.getenv("MORPH_ENDPOINT", "http://morph-chain.neofs.devenv:30333")
HTTP_GATE = os.getenv("HTTP_GATE", "http://http.neofs.devenv")
S3_GATE = os.getenv("S3_GATE", "https://s3.neofs.devenv:8080")

GAS_HASH = os.getenv("GAS_HASH", "0xd2a4cff31913016155e38e474a2c06d08be276cf")

NEOFS_CONTRACT = os.getenv("NEOFS_IR_CONTRACTS_NEOFS")

ASSETS_DIR = os.getenv("ASSETS_DIR", "TemporaryDir")
DEVENV_PATH = os.getenv("DEVENV_PATH", os.path.join("..", "neofs-dev-env"))

# Password of wallet owned by user on behalf of whom we are running tests
WALLET_PASS = os.getenv("WALLET_PASS", "")

# Load node parameters
LOAD_NODES = os.getenv("LOAD_NODES", "").split(",")
LOAD_NODE_SSH_USER = os.getenv("LOAD_NODE_SSH_USER", "root")
LOAD_NODE_SSH_PRIVATE_KEY_PATH = os.getenv("LOAD_NODE_SSH_PRIVATE_KEY_PATH")
BACKGROUND_WRITERS_COUNT = os.getenv("BACKGROUND_WRITERS_COUNT", 10)
BACKGROUND_READERS_COUNT = os.getenv("BACKGROUND_READERS_COUNT", 10)
BACKGROUND_OBJ_SIZE = os.getenv("BACKGROUND_OBJ_SIZE", 1024)
BACKGROUND_LOAD_MAX_TIME = os.getenv("BACKGROUND_LOAD_MAX_TIME", 600)

# Configuration of storage nodes
# TODO: we should use hosting instead of all these variables
STORAGE_RPC_ENDPOINT_1 = os.getenv("STORAGE_RPC_ENDPOINT_1", "s01.neofs.devenv:8080")
STORAGE_RPC_ENDPOINT_2 = os.getenv("STORAGE_RPC_ENDPOINT_2", "s02.neofs.devenv:8080")
STORAGE_RPC_ENDPOINT_3 = os.getenv("STORAGE_RPC_ENDPOINT_3", "s03.neofs.devenv:8080")
STORAGE_RPC_ENDPOINT_4 = os.getenv("STORAGE_RPC_ENDPOINT_4", "s04.neofs.devenv:8080")

STORAGE_CONTROL_ENDPOINT_1 = os.getenv("STORAGE_CONTROL_ENDPOINT_1", "s01.neofs.devenv:8081")
STORAGE_CONTROL_ENDPOINT_2 = os.getenv("STORAGE_CONTROL_ENDPOINT_2", "s02.neofs.devenv:8081")
STORAGE_CONTROL_ENDPOINT_3 = os.getenv("STORAGE_CONTROL_ENDPOINT_3", "s03.neofs.devenv:8081")
STORAGE_CONTROL_ENDPOINT_4 = os.getenv("STORAGE_CONTROL_ENDPOINT_4", "s04.neofs.devenv:8081")

STORAGE_WALLET_PATH_1 = os.getenv(
    "STORAGE_WALLET_PATH_1", os.path.join(DEVENV_PATH, "services", "storage", "wallet01.json")
)
STORAGE_WALLET_PATH_2 = os.getenv(
    "STORAGE_WALLET_PATH_2", os.path.join(DEVENV_PATH, "services", "storage", "wallet02.json")
)
STORAGE_WALLET_PATH_3 = os.getenv(
    "STORAGE_WALLET_PATH_3", os.path.join(DEVENV_PATH, "services", "storage", "wallet03.json")
)
STORAGE_WALLET_PATH_4 = os.getenv(
    "STORAGE_WALLET_PATH_4", os.path.join(DEVENV_PATH, "services", "storage", "wallet04.json")
)
STORAGE_WALLET_PATH = STORAGE_WALLET_PATH_1
STORAGE_WALLET_PASS = os.getenv("STORAGE_WALLET_PASS", "")

NEOFS_NETMAP_DICT = {
    "s01": {
        "rpc": STORAGE_RPC_ENDPOINT_1,
        "control": STORAGE_CONTROL_ENDPOINT_1,
        "wallet_path": STORAGE_WALLET_PATH_1,
        "UN-LOCODE": "RU MOW",
    },
    "s02": {
        "rpc": STORAGE_RPC_ENDPOINT_2,
        "control": STORAGE_CONTROL_ENDPOINT_2,
        "wallet_path": STORAGE_WALLET_PATH_2,
        "UN-LOCODE": "RU LED",
    },
    "s03": {
        "rpc": STORAGE_RPC_ENDPOINT_3,
        "control": STORAGE_CONTROL_ENDPOINT_3,
        "wallet_path": STORAGE_WALLET_PATH_3,
        "UN-LOCODE": "SE STO",
    },
    "s04": {
        "rpc": STORAGE_RPC_ENDPOINT_4,
        "control": STORAGE_CONTROL_ENDPOINT_4,
        "wallet_path": STORAGE_WALLET_PATH_4,
        "UN-LOCODE": "FI HEL",
    },
}
NEOFS_NETMAP = [node["rpc"] for node in NEOFS_NETMAP_DICT.values()]

# Paths to CLI executables on machine that runs tests
NEOGO_EXECUTABLE = os.getenv("NEOGO_EXECUTABLE", "neo-go")
NEOFS_CLI_EXEC = os.getenv("NEOFS_CLI_EXEC", "neofs-cli")
NEOFS_AUTHMATE_EXEC = os.getenv("NEOFS_AUTHMATE_EXEC", "neofs-authmate")
NEOFS_ADM_EXEC = os.getenv("NEOFS_ADM_EXEC", "neofs-adm")

MAINNET_WALLET_PATH = os.getenv(
    "MAINNET_WALLET_PATH", os.path.join(DEVENV_PATH, "services", "chain", "node-wallet.json")
)
MAINNET_SINGLE_ADDR = os.getenv("MAINNET_SINGLE_ADDR", "NfgHwwTi3wHAS8aFAN243C5vGbkYDpqLHP")
MAINNET_WALLET_PASS = os.getenv("MAINNET_WALLET_PASS", "one")

IR_WALLET_PATH = os.getenv("IR_WALLET_PATH", os.path.join(DEVENV_PATH, "services", "ir", "az.json"))
IR_WALLET_PASS = os.getenv("IR_WALLET_PASS", "one")

S3_GATE_WALLET_PATH = os.getenv(
    "S3_GATE_WALLET_PATH", os.path.join(DEVENV_PATH, "services", "s3_gate", "wallet.json")
)
S3_GATE_WALLET_PASS = os.getenv("S3_GATE_WALLET_PASS", "s3")

# Config for neofs-adm utility. Optional if tests are running against devenv
NEOFS_ADM_CONFIG_PATH = os.getenv("NEOFS_ADM_CONFIG_PATH")

FREE_STORAGE = os.getenv("FREE_STORAGE", "false").lower() == "true"
BIN_VERSIONS_FILE = os.getenv("BIN_VERSIONS_FILE")

HOSTING_CONFIG_FILE = os.getenv("HOSTING_CONFIG_FILE", ".devenv.hosting.yaml")
STORAGE_NODE_SERVICE_NAME_REGEX = r"s\d\d"
HTTP_GATE_SERVICE_NAME_REGEX = r"http-gate\d\d"
S3_GATE_SERVICE_NAME_REGEX = r"s3-gate\d\d"

# Generate wallet configs
# TODO: we should move all info about wallet configs to fixtures
WALLET_CONFIG = os.path.join(os.getcwd(), "wallet_config.yml")
with open(WALLET_CONFIG, "w") as file:
    yaml.dump({"password": WALLET_PASS}, file)

STORAGE_WALLET_CONFIG = os.path.join(os.getcwd(), "storage_wallet_config.yml")
with open(STORAGE_WALLET_CONFIG, "w") as file:
    yaml.dump({"password": STORAGE_WALLET_PASS}, file)

MAINNET_WALLET_CONFIG = os.path.join(os.getcwd(), "mainnet_wallet_config.yml")
with open(MAINNET_WALLET_CONFIG, "w") as file:
    yaml.dump({"password": MAINNET_WALLET_PASS}, file)

IR_WALLET_CONFIG = os.path.join(os.getcwd(), "ir_wallet_config.yml")
with open(IR_WALLET_CONFIG, "w") as file:
    yaml.dump({"password": IR_WALLET_PASS}, file)
