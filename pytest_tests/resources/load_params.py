import os

# Load node parameters
LOAD_NODES = os.getenv("LOAD_NODES", "127.0.0.1").split(",")
LOAD_NODE_SSH_USER = os.getenv("LOAD_NODE_SSH_USER", "k6")
LOAD_NODE_SSH_PRIVATE_KEY_PATH = os.getenv(
    "LOAD_NODE_SSH_PRIVATE_KEY_PATH", "../neofs-dev-env/services/k6_node/id_ed25519"
)
BACKGROUND_WRITERS_COUNT = os.getenv("BACKGROUND_WRITERS_COUNT", 10)
BACKGROUND_READERS_COUNT = os.getenv("BACKGROUND_READERS_COUNT", 10)
BACKGROUND_OBJ_SIZE = os.getenv("BACKGROUND_OBJ_SIZE", 1024)
BACKGROUND_LOAD_MAX_TIME = os.getenv("BACKGROUND_LOAD_MAX_TIME", 600)

# Load run parameters

OBJ_SIZE = [int(o) for o in os.getenv("OBJ_SIZE", "1000").split(",")]
CONTAINERS_COUNT = [int(c) for c in os.getenv("CONTAINERS_COUNT", "1").split(",")]
OUT_FILE = os.getenv("OUT_FILE", "1mb_200.json").split(",")
OBJ_COUNT = [int(o) for o in os.getenv("OBJ_COUNT", "4").split(",")]
WRITERS = [int(w) for w in os.getenv("WRITERS", "200").split(",")]
READERS = [int(r) for r in os.getenv("READER", "0").split(",")]
DELETERS = [int(d) for d in os.getenv("DELETERS", "0").split(",")]
LOAD_TIME = [int(ld) for ld in os.getenv("LOAD_TIME", "200").split(",")]
LOAD_TYPE = os.getenv("LOAD_TYPE", "grpc").split(",")
LOAD_NODES_COUNT = [int(ldc) for ldc in os.getenv("LOAD_NODES_COUNT", "1").split(",")]
STORAGE_NODE_COUNT = [int(s) for s in os.getenv("STORAGE_NODE_COUNT", "4").split(",")]
CONTAINER_PLACEMENT_POLICY = os.getenv(
    "CONTAINER_PLACEMENT_POLICY", "REP 1 IN X CBF 1 SELECT 1  FROM * AS X"
)
