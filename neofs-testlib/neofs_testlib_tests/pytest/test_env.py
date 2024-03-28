import json
import os
import re
import uuid
from time import sleep

import boto3
import pexpect
import pytest
import requests
from botocore.config import Config

from neofs_testlib.env.env import NeoFSEnv, NodeWallet
from neofs_testlib.utils.wallet import get_last_public_key_from_wallet, init_wallet


def _run_with_passwd(cmd: str, password: str) -> str:
    child = pexpect.spawn(cmd)
    child.delaybeforesend = 1
    child.expect(".*")
    child.sendline(f"{password}\r")
    child.wait()
    cmd = child.read()
    return cmd.decode()


@pytest.fixture
def neofs_env(request):
    if request.config.getoption("--load-env"):
        neofs_env = NeoFSEnv.load(request.config.getoption("--load-env"))
    else:
        neofs_env = NeoFSEnv.simple()

    yield neofs_env

    if request.config.getoption("--persist-env"):
        neofs_env.persist()
    else:
        if not request.config.getoption("--load-env"):
            neofs_env.kill()


@pytest.fixture
def wallet() -> NodeWallet:
    wallet_name = f"{str(uuid.uuid4())}.json"
    wallet_path = os.path.join(os.getcwd(), wallet_name)
    wallet_password = "password"
    wallet_address = init_wallet(wallet_path, wallet_password)
    return NodeWallet(path=wallet_path, address=wallet_address, password=wallet_password)


@pytest.fixture
def s3_creds(neofs_env: NeoFSEnv, zero_fee, wallet: NodeWallet) -> tuple:
    bucket = str(uuid.uuid4())
    s3_bearer_rules = "pytest_tests/s3_bearer_rules.json"

    gate_public_key = get_last_public_key_from_wallet(
        neofs_env.s3_gw.wallet.path, neofs_env.s3_gw.wallet.password
    )
    cmd = (
        f"{neofs_env.neofs_s3_authmate_path} --debug --with-log --timeout 1m "
        f"issue-secret --wallet {wallet.path} --gate-public-key={gate_public_key} "
        f"--peer {neofs_env.storage_nodes[0].endpoint} --container-friendly-name {bucket} "
        f"--bearer-rules {s3_bearer_rules} --container-placement-policy 'REP 1' "
        f"--container-policy container_policy.json"
    )
    output = _run_with_passwd(cmd, wallet.password)

    # output contains some debug info and then several JSON structures, so we find each
    # JSON structure by curly brackets (naive approach, but works while JSON is not nested)
    # and then we take JSON containing secret_access_key
    json_blocks = re.findall(r"\{.*?\}", output, re.DOTALL)
    for json_block in json_blocks:
        parsed_json_block = json.loads(json_block)
        if "secret_access_key" in parsed_json_block:
            return (
                parsed_json_block["container_id"],
                bucket,
                parsed_json_block["access_key_id"],
                parsed_json_block["secret_access_key"],
                parsed_json_block["owner_private_key"],
            )
    raise AssertionError("Can't get s3 creds")


@pytest.fixture
def zero_fee(neofs_env: NeoFSEnv):
    neofs_env.neofs_adm().morph.set_config(
        rpc_endpoint=f"http://{neofs_env.morph_rpc}",
        alphabet_wallets=neofs_env.alphabet_wallets_dir,
        post_data=f"ContainerFee=0 ContainerAliasFee=0",
    )


def test_s3_gw_put_get(neofs_env: NeoFSEnv, s3_creds, wallet: NodeWallet):
    (
        cid,
        bucket,
        access_key_id,
        secret_access_key,
        _,
    ) = s3_creds

    cli = neofs_env.neofs_cli(neofs_env.generate_cli_config(wallet))
    result = cli.container.list(rpc_endpoint=neofs_env.sn_rpc, wallet=wallet.path)
    containers_list = result.stdout.split()
    assert cid in containers_list, f"Expected cid {cid} in {containers_list}"

    session = boto3.Session()
    config = Config(
        retries={
            "max_attempts": 1,
            "mode": "standard",
        }
    )

    s3_client = session.client(
        service_name="s3",
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        config=config,
        endpoint_url=f"https://{neofs_env.s3_gw.address}",
        verify=False,
    )

    bucket_name = str(uuid.uuid4())
    params = {"Bucket": bucket_name, "CreateBucketConfiguration": {"LocationConstraint": "rep-1"}}
    s3_client.create_bucket(**params)
    sleep(5)

    filename = neofs_env._generate_temp_file()

    with open(filename, "w") as file:
        file.write("123456789")

    with open(filename, "rb") as file:
        file_content = file.read()

    filekey = os.path.basename(filename)
    s3_client.put_object(**{"Body": file_content, "Bucket": bucket_name, "Key": filekey})
    s3_client.get_object(**{"Bucket": bucket_name, "Key": filekey})

@pytest.mark.parametrize("gw_type", ["HTTP", "REST"])
def test_gateways_put_get(neofs_env: NeoFSEnv, wallet: NodeWallet, zero_fee, gw_type):
    cli = neofs_env.neofs_cli(neofs_env.generate_cli_config(wallet))

    result = cli.container.create(
        rpc_endpoint=neofs_env.sn_rpc,
        wallet=wallet.path,
        policy="REP 1 IN X CBF 1 SELECT 1 FROM * AS X",
        basic_acl="0FBFBFFF",
        await_mode=True,
    )

    lines = result.stdout.split("\n")
    for line in lines:
        if line.startswith("container ID:"):
            cid = line.split(": ")[1]

    result = cli.container.list(rpc_endpoint=neofs_env.sn_rpc, wallet=wallet.path)
    containers = result.stdout.split()
    assert cid in containers

    filename = neofs_env._generate_temp_file()

    with open(filename, "w") as file:
        file.write("123456789")

    if gw_type == "HTTP":
        request = f"http://{neofs_env.http_gw.address}/upload/{cid}"
    else:
        request = f"http://{neofs_env.rest_gw.address}/v1/upload/{cid}"
    files = {"upload_file": open(filename, "rb")}
    body = {"filename": filename}
    resp = requests.post(request, files=files, data=body)

    if not resp.ok:
        raise Exception(
            f"""Failed to get object via {gw_type} gate:
                request: {resp.request.path_url},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}"""
        )

    oid = resp.json().get("object_id")

    download_attribute = "?download=true"
    if gw_type == "HTTP":
        request = f"http://{neofs_env.http_gw.address}/get/{cid}/{oid}{download_attribute}"
    else:
        request = f"http://{neofs_env.rest_gw.address}/v1/get/{cid}/{oid}{download_attribute}"

    resp = requests.get(request, stream=True)

    if not resp.ok:
        raise Exception(
            f"""Failed to get object via {gw_type} gate:
                request: {resp.request.path_url},
                response: {resp.text},
                status code: {resp.status_code} {resp.reason}"""
        )


def test_node_metabase_resync(neofs_env: NeoFSEnv, wallet: NodeWallet, zero_fee):
    for node in neofs_env.storage_nodes:
        node.set_metabase_resync(True)
        node.set_metabase_resync(False)
    test_http_gw_put_get(neofs_env, wallet, zero_fee)


@pytest.mark.parametrize("data_type", ["meta", "all"])
def test_node_delete_metadata_and_data(neofs_env: NeoFSEnv, wallet: NodeWallet, zero_fee, data_type: str):
    for node in neofs_env.storage_nodes:
        if data_type == "meta":
            node.set_metabase_resync(True)
            node.delete_metadata()
        else:
            node.delete_data()
        node.start(fresh=False)
    test_http_gw_put_get(neofs_env, wallet, zero_fee)
    