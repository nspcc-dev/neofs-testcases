import os

import allure
import pytest
from python_keywords.container import list_containers

from steps import s3_gate_bucket
from steps.aws_cli_client import AwsCliClient


class TestS3GateBase:
    s3_client = None

    @pytest.fixture(scope="class", autouse=True)
    @allure.title("[Class/Autouse]: Create S3 client")
    def s3_client(self, prepare_wallet_and_deposit, request):
        wallet = prepare_wallet_and_deposit
        s3_bearer_rules_file = f"{os.getcwd()}/robot/resources/files/s3_bearer_rules.json"

        (
            cid,
            bucket,
            access_key_id,
            secret_access_key,
            owner_private_key,
        ) = s3_gate_bucket.init_s3_credentials(wallet, s3_bearer_rules_file=s3_bearer_rules_file)
        containers_list = list_containers(wallet)
        assert cid in containers_list, f"Expected cid {cid} in {containers_list}"

        if request.param == "aws cli":
            try:
                client = AwsCliClient(access_key_id, secret_access_key)
            except Exception as err:
                if "command was not found or was not executable" in str(err):
                    pytest.skip("AWS CLI was not found")
                else:
                    raise RuntimeError("Error on creating instance for AwsCliClient") from err
        else:
            client = s3_gate_bucket.config_s3_client(access_key_id, secret_access_key)
        TestS3GateBase.s3_client = client
        TestS3GateBase.wallet = wallet
