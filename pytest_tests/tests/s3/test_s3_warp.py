import os
from pathlib import Path

import allure
import jinja2
import pytest
from s3.s3_base import TestNeofsS3Base


def pytest_generate_tests(metafunc):
    if "s3_client" in metafunc.fixturenames:
        metafunc.parametrize("s3_client", ["boto3"], indirect=True)


class TestS3Warp(TestNeofsS3Base):
    @pytest.fixture(scope="class")
    @allure.title("Redeploy s3 gw without tls")
    def s3_gw_without_tls(self):
        self.neofs_env.s3_gw.stop()
        self.neofs_env.s3_gw.tls_enabled = False
        self.neofs_env.s3_gw.start(fresh=False)
        yield
        self.neofs_env.s3_gw.stop()
        self.neofs_env.s3_gw.tls_enabled = True
        self.neofs_env.s3_gw.start(fresh=False)

    @pytest.fixture(scope="class")
    def warp_config(self, temp_directory: str):
        jinja_env = jinja2.Environment()
        config_template = Path(f"{os.getcwd()}/pytest_tests/data/warp/get.yml").read_text()
        jinja_template = jinja_env.from_string(config_template)
        rendered_config = jinja_template.render(
            region="us-east-1",
            access_key=self.access_key_id,
            secret_key=self.secret_access_key,
            host=self.neofs_env.s3_gw.address,
        )
        resulted_config_path = f"{temp_directory}/warp_config"
        with open(resulted_config_path, mode="w") as fp:
            fp.write(rendered_config)
        yield resulted_config_path

    @allure.title("Test S3: Warp Get Benchmark")
    def test_s3_warp_get(self, s3_gw_without_tls, warp_config: str):
        result = self.neofs_env.shell.exec(f"./warp run {warp_config}")
        assert "ERROR" not in result.stderr, "Errors in warp stderr"
        assert "Errors" not in result.stdout, "Errors in warp stdout"
