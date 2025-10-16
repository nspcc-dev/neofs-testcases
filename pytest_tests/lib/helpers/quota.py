import re

from neofs_env.neofs_env_test_base import TestNeofsBase
from neofs_testlib.cli import NeofsAdm
from neofs_testlib.env.env import NodeWallet


class TestQuotaBase(TestNeofsBase):
    def _check_soft_quota_warning_in_logs(self, cid: str, start_line: int = 0, expect_warning: bool = True):
        with open(self.neofs_env.storage_nodes[0].stderr) as sn_node_logs:
            lines = sn_node_logs.readlines()
            lines_to_check = lines[start_line:] if start_line > 0 else lines
            found_soft_quota_limit_warning = False
            for line in lines_to_check:
                if "soft quota limit has been reached" in line and cid in line:
                    found_soft_quota_limit_warning = True
                    break

            if expect_warning:
                assert found_soft_quota_limit_warning, (
                    f"Not found soft quota warning in sn logs: {self.neofs_env.storage_nodes[0].stderr}"
                )
            else:
                assert not found_soft_quota_limit_warning, (
                    f"Unexpected soft quota warning found in sn logs: {self.neofs_env.storage_nodes[0].stderr}"
                )

    def _get_log_line_count(self) -> int:
        with open(self.neofs_env.storage_nodes[0].stderr, "rb") as sn_node_logs:
            return sum(1 for _ in sn_node_logs)

    def get_container_quota(self, wallet: NodeWallet, cid: str) -> dict:
        neofs_adm: NeofsAdm = self.neofs_env.neofs_adm()

        raw_container_quota_output = neofs_adm.fschain.container_quota(
            rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
            cid=cid,
            wallet=wallet.path,
            wallet_password=wallet.password,
        ).stdout

        pattern = re.compile(
            r"^(?P<cid>\S+)\s+container quotas:\s*"
            r"Soft limit:\s*(?P<soft>\d+)\s*"
            r"Hard limit:\s*(?P<hard>\d+)",
            re.MULTILINE,
        )

        match = pattern.search(raw_container_quota_output)
        if not match:
            raise ValueError("Output format not recognized")

        return {
            "cid": match.group("cid"),
            "soft": int(match.group("soft")),
            "hard": int(match.group("hard")),
        }

    def get_and_verify_container_quota(self, wallet: NodeWallet, cid: str, expected_soft: int, expected_hard: int):
        quota_dict = self.get_container_quota(wallet, cid)
        assert quota_dict["cid"] == cid, f"Invalid cid, expected: {cid}, got: {quota_dict['cid']}"
        assert quota_dict["soft"] == expected_soft, (
            f"Invalid soft quota, expected: {expected_soft}, got: {quota_dict['soft']}"
        )
        assert quota_dict["hard"] == expected_hard, (
            f"Invalid hard quota, expected: {expected_hard}, got: {quota_dict['hard']}"
        )
