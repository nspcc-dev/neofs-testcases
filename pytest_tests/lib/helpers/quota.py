import re

from neofs_env.neofs_env_test_base import TestNeofsBase
from neofs_testlib.cli import NeofsAdm
from neofs_testlib.env.env import NodeWallet
from tenacity import retry, stop_after_delay, wait_fixed


class TestQuotaBase(TestNeofsBase):
    _REPORT_PATTERN = re.compile(
        r"Reporter's pubic Key: (?P<key>[a-f0-9]+):\s+"
        r"Size: (?P<size>\d+)\s+"
        r"Objects: (?P<objects>\d+)\s+"
        r"Update epoch: (?P<epoch>\d+)",
        re.MULTILINE,
    )

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

    @retry(stop=stop_after_delay(10), wait=wait_fixed(1), reraise=True)
    def wait_until_quota_values_reported(
        self,
        cid: str,
        previous_report: str = None,
        expected_objects: int = None,
    ) -> str:
        neofs_adm = self.neofs_env.neofs_adm()
        report_output = neofs_adm.fschain.load_report(
            cid=cid,
            rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
        ).stdout

        if previous_report:
            if not self._has_report_been_updated(previous_report, report_output):
                raise AssertionError(
                    f"Report has not been updated yet. "
                    f"Previous report:\n{previous_report}\nCurrent report:\n{report_output}"
                )

        self._validate_quota_values(report_output, expected_objects, previous_report)
        return report_output

    def _has_report_been_updated(self, previous_report: str, current_report: str) -> bool:
        previous_reports = {}
        for match in self._REPORT_PATTERN.finditer(previous_report):
            key = match.group("key")
            previous_reports[key] = {
                "size": int(match.group("size")),
                "objects": int(match.group("objects")),
                "epoch": int(match.group("epoch")),
            }

        current_reports = {}
        for match in self._REPORT_PATTERN.finditer(current_report):
            key = match.group("key")
            current_reports[key] = {
                "size": int(match.group("size")),
                "objects": int(match.group("objects")),
                "epoch": int(match.group("epoch")),
            }

        if set(current_reports.keys()) != set(previous_reports.keys()):
            return True

        for key, prev_data in previous_reports.items():
            curr_data = current_reports[key]
            if (
                prev_data["size"] != curr_data["size"]
                or prev_data["objects"] != curr_data["objects"]
                or prev_data["epoch"] != curr_data["epoch"]
            ):
                return True

        return False

    def _validate_quota_values(
        self, report_output: str, expected_objects: int = None, previous_report: str = None
    ) -> None:
        if expected_objects is None:
            return

        reports = list(self._REPORT_PATTERN.finditer(report_output))
        if not reports:
            raise AssertionError(f"No valid reports found in output:\n{report_output}")

        current_reports = {}
        for match in reports:
            key = match.group("key")
            current_reports[key] = {
                "size": int(match.group("size")),
                "objects": int(match.group("objects")),
                "epoch": int(match.group("epoch")),
            }

        current_total = sum(r["objects"] for r in current_reports.values())

        if current_total != expected_objects:
            object_counts = [r["objects"] for r in current_reports.values()]
            error_msg = (
                f"Expected {expected_objects} objects total, but found {current_total} objects "
                f"across {len(reports)} report(s). Object counts per report: {object_counts}"
            )

            if previous_report:
                prev_reports_list = list(self._REPORT_PATTERN.finditer(previous_report))
                previous_reports = {}
                for match in prev_reports_list:
                    key = match.group("key")
                    previous_reports[key] = {
                        "size": int(match.group("size")),
                        "objects": int(match.group("objects")),
                        "epoch": int(match.group("epoch")),
                    }
                prev_total = sum(r["objects"] for r in previous_reports.values())
                error_msg += f" Previous total: {prev_total} objects across {len(previous_reports)} report(s)."

            raise AssertionError(error_msg)

    def get_quota_report(self, cid: str) -> str:
        neofs_adm = self.neofs_env.neofs_adm()
        return neofs_adm.fschain.load_report(
            cid=cid,
            rpc_endpoint=f"http://{self.neofs_env.fschain_rpc}",
        ).stdout
