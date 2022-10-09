import logging
import re

from common import NEOFS_ADM_EXEC, NEOFS_CLI_EXEC, WALLET_CONFIG
from neofs_testlib.cli import NeofsAdm, NeofsCli
from neofs_testlib.hosting import Hosting
from neofs_testlib.shell import Shell

logger = logging.getLogger("NeoLogger")


def get_local_binaries_versions(shell: Shell) -> dict[str, str]:
    versions = {}

    for binary in ["neo-go", "neofs-authmate"]:
        out = shell.exec(f"{binary} --version").stdout
        versions[binary] = _parse_version(out)

    neofs_cli = NeofsCli(shell, NEOFS_CLI_EXEC, WALLET_CONFIG)
    versions["neofs-cli"] = _parse_version(neofs_cli.version.get())

    try:
        neofs_adm = NeofsAdm(shell, NEOFS_ADM_EXEC)
        versions["neofs-adm"] = _parse_version(neofs_adm.version.get())
    except RuntimeError:
        logger.info(f"neofs-adm not installed")

    out = shell.exec("aws --version").stdout
    out_lines = out.split("\n")
    versions["AWS"] = out_lines[0] if out_lines else "Unknown"

    return versions


def get_remote_binaries_versions(hosting: Hosting) -> dict[str, str]:
    versions_by_host = {}
    for host in hosting.hosts:
        binaries = []
        for service_config in host.config.services:
            exec_path = service_config.attributes.get("exec_path")
            if exec_path:
                binaries.append(exec_path)
        for cli_config in host.config.clis:
            binaries.append(cli_config.exec_path)

        shell = host.get_shell()
        versions_at_host = {}
        for binary in binaries:
            try:
                result = shell.exec(f"{binary} --version")
                versions_at_host[service_config.name] = _parse_version(result.stdout)
            except Exception as exc:
                logger.error(f"Cannot get version for {exec_path} because of\n{exc}")
                versions_at_host[service_config.name] = "Unknown"
                continue
        versions_by_host[host.config.address] = versions_at_host

    # Consolidate versions across all hosts
    versions = {}
    for host, binary_versions in versions_by_host.items():
        for name, version in binary_versions.items():
            captured_version = versions.get(name)
            if captured_version:
                assert captured_version == version, f"Binary {name} has inconsistent version on host {host}"
            else:
                versions[name] = version
    return versions


def _parse_version(version_output: str) -> str:
    version = re.search(r"version[:\s]*v?(.+)", version_output, re.IGNORECASE)
    return version.group(1).strip() if version else "Unknown"
