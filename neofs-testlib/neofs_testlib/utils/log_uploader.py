import base64
import logging
import os
import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from shutil import which
from typing import Optional

logger = logging.getLogger("neofs.testlib.env")

DEFAULT_NEOFS_DOMAIN = "st1.storage.fs.neo.org"
DEFAULT_NEOFS_HTTP_GATE = "rest.fs.neo.org"


@dataclass
class NeofsConfig:
    container_id: str
    wallet_b64: str
    wallet_password: str
    domain: str = DEFAULT_NEOFS_DOMAIN
    http_gate: str = DEFAULT_NEOFS_HTTP_GATE
    lifetime: int = 0
    pipeline_id: Optional[str] = None

    @classmethod
    def from_env(cls) -> Optional["NeofsConfig"]:
        container_id = os.environ.get("NEOFS_LOGS_CID")
        wallet_b64 = os.environ.get("NEOFS_LOGS_WALLET_B64")
        wallet_password = os.environ.get("NEOFS_LOGS_WALLET_PASSWORD")

        if not (container_id and wallet_b64 and wallet_password):
            return None

        try:
            lifetime = int(os.environ.get("NEOFS_LOGS_LIFETIME", "0") or 0)
        except ValueError:
            lifetime = 0

        return cls(
            container_id=container_id,
            wallet_b64=wallet_b64,
            wallet_password=wallet_password,
            domain=os.environ.get("NEOFS_LOGS_DOMAIN") or DEFAULT_NEOFS_DOMAIN,
            http_gate=os.environ.get("NEOFS_LOGS_HTTP_GATE") or DEFAULT_NEOFS_HTTP_GATE,
            lifetime=lifetime,
            pipeline_id=os.environ.get("NEOFS_LOGS_PIPELINE_ID") or None,
        )


class NeofsUploader:
    UPLOAD_RETRIES = 5
    UPLOAD_RETRY_DELAY = 5
    UPLOAD_TIMEOUT = 600

    def __init__(self, config: NeofsConfig, cli_path: Optional[str] = None, work_dir: Optional[str] = None):
        self.config = config
        self.work_dir = Path(work_dir) if work_dir else Path.cwd()
        self._cli_path: Optional[str] = cli_path if cli_path and Path(cli_path).exists() else None
        self._wallet_path: Optional[str] = None

    def _run(self, cmd: str, timeout: int, env: Optional[dict] = None) -> str:
        result = subprocess.run(
            cmd,
            shell=True,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            env=env,
        )
        return result.stdout.strip()

    def _ensure_cli(self) -> str:
        if self._cli_path:
            return self._cli_path

        existing = which("neofs-cli")
        if existing:
            self._cli_path = existing
            return existing

        neofs_dir = self.work_dir / "neofs"
        neofs_dir.mkdir(parents=True, exist_ok=True)
        cli_path = neofs_dir / "neofs-cli"

        if not cli_path.exists():
            logger.info("Downloading neofs-cli for NeoFS upload")
            from neofs_testlib.env.env import NeoFSEnv

            cli_params = NeoFSEnv._generate_default_neofs_env_config()["binaries"]["neofs_cli"]
            NeoFSEnv.download_binary(cli_params["repo"], cli_params["version"], cli_params["file"], str(cli_path))

        self._cli_path = str(cli_path)
        return self._cli_path

    def _ensure_wallet(self) -> str:
        if self._wallet_path and Path(self._wallet_path).exists():
            return self._wallet_path

        wallet_path = self.work_dir / "neofs_upload_wallet.json"
        wallet_path.write_bytes(base64.b64decode(self.config.wallet_b64))
        self._wallet_path = str(wallet_path)
        return self._wallet_path

    def _current_epoch(self, cli: str) -> int:
        out = self._run(
            f"{cli} netmap epoch --rpc-endpoint grpcs://{self.config.domain}:8082",
            timeout=60,
        )
        return int(out)

    def public_url(self, neofs_path: str) -> str:
        return f"https://{self.config.http_gate}/{self.config.container_id}/{neofs_path}"

    def upload_file(self, local_path: str, neofs_path: str, content_type: Optional[str] = None) -> str:
        cli = self._ensure_cli()
        wallet = self._ensure_wallet()

        expire_at: Optional[int] = None
        if self.config.lifetime > 0:
            expire_at = self._current_epoch(cli) + self.config.lifetime
            logger.info(f"NeoFS object will expire at epoch {expire_at}")

        attrs = f"FilePath={neofs_path}"
        if content_type:
            attrs += f",Content-Type={content_type}"

        cmd = (
            f"{cli} --rpc-endpoint grpcs://{self.config.domain}:8082 --wallet {wallet} "
            f"object put --cid {self.config.container_id} --timeout {self.UPLOAD_TIMEOUT}s "
            f"--file {local_path} --attributes '{attrs}'"
        )
        if expire_at:
            cmd += f" --expire-at {expire_at}"

        env = dict(os.environ, NEOFS_CLI_PASSWORD=self.config.wallet_password)

        last_error: Optional[Exception] = None
        for attempt in range(1, self.UPLOAD_RETRIES + 1):
            try:
                logger.info(f"Uploading {local_path} to NeoFS path {neofs_path} (attempt {attempt})")
                self._run(cmd, timeout=self.UPLOAD_TIMEOUT, env=env)
                return self.public_url(neofs_path)
            except Exception as exc:
                last_error = exc
                logger.warning(f"NeoFS upload attempt {attempt}/{self.UPLOAD_RETRIES} failed: {exc}")
                if attempt < self.UPLOAD_RETRIES:
                    time.sleep(self.UPLOAD_RETRY_DELAY)

        raise RuntimeError(f"Failed to upload {local_path} to NeoFS after {self.UPLOAD_RETRIES} attempts: {last_error}")


def build_logs_neofs_path(config: Optional[NeofsConfig], env_id: str, name: str) -> str:
    pipeline_id = (config.pipeline_id if config else None) or "local"
    safe_name = name.replace(" ", "_").replace("/", "_")
    unique = f"{int(time.time())}-{uuid.uuid4().hex[:8]}"
    return f"neofs-env-logs/{pipeline_id}/{env_id}/{unique}/{safe_name}.zip"
