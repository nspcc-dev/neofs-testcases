import binascii
import hashlib
import hmac
import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from urllib.parse import quote, unquote

import requests
import yaml


@dataclass
class SberCloudConfig:
    access_key_id: Optional[str] = None
    secret_key: Optional[str] = None
    ecs_endpoint: Optional[str] = None
    project_id: Optional[str] = None

    @staticmethod
    def from_dict(config_dict: dict) -> 'SberCloudConfig':
        return SberCloudConfig(**config_dict)

    @staticmethod
    def from_yaml(config_path: str) -> 'SberCloudConfig':
        with open(config_path) as file:
            config_dict = yaml.load(file, Loader=yaml.FullLoader)
        return SberCloudConfig.from_dict(config_dict["sbercloud"])

    @staticmethod
    def from_env() -> 'SberCloudConfig':
        config_dict = {
            "access_key_id": os.getenv("SBERCLOUD_ACCESS_KEY_ID"),
            "secret_key": os.getenv("SBERCLOUD_SECRET_KEY"),
            "ecs_endpoint": os.getenv("SBERCLOUD_ECS_ENDPOINT"),
            "project_id": os.getenv("SBERCLOUD_PROJECT_ID"),
        }
        return SberCloudConfig.from_dict(config_dict)


class SberCloudAuthRequests:
    """
    Implements authentication mechanism with access key+secret key in accordance with:
    https://support.hc.sbercloud.ru/devg/apisign/api-sign-algorithm.html

    endpoint - represents endpoint of a specific service (listed at https://support.hc.sbercloud.ru/en-us/endpoint/index.html)
    base_path - is prefix for all request path's that will be sent via this instance.
    """

    ENCODING = "utf-8"
    ALGORITHM = "SDK-HMAC-SHA256"
    TIMESTAMP_FORMAT = "%Y%m%dT%H%M%SZ"

    def __init__(self, endpoint: str, access_key_id: str, secret_key: str, base_path: str = "") -> None:
        self.endpoint = endpoint
        self.base_path = base_path
        self.access_key_id = access_key_id
        self.secret_key = secret_key

    def get(self, path: str, query: Optional[dict] = None) -> requests.Response:
        return self._send_request("GET", path, query, content="")

    def post(self, path: str, query: Optional[dict] = None,
             data: Optional[dict] = None) -> requests.Response:
        content = json.dumps(data) if data else ""
        return self._send_request("POST", path, query, content)

    def _send_request(self, method: str, path: str, query: Optional[dict],
                      content: str) -> requests.Response:
        body = content.encode(self.ENCODING)
        if self.base_path:
            path = self.base_path + path

        timestamp = datetime.strftime(datetime.utcnow(), self.TIMESTAMP_FORMAT)
        headers = self._build_original_headers(timestamp, body)

        signed_headers = self._build_signed_headers(headers)
        canonical_request = self._build_canonical_request(method, path, query, body, headers,
                                                          signed_headers)
        signature = self._build_signature(timestamp, canonical_request)
        headers["Authorization"] = self._build_authorization_header(signature, signed_headers)

        query_string = "?" + self._build_canonical_query_string(query) if query else ""
        url = f"https://{self.endpoint}{path}{query_string}"

        response = requests.request(method, url, headers=headers, data=body)
        if response.status_code < 200 or response.status_code >= 300:
            raise AssertionError(f"Request to url={url} failed: status={response.status_code} "
                                 f"response={response.text})")
        return response

    def _build_original_headers(self, timestamp: str, body: bytes) -> dict[str, str]:
        headers = {}
        headers["X-Sdk-Date"] = timestamp
        headers["host"] = self.endpoint

        if body:
            headers["Content-Type"] = "application/json"
            headers["content-length"] = str(len(body))

        return headers

    def _build_signed_headers(self, headers: dict[str, str]) -> list[str]:
        return sorted(header_name.lower() for header_name in headers)

    def _build_canonical_request(self, method: str, path: str, query: Optional[dict], body: bytes,
                                 headers: dict[str, str], signed_headers: list[str]) -> str:
        canonical_headers = self._build_canonical_headers(headers, signed_headers)
        body_hash = self._calc_sha256_hash(body)
        canonical_url = self._build_canonical_url(path)
        canonical_query_string = self._build_canonical_query_string(query)

        return "\n".join([
            method.upper(),
            canonical_url,
            canonical_query_string,
            canonical_headers,
            ";".join(signed_headers),
            body_hash
        ])

    def _build_canonical_headers(self, headers: dict[str, str], signed_headers: list[str]) -> str:
        normalized_headers = {}
        for key, value in headers.items():
            normalized_key = key.lower()
            normalized_value = value.strip()
            normalized_headers[normalized_key] = normalized_value
            # Re-encode header in request itself
            headers[key] = normalized_value.encode(self.ENCODING).decode("iso-8859-1")

        # Join headers in the same order as they are sorted in signed_headers list
        joined_headers = "\n".join(f"{key}:{normalized_headers[key]}" for key in signed_headers)
        return joined_headers + "\n"

    def _calc_sha256_hash(self, value: bytes) -> str:
        sha256 = hashlib.sha256()
        sha256.update(value)
        return sha256.hexdigest()

    def _build_canonical_url(self, path: str) -> str:
        path_parts = unquote(path).split("/")
        canonical_url = "/".join(quote(path_part) for path_part in path_parts)

        if not canonical_url.endswith("/"):
            canonical_url += "/"
        return canonical_url

    def _build_canonical_query_string(self, query: Optional[dict]) -> str:
        if not query:
            return ""

        key_value_pairs = []
        for key in sorted(query.keys()):
            # NOTE: we do not support list values, as they are not used in API at the moment
            encoded_key = quote(key)
            encoded_value = quote(str(query[key]))
            key_value_pairs.append(f"{encoded_key}={encoded_value}")
        return "&".join(key_value_pairs)

    def _build_signature(self, timestamp: str, canonical_request: str) -> str:
        canonical_request_hash = self._calc_sha256_hash(canonical_request.encode(self.ENCODING))
        string_to_sign = f"{self.ALGORITHM}\n{timestamp}\n{canonical_request_hash}"

        hmac_digest = hmac.new(
            key=self.secret_key.encode(self.ENCODING),
            msg=string_to_sign.encode(self.ENCODING),
            digestmod=hashlib.sha256
        ).digest()
        signature = binascii.hexlify(hmac_digest).decode()

        return signature

    def _build_authorization_header(self, signature: str, signed_headers: list[str]) -> str:
        joined_signed_headers = ";".join(signed_headers)
        return f"{self.ALGORITHM} Access={self.access_key_id}, SignedHeaders={joined_signed_headers}, Signature={signature}"


class SberCloud:
    """
    Manages resources in Sbercloud via API.

    API reference:
    https://docs.sbercloud.ru/terraform/ug/topics/quickstart.html
    https://support.hc.sbercloud.ru/en-us/api/ecs/en-us_topic_0020212668.html
    """
    def __init__(self, config: SberCloudConfig) -> None:
        self.ecs_requests = SberCloudAuthRequests(
            endpoint=config.ecs_endpoint,
            base_path=f"/v1/{config.project_id}/cloudservers",
            access_key_id=config.access_key_id,
            secret_key=config.secret_key,
        )
        self.ecs_nodes = []  # Cached list of ecs servers

    def find_ecs_node_by_ip(self, ip: str, no_cache: bool = False) -> str:
        if not self.ecs_nodes or no_cache:
            self.ecs_nodes = self.get_ecs_nodes()
        nodes_by_ip = [
            node for node in self.ecs_nodes
            if ip in [
                node_ip['addr']
                for node_ips in node['addresses'].values()
                for node_ip in node_ips
            ]
        ]
        assert len(nodes_by_ip) == 1
        return nodes_by_ip[0]['id']

    def get_ecs_nodes(self) -> list[dict]:
        response = self.ecs_requests.get("/detail").json()
        return response["servers"]

    def start_node(self, node_id: Optional[str] = None, node_ip: Optional[str] = None) -> None:
        data = {
            'os-start': {
                'servers': [
                    {
                        'id': node_id or self.find_ecs_node_by_ip(node_ip)
                    }
                ]
            }
        }
        self.ecs_requests.post("/action", data=data)

    def stop_node(self, node_id: Optional[str] = None, node_ip: Optional[str] = None,
                  hard: bool = False) -> None:
        data = {
            'os-stop': {
                'type': 'HARD' if hard else 'SOFT',
                'servers': [
                    {
                        'id': node_id or self.find_ecs_node_by_ip(node_ip)
                    }
                ]
            }
        }
        self.ecs_requests.post("/action", data=data)
