import logging
import uuid

import allure
import pytest
from helpers.acl import (
    EACLAccess,
    EACLOperation,
    EACLRole,
    EACLRule,
    create_eacl,
    set_eacl,
    wait_for_cache_expired,
)
from helpers.container import create_container
from helpers.file_helper import generate_file
from helpers.rest_gate import (
    complete_bearer_token,
    get_rest_gateway_address,
    get_unsigned_bearer_token,
    get_via_rest_gate,
    head_via_rest_gate,
    upload_via_rest_gate,
)
from helpers.rest_gate import (
    create_container as rest_create_container,
)
from helpers.wellknown_acl import PUBLIC_ACL
from neofs_testlib.env.env import NodeWallet
from neofs_testlib.utils.converters import load_wallet
from rest_gw.rest_base import TestNeofsRestBase
from rest_gw.rest_utils import (
    generate_bearer_token_v2,
    generate_session_token_v2,
    get_object_and_verify_hashes,
    sign_bearer_token_v2,
)

logger = logging.getLogger("NeoLogger")


def _unique_container_name() -> str:
    return f"rest_bearer_v2_{uuid.uuid4()}"


_ALL_OBJECT_OPERATIONS = ("GET", "HEAD", "PUT", "DELETE", "SEARCH", "RANGE")

_ALL_OBJECT_OPERATIONS_EACL = (
    EACLOperation.GET,
    EACLOperation.HEAD,
    EACLOperation.PUT,
    EACLOperation.DELETE,
    EACLOperation.SEARCH,
    EACLOperation.GET_RANGE,
)


def _eacl_record_for_others(operation: str, action: str) -> dict:
    return {
        "operation": operation,
        "action": action,
        "filters": [],
        "targets": [{"role": "OTHERS"}],
    }


def _scoped_records_for_others(allowed: list[str]) -> list[dict]:
    deny_ops = [op for op in _ALL_OBJECT_OPERATIONS if op not in allowed]
    return [_eacl_record_for_others(op, "DENY") for op in deny_ops] + [
        _eacl_record_for_others(op, "ALLOW") for op in allowed
    ]


def _assert_get_denied(cid: str, oid: str, endpoint: str, headers: dict | None = None) -> None:
    resp = get_via_rest_gate(
        cid,
        oid,
        endpoint,
        headers=headers,
        expect_error=True,
        skip_options_verify=True,
    )
    assert not resp.ok, f"GET should have been denied: {resp.status_code} {resp.text}"
    assert "denied" in resp.text.lower(), f"Unexpected error message for denied GET: {resp.text}"


def _assert_head_denied(cid: str, oid: str, endpoint: str, headers: dict | None = None) -> None:
    resp = head_via_rest_gate(cid, oid, endpoint, headers=headers, expect_error=True)
    assert not resp.ok, f"HEAD should have been denied: {resp.status_code}"


class TestRestBearerV2(TestNeofsRestBase):
    PLACEMENT_RULE = "REP 2 IN X CBF 1 SELECT 2 FROM * AS X"

    @pytest.fixture(scope="class", autouse=True)
    @allure.title("[Class/Autouse]: Prepare wallets")
    def prepare_wallets(self, default_wallet: NodeWallet, user_wallet: NodeWallet, stranger_wallet: NodeWallet):
        TestRestBearerV2.owner_wallet = default_wallet
        TestRestBearerV2.user_wallet = user_wallet
        TestRestBearerV2.stranger_wallet = stranger_wallet

    @pytest.fixture(scope="class")
    def user_container(self) -> str:
        return create_container(
            wallet=self.owner_wallet.path,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            rule=self.PLACEMENT_RULE,
            basic_acl=PUBLIC_ACL,
        )

    @pytest.fixture(scope="class")
    def eacl_deny_for_others(self, user_container: str) -> None:
        with allure.step(f"Set deny ALL object operations for {EACLRole.OTHERS} via eACL"):
            rules = [
                EACLRule(access=EACLAccess.DENY, role=EACLRole.OTHERS, operation=op)
                for op in _ALL_OBJECT_OPERATIONS_EACL
            ]
            set_eacl(
                self.owner_wallet.path,
                user_container,
                create_eacl(user_container, rules, shell=self.shell),
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            wait_for_cache_expired()

    @pytest.fixture(scope="class")
    def gw_address(self, gw_endpoint: str) -> str:
        return get_rest_gateway_address(gw_endpoint)

    @allure.title("PUT bypasses eACL deny via NeoFS-Bearer-Token header (scheme={scheme})")
    @pytest.mark.parametrize(
        "scheme,wallet_connect",
        [
            pytest.param("DETERMINISTIC_SHA256", False, id="deterministic"),
            pytest.param("WALLETCONNECT", True, id="walletconnect"),
        ],
    )
    @pytest.mark.simple
    def test_v2_bearer_via_neofs_header(
        self,
        scheme: str,
        wallet_connect: bool,
        user_container: str,
        eacl_deny_for_others,
        gw_address: str,
        gw_endpoint: str,
    ):
        bearer = generate_bearer_token_v2(
            gw_endpoint,
            issuer_wallet=self.owner_wallet,
            records=_scoped_records_for_others(["PUT"]),
            owner=gw_address,
            wallet_connect=wallet_connect,
        )
        bearer_headers = {"NeoFS-Bearer-Token": bearer}

        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        oid = upload_via_rest_gate(
            cid=user_container,
            path=file_path,
            endpoint=gw_endpoint,
            headers=bearer_headers,
        )
        assert oid, f"Upload with v2 bearer ({scheme}) should succeed"

        verify_bearer = generate_bearer_token_v2(
            gw_endpoint,
            issuer_wallet=self.owner_wallet,
            records=_scoped_records_for_others(["GET", "HEAD", "RANGE"]),
            owner=gw_address,
        )

        def _read_via_gw(cid: str, oid: str, endpoint: str) -> str:
            return get_via_rest_gate(
                cid=cid,
                oid=oid,
                endpoint=endpoint,
                headers={"NeoFS-Bearer-Token": verify_bearer},
            )

        get_object_and_verify_hashes(
            oid=oid,
            file_name=file_path,
            wallet=self.owner_wallet.path,
            cid=user_container,
            shell=self.shell,
            nodes=self.neofs_env.storage_nodes,
            endpoint=gw_endpoint,
            object_getter=_read_via_gw,
        )

        with allure.step("GET via the same PUT-only bearer must be denied"):
            _assert_get_denied(user_container, oid, gw_endpoint, headers=bearer_headers)
        with allure.step("HEAD via the same PUT-only bearer must be denied"):
            _assert_head_denied(user_container, oid, gw_endpoint, headers=bearer_headers)

    @allure.title("Bearer accepted via Bearer cookie")
    @pytest.mark.simple
    def test_v2_bearer_via_cookie(
        self,
        user_container: str,
        eacl_deny_for_others,
        gw_address: str,
        gw_endpoint: str,
    ):
        bearer = generate_bearer_token_v2(
            gw_endpoint,
            issuer_wallet=self.owner_wallet,
            records=_scoped_records_for_others(["PUT"]),
            owner=gw_address,
        )

        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        oid = upload_via_rest_gate(
            cid=user_container,
            path=file_path,
            endpoint=gw_endpoint,
            cookies={"Bearer": bearer},
        )
        assert oid, "Upload with v2 bearer cookie should succeed"

        with allure.step("GET via the same PUT-only bearer (in NeoFS-Bearer-Token) must be denied"):
            _assert_get_denied(user_container, oid, gw_endpoint, headers={"NeoFS-Bearer-Token": bearer})

    @allure.title("Bearer accepted via Authorization: Bearer header")
    @pytest.mark.simple
    def test_v2_bearer_via_authorization_header(
        self,
        user_container: str,
        eacl_deny_for_others,
        gw_address: str,
        gw_endpoint: str,
    ):
        bearer = generate_bearer_token_v2(
            gw_endpoint,
            issuer_wallet=self.owner_wallet,
            records=_scoped_records_for_others(["PUT"]),
            owner=gw_address,
        )

        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        oid = upload_via_rest_gate(
            cid=user_container,
            path=file_path,
            endpoint=gw_endpoint,
            headers={"Authorization": f"Bearer {bearer}"},
        )
        assert oid, "Upload with v2 bearer in Authorization header should succeed"

        with allure.step("GET via the same PUT-only bearer must be denied"):
            _assert_get_denied(
                user_container,
                oid,
                gw_endpoint,
                headers={"Authorization": f"Bearer {bearer}"},
            )

    @allure.title("PUT/GET work end-to-end via NeoFS-Bearer-Token header")
    @pytest.mark.simple
    def test_v2_bearer_put_and_get_via_header(
        self,
        user_container: str,
        eacl_deny_for_others,
        gw_address: str,
        gw_endpoint: str,
    ):
        bearer = generate_bearer_token_v2(
            gw_endpoint,
            issuer_wallet=self.owner_wallet,
            records=_scoped_records_for_others(["PUT", "GET"]),
            owner=gw_address,
        )
        headers = {"NeoFS-Bearer-Token": bearer}

        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        oid = upload_via_rest_gate(
            cid=user_container,
            path=file_path,
            endpoint=gw_endpoint,
            headers=headers,
        )
        assert oid, "PUT with PUT/GET-scoped v2 bearer should succeed"

        with allure.step("GET with v2 bearer succeeds (in scope)"):
            resp = get_via_rest_gate(
                user_container,
                oid,
                gw_endpoint,
                headers=headers,
                return_response=True,
                skip_options_verify=True,
            )
            assert resp.ok, f"GET with v2 bearer should succeed: {resp.text}"

        with allure.step("HEAD with the same bearer must be denied (out of scope)"):
            _assert_head_denied(user_container, oid, gw_endpoint, headers=headers)

        with allure.step("HEAD without bearer must be denied (container locks down OTHERS)"):
            _assert_head_denied(user_container, oid, gw_endpoint)

        with allure.step("GET without bearer must be denied (container locks down OTHERS)"):
            _assert_get_denied(user_container, oid, gw_endpoint)

    @allure.title("Bearer + V2 session token used together")
    @pytest.mark.simple
    def test_v2_bearer_combined_with_session_token(
        self,
        gw_address: str,
        gw_endpoint: str,
    ):
        with allure.step("Create container via v2 session token (no eACL deny)"):
            container_session = generate_session_token_v2(
                gw_endpoint, self.owner_wallet, [{"verbs": ["CONTAINER_PUT"]}]
            )
            cid = rest_create_container(
                gw_endpoint,
                _unique_container_name(),
                self.PLACEMENT_RULE,
                PUBLIC_ACL,
                container_session,
            )

        with allure.step("Third-party user delegates OBJECT_PUT/GET on the container to the gateway"):
            session_token = generate_session_token_v2(
                gw_endpoint,
                self.user_wallet,
                [{"containerID": cid, "verbs": ["OBJECT_PUT", "OBJECT_GET"]}],
                targets=[gw_address],
            )

        with allure.step("Container owner issues bearer for the third-party user"):
            bearer = generate_bearer_token_v2(
                gw_endpoint,
                issuer_wallet=self.owner_wallet,
                records=_scoped_records_for_others(["PUT"]),
                owner=self.user_wallet.address,
            )

        combined_headers = {
            "Authorization": f"Bearer {session_token}",
            "NeoFS-Bearer-Token": bearer,
        }
        with allure.step("Upload with both bearer and session token at once"):
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oid = upload_via_rest_gate(
                cid=cid,
                path=file_path,
                endpoint=gw_endpoint,
                headers=combined_headers,
            )
            assert oid, "Upload with combined bearer+session must succeed"

        with allure.step("GET via PUT-only bearer must be denied even when session allows OBJECT_GET"):
            _assert_get_denied(cid, oid, gw_endpoint, headers=combined_headers)

    @allure.title("Bearer overrides eACL deny when used with V2 session token")
    @pytest.mark.simple
    def test_v2_bearer_combined_with_session_token_overrides_eacl(
        self,
        gw_address: str,
        gw_endpoint: str,
    ):
        with allure.step("Create container via v2 session token"):
            container_session = generate_session_token_v2(
                gw_endpoint, self.owner_wallet, [{"verbs": ["CONTAINER_PUT"]}]
            )
            cid = rest_create_container(
                gw_endpoint,
                _unique_container_name(),
                self.PLACEMENT_RULE,
                PUBLIC_ACL,
                container_session,
            )

        with allure.step("Lock down OTHERS for every object operation via eACL"):
            rules = [
                EACLRule(access=EACLAccess.DENY, role=EACLRole.OTHERS, operation=op)
                for op in _ALL_OBJECT_OPERATIONS_EACL
            ]
            set_eacl(
                self.owner_wallet.path,
                cid,
                create_eacl(cid, rules, shell=self.shell),
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            wait_for_cache_expired()

        with allure.step("Third-party user delegates OBJECT_PUT/GET/HEAD on the container to the gateway"):
            session_token = generate_session_token_v2(
                gw_endpoint,
                self.user_wallet,
                [{"containerID": cid, "verbs": ["OBJECT_PUT", "OBJECT_GET", "OBJECT_HEAD"]}],
                targets=[gw_address],
            )

        with allure.step("Container owner issues bearer for the third-party user"):
            bearer = generate_bearer_token_v2(
                gw_endpoint,
                issuer_wallet=self.owner_wallet,
                records=_scoped_records_for_others(["PUT"]),
                owner=self.user_wallet.address,
            )

        combined_headers = {
            "Authorization": f"Bearer {session_token}",
            "NeoFS-Bearer-Token": bearer,
        }
        with allure.step("Upload with both bearer and session token at once"):
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oid = upload_via_rest_gate(
                cid=cid,
                path=file_path,
                endpoint=gw_endpoint,
                headers=combined_headers,
            )
            assert oid, (
                "Upload with combined bearer+session must succeed: bearer's "
                "OTHERS:PUT:ALLOW should override container's eACL DENY"
            )

        with allure.step("GET via PUT-only bearer must be denied even when session allows OBJECT_GET"):
            _assert_get_denied(cid, oid, gw_endpoint, headers=combined_headers)
        with allure.step("HEAD via PUT-only bearer must be denied even when session allows OBJECT_HEAD"):
            _assert_head_denied(cid, oid, gw_endpoint, headers=combined_headers)

    @allure.title("All object operations denied without bearer when eACL restricts OTHERS")
    @pytest.mark.simple
    def test_v2_bearer_required_when_eacl_denies(
        self,
        user_container: str,
        eacl_deny_for_others,
        gw_address: str,
        gw_endpoint: str,
    ):
        with allure.step("PUT without bearer must be denied"):
            upload_via_rest_gate(
                cid=user_container,
                path=generate_file(self.neofs_env.get_object_size("simple_object_size")),
                endpoint=gw_endpoint,
                error_pattern="access to object operation denied",
            )

        with allure.step("Seed an object via a PUT-allowed bearer so we can probe reads"):
            seed_bearer = generate_bearer_token_v2(
                gw_endpoint,
                issuer_wallet=self.owner_wallet,
                records=_scoped_records_for_others(["PUT"]),
                owner=gw_address,
            )
            seeded_oid = upload_via_rest_gate(
                cid=user_container,
                path=generate_file(self.neofs_env.get_object_size("simple_object_size")),
                endpoint=gw_endpoint,
                headers={"NeoFS-Bearer-Token": seed_bearer},
            )

        with allure.step("GET without bearer must be denied"):
            _assert_get_denied(user_container, seeded_oid, gw_endpoint)
        with allure.step("HEAD without bearer must be denied"):
            _assert_head_denied(user_container, seeded_oid, gw_endpoint)

    @allure.title("Bearer issued for another user is rejected")
    @pytest.mark.simple
    def test_v2_bearer_owner_restriction(
        self,
        user_container: str,
        eacl_deny_for_others,
        gw_endpoint: str,
    ):
        bearer_for_other_user = generate_bearer_token_v2(
            gw_endpoint,
            issuer_wallet=self.owner_wallet,
            records=_scoped_records_for_others(["PUT"]),
            owner=self.user_wallet.address,
        )

        upload_via_rest_gate(
            cid=user_container,
            path=generate_file(self.neofs_env.get_object_size("simple_object_size")),
            endpoint=gw_endpoint,
            headers={"NeoFS-Bearer-Token": bearer_for_other_user},
            error_pattern="access to object operation denied",
        )

    @allure.title("Bearer signed by a stranger is rejected")
    @pytest.mark.simple
    def test_v2_bearer_invalid_signature(
        self,
        user_container: str,
        eacl_deny_for_others,
        gw_address: str,
        gw_endpoint: str,
    ):
        with allure.step("Form unsigned bearer body with owner wallet as issuer"):
            owner_neo3 = load_wallet(self.owner_wallet.path, self.owner_wallet.password)
            unsigned = get_unsigned_bearer_token(
                gw_endpoint,
                issuer=owner_neo3.accounts[0].address,
                records=_scoped_records_for_others(["PUT"]),
                owner=gw_address,
            )

        with allure.step("Sign body with stranger's key"):
            stranger_neo3 = load_wallet(self.stranger_wallet.path, self.stranger_wallet.password)
            stranger_acc = stranger_neo3.accounts[0]
            signature, pub_key, scheme = sign_bearer_token_v2(
                unsigned,
                stranger_acc.private_key,
                stranger_acc.public_key.to_array(),
            )

        with allure.step("Try to complete bearer signed by stranger"):
            resp = complete_bearer_token(
                gw_endpoint,
                token=unsigned,
                signature=signature,
                public_key=pub_key,
                scheme=scheme,
                expect_error=True,
            )

            if hasattr(resp, "ok"):
                with allure.step("Gateway rejected mismatched signer at /complete"):
                    assert not resp.ok, "Bearer with mismatched signer must be rejected"
                    assert "invalid" in resp.text.lower() or "signature" in resp.text.lower(), (
                        f"Unexpected error message: {resp.text}"
                    )
                    return

            invalid_bearer = resp

        with allure.step("Bearer was completed; storage must reject it at usage"):
            upload_via_rest_gate(
                cid=user_container,
                path=generate_file(self.neofs_env.get_object_size("simple_object_size")),
                endpoint=gw_endpoint,
                headers={"NeoFS-Bearer-Token": invalid_bearer},
                error_pattern="access to object operation denied",
            )

    @allure.title("Form bearer with malformed issuer is rejected")
    @pytest.mark.simple
    def test_v2_bearer_form_invalid_issuer(self, gw_endpoint: str):
        resp = get_unsigned_bearer_token(
            gw_endpoint,
            issuer="not-a-real-address",
            records=_scoped_records_for_others(["PUT"]),
            owner=self.owner_wallet.address,
            expect_error=True,
        )
        assert not resp.ok, "Forming unsigned bearer with bogus issuer must fail"
        assert "issuer" in resp.text.lower() or "invalid" in resp.text.lower(), f"Unexpected error message: {resp.text}"

    @allure.title("Bearer with restricted EACL only allows declared operations")
    @pytest.mark.simple
    def test_v2_bearer_eacl_scope(
        self,
        user_container: str,
        eacl_deny_for_others,
        gw_address: str,
        gw_endpoint: str,
    ):
        with allure.step("Seed an object via a PUT-allowed bearer so we can verify reads"):
            seed_bearer = generate_bearer_token_v2(
                gw_endpoint,
                issuer_wallet=self.owner_wallet,
                records=_scoped_records_for_others(["PUT"]),
                owner=gw_address,
            )
            seeded_oid = upload_via_rest_gate(
                cid=user_container,
                path=generate_file(self.neofs_env.get_object_size("simple_object_size")),
                endpoint=gw_endpoint,
                headers={"NeoFS-Bearer-Token": seed_bearer},
            )

        read_bearer = generate_bearer_token_v2(
            gw_endpoint,
            issuer_wallet=self.owner_wallet,
            records=_scoped_records_for_others(["GET", "HEAD", "RANGE"]),
            owner=gw_address,
        )
        read_headers = {"NeoFS-Bearer-Token": read_bearer}

        with allure.step("PUT with read-only bearer must be denied (out of scope)"):
            upload_via_rest_gate(
                cid=user_container,
                path=generate_file(self.neofs_env.get_object_size("simple_object_size")),
                endpoint=gw_endpoint,
                headers=read_headers,
                error_pattern="access to object operation denied",
            )

        with allure.step("GET with read-only bearer must succeed (in scope)"):
            resp = get_via_rest_gate(
                user_container,
                seeded_oid,
                gw_endpoint,
                headers=read_headers,
                return_response=True,
                skip_options_verify=True,
            )
            assert resp.ok, f"GET with read-only bearer should succeed: {resp.text}"

        with allure.step("HEAD with read-only bearer must succeed (in scope)"):
            head_resp = head_via_rest_gate(user_container, seeded_oid, gw_endpoint, headers=read_headers)
            assert head_resp.ok, f"HEAD with read-only bearer should succeed: {head_resp.status_code}"
            assert head_resp.headers["X-Object-Id"] == seeded_oid
