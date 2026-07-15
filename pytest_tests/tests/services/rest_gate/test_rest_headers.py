import json
import logging
import os
import uuid

import allure
import pytest
from helpers.complex_object_actions import get_link_object
from helpers.container import (
    create_container,
)
from helpers.file_helper import generate_file
from helpers.neofs_verbs import delete_object, lock_object, put_object_to_random_node
from helpers.rest_gate import (
    assert_hashes_are_equal,
    attr_into_header,
    attr_into_header_base64,
    decode_x_attributes_base64,
    get_object_by_attr_and_verify_hashes,
    get_via_rest_gate,
    get_via_rest_gate_by_attribute,
    head_via_rest_gate,
    head_via_rest_gate_by_attribute,
    try_to_get_object_and_expect_error,
    upload_via_rest_gate,
)
from helpers.storage_object_info import StorageObjectInfo
from helpers.wellknown_acl import PUBLIC_ACL
from pytest import FixtureRequest
from rest_gw.rest_base import TestNeofsRestBase

logger = logging.getLogger("NeoLogger")

OBJECT_TYPE_HEADER = "X-Object-Type"
X_ATTRIBUTES_HEADER = "X-Attributes"
X_ATTRIBUTES_BASE64_HEADER = "X-Attributes-Base64"
CONTENT_DISPOSITION_HEADER = "Content-Disposition"
FILENAME_ATTRIBUTE = "FileName"


@pytest.mark.sanity
class Test_rest_headers(TestNeofsRestBase):
    PLACEMENT_RULE = "REP 2 IN X CBF 1 SELECT 4 FROM * AS X"
    obj1_keys = ["Writer", "Chapter1", "Chapter2"]
    obj2_keys = ["Writer", "Ch$pter1", "Chapter2"]
    values = ["Leo Tolstoy", "peace", "w$r"]
    OBJECT_ATTRIBUTES = [
        {obj1_keys[0]: values[0], obj1_keys[1]: values[1], obj1_keys[2]: values[2]},
        {obj2_keys[0]: values[0], obj2_keys[1]: values[1], obj2_keys[2]: values[2]},
    ]

    @pytest.fixture(scope="class", autouse=True)
    @allure.title("[Class/Autouse]: Prepare wallet and deposit")
    def prepare_wallet(self, default_wallet):
        Test_rest_headers.wallet = default_wallet

    @pytest.fixture(
        params=[
            pytest.param("simple_object_size", id="simple object", marks=pytest.mark.simple),
            pytest.param("complex_object_size", id="complex object", marks=pytest.mark.complex),
        ],
        scope="class",
    )
    def storage_objects_with_attributes(self, request: FixtureRequest, gw_endpoint) -> list[StorageObjectInfo]:
        storage_objects = []
        wallet = self.wallet.path
        cid = create_container(
            wallet=self.wallet.path,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            rule=self.PLACEMENT_RULE,
            basic_acl=PUBLIC_ACL,
        )
        file_path = generate_file(self.neofs_env.get_object_size(request.param))
        for attributes in self.OBJECT_ATTRIBUTES:
            storage_object_id = upload_via_rest_gate(
                cid=cid,
                path=file_path,
                endpoint=gw_endpoint,
                headers=attr_into_header(attributes),
            )
            storage_object = StorageObjectInfo(cid, storage_object_id)
            storage_object.size = os.path.getsize(file_path)
            storage_object.wallet_file_path = wallet
            storage_object.file_path = file_path
            storage_object.attributes = attributes

            storage_objects.append(storage_object)

        yield storage_objects

    @allure.title("Get object1 by attribute")
    def test_object1_can_be_get_by_attr(self, storage_objects_with_attributes: list[StorageObjectInfo], gw_endpoint):
        """
        Test to get object#1 by attribute and comapre hashes

        Steps:
        1. Download object#1 with attributes [Chapter2=w$r] and compare hashes
        """

        storage_object_1 = storage_objects_with_attributes[0]

        with allure.step(
            f"Download object#1 via wget with attributes Chapter2: {storage_object_1.attributes['Chapter2']} and compare hashes"
        ):
            get_object_by_attr_and_verify_hashes(
                oid=storage_object_1.oid,
                file_name=storage_object_1.file_path,
                cid=storage_object_1.cid,
                attrs={"Chapter2": storage_object_1.attributes["Chapter2"]},
                endpoint=gw_endpoint,
            )

    @allure.title("Test get object2 with different attributes, then delete object2 and get object1")
    def test_object2_can_be_get_by_attr(self, storage_objects_with_attributes: list[StorageObjectInfo], gw_endpoint):
        """
        Test to get object2 with different attributes, then delete object2 and get object1 using 1st attribute. Note: obj1 and obj2 have the same attribute#1,
        and when obj2 is deleted you can get obj1 by 1st attribute

        Steps:
        1. Download object#2 with attributes [Chapter2=w$r] and compare hashes
        2. Download object#2 with attributes [Ch$pter1=peace] and compare hashes
        3. Delete object#2
        4. Download object#1 with attributes [Writer=Leo Tolstoy] and compare hashes
        """
        storage_object_1 = storage_objects_with_attributes[0]
        storage_object_2 = storage_objects_with_attributes[1]

        with allure.step(
            f"Download object#2 via wget with attributes [Chapter2={storage_object_2.attributes['Chapter2']}] / [Ch$pter1={storage_object_2.attributes['Ch$pter1']}]  and compare hashes"
        ):
            selected_attributes_object2 = [
                {"Chapter2": storage_object_2.attributes["Chapter2"]},
                {"Ch$pter1": storage_object_2.attributes["Ch$pter1"]},
            ]
            for attributes in selected_attributes_object2:
                get_object_by_attr_and_verify_hashes(
                    oid=storage_object_2.oid,
                    file_name=storage_object_2.file_path,
                    cid=storage_object_2.cid,
                    attrs=attributes,
                    endpoint=gw_endpoint,
                )
        with allure.step("Delete object#2 and verify is the container deleted"):
            delete_object(
                wallet=self.wallet.path,
                cid=storage_object_2.cid,
                oid=storage_object_2.oid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            error_pattern = "404 Not Found"
            try_to_get_object_and_expect_error(
                cid=storage_object_2.cid,
                oid=storage_object_2.oid,
                error_pattern=error_pattern,
                endpoint=gw_endpoint,
            )
            storage_objects_with_attributes.remove(storage_object_2)

        with allure.step(
            f"Download object#1 with attributes [Writer={storage_object_1.attributes['Writer']}] and compare hashes"
        ):
            key_value_pair = {"Writer": storage_object_1.attributes["Writer"]}
            get_object_by_attr_and_verify_hashes(
                oid=storage_object_1.oid,
                file_name=storage_object_1.file_path,
                cid=storage_object_1.cid,
                attrs=key_value_pair,
                endpoint=gw_endpoint,
            )


@pytest.mark.sanity
class Test_rest_object_type(TestNeofsRestBase):
    """Verify the ``X-Object-Type`` response header returned by the REST gateway.

    The gateway exposes the NeoFS object type (REGULAR/TOMBSTONE/LOCK/LINK) via the
    ``X-Object-Type`` header on object GET, HEAD and get-by-attribute responses.
    """

    PLACEMENT_RULE = "REP 2 IN X CBF 1 SELECT 4 FROM * AS X"
    LOCK_LIFETIME = 5

    @pytest.fixture(scope="class", autouse=True)
    @allure.title("[Class/Autouse]: Prepare wallet and deposit")
    def prepare_wallet(self, default_wallet):
        Test_rest_object_type.wallet = default_wallet

    @pytest.fixture(scope="class")
    def container(self) -> str:
        return create_container(
            wallet=self.wallet.path,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            rule=self.PLACEMENT_RULE,
            basic_acl=PUBLIC_ACL,
        )

    @allure.title("REGULAR object type in X-Object-Type header ({object_size})")
    @pytest.mark.parametrize(
        "object_size",
        [
            pytest.param("simple_object_size", id="simple object", marks=pytest.mark.simple),
            pytest.param("complex_object_size", id="complex object", marks=pytest.mark.complex),
        ],
    )
    def test_regular_object_type(self, container: str, gw_endpoint: str, object_size: str):
        """
        Test that a regular object reports the REGULAR type via X-Object-Type.

        Steps:
        1. Upload an object via REST gate (with a user attribute).
        2. HEAD the object by id and verify X-Object-Type == REGULAR.
        3. GET the object by id and verify X-Object-Type == REGULAR.
        4. HEAD the object by attribute and verify X-Object-Type == REGULAR.
        """
        attributes = {"FileName": "regular_object"}
        file_path = generate_file(self.neofs_env.get_object_size(object_size))

        with allure.step("Upload object via REST gate"):
            oid = upload_via_rest_gate(
                cid=container,
                path=file_path,
                endpoint=gw_endpoint,
                headers=attr_into_header(attributes),
            )

        with allure.step("HEAD object by id and verify X-Object-Type is REGULAR"):
            resp = head_via_rest_gate(cid=container, oid=oid, endpoint=gw_endpoint)
            assert resp.headers.get(OBJECT_TYPE_HEADER) == "REGULAR", (
                f"Expected {OBJECT_TYPE_HEADER}=REGULAR on HEAD, got {resp.headers.get(OBJECT_TYPE_HEADER)}"
            )

        with allure.step("GET object by id and verify X-Object-Type is REGULAR"):
            resp = get_via_rest_gate(cid=container, oid=oid, endpoint=gw_endpoint, return_response=True)
            assert resp.headers.get(OBJECT_TYPE_HEADER) == "REGULAR", (
                f"Expected {OBJECT_TYPE_HEADER}=REGULAR on GET, got {resp.headers.get(OBJECT_TYPE_HEADER)}"
            )

        with allure.step("HEAD object by attribute and verify X-Object-Type is REGULAR"):
            resp = head_via_rest_gate_by_attribute(cid=container, attribute=attributes, endpoint=gw_endpoint)
            assert resp.headers.get(OBJECT_TYPE_HEADER) == "REGULAR", (
                f"Expected {OBJECT_TYPE_HEADER}=REGULAR on HEAD by attribute, got {resp.headers.get(OBJECT_TYPE_HEADER)}"
            )

    @allure.title("LOCK object type in X-Object-Type header")
    @pytest.mark.simple
    def test_lock_object_type(self, container: str, gw_endpoint: str):
        """
        Test that a lock object reports the LOCK type via X-Object-Type.

        Steps:
        1. Upload a regular object.
        2. Lock it, obtaining the lock object id.
        3. HEAD the lock object by id and verify X-Object-Type == LOCK.
        """
        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))

        with allure.step("Upload object and lock it"):
            oid = put_object_to_random_node(
                wallet=self.wallet.path,
                path=file_path,
                cid=container,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )
            lock_oid = lock_object(
                wallet=self.wallet.path,
                cid=container,
                oid=oid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                lifetime=self.LOCK_LIFETIME,
            )

        with allure.step("HEAD lock object by id and verify X-Object-Type is LOCK"):
            resp = head_via_rest_gate(cid=container, oid=lock_oid, endpoint=gw_endpoint)
            assert resp.headers.get(OBJECT_TYPE_HEADER) == "LOCK", (
                f"Expected {OBJECT_TYPE_HEADER}=LOCK, got {resp.headers.get(OBJECT_TYPE_HEADER)}"
            )

    @allure.title("TOMBSTONE object type in X-Object-Type header")
    @pytest.mark.simple
    def test_tombstone_object_type(self, container: str, gw_endpoint: str):
        """
        Test that a tombstone object reports the TOMBSTONE type via X-Object-Type.

        Steps:
        1. Upload a regular object.
        2. Delete it, obtaining the tombstone object id.
        3. HEAD the tombstone object by id and verify X-Object-Type == TOMBSTONE.
        """
        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))

        with allure.step("Upload object and delete it"):
            oid = put_object_to_random_node(
                wallet=self.wallet.path,
                path=file_path,
                cid=container,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )
            tombstone_oid = delete_object(
                wallet=self.wallet.path,
                cid=container,
                oid=oid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("HEAD tombstone object by id and verify X-Object-Type is TOMBSTONE"):
            resp = head_via_rest_gate(cid=container, oid=tombstone_oid, endpoint=gw_endpoint)
            assert resp.headers.get(OBJECT_TYPE_HEADER) == "TOMBSTONE", (
                f"Expected {OBJECT_TYPE_HEADER}=TOMBSTONE, got {resp.headers.get(OBJECT_TYPE_HEADER)}"
            )

    @allure.title("LINK object type in X-Object-Type header")
    @pytest.mark.complex
    def test_link_object_type(self, container: str, gw_endpoint: str):
        """
        Test that a link object reports the LINK type via X-Object-Type.

        Steps:
        1. Upload a complex (split) object.
        2. Resolve the link object id of the complex object.
        3. HEAD the link object by id and verify X-Object-Type == LINK.
        """
        file_path = generate_file(self.neofs_env.get_object_size("complex_object_size"))

        with allure.step("Upload complex object and resolve its link object"):
            oid = put_object_to_random_node(
                wallet=self.wallet.path,
                path=file_path,
                cid=container,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )
            link_oid = get_link_object(
                wallet=self.wallet.path,
                cid=container,
                oid=oid,
                neofs_env=self.neofs_env,
            )

        with allure.step("HEAD link object by id and verify X-Object-Type is LINK"):
            resp = head_via_rest_gate(cid=container, oid=link_oid, endpoint=gw_endpoint)
            assert resp.headers.get(OBJECT_TYPE_HEADER) == "LINK", (
                f"Expected {OBJECT_TYPE_HEADER}=LINK, got {resp.headers.get(OBJECT_TYPE_HEADER)}"
            )


@pytest.mark.sanity
class Test_rest_encoded_attributes(TestNeofsRestBase):
    PLACEMENT_RULE = "REP 2 IN X CBF 1 SELECT 4 FROM * AS X"

    @pytest.fixture(scope="class", autouse=True)
    @allure.title("[Class/Autouse]: Prepare wallet and deposit")
    def prepare_wallet(self, default_wallet):
        Test_rest_encoded_attributes.wallet = default_wallet

    @pytest.fixture(scope="class")
    def container(self) -> str:
        return create_container(
            wallet=self.wallet.path,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            rule=self.PLACEMENT_RULE,
            basic_acl=PUBLIC_ACL,
        )

    @staticmethod
    def _verify_encoded_attributes(resp, expected: dict) -> dict:
        encoded = resp.headers.get(X_ATTRIBUTES_BASE64_HEADER)
        assert encoded, f"Expected {X_ATTRIBUTES_BASE64_HEADER} header in response, got headers: {dict(resp.headers)}"
        decoded = decode_x_attributes_base64(encoded)
        for key, value in expected.items():
            assert decoded.get(key) == value, (
                f"Attribute {key!r} mismatch in {X_ATTRIBUTES_BASE64_HEADER}: "
                f"expected {value!r}, got {decoded.get(key)!r}"
            )
        return decoded

    @allure.title("Non-ASCII attributes full round-trip via X-Attributes-Base64 ({object_size})")
    @pytest.mark.parametrize(
        "object_size",
        [
            pytest.param("simple_object_size", id="simple object", marks=pytest.mark.simple),
            pytest.param("complex_object_size", id="complex object", marks=pytest.mark.complex),
        ],
    )
    def test_non_ascii_attributes_round_trip(self, container: str, gw_endpoint: str, object_size: str):
        unique = uuid.uuid4().hex
        non_ascii_value = f"Война и мир {unique}"
        non_ascii_key = "Автор"
        non_ascii_key_value = f"Толстой {unique}"
        attributes = {
            "Writer": "Лев Толстой",
            "Chapter": non_ascii_value,
            "Emoji": "🚀 neofs rocket",
            "Mixed": "a$b-Ünïcödé",
            non_ascii_key: non_ascii_key_value,
        }
        file_path = generate_file(self.neofs_env.get_object_size(object_size))

        with allure.step("Upload object with non-ASCII keys and values via X-Attributes-Base64 header"):
            oid = upload_via_rest_gate(
                cid=container,
                path=file_path,
                endpoint=gw_endpoint,
                headers=attr_into_header_base64(attributes),
            )

        with allure.step("HEAD object and verify X-Attributes-Base64 attributes and omitted plain header"):
            resp = head_via_rest_gate(cid=container, oid=oid, endpoint=gw_endpoint)
            self._verify_encoded_attributes(resp, attributes)
            assert resp.headers.get(X_ATTRIBUTES_HEADER) is None, (
                f"Plain {X_ATTRIBUTES_HEADER} header must be omitted for non-ASCII attributes, "
                f"got {resp.headers.get(X_ATTRIBUTES_HEADER)!r}"
            )

        with allure.step("GET object by id and verify non-ASCII attributes and payload"):
            resp = get_via_rest_gate(cid=container, oid=oid, endpoint=gw_endpoint, return_response=True)
            self._verify_encoded_attributes(resp, attributes)
            got_file_path = get_via_rest_gate(cid=container, oid=oid, endpoint=gw_endpoint)
            assert_hashes_are_equal(file_path, got_file_path, got_file_path)

        with allure.step("GET object by non-ASCII attribute value and by non-ASCII attribute key"):
            get_object_by_attr_and_verify_hashes(
                oid=oid,
                file_name=file_path,
                cid=container,
                attrs={"Chapter": non_ascii_value},
                endpoint=gw_endpoint,
            )
            get_object_by_attr_and_verify_hashes(
                oid=oid,
                file_name=file_path,
                cid=container,
                attrs={non_ascii_key: non_ascii_key_value},
                endpoint=gw_endpoint,
            )

        with allure.step("HEAD object by non-ASCII attribute and verify object id and attributes"):
            resp = head_via_rest_gate_by_attribute(
                cid=container, attribute={"Chapter": non_ascii_value}, endpoint=gw_endpoint
            )
            assert resp.headers.get("X-Object-Id") == oid, (
                f"Expected X-Object-Id={oid}, got {resp.headers.get('X-Object-Id')}"
            )
            self._verify_encoded_attributes(resp, attributes)

    @allure.title("Plain X-Attributes header is emitted only for header-safe attributes")
    @pytest.mark.simple
    @pytest.mark.parametrize(
        "attributes, upload_base64, expect_plain",
        [
            pytest.param(
                {"Writer": "Leo Tolstoy", "Chapter": "War and Peace"},
                False,
                True,
                id="ascii only",
            ),
            pytest.param({"Writer": "Лев Толстой", "Chapter": "Война и мир"}, True, False, id="non-ascii value"),
            pytest.param({"Автор": "Tolstoy", "Book": "War and Peace"}, True, False, id="non-ascii key"),
            pytest.param(
                {"AsciiKey": "ascii value", "Number": "12345", "Кириллица": "Значение"},
                True,
                False,
                id="mixed ascii and non-ascii",
            ),
            pytest.param({"Quote": 'he said "hi"', "Plain": "ok"}, True, False, id="ascii header-unsafe value"),
        ],
    )
    def test_plain_x_attributes_header_gating(
        self, container: str, gw_endpoint: str, attributes: dict, upload_base64: bool, expect_plain: bool
    ):
        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        headers = attr_into_header_base64(attributes) if upload_base64 else attr_into_header(attributes)

        with allure.step(f"Upload object (base64={upload_base64}) with attributes {attributes}"):
            oid = upload_via_rest_gate(cid=container, path=file_path, endpoint=gw_endpoint, headers=headers)

        with allure.step("HEAD object and verify X-Attributes-Base64 carries all attributes"):
            resp = head_via_rest_gate(cid=container, oid=oid, endpoint=gw_endpoint)
            decoded = self._verify_encoded_attributes(resp, attributes)

        with allure.step(f"Verify plain X-Attributes header is {'present' if expect_plain else 'omitted'}"):
            plain = resp.headers.get(X_ATTRIBUTES_HEADER)
            if expect_plain:
                assert plain is not None, (
                    f"Plain {X_ATTRIBUTES_HEADER} header must be present for header-safe ASCII attributes"
                )
                assert json.loads(plain) == decoded, (
                    f"{X_ATTRIBUTES_HEADER} (plain JSON) must match {X_ATTRIBUTES_BASE64_HEADER}: "
                    f"{plain!r} != {decoded}"
                )
            else:
                assert plain is None, (
                    f"Plain {X_ATTRIBUTES_HEADER} header must be omitted for header-unsafe attributes, got {plain!r}"
                )

    @allure.title("X-Attributes-Base64 takes precedence over X-Attributes on upload")
    @pytest.mark.simple
    def test_base64_header_takes_precedence(self, container: str, gw_endpoint: str):
        marker = uuid.uuid4().hex
        plain_attributes = {"Source": "plain-header", "Marker": f"plain-{marker}"}
        base64_attributes = {"Source": "base64-header", "Marker": f"base64-{marker}"}
        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))

        with allure.step("Upload object with conflicting X-Attributes and X-Attributes-Base64 headers"):
            headers = {**attr_into_header(plain_attributes), **attr_into_header_base64(base64_attributes)}
            oid = upload_via_rest_gate(
                cid=container,
                path=file_path,
                endpoint=gw_endpoint,
                headers=headers,
            )

        with allure.step("HEAD object and verify X-Attributes-Base64 values win"):
            resp = head_via_rest_gate(cid=container, oid=oid, endpoint=gw_endpoint)
            decoded = self._verify_encoded_attributes(resp, base64_attributes)
            assert decoded.get("Source") != plain_attributes["Source"], (
                f"X-Attributes-Base64 must take precedence, got Source={decoded.get('Source')!r}"
            )

        with allure.step("Verify object is resolvable by the X-Attributes-Base64 attribute value"):
            resp = get_via_rest_gate_by_attribute(
                cid=container,
                attribute={"Marker": base64_attributes["Marker"]},
                endpoint=gw_endpoint,
                return_response=True,
            )
            self._verify_encoded_attributes(resp, base64_attributes)

    @allure.title("Upload with malformed X-Attributes-Base64 header is rejected")
    @pytest.mark.simple
    def test_upload_invalid_base64_attributes_returns_error(self, container: str, gw_endpoint: str):
        file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))

        with allure.step("Upload object with an invalid (non-base64) X-Attributes-Base64 header"):
            upload_via_rest_gate(
                cid=container,
                path=file_path,
                endpoint=gw_endpoint,
                headers={X_ATTRIBUTES_BASE64_HEADER: "@@@ not valid base64 @@@"},
                error_pattern="could not decode header X-Attributes-Base64",
            )

    @allure.title("Content-Disposition filename is omitted for a non-ASCII FileName")
    @pytest.mark.simple
    def test_non_ascii_filename_content_disposition(self, container: str, gw_endpoint: str):
        ascii_name = f"war_and_peace_{uuid.uuid4().hex}.txt"
        non_ascii_name = f"Война_и_мир_{uuid.uuid4().hex}.txt"

        with allure.step("Upload object with an ASCII FileName and verify Content-Disposition"):
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oid = upload_via_rest_gate(
                cid=container,
                path=file_path,
                endpoint=gw_endpoint,
                headers=attr_into_header_base64({FILENAME_ATTRIBUTE: ascii_name}),
            )
            resp = get_via_rest_gate(cid=container, oid=oid, endpoint=gw_endpoint, return_response=True)
            content_disposition = resp.headers.get(CONTENT_DISPOSITION_HEADER, "")
            assert f"filename={ascii_name}" in content_disposition, (
                f"Expected filename={ascii_name!r} in {CONTENT_DISPOSITION_HEADER}, got {content_disposition!r}"
            )

        with allure.step("Upload object with a non-ASCII FileName"):
            file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
            oid = upload_via_rest_gate(
                cid=container,
                path=file_path,
                endpoint=gw_endpoint,
                headers=attr_into_header_base64({FILENAME_ATTRIBUTE: non_ascii_name}),
            )

        with allure.step("Verify Content-Disposition omits filename= but X-Attributes-Base64 keeps FileName"):
            resp = get_via_rest_gate(cid=container, oid=oid, endpoint=gw_endpoint, return_response=True)
            content_disposition = resp.headers.get(CONTENT_DISPOSITION_HEADER, "")
            assert "filename=" not in content_disposition, (
                f"{CONTENT_DISPOSITION_HEADER} must not carry a non-ASCII filename, got {content_disposition!r}"
            )
            self._verify_encoded_attributes(resp, {FILENAME_ATTRIBUTE: non_ascii_name})
