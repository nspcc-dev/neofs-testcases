import allure
import pytest
from helpers.acl import (
    EACLAccess,
    EACLFilter,
    EACLFilters,
    EACLHeaderType,
    EACLMatchType,
    EACLOperation,
    EACLRole,
    EACLRule,
    create_eacl,
    form_bearertoken_file,
    get_eacl,
    set_eacl,
    wait_for_cache_expired,
)
from helpers.container import create_container, delete_container
from helpers.container_access import check_full_access_to_container, check_no_access_to_container
from helpers.file_helper import generate_file
from helpers.grpc_responses import INVALID_RULES, OBJECT_ACCESS_DENIED
from helpers.neofs_verbs import put_object_to_random_node
from helpers.object_access import can_get_head_object, can_get_object, can_put_object
from helpers.wellknown_acl import PUBLIC_ACL
from neofs_env.neofs_env_test_base import NeofsEnvTestBase


@pytest.mark.acl
@pytest.mark.acl_filters
class TestEACLFilters(NeofsEnvTestBase):
    #  SPEC: https://github.com/nspcc-dev/neofs-spec/blob/master/01-arch/07-acl.md
    ATTRIBUTE = {"check_key": "check_value"}
    OTHER_ATTRIBUTE = {"check_key": "other_value"}
    SET_HEADERS = {
        "key_one": "check_value",
        "x_key": "xvalue",
        "check_key": "check_value",
    }
    OTHER_HEADERS = {
        "key_one": "check_value",
        "x_key": "other_value",
        "check_key": "other_value",
    }
    REQ_EQUAL_FILTER = EACLFilter(
        key="check_key", value="check_value", header_type=EACLHeaderType.REQUEST
    )
    NOT_REQ_EQUAL_FILTER = EACLFilter(
        key="check_key",
        value="other_value",
        match_type=EACLMatchType.STRING_NOT_EQUAL,
        header_type=EACLHeaderType.REQUEST,
    )
    OBJ_EQUAL_FILTER = EACLFilter(
        key="check_key", value="check_value", header_type=EACLHeaderType.OBJECT
    )
    NOT_OBJ_EQUAL_FILTER = EACLFilter(
        key="check_key",
        value="other_value",
        match_type=EACLMatchType.STRING_NOT_EQUAL,
        header_type=EACLHeaderType.OBJECT,
    )
    OBJECT_COUNT = 5
    OBJECT_ATTRIBUTES_FILTER_SUPPORTED_OPERATIONS = [
        EACLOperation.GET,
        EACLOperation.HEAD,
        EACLOperation.PUT,
    ]
    OBJECT_NUMERIC_KEY_ATTR_NAME = "numeric_value"
    OBJECT_NUMERIC_VALUES = [-(2**64) - 1, -1, 0, 1, 10, 2**64 + 1]
    EXPIRATION_OBJECT_ATTR = "__NEOFS__EXPIRATION_EPOCH"
    PAYLOAD_LENGTH_OBJECT_ATTR = "$Object:payloadLength"
    CREATION_EPOCH_OBJECT_ATTR = "$Object:creationEpoch"
    OPERATION_NOT_ALLOWED_ERROR_MESSAGE = "GET is not allowed for this object, while it should be"
    OPERATION_ALLOWED_ERROR_MESSAGE = "GET is allowed for this object, while it shouldn't be"

    @pytest.fixture(scope="function")
    def eacl_container(self, wallets):
        user_wallet = wallets.get_wallet()

        with allure.step("Create eACL public container"):
            cid = create_container(
                user_wallet.wallet_path,
                basic_acl=PUBLIC_ACL,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        yield cid

        with allure.step("Delete eACL public container"):
            delete_container(
                user_wallet.wallet_path,
                cid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

    @pytest.fixture(scope="function")
    def eacl_container_with_objects(self, wallets, file_path):
        user_wallet = wallets.get_wallet()
        with allure.step("Create eACL public container"):
            cid = create_container(
                user_wallet.wallet_path,
                basic_acl=PUBLIC_ACL,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Add test objects to container"):
            objects_with_header = [
                put_object_to_random_node(
                    user_wallet.wallet_path,
                    file_path,
                    cid,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                    attributes={**self.SET_HEADERS, "key": val},
                )
                for val in range(self.OBJECT_COUNT)
            ]

            objects_with_other_header = [
                put_object_to_random_node(
                    user_wallet.wallet_path,
                    file_path,
                    cid,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                    attributes={**self.OTHER_HEADERS, "key": val},
                )
                for val in range(self.OBJECT_COUNT)
            ]

            objects_without_header = [
                put_object_to_random_node(
                    user_wallet.wallet_path,
                    file_path,
                    cid,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                )
                for _ in range(self.OBJECT_COUNT)
            ]

        yield cid, objects_with_header, objects_with_other_header, objects_without_header, file_path

        with allure.step("Delete eACL public container"):
            delete_container(
                user_wallet.wallet_path,
                cid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

    @pytest.mark.parametrize(
        "match_type", [EACLMatchType.STRING_EQUAL, EACLMatchType.STRING_NOT_EQUAL]
    )
    def test_extended_acl_filters_request(self, wallets, eacl_container_with_objects, match_type):
        allure.dynamic.title(f"Validate NeoFS operations with request filter: {match_type.name}")
        user_wallet = wallets.get_wallet()
        other_wallet = wallets.get_wallet(EACLRole.OTHERS)
        (
            cid,
            objects_with_header,
            objects_with_other_header,
            objects_without_header,
            file_path,
        ) = eacl_container_with_objects

        with allure.step("Deny all operations for other with eACL request filter"):
            equal_filter = EACLFilter(**self.REQ_EQUAL_FILTER.__dict__)
            equal_filter.match_type = match_type
            eacl_deny = [
                EACLRule(
                    access=EACLAccess.DENY,
                    role=EACLRole.OTHERS,
                    filters=EACLFilters([equal_filter]),
                    operation=op,
                )
                for op in EACLOperation
            ]
            set_eacl(
                user_wallet.wallet_path,
                cid,
                create_eacl(cid, eacl_deny, shell=self.shell),
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            wait_for_cache_expired()

        # Filter denies requests where "check_key {match_type} ATTRIBUTE", so when match_type
        # is STRING_EQUAL, then requests with "check_key=OTHER_ATTRIBUTE" will be allowed while
        # requests with "check_key=ATTRIBUTE" will be denied, and vice versa
        allow_headers = (
            self.OTHER_ATTRIBUTE if match_type == EACLMatchType.STRING_EQUAL else self.ATTRIBUTE
        )
        deny_headers = (
            self.ATTRIBUTE if match_type == EACLMatchType.STRING_EQUAL else self.OTHER_ATTRIBUTE
        )
        # We test on 3 groups of objects with various headers,
        # but eACL rule should ignore object headers and
        # work only based on request headers
        for oid in (
            objects_with_header,
            objects_with_other_header,
            objects_without_header,
        ):
            with allure.step("Check other has full access when sending request without headers"):
                check_full_access_to_container(
                    other_wallet.wallet_path,
                    cid,
                    oid.pop(),
                    file_path,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                )

            with allure.step(
                "Check other has full access when sending request with allowed headers"
            ):
                check_full_access_to_container(
                    other_wallet.wallet_path,
                    cid,
                    oid.pop(),
                    file_path,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                    xhdr=allow_headers,
                )

            with allure.step("Check other has no access when sending request with denied headers"):
                check_no_access_to_container(
                    other_wallet.wallet_path,
                    cid,
                    oid.pop(),
                    file_path,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                    xhdr=deny_headers,
                )

            with allure.step(
                "Check other has full access when sending request "
                "with denied headers and using bearer token"
            ):
                bearer_other = form_bearertoken_file(
                    user_wallet.wallet_path,
                    cid,
                    [
                        EACLRule(operation=op, access=EACLAccess.ALLOW, role=EACLRole.OTHERS)
                        for op in EACLOperation
                    ],
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                )
                check_full_access_to_container(
                    other_wallet.wallet_path,
                    cid,
                    oid.pop(),
                    file_path,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                    xhdr=deny_headers,
                    bearer=bearer_other,
                )

    @pytest.mark.parametrize(
        "match_type", [EACLMatchType.STRING_EQUAL, EACLMatchType.STRING_NOT_EQUAL]
    )
    def test_extended_acl_deny_filters_object(
        self, wallets, eacl_container_with_objects, match_type
    ):
        allure.dynamic.title(
            f"Validate NeoFS operations with deny user headers filter: {match_type.name}"
        )
        user_wallet = wallets.get_wallet()
        other_wallet = wallets.get_wallet(EACLRole.OTHERS)
        (
            cid,
            objects_with_header,
            objects_with_other_header,
            objs_without_header,
            file_path,
        ) = eacl_container_with_objects

        with allure.step("Deny all operations for other with object filter"):
            equal_filter = EACLFilter(**self.OBJ_EQUAL_FILTER.__dict__)
            equal_filter.match_type = match_type
            eacl_deny = [
                EACLRule(
                    access=EACLAccess.DENY,
                    role=EACLRole.OTHERS,
                    filters=EACLFilters([equal_filter]),
                    operation=op,
                )
                for op in self.OBJECT_ATTRIBUTES_FILTER_SUPPORTED_OPERATIONS
            ]
            set_eacl(
                user_wallet.wallet_path,
                cid,
                create_eacl(cid, eacl_deny, shell=self.shell),
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            wait_for_cache_expired()

        allow_objects = (
            objects_with_other_header
            if match_type == EACLMatchType.STRING_EQUAL
            else objects_with_header
        )
        deny_objects = (
            objects_with_header
            if match_type == EACLMatchType.STRING_EQUAL
            else objects_with_other_header
        )

        # We will attempt requests with various headers,
        # but eACL rule should ignore request headers and validate
        # only object headers
        for xhdr in (self.ATTRIBUTE, self.OTHER_ATTRIBUTE, None):
            with allure.step("Check other have full access to objects without attributes"):
                check_full_access_to_container(
                    other_wallet.wallet_path,
                    cid,
                    objs_without_header.pop(),
                    file_path,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                    xhdr=xhdr,
                )

            with allure.step("Check other have full access to objects without deny attribute"):
                check_full_access_to_container(
                    other_wallet.wallet_path,
                    cid,
                    allow_objects.pop(),
                    file_path,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                    xhdr=xhdr,
                )

            with allure.step("Check other have no access to objects with deny attribute"):
                with pytest.raises(AssertionError):
                    assert can_get_head_object(
                        other_wallet.wallet_path,
                        cid,
                        deny_objects[0],
                        shell=self.shell,
                        endpoint=self.neofs_env.sn_rpc,
                        xhdr=xhdr,
                    )
                with pytest.raises(AssertionError):
                    assert can_get_object(
                        other_wallet.wallet_path,
                        cid,
                        deny_objects[0],
                        file_path,
                        shell=self.shell,
                        neofs_env=self.neofs_env,
                        xhdr=xhdr,
                    )

            with allure.step(
                "Check other have access to objects with deny attribute and using bearer token"
            ):
                bearer_other = form_bearertoken_file(
                    user_wallet.wallet_path,
                    cid,
                    [
                        EACLRule(
                            operation=op,
                            access=EACLAccess.ALLOW,
                            role=EACLRole.OTHERS,
                        )
                        for op in EACLOperation
                    ],
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                )
                check_full_access_to_container(
                    other_wallet.wallet_path,
                    cid,
                    deny_objects.pop(),
                    file_path,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                    xhdr=xhdr,
                    bearer=bearer_other,
                )

        allow_attribute = (
            self.OTHER_ATTRIBUTE if match_type == EACLMatchType.STRING_EQUAL else self.ATTRIBUTE
        )
        with allure.step("Check other can PUT objects without denied attribute"):
            assert can_put_object(
                other_wallet.wallet_path,
                cid,
                file_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
                attributes=allow_attribute,
            )
            assert can_put_object(
                other_wallet.wallet_path, cid, file_path, shell=self.shell, neofs_env=self.neofs_env
            )

        deny_attribute = (
            self.ATTRIBUTE if match_type == EACLMatchType.STRING_EQUAL else self.OTHER_ATTRIBUTE
        )
        with allure.step("Check other can not PUT objects with denied attribute"):
            with pytest.raises(AssertionError):
                assert can_put_object(
                    other_wallet.wallet_path,
                    cid,
                    file_path,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                    attributes=deny_attribute,
                )

        with allure.step(
            "Check other can PUT objects with denied attribute and using bearer token"
        ):
            bearer_other_for_put = form_bearertoken_file(
                user_wallet.wallet_path,
                cid,
                [
                    EACLRule(
                        operation=EACLOperation.PUT,
                        access=EACLAccess.ALLOW,
                        role=EACLRole.OTHERS,
                    )
                ],
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            assert can_put_object(
                other_wallet.wallet_path,
                cid,
                file_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
                attributes=deny_attribute,
                bearer=bearer_other_for_put,
            )

    @pytest.mark.parametrize(
        "match_type", [EACLMatchType.STRING_EQUAL, EACLMatchType.STRING_NOT_EQUAL]
    )
    def test_extended_acl_allow_filters_object(
        self, wallets, eacl_container_with_objects, match_type
    ):
        allure.dynamic.title(
            "Testcase to validate NeoFS operation with allow eACL user headers filters:"
            f"{match_type.name}"
        )
        user_wallet = wallets.get_wallet()
        other_wallet = wallets.get_wallet(EACLRole.OTHERS)
        (
            cid,
            objects_with_header,
            objects_with_other_header,
            objects_without_header,
            file_path,
        ) = eacl_container_with_objects

        with allure.step(
            "Deny all operations for others except few operations allowed by object filter"
        ):
            equal_filter = EACLFilter(**self.OBJ_EQUAL_FILTER.__dict__)
            equal_filter.match_type = match_type
            eacl = [
                EACLRule(
                    access=EACLAccess.ALLOW,
                    role=EACLRole.OTHERS,
                    filters=EACLFilters([equal_filter]),
                    operation=op,
                )
                for op in self.OBJECT_ATTRIBUTES_FILTER_SUPPORTED_OPERATIONS
            ] + [
                EACLRule(access=EACLAccess.DENY, role=EACLRole.OTHERS, operation=op)
                for op in self.OBJECT_ATTRIBUTES_FILTER_SUPPORTED_OPERATIONS
            ]
            set_eacl(
                user_wallet.wallet_path,
                cid,
                create_eacl(cid, eacl, shell=self.shell),
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            wait_for_cache_expired()

        if match_type == EACLMatchType.STRING_EQUAL:
            allow_objects = objects_with_header
            deny_objects = objects_with_other_header
            allow_attribute = self.ATTRIBUTE
            deny_attribute = self.OTHER_ATTRIBUTE
        else:
            allow_objects = objects_with_other_header
            deny_objects = objects_with_header
            allow_attribute = self.OTHER_ATTRIBUTE
            deny_attribute = self.ATTRIBUTE

        with allure.step(f"Check other cannot get and put objects without attributes"):
            oid = objects_without_header.pop()
            with pytest.raises(AssertionError):
                assert can_get_head_object(
                    other_wallet.wallet_path,
                    cid,
                    oid,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                )
            with pytest.raises(AssertionError):
                assert can_get_object(
                    other_wallet.wallet_path,
                    cid,
                    oid,
                    file_path,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                )
            with pytest.raises(AssertionError):
                assert can_put_object(
                    other_wallet.wallet_path,
                    cid,
                    file_path,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                )

        with allure.step(
            "Check other can get and put objects without attributes and using bearer token"
        ):
            bearer_other = form_bearertoken_file(
                user_wallet.wallet_path,
                cid,
                [
                    EACLRule(
                        operation=op,
                        access=EACLAccess.ALLOW,
                        role=EACLRole.OTHERS,
                    )
                    for op in EACLOperation
                ],
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            assert can_get_head_object(
                other_wallet.wallet_path,
                cid,
                objects_without_header[0],
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                bearer=bearer_other,
            )
            assert can_get_object(
                other_wallet.wallet_path,
                cid,
                objects_without_header[0],
                file_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
                bearer=bearer_other,
            )
            assert can_put_object(
                other_wallet.wallet_path,
                cid,
                file_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
                bearer=bearer_other,
            )

        with allure.step(f"Check other can get objects with attributes matching the filter"):
            oid = allow_objects.pop()
            assert can_get_head_object(
                other_wallet.wallet_path,
                cid,
                oid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            assert can_get_object(
                other_wallet.wallet_path,
                cid,
                oid,
                file_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )
            assert can_put_object(
                other_wallet.wallet_path,
                cid,
                file_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
                attributes=allow_attribute,
            )

        with allure.step("Check other cannot get objects without attributes matching the filter"):
            with pytest.raises(AssertionError):
                assert can_get_head_object(
                    other_wallet.wallet_path,
                    cid,
                    deny_objects[0],
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                )
            with pytest.raises(AssertionError):
                assert can_get_object(
                    other_wallet.wallet_path,
                    cid,
                    deny_objects[0],
                    file_path,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                )
            with pytest.raises(AssertionError):
                assert can_put_object(
                    other_wallet.wallet_path,
                    cid,
                    file_path,
                    attributes=deny_attribute,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                )

        with allure.step(
            "Check other can get objects without attributes matching the filter "
            "and using bearer token"
        ):
            oid = deny_objects.pop()
            assert can_get_head_object(
                other_wallet.wallet_path,
                cid,
                oid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
                bearer=bearer_other,
            )
            assert can_get_object(
                other_wallet.wallet_path,
                cid,
                oid,
                file_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
                bearer=bearer_other,
            )
            assert can_put_object(
                other_wallet.wallet_path,
                cid,
                file_path,
                shell=self.shell,
                neofs_env=self.neofs_env,
                attributes=deny_attribute,
                bearer=bearer_other,
            )

    @pytest.mark.parametrize(
        "operator",
        [
            EACLMatchType.NUM_GT,
            EACLMatchType.NUM_GE,
            EACLMatchType.NUM_LT,
            EACLMatchType.NUM_LE,
        ],
    )
    @pytest.mark.parametrize(
        "object_size",
        [pytest.lazy_fixture("simple_object_size"), pytest.lazy_fixture("complex_object_size")],
        ids=["simple object", "complex object"],
    )
    def test_extended_acl_numeric_values(self, wallets, operator, eacl_container, object_size):
        user_wallet = wallets.get_wallet()

        cid = eacl_container
        objects = []

        with allure.step("Add test objects to container"):
            for numeric_value in self.OBJECT_NUMERIC_VALUES:
                file_path = generate_file(object_size)

                objects.append(
                    {
                        self.OBJECT_NUMERIC_KEY_ATTR_NAME: numeric_value,
                        "id": put_object_to_random_node(
                            user_wallet.wallet_path,
                            file_path,
                            cid,
                            shell=self.shell,
                            neofs_env=self.neofs_env,
                            attributes={self.OBJECT_NUMERIC_KEY_ATTR_NAME: numeric_value},
                        ),
                        "file_path": file_path,
                    }
                )

        with allure.step(f"GET objects with any numeric value attribute should be allowed"):
            for obj in objects:
                assert can_get_object(
                    user_wallet.wallet_path,
                    cid,
                    obj["id"],
                    obj["file_path"],
                    self.shell,
                    neofs_env=self.neofs_env,
                ), self.OPERATION_NOT_ALLOWED_ERROR_MESSAGE

        for numeric_value in self.OBJECT_NUMERIC_VALUES:
            with allure.step(f"Deny GET for all objects {operator.value} {numeric_value}"):
                eacl_deny = [
                    EACLRule(
                        access=EACLAccess.DENY,
                        role=EACLRole.USER,
                        filters=EACLFilters(
                            [
                                EACLFilter(
                                    header_type=EACLHeaderType.OBJECT,
                                    match_type=operator,
                                    key=self.OBJECT_NUMERIC_KEY_ATTR_NAME,
                                    value=numeric_value,
                                )
                            ]
                        ),
                        operation=EACLOperation.GET,
                    )
                ]
                set_eacl(
                    user_wallet.wallet_path,
                    cid,
                    create_eacl(cid, eacl_deny, shell=self.shell),
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                )
                wait_for_cache_expired()
                get_eacl(
                    user_wallet.wallet_path,
                    cid,
                    shell=self.shell,
                    endpoint=self.neofs_env.sn_rpc,
                )

            with allure.step(
                f"GET object with numeric value attribute {operator.value} {numeric_value} should be denied"
            ):
                for obj in objects:
                    if operator.compare(obj[self.OBJECT_NUMERIC_KEY_ATTR_NAME], numeric_value):
                        assert not can_get_object(
                            user_wallet.wallet_path,
                            cid,
                            obj["id"],
                            obj["file_path"],
                            self.shell,
                            neofs_env=self.neofs_env,
                        ), self.OPERATION_ALLOWED_ERROR_MESSAGE

            with allure.step(
                f"GET object with numeric value attribute not {operator.value} {numeric_value} should be allowed"
            ):
                for obj in objects:
                    if not operator.compare(obj[self.OBJECT_NUMERIC_KEY_ATTR_NAME], numeric_value):
                        assert can_get_object(
                            user_wallet.wallet_path,
                            cid,
                            obj["id"],
                            obj["file_path"],
                            self.shell,
                            neofs_env=self.neofs_env,
                        ), self.OPERATION_NOT_ALLOWED_ERROR_MESSAGE

    @pytest.mark.parametrize(
        "operator",
        [
            EACLMatchType.NUM_GT,
            EACLMatchType.NUM_GE,
            EACLMatchType.NUM_LT,
            EACLMatchType.NUM_LE,
        ],
    )
    @pytest.mark.parametrize(
        "invalid_attr_value",
        ["abc", "92.1", "93-1"],
    )
    def test_extended_acl_numeric_values_invalid_filters(
        self, wallets, operator, eacl_container, simple_object_size, invalid_attr_value
    ):
        user_wallet = wallets.get_wallet()

        cid = eacl_container
        oid = None

        with allure.step("Add test object to container"):
            file_path = generate_file(simple_object_size)

            oid = put_object_to_random_node(
                user_wallet.wallet_path,
                file_path,
                cid,
                shell=self.shell,
                neofs_env=self.neofs_env,
                attributes={self.OBJECT_NUMERIC_KEY_ATTR_NAME: 0},
            )

        with allure.step(f"GET object with numeric value attribute should be allowed"):
            assert can_get_object(
                user_wallet.wallet_path,
                cid,
                oid,
                file_path,
                self.shell,
                neofs_env=self.neofs_env,
            ), self.OPERATION_NOT_ALLOWED_ERROR_MESSAGE

        with allure.step(f"Deny GET for all objects {operator.value} {invalid_attr_value}"):
            eacl_deny = [
                EACLRule(
                    access=EACLAccess.DENY,
                    role=EACLRole.USER,
                    filters=EACLFilters(
                        [
                            EACLFilter(
                                header_type=EACLHeaderType.OBJECT,
                                match_type=operator,
                                key=self.OBJECT_NUMERIC_KEY_ATTR_NAME,
                                value=invalid_attr_value,
                            )
                        ]
                    ),
                    operation=EACLOperation.GET,
                )
            ]
            with pytest.raises(Exception, match=INVALID_RULES):
                create_eacl(cid, eacl_deny, shell=self.shell)

    def test_extended_acl_numeric_values_attr_str_filter_numeric(
        self, wallets, eacl_container, simple_object_size
    ):
        operator = EACLMatchType.NUM_GT
        user_wallet = wallets.get_wallet()

        cid = eacl_container
        oid = None

        with allure.step("Add test object to container"):
            file_path = generate_file(simple_object_size)

            oid = put_object_to_random_node(
                user_wallet.wallet_path,
                file_path,
                cid,
                shell=self.shell,
                neofs_env=self.neofs_env,
                attributes={self.OBJECT_NUMERIC_KEY_ATTR_NAME: "abc"},
            )

        with allure.step(f"GET object with numeric value attribute should be allowed"):
            assert can_get_object(
                user_wallet.wallet_path,
                cid,
                oid,
                file_path,
                self.shell,
                neofs_env=self.neofs_env,
            ), self.OPERATION_NOT_ALLOWED_ERROR_MESSAGE

        with allure.step(f"Deny GET for all objects {operator.value} 0"):
            eacl_deny = [
                EACLRule(
                    access=EACLAccess.DENY,
                    role=EACLRole.USER,
                    filters=EACLFilters(
                        [
                            EACLFilter(
                                header_type=EACLHeaderType.OBJECT,
                                match_type=operator,
                                key=self.OBJECT_NUMERIC_KEY_ATTR_NAME,
                                value=0,
                            )
                        ]
                    ),
                    operation=EACLOperation.GET,
                )
            ]
            set_eacl(
                user_wallet.wallet_path,
                cid,
                create_eacl(cid, eacl_deny, shell=self.shell),
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            wait_for_cache_expired()
            get_eacl(
                user_wallet.wallet_path,
                cid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step(f"GET object with numeric value attribute should be allowed"):
            assert can_get_object(
                user_wallet.wallet_path,
                cid,
                oid,
                file_path,
                self.shell,
                neofs_env=self.neofs_env,
            ), self.OPERATION_NOT_ALLOWED_ERROR_MESSAGE

    def test_extended_acl_numeric_values_expiration_attr(
        self, wallets, eacl_container, complex_object_size
    ):
        user_wallet = wallets.get_wallet()

        cid = eacl_container
        oid = None

        epoch = self.get_epoch()

        with allure.step(f"Set EACLs for GET/PUT to restrict operations with expiration attribute"):
            eacl_deny = [
                EACLRule(
                    access=EACLAccess.DENY,
                    role=EACLRole.USER,
                    filters=EACLFilters(
                        [
                            EACLFilter(
                                header_type=EACLHeaderType.OBJECT,
                                match_type=EACLMatchType.NUM_GE,
                                key=self.EXPIRATION_OBJECT_ATTR,
                                value=epoch + 2,
                            ),
                        ]
                    ),
                    operation=EACLOperation.GET,
                ),
                EACLRule(
                    access=EACLAccess.DENY,
                    role=EACLRole.USER,
                    filters=EACLFilters(
                        [
                            EACLFilter(
                                header_type=EACLHeaderType.OBJECT,
                                match_type=EACLMatchType.NUM_GT,
                                key=self.EXPIRATION_OBJECT_ATTR,
                                value=epoch + 2,
                            ),
                        ]
                    ),
                    operation=EACLOperation.PUT,
                ),
                EACLRule(
                    access=EACLAccess.DENY,
                    role=EACLRole.USER,
                    filters=EACLFilters(
                        [
                            EACLFilter(
                                header_type=EACLHeaderType.OBJECT,
                                match_type=EACLMatchType.NUM_LT,
                                key=self.EXPIRATION_OBJECT_ATTR,
                                value=epoch + 2,
                            ),
                        ]
                    ),
                    operation=EACLOperation.PUT,
                ),
            ]
            set_eacl(
                user_wallet.wallet_path,
                cid,
                create_eacl(cid, eacl_deny, shell=self.shell),
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            wait_for_cache_expired()
            get_eacl(
                user_wallet.wallet_path,
                cid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Add test object to container"):
            file_path = generate_file(complex_object_size)

            oid = put_object_to_random_node(
                user_wallet.wallet_path,
                file_path,
                cid,
                shell=self.shell,
                neofs_env=self.neofs_env,
                expire_at=epoch + 2,
            )

            with allure.step(f"PUT object should not be allowed because value is GT epoch + 2"):
                with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
                    put_object_to_random_node(
                        user_wallet.wallet_path,
                        file_path,
                        cid,
                        shell=self.shell,
                        neofs_env=self.neofs_env,
                        expire_at=epoch + 3,
                    )

            with allure.step(f"PUT object should not be allowed because value is LT epoch + 2"):
                with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
                    put_object_to_random_node(
                        user_wallet.wallet_path,
                        file_path,
                        cid,
                        shell=self.shell,
                        neofs_env=self.neofs_env,
                        expire_at=epoch + 1,
                    )

        with allure.step(f"GET object should not be allowed due to the value of expiration"):
            assert not can_get_object(
                user_wallet.wallet_path,
                cid,
                oid,
                file_path,
                self.shell,
                neofs_env=self.neofs_env,
            ), self.OPERATION_ALLOWED_ERROR_MESSAGE

    def test_extended_acl_numeric_values_payload_attr(
        self, wallets, eacl_container, complex_object_size
    ):
        user_wallet = wallets.get_wallet()

        cid = eacl_container

        with allure.step(f"Set EACLs for PUT to restrict small objects"):
            eacl_deny = [
                EACLRule(
                    access=EACLAccess.DENY,
                    role=EACLRole.USER,
                    filters=EACLFilters(
                        [
                            EACLFilter(
                                header_type=EACLHeaderType.OBJECT,
                                match_type=EACLMatchType.NUM_LT,
                                key=self.PAYLOAD_LENGTH_OBJECT_ATTR,
                                value=complex_object_size + 1,
                            ),
                        ]
                    ),
                    operation=EACLOperation.PUT,
                ),
            ]
            set_eacl(
                user_wallet.wallet_path,
                cid,
                create_eacl(cid, eacl_deny, shell=self.shell),
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            wait_for_cache_expired()
            get_eacl(
                user_wallet.wallet_path,
                cid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        small_file_path = generate_file(complex_object_size)

        with allure.step(
            f"PUT object should not be allowed because size is LT {complex_object_size + 1}"
        ):
            with pytest.raises(Exception, match=OBJECT_ACCESS_DENIED):
                put_object_to_random_node(
                    user_wallet.wallet_path,
                    small_file_path,
                    cid,
                    shell=self.shell,
                    neofs_env=self.neofs_env,
                )

        big_file_path = generate_file(complex_object_size + 1)

        with allure.step(
            f"PUT object should be allowed because size is EQ {complex_object_size + 1}"
        ):
            oid1 = put_object_to_random_node(
                user_wallet.wallet_path,
                big_file_path,
                cid,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step(f"GET object should be allowed"):
            assert can_get_object(
                user_wallet.wallet_path,
                cid,
                oid1,
                big_file_path,
                self.shell,
                neofs_env=self.neofs_env,
            ), self.OPERATION_NOT_ALLOWED_ERROR_MESSAGE

        very_big_file_path = generate_file(complex_object_size * 2)

        with allure.step(
            f"PUT object should be allowed because value is GT {complex_object_size + 1}"
        ):
            oid2 = put_object_to_random_node(
                user_wallet.wallet_path,
                very_big_file_path,
                cid,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step(f"GET object should be allowed"):
            assert can_get_object(
                user_wallet.wallet_path,
                cid,
                oid2,
                very_big_file_path,
                self.shell,
                neofs_env=self.neofs_env,
            ), self.OPERATION_NOT_ALLOWED_ERROR_MESSAGE

    def test_extended_acl_numeric_values_epoch_attr(
        self, wallets, eacl_container, complex_object_size
    ):
        user_wallet = wallets.get_wallet()

        epoch = self.get_epoch()

        cid = eacl_container

        with allure.step(f"Set EACLs for GET to restrict old objects"):
            eacl_deny = [
                EACLRule(
                    access=EACLAccess.DENY,
                    role=EACLRole.USER,
                    filters=EACLFilters(
                        [
                            EACLFilter(
                                header_type=EACLHeaderType.OBJECT,
                                match_type=EACLMatchType.NUM_LT,
                                key=self.CREATION_EPOCH_OBJECT_ATTR,
                                value=epoch + 1,
                            ),
                        ]
                    ),
                    operation=EACLOperation.GET,
                ),
            ]
            set_eacl(
                user_wallet.wallet_path,
                cid,
                create_eacl(cid, eacl_deny, shell=self.shell),
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )
            wait_for_cache_expired()
            get_eacl(
                user_wallet.wallet_path,
                cid,
                shell=self.shell,
                endpoint=self.neofs_env.sn_rpc,
            )

        with allure.step("Add test object to container"):
            file_path = generate_file(complex_object_size)

            oid = put_object_to_random_node(
                user_wallet.wallet_path,
                file_path,
                cid,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step(f"GET object should not be allowed"):
            assert not can_get_object(
                user_wallet.wallet_path,
                cid,
                oid,
                file_path,
                self.shell,
                neofs_env=self.neofs_env,
            ), self.OPERATION_ALLOWED_ERROR_MESSAGE

        self.tick_epoch()

        with allure.step("Add test object to container"):
            file_path = generate_file(complex_object_size)

            oid = put_object_to_random_node(
                user_wallet.wallet_path,
                file_path,
                cid,
                shell=self.shell,
                neofs_env=self.neofs_env,
            )

        with allure.step(f"GET object should be allowed"):
            assert can_get_object(
                user_wallet.wallet_path,
                cid,
                oid,
                file_path,
                self.shell,
                neofs_env=self.neofs_env,
            ), self.OPERATION_NOT_ALLOWED_ERROR_MESSAGE
