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
    set_eacl,
    wait_for_cache_expired,
)
from helpers.container import create_container, delete_container
from helpers.container_access import check_full_access_to_container, check_no_access_to_container
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
