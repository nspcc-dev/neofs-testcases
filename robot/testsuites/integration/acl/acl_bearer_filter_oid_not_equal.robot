*** Settings ***
Variables   ../../../variables/common.py
Library     ../${RESOURCES}/neofs.py
Library     ../${RESOURCES}/payment_neogo.py

Library     Collections

Resource    common_steps_acl_bearer.robot
Resource    ../${RESOURCES}/payment_operations.robot
Resource    ../${RESOURCES}/setup_teardown.robot


*** Test cases ***
BearerToken Operations with Filter OID NotEqual
    [Documentation]         Testcase to validate NeoFS operations with BearerToken with Filter OID NotEqual.
    [Tags]                  ACL  NeoFS  NeoCLI BearerToken
    [Timeout]               20 min

    [Setup]                 Setup

                            Generate Keys
                            Prepare eACL Role rules

                            Log    Check Bearer token with simple object
                            Generate file    ${SIMPLE_OBJ_SIZE}
                            Check eACL Deny and Allow All Bearer Filter OID NotEqual

                            Log    Check Bearer token with complex object

                            Generate file    ${COMPLEX_OBJ_SIZE}
                            Check eACL Deny and Allow All Bearer Filter OID NotEqual

    [Teardown]              Teardown    acl_bearer_filter_oid_not_equal



*** Keywords ***


Prepare eACL Role rules
                            Log	                    Set eACL for different Role cases

    # eACL rules for all operations and similar permissions
    @{Roles} =	        Create List    OTHERS    USER    SYSTEM
    FOR	${role}	IN	@{Roles}
        ${rule1} =              Create Dictionary    Operation=GET             Access=DENY    Role=${role}
        ${rule2} =              Create Dictionary    Operation=HEAD            Access=DENY    Role=${role}
        ${rule3} =              Create Dictionary    Operation=PUT             Access=DENY    Role=${role}
        ${rule4} =              Create Dictionary    Operation=DELETE          Access=DENY    Role=${role}
        ${rule5} =              Create Dictionary    Operation=SEARCH          Access=DENY    Role=${role}
        ${rule6} =              Create Dictionary    Operation=GETRANGE        Access=DENY    Role=${role}
        ${rule7} =              Create Dictionary    Operation=GETRANGEHASH    Access=DENY    Role=${role}

        ${eACL_gen} =           Create List    ${rule1}    ${rule2}    ${rule3}    ${rule4}    ${rule5}    ${rule6}    ${rule7}
                                Form eACL json common file    gen_eacl_deny_all_${role}    ${eACL_gen}
                                Set Global Variable    ${EACL_DENY_ALL_${role}}       gen_eacl_deny_all_${role}
    END

Check eACL Deny and Allow All Bearer Filter OID NotEqual
    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER}
    ${S_OID_USER_2} =       Put object                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${EMPTY}
    ${D_OID_USER} =         Put object                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER_DEL}
    @{S_OBJ_H} =	        Create List	                        ${S_OID_USER}


                            Put object                 ${USER_KEY}    ${FILE_S}     ${CID}                   ${EMPTY}              ${FILE_OTH_HEADER}
                            Get object               ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}              local_file_eacl
                            Search object                       ${USER_KEY}    ${CID}        ${EMPTY}                 ${EMPTY}              ${FILE_USR_HEADER}         ${S_OBJ_H}
                            Head object                         ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}
                            Get Range                           ${USER_KEY}    ${CID}        ${S_OID_USER}            s_get_range            ${EMPTY}              0:256
                            Delete object                       ${USER_KEY}    ${CID}        ${D_OID_USER}            ${EMPTY}

                            Set eACL                            ${USER_KEY}    ${CID}        ${EACL_DENY_ALL_USER}

                            # The current ACL cache lifetime is 30 sec
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

    ${filters}=             Create Dictionary    headerType=OBJECT    matchType=STRING_NOT_EQUAL    key=$Object:objectID    value=${S_OID_USER_2}

    ${rule1}=               Create Dictionary    Operation=GET             Access=ALLOW    Role=USER    Filters=${filters}
    ${rule2}=               Create Dictionary    Operation=HEAD            Access=ALLOW    Role=USER    Filters=${filters}
    ${rule3}=               Create Dictionary    Operation=PUT             Access=ALLOW    Role=USER    Filters=${filters}
    ${rule4}=               Create Dictionary    Operation=DELETE          Access=ALLOW    Role=USER    Filters=${filters}
    ${rule5}=               Create Dictionary    Operation=SEARCH          Access=ALLOW    Role=USER    Filters=${filters}
    ${rule6}=               Create Dictionary    Operation=GETRANGE        Access=ALLOW    Role=USER    Filters=${filters}
    ${rule7}=               Create Dictionary    Operation=GETRANGEHASH    Access=ALLOW    Role=USER    Filters=${filters}

    ${eACL_gen}=            Create List    ${rule1}    ${rule2}    ${rule3}    ${rule4}    ${rule5}    ${rule6}    ${rule7}

                            Form BearerToken file               ${USER_KEY}    ${CID}    bearer_allow_all_user   ${eACL_gen}   100500

                            Run Keyword And Expect Error        *
                            ...  Put object            ${USER_KEY}    ${FILE_S}     ${CID}                   ${EMPTY}              ${FILE_USR_HEADER}
                            Run Keyword And Expect Error        *
                            ...  Get object          ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}              local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Search object                  ${USER_KEY}    ${CID}        ${EMPTY}                 ${EMPTY}              ${FILE_USR_HEADER}          ${S_OBJ_H}
                            Run Keyword And Expect Error        *
                            ...  Head object                    ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}
                            Run Keyword And Expect Error        *
                            ...  Get Range                      ${USER_KEY}    ${CID}        ${S_OID_USER}            s_get_range            ${EMPTY}              0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                  ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}
                            Run Keyword And Expect Error        *
                            ...  Search object                  ${USER_KEY}    ${CID}        ${EMPTY}                 bearer_allow_all_user               ${FILE_USR_HEADER}             ${S_OBJ_H}

                            Put object                 ${USER_KEY}    ${FILE_S}     ${CID}                   bearer_allow_all_user               ${FILE_OTH_HEADER}

                            Get object               ${USER_KEY}    ${CID}        ${S_OID_USER}            bearer_allow_all_user               local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Get object          ${USER_KEY}    ${CID}        ${S_OID_USER_2}          bearer_allow_all_user               local_file_eacl

                            Get Range                           ${USER_KEY}    ${CID}        ${S_OID_USER}            s_get_range            bearer_allow_all_user               0:256
                            Run Keyword And Expect Error        *
                            ...  Get Range                      ${USER_KEY}    ${CID}        ${S_OID_USER_2}          s_get_range            bearer_allow_all_user               0:256

                            Head object                         ${USER_KEY}    ${CID}        ${S_OID_USER}            bearer_allow_all_user
                            Run Keyword And Expect Error        *
                            ...  Head object                    ${USER_KEY}    ${CID}        ${S_OID_USER_2}          bearer_allow_all_user

                            Delete object                       ${USER_KEY}    ${CID}        ${S_OID_USER}            bearer_allow_all_user

                            Run Keyword And Expect Error        *
                            ...  Delete object                  ${USER_KEY}    ${CID}        ${D_OID_USER_2}          bearer_allow_all_user
