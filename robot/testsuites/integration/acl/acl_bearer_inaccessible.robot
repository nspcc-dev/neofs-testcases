*** Settings ***

Variables   ../../../variables/common.py
Library     ../${RESOURCES}/neofs.py
Library     ../${RESOURCES}/payment_neogo.py

Library     Collections
Resource    common_steps_acl_bearer.robot
Resource    ../${RESOURCES}/payment_operations.robot
Resource    ../${RESOURCES}/setup_teardown.robot

*** Test cases ***
BearerToken Operations for Inaccessible Container
    [Documentation]         Testcase to validate NeoFS operations with BearerToken for Inaccessible Container.
    [Tags]                  ACL  NeoFS  NeoCLI BearerToken
    [Timeout]               20 min

    [Setup]                 Setup

                            Generate Keys
                            Prepare eACL Role rules

                            Log    Check Bearer token with simple object
                            Generate file    ${SIMPLE_OBJ_SIZE}
                            Check Container Inaccessible and Allow All Bearer

                            Log    Check Bearer token with complex object

                            Generate file    ${COMPLEX_OBJ_SIZE}
                            Check Container Inaccessible and Allow All Bearer

    [Teardown]              Teardown    acl_bearer_inaccessible

*** Keywords ***

Check Container Inaccessible and Allow All Bearer
    ${CID} =                Create Container Inaccessible

                            Run Keyword And Expect Error        *
                            ...  Put object            ${USER_KEY}    ${FILE_S}     ${CID}           ${EMPTY}       ${FILE_USR_HEADER}
                            Run Keyword And Expect Error        *
                            ...  Get object          ${USER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Search object                  ${USER_KEY}    ${CID}        ${EMPTY}         ${EMPTY}       ${FILE_USR_HEADER}
                            Run Keyword And Expect Error        *
                            ...  Head object                    ${USER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}
                            Run Keyword And Expect Error        *
                            ...  Get Range                      ${USER_KEY}    ${CID}        ${S_OID_USER}    s_get_range    ${EMPTY}              0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                  ${USER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}

    ${rule1}=               Create Dictionary    Operation=PUT             Access=ALLOW    Role=USER
    ${rule2}=               Create Dictionary    Operation=SEARCH          Access=ALLOW    Role=USER

    ${eACL_gen}=            Create List    ${rule1}    ${rule2}

                            Form BearerToken file               ${USER_KEY}    ${CID}    bearer_allow_all_user   ${eACL_gen}   100500

                            Run Keyword And Expect Error        *
                            ...  Put object            ${USER_KEY}    ${FILE_S}     ${CID}           bearer_allow_all_user       ${FILE_USR_HEADER}
                            Run Keyword And Expect Error        *
                            ...  Search object                  ${USER_KEY}    ${CID}        ${EMPTY}         bearer_allow_all_user       ${FILE_USR_HEADER}
