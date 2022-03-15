*** Settings ***
Variables   common.py

Library     Collections
Library     neofs.py
Library     neofs_verbs.py
Library     acl.py
Library     payment_neogo.py

Resource    eacl_tables.robot
Resource    common_steps_acl_bearer.robot
Resource    payment_operations.robot
Resource    setup_teardown.robot

*** Test cases ***
BearerToken Operations for Inaccessible Container
    [Documentation]         Testcase to validate NeoFS operations with BearerToken for Inaccessible Container.
    [Tags]                  ACL   BearerToken
    [Timeout]               20 min

    [Setup]                 Setup

    ${_}   ${_}     ${USER_KEY} =   Prepare Wallet And Deposit

                            Log    Check Bearer token with simple object
    ${FILE_S} =             Generate file    ${SIMPLE_OBJ_SIZE}
                            Check Container Inaccessible and Allow All Bearer    ${USER_KEY}    ${FILE_S}

                            Log    Check Bearer token with complex object
    ${FILE_S} =             Generate file    ${COMPLEX_OBJ_SIZE}
                            Check Container Inaccessible and Allow All Bearer    ${USER_KEY}    ${FILE_S}

    [Teardown]              Teardown    acl_bearer_inaccessible

*** Keywords ***

Check Container Inaccessible and Allow All Bearer
    [Arguments]    ${USER_KEY}    ${FILE_S}

    ${CID} =    Create Container Inaccessible    ${USER_KEY}
                Prepare eACL Role rules    ${CID}

                Run Keyword And Expect Error        *
                ...  Put object        ${USER_KEY}    ${FILE_S}     ${CID}    user_headers=${FILE_USR_HEADER}
                Run Keyword And Expect Error        *
                ...  Get object        ${USER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       local_file_eacl
                Run Keyword And Expect Error        *
                ...  Search object     ${USER_KEY}    ${CID}        ${EMPTY}         ${EMPTY}       ${FILE_USR_HEADER}
                Run Keyword And Expect Error        *
                ...  Head object       ${USER_KEY}    ${CID}        ${S_OID_USER}
                Run Keyword And Expect Error        *
                ...  Get Range         ${USER_KEY}    ${CID}        ${S_OID_USER}    s_get_range    ${EMPTY}      0:256
                Run Keyword And Expect Error        *
                ...  Delete object     ${USER_KEY}    ${CID}        ${S_OID_USER}

    ${rule1} =          Create Dictionary       Operation=PUT           Access=ALLOW    Role=USER
    ${rule2} =          Create Dictionary       Operation=SEARCH        Access=ALLOW    Role=USER
    ${eACL_gen} =       Create List             ${rule1}    ${rule2}

    ${EACL_TOKEN} =     Form BearerToken File       ${USER_KEY}    ${CID}   ${eACL_gen}

                Run Keyword And Expect Error        *
                ...  Put object        ${USER_KEY}    ${FILE_S}     ${CID}       bearer=${EACL_TOKEN}       user_headers=${FILE_USR_HEADER}
                Run Keyword And Expect Error        *
                ...  Search object     ${USER_KEY}    ${CID}        ${EMPTY}     ${EACL_TOKEN}       ${FILE_USR_HEADER}
