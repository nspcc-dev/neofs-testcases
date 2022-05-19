*** Settings ***
Variables   common.py

Library     Collections
Library     acl.py
Library     container.py
Library     neofs.py
Library     neofs_verbs.py

Resource    common_steps_acl_bearer.robot
Resource    eacl_tables.robot
Resource    payment_operations.robot
Resource    setup_teardown.robot
Resource    storage_group.robot


*** Test cases ***
BearerToken Operations
    [Documentation]         Testcase to validate NeoFS operations with BearerToken.
    [Tags]                  ACL  BearerToken
    [Timeout]               20 min

    [Setup]                 Setup

    ${WALLET}   ${_}     ${_} =   Prepare Wallet And Deposit

    ${FILE_S}    ${_} =     Generate file    ${SIMPLE_OBJ_SIZE}
                            Check eACL Deny and Allow All Bearer    Simple    ${WALLET}    ${FILE_S}

    ${FILE_S}    ${_} =     Generate file    ${COMPLEX_OBJ_SIZE}
                            Check eACL Deny and Allow All Bearer    Complex    ${WALLET}    ${FILE_S}

    [Teardown]              Teardown    acl_bearer_allow_storagegroup



*** Keywords ***

Check eACL Deny and Allow All Bearer
    [Arguments]         ${RUN_TYPE}    ${WALLET}    ${FILE_S}

    ${CID} =            Create Container            ${WALLET}   basic_acl=eacl-public-read-write
    ${OID} =            Put object      ${WALLET}    ${FILE_S}    ${CID}
    @{OBJECTS} =        Create List     ${OID}

                        Run Storage Group Operations and Expect Success
                            ...     ${WALLET}   ${CID}  ${OBJECTS}  ${RUN_TYPE}

    ${SG} =             Put Storagegroup    ${WALLET}    ${CID}   ${OBJECTS}

                        Prepare eACL Role rules    ${CID}
                        Set eACL            ${WALLET}    ${CID}        ${EACL_DENY_ALL_USER}

                        Run Storage Group Operations and Expect Failure
                            ...     ${WALLET}   ${CID}  ${OBJECTS}  ${SG}

    ${RULE_GET} =       Create Dictionary    Operation=GET             Access=ALLOW    Role=USER
    ${RULE_HEAD} =      Create Dictionary    Operation=HEAD            Access=ALLOW    Role=USER
    ${RULE_PUT} =       Create Dictionary    Operation=PUT             Access=ALLOW    Role=USER
    ${RULE_DELETE} =    Create Dictionary    Operation=DELETE          Access=ALLOW    Role=USER
    ${RULE_SEARCH} =    Create Dictionary    Operation=SEARCH          Access=ALLOW    Role=USER

    ${eACL_gen}=        Create List
                        ...     ${RULE_GET}
                        ...     ${RULE_HEAD}
                        ...     ${RULE_PUT}
                        ...     ${RULE_DELETE}
                        ...     ${RULE_SEARCH}
    ${EACL_TOKEN} =     Form BearerToken File       ${WALLET}    ${CID}    ${eACL_gen}

                        Run Storage Group Operations With Bearer Token
                        ...     ${WALLET}   ${CID}  ${OBJECTS}  ${EACL_TOKEN}   ${RUN_TYPE}
