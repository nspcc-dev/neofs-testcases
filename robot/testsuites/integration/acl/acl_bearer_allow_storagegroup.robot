*** Settings ***
Variables   common.py

Library     Collections
Library     acl.py
Library     neofs.py
Library     neofs_verbs.py
Library     payment_neogo.py

Resource    eacl_tables.robot
Resource    common_steps_acl_bearer.robot
Resource    payment_operations.robot
Resource    setup_teardown.robot
Resource    complex_object_operations.robot


*** Test cases ***
BearerToken Operations
    [Documentation]         Testcase to validate NeoFS operations with BearerToken.
    [Tags]                  ACL  BearerToken
    [Timeout]               20 min

    [Setup]                 Setup

    ${WALLET}   ${_}     ${_} =   Prepare Wallet And Deposit

                            Log    Check Bearer token with simple object
    ${FILE_S} =             Generate file    ${SIMPLE_OBJ_SIZE}
                            Check eACL Deny and Allow All Bearer    Simple    ${WALLET}    ${FILE_S}

                            Log    Check Bearer token with complex object
    ${FILE_S} =             Generate file    ${COMPLEX_OBJ_SIZE}
                            Check eACL Deny and Allow All Bearer    Complex    ${WALLET}    ${FILE_S}


    [Teardown]              Teardown    acl_bearer_allow_storagegroup



*** Keywords ***

Check eACL Deny and Allow All Bearer
    [Arguments]     ${RUN_TYPE}    ${WALLET}    ${FILE_S}

    ${CID} =                Create Container Public    ${WALLET}
    ${S_OID_USER} =         Put object    ${WALLET}    ${FILE_S}    ${CID}
                            Prepare eACL Role rules    ${CID}


    # Storage group Operations (Put, List, Get, Delete)
    ${SG_OID_INV} =     Put Storagegroup    ${WALLET}    ${CID}   ${EMPTY}    ${S_OID_USER}
    ${SG_OID_1} =       Put Storagegroup    ${WALLET}    ${CID}   ${EMPTY}    ${S_OID_USER}
                        List Storagegroup    ${WALLET}    ${CID}   ${EMPTY}    ${SG_OID_1}  ${SG_OID_INV}
    @{EXPECTED_OIDS} =  Run Keyword If      "${RUN_TYPE}" == "Complex"
                        ...     Get Object Parts By Link Object    ${WALLET}    ${CID}   ${S_OID_USER}
                        ...     ELSE IF      "${RUN_TYPE}" == "Simple"   Create List   ${S_OID_USER}
                        Get Storagegroup    ${WALLET}    ${CID}    ${SG_OID_1}   ${EMPTY}    ${EMPTY}    @{EXPECTED_OIDS}
                        Delete Storagegroup    ${WALLET}    ${CID}    ${SG_OID_1}    ${EMPTY}

                        Set eACL            ${WALLET}    ${CID}        ${EACL_DENY_ALL_USER}

                        # The current ACL cache lifetime is 30 sec
                        Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

    ${rule1}=           Create Dictionary    Operation=GET             Access=ALLOW    Role=USER
    ${rule2}=           Create Dictionary    Operation=HEAD            Access=ALLOW    Role=USER
    ${rule3}=           Create Dictionary    Operation=PUT             Access=ALLOW    Role=USER
    ${rule4}=           Create Dictionary    Operation=DELETE          Access=ALLOW    Role=USER
    ${rule5}=           Create Dictionary    Operation=SEARCH          Access=ALLOW    Role=USER
    ${rule6}=           Create Dictionary    Operation=GETRANGE        Access=ALLOW    Role=USER
    ${rule7}=           Create Dictionary    Operation=GETRANGEHASH    Access=ALLOW    Role=USER

    ${eACL_gen}=        Create List    ${rule1}    ${rule2}    ${rule3}    ${rule4}    ${rule5}    ${rule6}    ${rule7}

    ${EACL_TOKEN} =     Form BearerToken File       ${WALLET}    ${CID}    ${eACL_gen}

                        # All storage groups should fail without bearer token
                        Run Keyword And Expect Error        *
                        ...  Put Storagegroup    ${WALLET}    ${CID}   ${EMPTY}    ${S_OID_USER}
                        Run Keyword And Expect Error        *
                        ...  List Storagegroup    ${WALLET}    ${CID}   ${EMPTY}    ${SG_OID_1}  ${SG_OID_INV}
                        Run Keyword And Expect Error        *
                        ...  Get Storagegroup    ${WALLET}    ${CID}    ${SG_OID_1}   ${EMPTY}    ${EMPTY}    @{EXPECTED_OIDS}
                        Run Keyword And Expect Error        *
                        ...  Delete Storagegroup    ${WALLET}    ${CID}    ${SG_OID_1}    ${EMPTY}

    # Storagegroup should passed with User group key and bearer token
    ${SG_OID_NEW} =     Put Storagegroup        ${WALLET}    ${CID}    ${EACL_TOKEN}    ${S_OID_USER}
                        List Storagegroup       ${WALLET}    ${CID}    ${EACL_TOKEN}    ${SG_OID_NEW}     ${SG_OID_INV}
                        Get Storagegroup        ${WALLET}    ${CID}    ${SG_OID_INV}    ${EACL_TOKEN}     ${EMPTY}    @{EXPECTED_OIDS}
                        Delete Storagegroup     ${WALLET}    ${CID}    ${SG_OID_INV}    ${EACL_TOKEN}
