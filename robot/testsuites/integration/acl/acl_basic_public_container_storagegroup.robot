*** Settings ***
Variables    common.py

Library      neofs.py
Library      payment_neogo.py
Library      contract_keywords.py

Resource     common_steps_acl_basic.robot
Resource     payment_operations.robot
Resource     setup_teardown.robot


*** Test cases ***
Basic ACL Operations for Public Container
    [Documentation]         Testcase to validate NeoFS operations with ACL for Public Container.
    [Tags]                  ACL  NeoFS  NeoCLI
    [Timeout]               20 min

    [Setup]                 Setup

    ${WALLET}   ${ADDR}     ${USER_KEY} =   Prepare Wallet And Deposit  
    ${WALLET_OTH}   ${ADDR_OTH}     ${OTHER_KEY} =   Prepare Wallet And Deposit

    ${PUBLIC_CID} =         Create Public Container    ${USER_KEY}
    ${FILE_S}    ${FILE_S_HASH} =                        Generate file    ${SIMPLE_OBJ_SIZE}
                            Check Public Container    Simple    ${USER_KEY}    ${FILE_S}    ${PUBLIC_CID}    ${OTHER_KEY}

    ${PUBLIC_CID} =         Create Public Container    ${USER_KEY}
    ${FILE_S}    ${FILE_S_HASH} =             Generate file    ${COMPLEX_OBJ_SIZE}
                            Check Public Container    Complex    ${USER_KEY}    ${FILE_S}    ${PUBLIC_CID}    ${OTHER_KEY}

    [Teardown]              Teardown    acl_basic_public_container_storagegroup


*** Keywords ***

Check Public Container
    [Arguments]     ${RUN_TYPE}    ${USER_KEY}    ${FILE_S}    ${PUBLIC_CID}    ${OTHER_KEY}

    # Storage group Operations (Put, List, Get, Delete)
                            Log    Storage group Operations for each Role keys

    # Put target object to use in storage groups
    ${S_OID} =              Put object    ${USER_KEY}    ${FILE_S}    ${PUBLIC_CID}    ${EMPTY}    ${EMPTY}

    @{ROLES_KEYS_PASS} =    Create List    ${USER_KEY}    ${OTHER_KEY}
    @{ROLES_KEYS_SYS} =     Create List    ${NEOFS_IR_WIF}    ${NEOFS_SN_WIF}

    FOR	${ROLE_KEY}	IN	@{ROLES_KEYS_PASS}
        ${SG_OID_USERS} =    Put Storagegroup    ${ROLE_KEY}    ${PUBLIC_CID}   ${EMPTY}    ${S_OID}
                            List Storagegroup    ${ROLE_KEY}    ${PUBLIC_CID}   ${EMPTY}    ${SG_OID_USERS}
        @{EXPECTED_OIDS} =  Run Keyword If    "${RUN_TYPE}" == "Complex"    Get Split objects    ${ROLE_KEY}    ${PUBLIC_CID}   ${S_OID}
                            ...    ELSE IF   "${RUN_TYPE}" == "Simple"    Create List   ${S_OID}
                            Get Storagegroup    ${ROLE_KEY}    ${PUBLIC_CID}    ${SG_OID_USERS}   ${EMPTY}    ${EMPTY}    @{EXPECTED_OIDS}
                            Delete Storagegroup    ${ROLE_KEY}    ${PUBLIC_CID}    ${SG_OID_USERS}    ${EMPTY}
                            Tick Epoch
    END
    FOR	${ROLE_KEY}	IN	@{ROLES_KEYS_SYS}
        ${SG_OID_SYS} =     Put Storagegroup    ${ROLE_KEY}    ${PUBLIC_CID}   ${EMPTY}    ${S_OID}
                            List Storagegroup    ${ROLE_KEY}    ${PUBLIC_CID}   ${EMPTY}    ${SG_OID_SYS}
        @{EXPECTED_OIDS} =  Run Keyword If    "${RUN_TYPE}" == "Complex"    Get Split objects    ${ROLE_KEY}    ${PUBLIC_CID}   ${S_OID}
                            ...    ELSE IF   "${RUN_TYPE}" == "Simple"    Create List   ${S_OID}
                            Get Storagegroup    ${ROLE_KEY}    ${PUBLIC_CID}    ${SG_OID_SYS}   ${EMPTY}    ${EMPTY}    @{EXPECTED_OIDS}
                            Run Keyword And Expect Error        *
                            ...  Delete Storagegroup    ${ROLE_KEY}    ${PUBLIC_CID}    ${SG_OID_SYS}    ${EMPTY}
                            Delete Storagegroup    ${USER_KEY}    ${PUBLIC_CID}    ${SG_OID_SYS}    ${EMPTY}
                            Tick Epoch
    END
