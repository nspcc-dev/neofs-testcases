*** Settings ***
Variables    common.py

Library      neofs.py
Library      neofs_verbs.py
Library      payment_neogo.py
Library      contract_keywords.py

Resource     common_steps_acl_basic.robot
Resource     payment_operations.robot
Resource     setup_teardown.robot
Resource     complex_object_operations.robot


*** Test cases ***
Basic ACL Operations for Public Container
    [Documentation]         Testcase to validate NeoFS operations with ACL for Public Container.
    [Tags]                  ACL
    [Timeout]               20 min

    [Setup]                 Setup

    ${WALLET}   ${_}     ${_} =   Prepare Wallet And Deposit
    ${WALLET_OTH}   ${_}     ${_} =   Prepare Wallet And Deposit

    ${PUBLIC_CID} =         Create Public Container    ${WALLET}
    ${FILE_S}    ${_} =     Generate file    ${SIMPLE_OBJ_SIZE}
                            Check Public Container    Simple    ${WALLET}    ${FILE_S}    ${PUBLIC_CID}    ${WALLET_OTH}

    ${PUBLIC_CID} =         Create Public Container    ${WALLET}
    ${FILE_S}    ${_} =     Generate file    ${COMPLEX_OBJ_SIZE}
                            Check Public Container    Complex    ${WALLET}    ${FILE_S}    ${PUBLIC_CID}    ${WALLET_OTH}

    [Teardown]              Teardown    acl_basic_public_container_storagegroup


*** Keywords ***

Check Public Container
    [Arguments]     ${RUN_TYPE}    ${USER_WALLET}    ${FILE_S}    ${PUBLIC_CID}    ${WALLET_OTH}

    # Storage group Operations (Put, List, Get, Delete)
                            Log    Storage group Operations for each Role keys

    # Put target object to use in storage groups
    ${S_OID} =              Put object    ${USER_WALLET}    ${FILE_S}    ${PUBLIC_CID}

    ${WALLET_SN}    ${ADDR_SN} =     Prepare Wallet with WIF And Deposit    ${NEOFS_SN_WIF}
    ${WALLET_IR}    ${ADDR_IR} =     Prepare Wallet with WIF And Deposit    ${NEOFS_IR_WIF}
    
    @{ROLES_WALLETS_PASS} =    Create List    ${USER_WALLET}    ${WALLET_OTH}
    @{ROLES_WALLETS_SYS} =     Create List    ${WALLET_IR}    ${WALLET_SN}
    
    FOR	${ROLE_WALLET}	IN	@{ROLES_WALLETS_PASS}
        ${SG_OID_USERS} =    Put Storagegroup    ${ROLE_WALLET}    ${PUBLIC_CID}   ${EMPTY}    ${S_OID}
                            List Storagegroup    ${ROLE_WALLET}    ${PUBLIC_CID}   ${EMPTY}    ${SG_OID_USERS}
        @{EXPECTED_OIDS} =  Run Keyword If    "${RUN_TYPE}" == "Complex"
                            ...     Get Object Parts By Link Object    ${ROLE_WALLET}    ${PUBLIC_CID}   ${S_OID}
                            ...     ELSE IF   "${RUN_TYPE}" == "Simple"    Create List   ${S_OID}
                            Get Storagegroup    ${ROLE_WALLET}    ${PUBLIC_CID}    ${SG_OID_USERS}   ${EMPTY}    ${EMPTY}    @{EXPECTED_OIDS}
                            Delete Storagegroup    ${ROLE_WALLET}    ${PUBLIC_CID}    ${SG_OID_USERS}    ${EMPTY}
                            Tick Epoch
    END
    FOR	${ROLE_WALLET}	IN	@{ROLES_WALLETS_SYS}
        ${SG_OID_SYS} =     Put Storagegroup    ${ROLE_WALLET}    ${PUBLIC_CID}   ${EMPTY}    ${S_OID}
                            List Storagegroup    ${ROLE_WALLET}    ${PUBLIC_CID}   ${EMPTY}    ${SG_OID_SYS}
        @{EXPECTED_OIDS} =  Run Keyword If    "${RUN_TYPE}" == "Complex"
                            ...     Get Object Parts By Link Object    ${ROLE_WALLET}    ${PUBLIC_CID}   ${S_OID}
                            ...     ELSE IF   "${RUN_TYPE}" == "Simple"    Create List   ${S_OID}
                            Get Storagegroup    ${ROLE_WALLET}    ${PUBLIC_CID}    ${SG_OID_SYS}   ${EMPTY}    ${EMPTY}    @{EXPECTED_OIDS}
                            Run Keyword And Expect Error        *
                            ...  Delete Storagegroup    ${ROLE_WALLET}    ${PUBLIC_CID}    ${SG_OID_SYS}    ${EMPTY}
                            Delete Storagegroup    ${USER_WALLET}    ${PUBLIC_CID}    ${SG_OID_SYS}    ${EMPTY}
                            Tick Epoch
    END
