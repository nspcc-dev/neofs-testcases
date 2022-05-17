*** Settings ***
Variables    common.py

Library     container.py
Library     neofs.py
Library     neofs_verbs.py
Library     payment_neogo.py
Library     storage_group.py

Resource    common_steps_acl_basic.robot
Resource    payment_operations.robot
Resource    setup_teardown.robot
Resource    storage_group.robot


*** Test cases ***
Basic ACL Operations for Read-Only Container
    [Documentation]         Testcase to validate NeoFS operations with ACL for Read-Only Container.
    [Tags]                  ACL
    [Timeout]               20 min

    [Setup]                 Setup

    ${WALLET}   ${_}     ${_} =         Prepare Wallet And Deposit
    ${WALLET_OTH}   ${_}     ${_} =     Prepare Wallet And Deposit

    ${READONLY_CID} =       Create Container    ${WALLET}   basic_acl=public-read
    ${FILE_S}    ${_} =     Generate file    ${SIMPLE_OBJ_SIZE}
                            Check Read-Only Container    Simple    ${WALLET}    ${FILE_S}    ${READONLY_CID}    ${WALLET_OTH}

    ${READONLY_CID} =       Create Container    ${WALLET}   basic_acl=public-read
    ${FILE_S}    ${_} =     Generate file    ${COMPLEX_OBJ_SIZE}
                            Check Read-Only Container    Complex    ${WALLET}    ${FILE_S}    ${READONLY_CID}    ${WALLET_OTH}

    [Teardown]              Teardown    acl_basic_readonly_container_storagegroup


*** Keywords ***


Check Read-Only Container
    [Arguments]     ${RUN_TYPE}    ${USER_WALLET}    ${FILE}    ${READONLY_CID}    ${WALLET_OTH}

    ${WALLET_IR}    ${ADDR_IR} =     Prepare Wallet with WIF And Deposit    ${NEOFS_IR_WIF}

    ${OID} =                Put object      ${USER_WALLET}    ${FILE}    ${READONLY_CID}
    @{OBJECTS} =            Create List     ${OID}

    ${SG_1} =               Put Storagegroup    ${USER_WALLET}    ${READONLY_CID}   ${OBJECTS}

    Run Storage Group Operations And Expect Success
    ...     ${USER_WALLET}  ${READONLY_CID}     ${OBJECTS}  ${RUN_TYPE}

    Run Storage Group Operations On Other's Behalf in RO Container
    ...     ${USER_WALLET}  ${READONLY_CID}     ${OBJECTS}  ${RUN_TYPE}

    Run Storage Group Operations On System's Behalf in RO Container
    ...                     ${READONLY_CID}     ${OBJECTS}  ${RUN_TYPE}
