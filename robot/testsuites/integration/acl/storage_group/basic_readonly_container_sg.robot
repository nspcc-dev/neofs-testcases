*** Settings ***
Variables    common.py

Library     container.py
Library     neofs_verbs.py
Library     utility_keywords.py

Resource    payment_operations.robot
Resource    storage_group.robot


*** Test cases ***
Basic ACL Operations for Read-Only Container
    [Documentation]         Testcase to validate NeoFS operations with ACL for Read-Only Container.
    [Tags]                  ACL
    [Timeout]               20 min


                            Check Read-Only Container    Simple
                            Check Read-Only Container    Complex



*** Keywords ***


Check Read-Only Container
    [Arguments]     ${COMPLEXITY}

    ${FILE}    ${_} =       Run Keyword If      """${COMPLEXITY}""" == """Simple"""
                            ...         Generate file    ${SIMPLE_OBJ_SIZE}
                            ...     ELSE
                            ...         Generate file    ${COMPLEX_OBJ_SIZE}

    ${USER_WALLET}
    ...     ${_}
    ...     ${_} =          Prepare Wallet And Deposit
    ${WALLET_OTH}
    ...     ${_}
    ...     ${_} =          Prepare Wallet And Deposit
    ${READONLY_CID} =       Create Container    ${USER_WALLET}   basic_acl=public-read

    ${OID} =                Put object      ${USER_WALLET}    ${FILE}    ${READONLY_CID}
    @{OBJECTS} =            Create List     ${OID}

    ${SG_1} =               Put Storagegroup    ${USER_WALLET}    ${READONLY_CID}   ${OBJECTS}

    Run Storage Group Operations And Expect Success
    ...     ${USER_WALLET}  ${READONLY_CID}     ${OBJECTS}  ${COMPLEXITY}

    Run Storage Group Operations On Other's Behalf in RO Container
    ...     ${USER_WALLET}  ${READONLY_CID}     ${OBJECTS}  ${COMPLEXITY}

    Run Storage Group Operations On System's Behalf in RO Container
    ...                     ${READONLY_CID}     ${OBJECTS}  ${COMPLEXITY}
