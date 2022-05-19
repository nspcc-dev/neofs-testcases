*** Settings ***
Variables    common.py

Library     container.py
Library     neofs_verbs.py
Library     utility_keywords.py

Resource    payment_operations.robot
Resource    setup_teardown.robot
Resource    storage_group.robot


*** Test cases ***
Basic ACL Operations for Private Container
    [Documentation]         Storage Group operations with Private Container.
    [Tags]                  ACL
    [Timeout]               10 min

    [Setup]                 Setup

    ${WALLET}   ${_}     ${_} =   Prepare Wallet And Deposit
    ${WALLET_OTH}   ${_}     ${_} =   Prepare Wallet And Deposit

    ${PRIV_CID} =           Create Container    ${WALLET}
    ${FILE_S}    ${_} =     Generate file    ${SIMPLE_OBJ_SIZE}
                            Check Private Container    Simple    ${WALLET}    ${FILE_S}    ${PRIV_CID}    ${WALLET_OTH}

    ${PRIV_CID} =           Create Container    ${WALLET}
    ${FILE_S}    ${_} =     Generate file    ${COMPLEX_OBJ_SIZE}
                            Check Private Container    Complex    ${WALLET}    ${FILE_S}    ${PRIV_CID}    ${WALLET_OTH}

    [Teardown]              Teardown    acl_basic_private_container_storagegroup


*** Keywords ***

Check Private Container
    [Arguments]     ${RUN_TYPE}    ${USER_WALLET}    ${FILE_S}    ${PRIV_CID}    ${OTHER_WALLET}

    ${WALLET_IR}    ${ADDR_IR} =     Prepare Wallet with WIF And Deposit    ${NEOFS_IR_WIF}

    ${OID} =            Put object      ${USER_WALLET}    ${FILE_S}    ${PRIV_CID}
    @{OBJECTS} =        Create List     ${OID}

                        Run Storage Group Operations And Expect Success
                        ...     ${USER_WALLET}    ${PRIV_CID}   ${OBJECTS}      ${RUN_TYPE}

                        Run Storage Group Operations And Expect Failure
                        ...     ${OTHER_WALLET}    ${PRIV_CID}   ${OBJECTS}     ${RUN_TYPE}

    # In private container, Inner Ring is allowed to read (Storage Group List and Get),
    # so using here keyword for read-only container.
                        Run Storage Group Operations On System's Behalf In RO Container
                        ...                         ${PRIV_CID}   ${OBJECTS}    ${RUN_TYPE}
