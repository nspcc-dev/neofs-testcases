*** Settings ***
Variables    common.py

Library     container.py
Library     neofs_verbs.py
Library     utility_keywords.py

Resource    payment_operations.robot
Resource    storage_group.robot


*** Test cases ***
Basic ACL Operations for Public Container
    [Documentation]         Testcase to validate NeoFS operations with ACL for Public Container.
    [Tags]                  ACL
    [Timeout]               20 min


    ${WALLET}   ${_}     ${_} =   Prepare Wallet And Deposit
    ${WALLET_OTH}   ${_}     ${_} =   Prepare Wallet And Deposit

    ${PUBLIC_CID} =         Create Container    ${WALLET}   basic_acl=public-read-write
    ${FILE_S}    ${_} =     Generate file    ${SIMPLE_OBJ_SIZE}
                            Check Public Container    Simple    ${WALLET}    ${FILE_S}    ${PUBLIC_CID}    ${WALLET_OTH}

    ${PUBLIC_CID} =         Create Container    ${WALLET}   basic_acl=public-read-write
    ${FILE_S}    ${_} =     Generate file    ${COMPLEX_OBJ_SIZE}
                            Check Public Container    Complex    ${WALLET}    ${FILE_S}    ${PUBLIC_CID}    ${WALLET_OTH}



*** Keywords ***

Check Public Container
    [Arguments]     ${RUN_TYPE}    ${USER_WALLET}    ${FILE_S}    ${PUBLIC_CID}    ${WALLET_OTH}

    ${OID} =            Put object    ${USER_WALLET}    ${FILE_S}    ${PUBLIC_CID}
    @{OBJECTS} =        Create List      ${OID}

                        Run Storage Group Operations And Expect Success
                        ...     ${USER_WALLET}      ${PUBLIC_CID}   ${OBJECTS}  ${RUN_TYPE}

                        Run Storage Group Operations And Expect Success
                        ...     ${WALLET_OTH}       ${PUBLIC_CID}   ${OBJECTS}  ${RUN_TYPE}

                        # System isn't allowed to DELETE in Public Container
                        Run Storage Group Operations On System's Behalf In RO Container
                        ...     ${PUBLIC_CID}   ${OBJECTS}  ${RUN_TYPE}
