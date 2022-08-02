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


                            Check Private Container    Simple
                            Check Private Container    Complex



*** Keywords ***

Check Private Container
    [Arguments]     ${COMPLEXITY}

    ${FILE_S}    ${_} =     Run Keyword If      """${COMPLEXITY}""" == """Simple"""
                            ...         Generate file    ${SIMPLE_OBJ_SIZE}
                            ...     ELSE
                            ...         Generate file    ${COMPLEX_OBJ_SIZE}

    ${USER_WALLET}   ${_}     ${_} =   Prepare Wallet And Deposit
    ${OTHER_WALLET}   ${_}     ${_} =   Prepare Wallet And Deposit
    ${PRIV_CID} =       Create Container    ${USER_WALLET}

    ${OID} =            Put object      ${USER_WALLET}    ${FILE_S}    ${PRIV_CID}
    @{OBJECTS} =        Create List     ${OID}
    ${SG} =             Put Storagegroup    ${USER_WALLET}  ${PRIV_CID}  ${OBJECTS}

                        Run Storage Group Operations And Expect Success
                        ...     ${USER_WALLET}    ${PRIV_CID}   ${OBJECTS}      ${COMPLEXITY}

                        Run Storage Group Operations And Expect Failure
                        ...     ${OTHER_WALLET}    ${PRIV_CID}   ${OBJECTS}     ${SG}

    # In private container, Inner Ring is allowed to read (Storage Group List and Get),
    # so using here keyword for read-only container.
                        Run Storage Group Operations On System's Behalf In RO Container
                        ...                         ${PRIV_CID}   ${OBJECTS}    ${COMPLEXITY}
