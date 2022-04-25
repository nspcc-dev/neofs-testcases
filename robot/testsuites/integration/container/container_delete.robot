*** Settings ***
Variables   common.py

Library     container.py
Library     wallet_keywords.py
Library     contract_keywords.py
Library     Collections

Resource    setup_teardown.robot
Resource    payment_operations.robot

*** Test Cases ***
Delete Containers
    [Documentation]     Testcase to check if containers can be deleted by its owner only.
    [Tags]              Container
    [Timeout]           2 min

    [Setup]             Setup

    ${_}   ${_}     ${USER_KEY} =   Prepare Wallet And Deposit
    ${_}   ${_}     ${OTHER_KEY} =   Prepare Wallet And Deposit

    ${CID} =            Create container    ${USER_KEY}

    ################################################################
    # No explicit error is expected upon container deletion attempt
    ################################################################
                        Delete Container    ${OTHER_KEY}    ${CID}
                        Tick Epoch
    @{CONTAINERS} =     List Containers     ${USER_KEY}
                        List Should Contain Value
                            ...     ${CONTAINERS}
                            ...     ${CID}
                            ...     msg="A key which doesn't owe the container is able to delete ${CID}"

                        Delete Container    ${USER_KEY}     ${CID}
                        Tick Epoch
    @{CONTAINERS} =     List Containers     ${USER_KEY}
                        List Should Not Contain Value
                            ...     ${CONTAINERS}
                            ...     ${CID}
                            ...     msg="${CID} is still in container list"

    ###################################################################################
    # If one tries to delete an already deleted container, they should expect success.
    ###################################################################################
                        Delete Container    ${USER_KEY}     ${CID}

    [Teardown]          Teardown    container_delete
