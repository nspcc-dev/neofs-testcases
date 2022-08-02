*** Settings ***
Variables   common.py

Library     container.py
Library     Collections

Resource    setup_teardown.robot
Resource    payment_operations.robot

*** Variables ***
# The timeout during which the container should be deleted. The deletion after
# this time isn't guaranteed, but is expected as we run the test in an
# isolated environment.
${DELETE_TIMEOUT} =       30s


*** Test Cases ***
Delete Containers
    [Documentation]     Testcase to check if containers can be deleted by its owner only.
    [Tags]              Container
    [Timeout]           3 min


    ${WALLET}
    ...     ${_}
    ...     ${_} =      Prepare Wallet And Deposit
    ${ANOTHER_WALLET}
    ...     ${_}
    ...     ${_} =      Prepare Wallet And Deposit

    ${CID} =            Create container    ${WALLET}

    ################################################################
    # No explicit error is expected upon container deletion attempt
    ################################################################
                        Delete Container    ${ANOTHER_WALLET}    ${CID}
                        Sleep               ${DELETE_TIMEOUT}
    @{CONTAINERS} =     List Containers     ${WALLET}
                        List Should Contain Value
                            ...     ${CONTAINERS}
                            ...     ${CID}
                            ...     msg="A key which doesn't owe the container is able to delete ${CID}"

                        Delete Container    ${WALLET}     ${CID}
                        Sleep               ${DELETE_TIMEOUT}
    @{CONTAINERS} =     List Containers     ${WALLET}
                        List Should Not Contain Value
                            ...     ${CONTAINERS}
                            ...     ${CID}
                            ...     msg="${CID} is still in container list"

    ###################################################################################
    # If one tries to delete an already deleted container, they should expect success.
    ###################################################################################
                        Delete Container    ${WALLET}     ${CID}

