*** Settings ***
Variables   common.py
Variables   wellknown_acl.py

Library     neofs.py
Library     payment_neogo.py
Library     wallet_keywords.py
Library     contract_keywords.py

Resource    setup_teardown.robot
Resource    payment_operations.robot

*** Variables ***
${CONTAINER_WAIT_INTERVAL} =    1 min

*** Test Cases ***
Delete Containers
    [Documentation]     Testcase to check if containers can be deleted.
    [Tags]              Container  NeoFS  NeoCLI
    [Timeout]           10 min

    [Setup]             Setup

    ${WALLET}   ${ADDR}     ${USER_KEY} =   Prepare Wallet And Deposit
    ${WALLET_OTH}   ${ADDR_OTH}     ${OTHER_KEY} =   Prepare Wallet And Deposit

    ${CID} =            Create container    ${USER_KEY}    ${PRIVATE_ACL_F}      ${COMMON_PLACEMENT_RULE}
                        Wait Until Keyword Succeeds    ${MORPH_BLOCK_TIME}    ${CONTAINER_WAIT_INTERVAL}
                        ...     Container Existing     ${USER_KEY}    ${CID}

    ################################################################
    # No explicit error is expected upon container deletion attempt
    ################################################################
                        Delete Container    ${CID}    ${OTHER_KEY}
                        Tick Epoch
                        Get container attributes    ${USER_KEY}    ${CID}

                        Delete Container    ${CID}    ${USER_KEY}
                        Tick Epoch
                        Run Keyword And Expect Error    *
                        ...  Get container attributes    ${USER_KEY}    ${CID}

                        Log    If one tries to delete an already deleted container, they should expect success.
                        Delete Container    ${CID}    ${USER_KEY}  

    [Teardown]                  Teardown    container_delete
