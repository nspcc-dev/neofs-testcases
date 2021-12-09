*** Settings ***
Variables   common.py
Variables   wellknown_acl.py

Library     neofs.py
Library     payment_neogo.py
Library     wallet_keywords.py

Resource    setup_teardown.robot
Resource    payment_operations.robot

*** Variables ***
${CONTAINER_WAIT_INTERVAL} =    1 min

*** Test Cases ***
Delete Containers
    [Documentation]             Testcase to check if containers can be deleted.
    [Tags]                      Container  NeoFS  NeoCLI
    [Timeout]                   10 min

    [Setup]                     Setup

    ${WALLET}   ${ADDR}     ${USER_KEY} =   Init Wallet with Address    ${ASSETS_DIR}
    Payment Operations      ${ADDR}         ${USER_KEY}
    ${WALLET_OTH}   ${ADDR_OTH}     ${OTHER_KEY} =   Init Wallet with Address    ${ASSETS_DIR}
    Payment Operations      ${ADDR_OTH}         ${OTHER_KEY}

    ${CID} =                    Create container    ${USER_KEY}    ${PUBLIC_ACL}      ${COMMON_PLACEMENT_RULE}
                                Wait Until Keyword Succeeds    ${MORPH_BLOCK_TIME}    ${CONTAINER_WAIT_INTERVAL}
                                ...     Container Existing     ${USER_KEY}    ${CID}

                                Run Keyword And Expect Error    *
                                ...    Delete Container    ${CID}    ${OTHER_KEY}

                                Delete Container    ${CID}    ${USER_KEY}

    ${EXPECTED_ERROR} =         Run Keyword And Expect Error    *
                                ...    Delete Container    ${CID}    ${USER_KEY}
                                Log    Container cannot be deleted: ${EXPECTED_ERROR}

    [Teardown]                  Teardown    container_delete
