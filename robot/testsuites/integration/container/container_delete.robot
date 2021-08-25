*** Settings ***
Variables   ../../../variables/common.py

Library     ../${RESOURCES}/neofs.py
Library     ../${RESOURCES}/payment_neogo.py
Library     ${KEYWORDS}/wallet_keywords.py

Resource    ../${RESOURCES}/setup_teardown.robot
Resource    ../${RESOURCES}/payment_operations.robot

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

    ${CID} =                    Create container       ${USER_KEY}        0x1FFFFFFF              ${COMMON_PLACEMENT_RULE}
                                Container Existing     ${USER_KEY}        ${CID}   

                                Run Keyword And Expect Error    *
                                ...    Delete Container    ${CID}    ${OTHER_KEY}      

                                Delete Container    ${CID}    ${USER_KEY}

    ${EXPECTED_ERROR} =         Run Keyword And Expect Error    *
                                ...    Delete Container    ${CID}    ${USER_KEY}
                                Log    Container cannot be deleted: ${EXPECTED_ERROR} 

    [Teardown]                  Teardown    container_delete 
    