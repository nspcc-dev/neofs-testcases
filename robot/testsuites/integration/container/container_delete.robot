*** Settings ***
Variables   ../../../variables/common.py

Library     ../${RESOURCES}/neofs.py
Library     ../${RESOURCES}/payment_neogo.py
Library     ${KEYWORDS}/wallet_keywords.py

Resource    ../${RESOURCES}/setup_teardown.robot
Resource    ../${RESOURCES}/payment_operations.robot

*** Variables ***

*** Test Cases ***
Delete Containers
    [Documentation]             Testcase to check if containers can be deleted.
    [Tags]                      Container  NeoFS  NeoCLI
    [Timeout]                   10 min

    [Setup]                     Setup

    ${WALLET}   ${ADDR}     ${USER_KEY} =   Init Wallet with Address    ${ASSETS_DIR}
    Payment Operations      ${ADDR}         ${USER_KEY}

    ${PRIV_CID} =               Create container       ${USER_KEY}        0x18888888              ${COMMON_PLACEMENT_RULE}
                                Container Existing     ${USER_KEY}        ${PRIV_CID}

    ${PUBLIC_CID} =             Create container       ${USER_KEY}        0x1FFFFFFF              ${COMMON_PLACEMENT_RULE}
                                Container Existing     ${USER_KEY}        ${PUBLIC_CID}

    ${READONLY_CID} =           Create container       ${USER_KEY}        0x1FFF88FF              ${COMMON_PLACEMENT_RULE}
                                Container Existing     ${USER_KEY}        ${READONLY_CID}              

    
                                Delete Container    ${PRIV_CID}    ${USER_KEY}
                                Delete Container    ${PUBLIC_CID}    ${USER_KEY}
                                Delete Container    ${READONLY_CID}    ${USER_KEY}     

    [Teardown]                  Teardown    container_delete 