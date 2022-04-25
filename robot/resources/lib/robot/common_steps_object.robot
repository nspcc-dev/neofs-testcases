*** Settings ***
Variables   common.py

Library     container.py
Library     wallet_keywords.py
Library     rpc_call_keywords.py

*** Keywords ***

Prepare container
    [Arguments]     ${WIF}    ${WALLET}
    ${NEOFS_BALANCE} =  Get NeoFS Balance       ${WIF}
    ${CID} =            Create container          ${WALLET}

    ${NEW_NEOFS_BALANCE} =  Get NeoFS Balance     ${WIF}
    Should Be True      ${NEW_NEOFS_BALANCE} < ${NEOFS_BALANCE}
    ${CONTAINER_FEE} =  Evaluate      ${NEOFS_BALANCE} - ${NEW_NEOFS_BALANCE}
    Log                 Container fee is ${CONTAINER_FEE}

    [Return]    ${CID}
