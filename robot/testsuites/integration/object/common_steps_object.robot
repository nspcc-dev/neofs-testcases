*** Settings ***
Variables   ../../../variables/common.py

Library     ${KEYWORDS}/wallet_keywords.py
Library     ${KEYWORDS}/rpc_call_keywords.py

*** Variables ***
${FILE_USR_HEADER} =    key1=1,key2=abc
${FILE_USR_HEADER_OTH} =    key1=2
${UNEXIST_OID} =        B2DKvkHnLnPvapbDgfpU1oVUPuXQo5LTfKVxmNDZXQff
${PLACEMENT_RULE} =    REP 2 IN X CBF 1 SELECT 2 FROM * AS X

*** Keywords ***

Prepare container
    [Arguments]     ${WIF}
    ${NEOFS_BALANCE} =  Get NeoFS Balance       ${WIF}

    ${CID} =            Create container          ${WIF}   ${EMPTY}     ${PLACEMENT_RULE}
                        Container Existing        ${WIF}   ${CID}

    ${NEW_NEOFS_BALANCE} =  Get NeoFS Balance     ${WIF}
    Should Be True      ${NEW_NEOFS_BALANCE} < ${NEOFS_BALANCE}
    ${CONTAINER_FEE} =  Evaluate      ${NEOFS_BALANCE} - ${NEW_NEOFS_BALANCE}
    Log                 Container fee is ${CONTAINER_FEE}

    Set Global Variable       ${CID}    ${CID}
