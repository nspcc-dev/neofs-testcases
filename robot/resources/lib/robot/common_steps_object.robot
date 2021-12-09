*** Settings ***
Variables   common.py

Library     wallet_keywords.py
Library     rpc_call_keywords.py

*** Variables ***
${FILE_USR_HEADER} =    key1=1,key2=abc
${FILE_USR_HEADER_OTH} =    key1=2
${UNEXIST_OID} =        B2DKvkHnLnPvapbDgfpU1oVUPuXQo5LTfKVxmNDZXQff
${PLACEMENT_RULE} =    REP 2 IN X CBF 1 SELECT 2 FROM * AS X
${CONTAINER_WAIT_INTERVAL} =    1 min

*** Keywords ***

Prepare container
    [Arguments]     ${WIF}
    ${NEOFS_BALANCE} =  Get NeoFS Balance       ${WIF}

    ${CID} =            Create container          ${WIF}   ${EMPTY}     ${PLACEMENT_RULE}
                        Wait Until Keyword Succeeds        ${MORPH_BLOCK_TIME}       ${CONTAINER_WAIT_INTERVAL}
                        ...     Container Existing         ${WIF}   ${CID}

    ${NEW_NEOFS_BALANCE} =  Get NeoFS Balance     ${WIF}
    Should Be True      ${NEW_NEOFS_BALANCE} < ${NEOFS_BALANCE}
    ${CONTAINER_FEE} =  Evaluate      ${NEOFS_BALANCE} - ${NEW_NEOFS_BALANCE}
    Log                 Container fee is ${CONTAINER_FEE}

    Set Global Variable       ${CID}    ${CID}
