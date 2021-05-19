*** Settings ***
Variables   ../../../variables/common.py

Library     ${KEYWORDS}/wallet_keywords.py
Library     ${KEYWORDS}/rpc_call_keywords.py

*** Variables ***
${FILE_USR_HEADER} =    key1=1,key2=abc
${FILE_USR_HEADER_OTH} =    key1=2
${UNEXIST_OID} =        B2DKvkHnLnPvapbDgfpU1oVUPuXQo5LTfKVxmNDZXQff
${TRANSFER_AMOUNT} =    15
${DEPOSIT_AMOUNT} =     10
${EMPTY_ACL} =          ""

*** Keywords ***

Payment operations
    ${WALLET}   ${ADDR}     ${PRIV_KEY} =   Init Wallet with Address    ${TEMP_DIR}
    ${TX} =             Transfer Mainnet Gas                  ${MAINNET_WALLET_WIF}    ${ADDR}     ${TRANSFER_AMOUNT}

                        Wait Until Keyword Succeeds           ${MAINNET_TIMEOUT}    ${MAINNET_BLOCK_TIME}
                        ...  Transaction accepted in block    ${TX}

    ${MAINNET_BALANCE} =    Get Mainnet Balance                   ${ADDR}
    Should Be Equal As Numbers                                    ${MAINNET_BALANCE}  ${TRANSFER_AMOUNT}

    ${SCRIPT_HASH} =    Get ScriptHash                        ${PRIV_KEY}

    ${TX_DEPOSIT} =     NeoFS Deposit                         ${WALLET}    ${ADDR}    ${SCRIPT_HASH}    ${DEPOSIT_AMOUNT}
                        Wait Until Keyword Succeeds           ${MAINNET_TIMEOUT}    ${MAINNET_BLOCK_TIME}
                        ...  Transaction accepted in block    ${TX_DEPOSIT}

    ${NEOFS_BALANCE} =  Get NeoFS Balance       ${PRIV_KEY}
    Should Be Equal As Numbers                  ${NEOFS_BALANCE}    ${DEPOSIT_AMOUNT}

                        Set Global Variable                   ${PRIV_KEY}    ${PRIV_KEY}
                        Set Global Variable                   ${ADDR}    ${ADDR}

Prepare container
    ${CID} =            Create container                      ${PRIV_KEY}   ${EMPTY_ACL}     ${COMMON_PLACEMENT_RULE}
                        Container Existing                    ${PRIV_KEY}   ${CID}

    ${NEOFS_BALANCE} =  Get NeoFS Balance     ${PRIV_KEY}
    Should Be True      ${NEOFS_BALANCE} < ${DEPOSIT_AMOUNT}
    ${CONTAINER_FEE} =  Evaluate      ${DEPOSIT_AMOUNT} - ${NEOFS_BALANCE}
    Log                 Container fee is ${CONTAINER_FEE}

                        Set Global Variable                   ${CID}    ${CID}
