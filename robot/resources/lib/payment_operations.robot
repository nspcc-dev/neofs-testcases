*** Settings ***
Variables   ../../variables/common.py

Library     wallet_keywords.py
Library     rpc_call_keywords.py

*** Variables ***
${TRANSFER_AMOUNT} =    ${30}
${DEPOSIT_AMOUNT} =     ${25}


*** Keywords ***

Generate Keys
    ${WALLET}   ${ADDR}     ${USER_KEY_GEN} =   Init Wallet with Address    ${ASSETS_DIR}
    ${WALLET_OTH}   ${ADDR_OTH}     ${OTHER_KEY_GEN} =   Init Wallet with Address    ${ASSETS_DIR}

    Set Global Variable     ${USER_KEY}          ${USER_KEY_GEN}
    Set Global Variable     ${OTHER_KEY}         ${OTHER_KEY_GEN}
    Set Global Variable     ${SYSTEM_KEY_IR}     ${NEOFS_IR_WIF}
    Set Global Variable     ${SYSTEM_KEY_SN}     ${NEOFS_SN_WIF}

    Payment Operations      ${ADDR}         ${USER_KEY}
    Payment Operations      ${ADDR_OTH}     ${OTHER_KEY}


Payment Operations
    [Arguments]   ${ADDR}     ${WIF}

    ${TX} =       Transfer Mainnet Gas                ${MAINNET_WALLET_WIF}    ${ADDR}     ${TRANSFER_AMOUNT}
                  Wait Until Keyword Succeeds         ${MAINNET_TIMEOUT}       ${MAINNET_BLOCK_TIME}
                  ...  Transaction accepted in block  ${TX}

    ${MAINNET_BALANCE} =    Get Mainnet Balance     ${ADDR}
    Should Be Equal As Numbers                      ${MAINNET_BALANCE}  ${TRANSFER_AMOUNT}

    ${TX_DEPOSIT} =         NeoFS Deposit           ${WIF}      ${DEPOSIT_AMOUNT}
                            Wait Until Keyword Succeeds         ${MAINNET_TIMEOUT}  ${MAINNET_BLOCK_TIME}
                            ...  Transaction accepted in block  ${TX_DEPOSIT}
    # Now we have TX in main chain, but deposit might not propagate into the side chain yet.
    # For certainty, sleeping during one morph block.
    Sleep                   ${MORPH_BLOCK_TIME}

    ${NEOFS_BALANCE} =      Get NeoFS Balance       ${WIF}
    Should Be Equal As Numbers      ${NEOFS_BALANCE}    ${DEPOSIT_AMOUNT}
