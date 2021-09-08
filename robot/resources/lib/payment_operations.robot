*** Settings ***
Variables   ../../variables/common.py

Library     wallet_keywords.py
Library     rpc_call_keywords.py
Library     payment_neogo.py

*** Variables ***
${TRANSFER_AMOUNT} =    ${30}
${DEPOSIT_AMOUNT} =     ${25}


*** Keywords ***

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

Prepare Wallet And Deposit
    [Arguments]    ${DEPOSIT}=${30}
    
    Log    Deposit equals ${DEPOSIT}
    ${WALLET}    ${ADDR}    ${WIF} =    Init Wallet with Address    ${ASSETS_DIR}
    ${TX} =    Transfer Mainnet Gas                ${MAINNET_WALLET_WIF}    ${ADDR}    ${DEPOSIT+1}
               Wait Until Keyword Succeeds         ${MAINNET_TIMEOUT}    ${MAINNET_BLOCK_TIME}
               ...    Transaction accepted in block    ${TX}

    ${TX_DEPOSIT} =    NeoFS Deposit           ${WIF}          ${DEPOSIT}
                       Wait Until Keyword Succeeds             ${MAINNET_TIMEOUT}    ${MAINNET_BLOCK_TIME}
                       ...    Transaction accepted in block    ${TX_DEPOSIT}
    # Now we have TX in main chain, but deposit might not propagate into the side chain yet.
    # For certainty, sleeping during one morph block.
    Sleep               ${MORPH_BLOCK_TIME}

    [Return]    ${WALLET}    ${ADDR}    ${WIF}
