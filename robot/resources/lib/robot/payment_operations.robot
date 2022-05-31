*** Settings ***
Variables   common.py

Library     wallet_keywords.py
Library     rpc_call_keywords.py
Library     payment_neogo.py


*** Keywords ***

Prepare Wallet And Deposit
    [Arguments]    ${DEPOSIT}=${30}
    
    Log    Deposit equals ${DEPOSIT}
    ${WALLET}    ${ADDR}    ${WIF} =    Init Wallet with Address    ${ASSETS_DIR}
    ${TX} =    Transfer Mainnet Gas                ${MAINNET_WALLET_WIF}    ${ADDR}    ${DEPOSIT+1}
               Wait Until Keyword Succeeds         ${MAINNET_TIMEOUT}    ${MAINNET_BLOCK_TIME}
               ...    Transaction accepted in block    ${TX}

    NeoFS Deposit           ${WIF}          ${DEPOSIT}
    # Now we have TX in main chain, but deposit might not propagate into the side chain yet.
    # For certainty, sleeping during one morph block.
    Sleep               ${MORPH_BLOCK_TIME}

    [Return]    ${WALLET}    ${ADDR}    ${WIF}

Prepare Wallet with WIF And Deposit
    [Arguments]    ${WIF}    ${DEPOSIT}=${30}    

    ${WALLET}    ${ADDR} =    Init Wallet from WIF    ${ASSETS_DIR}    ${WIF}
    ${TX} =             Transfer Mainnet Gas                ${MAINNET_WALLET_WIF}    ${ADDR}    ${DEPOSIT+1}
                        Wait Until Keyword Succeeds         ${MAINNET_TIMEOUT}    ${MAINNET_BLOCK_TIME}
                        ...    Transaction accepted in block    ${TX}

    NeoFS Deposit           ${WIF}          ${DEPOSIT}
    Sleep               ${MORPH_BLOCK_TIME}

    [Return]    ${WALLET}    ${ADDR}
