*** Settings ***
Variables   common.py

Library     utility_keywords.py
Library     payment_neogo.py


*** Keywords ***

Prepare Wallet And Deposit
    [Arguments]    ${DEPOSIT}=${30}

    ${WALLET}
    ...     ${ADDR}
    ...     ${WIF} =    Generate Wallet
                        Transfer Mainnet Gas        ${WALLET}   ${DEPOSIT+1}
                        NeoFS Deposit               ${WALLET}   ${DEPOSIT}
    # Now we have TX in main chain, but deposit might not propagate into the side chain yet.
    # For certainty, sleeping during one morph block.
                        Sleep                       ${MORPH_BLOCK_TIME}

    [Return]    ${WALLET}    ${ADDR}    ${WIF}
