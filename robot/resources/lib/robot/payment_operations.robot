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

# TODO: should be deleted in the scope of https://github.com/nspcc-dev/neofs-testcases/issues/191
Prepare Wallet with WIF And Deposit
    [Arguments]    ${WIF}    ${DEPOSIT}=${30}

    ${WALLET}
    ...     ${ADDR} =   Init Wallet from WIF    ${ASSETS_DIR}    ${WIF}
                        Transfer Mainnet Gas    ${WALLET}    ${DEPOSIT+1}
                        NeoFS Deposit           ${WALLET}    ${DEPOSIT}
                        Sleep                   ${MORPH_BLOCK_TIME}

    [Return]    ${WALLET}    ${ADDR}
