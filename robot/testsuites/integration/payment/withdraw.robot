*** Settings ***
Variables   ../../../variables/common.py

Library     ../${RESOURCES}/neofs.py
Library     ../${RESOURCES}/payment_neogo.py
Library     ${KEYWORDS}/wallet.py
Library     ../${RESOURCES}/utility_keywords.py

*** Variables ***
${DEPOSIT_AMOUNT} =     10
${WITHDRAW_AMOUNT} =    10

*** Test cases ***
NeoFS Deposit and Withdraw
    [Documentation]         Testcase to validate NeoFS Withdraw operation.
    [Tags]                  Withdraw  NeoFS  NeoCLI
    [Timeout]               10 min

    [Setup]                 Create Temporary Directory

    ${WALLET}   ${ADDR}     ${PRIV_KEY} =   Init Wallet with Address    ${TEMP_DIR}
    ${TX} =                 Transfer Mainnet Gas                  wallets/wallet.json     ${DEF_WALLET_ADDR}      ${ADDR}     15
                            Wait Until Keyword Succeeds           1 min                   15 sec
                            ...  Transaction accepted in block    ${TX}
                            Get Transaction                       ${TX}
    ${MAINNET_BALANCE} =    Expected Mainnet Balance              ${ADDR}                 15

    ${SCRIPT_HASH} =        Get ScriptHash                        ${PRIV_KEY}


    ${TX_DEPOSIT} =         NeoFS Deposit                         ${WALLET}              ${ADDR}    ${SCRIPT_HASH}    ${DEPOSIT_AMOUNT}
                            Wait Until Keyword Succeeds           1 min                  15 sec
                            ...  Transaction accepted in block    ${TX_DEPOSIT}
                            Get Transaction                       ${TX_DEPOSIT}



    # Expected amount diff will be formed from deposit amount and contract fee
    ${EXPECTED_DIFF} =      Evaluate                              -${DEPOSIT_AMOUNT}-${NEOFS_CONTRACT_DEPOSIT_GAS_FEE}
    ${DEPOSIT_BALANCE} =    Expected Mainnet Balance Diff         ${ADDR}                ${MAINNET_BALANCE}    ${EXPECTED_DIFF}

    ${NEOFS_BALANCE} =      Get Balance                           ${PRIV_KEY}

    ${TX} =                 Withdraw Mainnet Gas                  ${WALLET}              ${ADDR}    ${SCRIPT_HASH}    ${WITHDRAW_AMOUNT}
                            Wait Until Keyword Succeeds           1 min                  15 sec
                            ...  Transaction accepted in block    ${TX}

                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                            Get Balance                           ${PRIV_KEY}
                            Mainnet Balance                       ${ADDR}

                            Expected Balance                      ${PRIV_KEY}            ${NEOFS_BALANCE}    -${WITHDRAW_AMOUNT}

     # Expected amount diff will be formed from withdrawal amount and contract fee
     ${EXPECTED_DIFF_W} =   Evaluate                              ${WITHDRAW_AMOUNT}-${NEOFS_CONTRACT_WITHDRAW_GAS_FEE}
                            Expected Mainnet Balance Diff         ${ADDR}                ${DEPOSIT_BALANCE}    ${EXPECTED_DIFF_W}

    [Teardown]              Cleanup

*** Keywords ***

Cleanup
                            Cleanup Files
                            Get Docker Logs    withdraw
