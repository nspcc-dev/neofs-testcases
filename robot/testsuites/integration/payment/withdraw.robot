*** Settings ***
Variables   ../../../variables/common.py

Library     ../${RESOURCES}/neofs.py
Library     ../${RESOURCES}/payment_neogo.py
Library     ../${RESOURCES}/utility_keywords.py
Library     ${KEYWORDS}/wallet_keywords.py
Library     ${KEYWORDS}/rpc_call_keywords.py

*** Variables ***
${DEPOSIT_AMOUNT} =     ${10}
${WITHDRAW_AMOUNT} =    ${10}
${TRANSFER_AMOUNT} =    ${15}

*** Test cases ***
NeoFS Deposit and Withdraw
    [Documentation]         Testcase to validate NeoFS Withdraw operation.
    [Tags]                  Withdraw  NeoFS  NeoCLI
    [Timeout]               10 min

    [Setup]                 Create Temporary Directory

    ${WALLET}   ${ADDR}     ${PRIV_KEY} =   Init Wallet with Address    ${TEMP_DIR}
    ${SCRIPT_HASH} =        Get ScriptHash                        ${PRIV_KEY}

    ##########################################################
    # Transferring GAS from initial wallet to our test wallet
    ##########################################################
    ${TX} =                 Transfer Mainnet Gas                  ${MAINNET_WALLET_WIF}     ${ADDR}     ${TRANSFER_AMOUNT}
                            Wait Until Keyword Succeeds           ${MAINNET_TIMEOUT}        ${MAINNET_BLOCK_TIME}
                            ...  Transaction accepted in block    ${TX}
    ${MAINNET_BALANCE} =    Get Mainnet Balance                   ${ADDR}
    Should Be Equal As Numbers                                    ${MAINNET_BALANCE}  ${TRANSFER_AMOUNT}

    ############################
    # Making deposit into NeoFS
    ############################
    ${TX_DEPOSIT} =         NeoFS Deposit                         ${WALLET}              ${ADDR}    ${SCRIPT_HASH}    ${DEPOSIT_AMOUNT}
                            Wait Until Keyword Succeeds           ${MAINNET_TIMEOUT}     ${MAINNET_BLOCK_TIME}
                            ...  Transaction accepted in block    ${TX_DEPOSIT}

    ${MAINNET_BALANCE} =    Get Mainnet Balance     ${ADDR}
    ${EXPECTED_BALANCE} =   Evaluate                ${TRANSFER_AMOUNT}-${DEPOSIT_AMOUNT}
    Should Be True          ${MAINNET_BALANCE} < ${EXPECTED_BALANCE}
    ${DEPOSIT_FEE} =        Evaluate       ${EXPECTED_BALANCE} - ${MAINNET_BALANCE}
    Log                     Deposit fee is ${DEPOSIT_FEE}

    ${NEOFS_BALANCE} =      Get NeoFS Balance                     ${PRIV_KEY}
    Should Be Equal As Numbers                ${NEOFS_BALANCE}    ${DEPOSIT_AMOUNT}

    # TODO: try to withdraw more than was deposited

    ###########################
    # Withdrawing deposit back
    ###########################
    ${TX} =                 Withdraw Mainnet Gas                  ${WALLET}              ${ADDR}    ${SCRIPT_HASH}    ${WITHDRAW_AMOUNT}
                            Wait Until Keyword Succeeds           ${MAINNET_TIMEOUT}     ${MAINNET_BLOCK_TIME}
                            ...  Transaction accepted in block    ${TX}

                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

    ${NEOFS_BALANCE} =      Get NeoFS Balance                     ${PRIV_KEY}
    ${EXPECTED_BALANCE} =   Evaluate                              ${DEPOSIT_AMOUNT} - ${WITHDRAW_AMOUNT}
    Should Be Equal As Numbers                ${NEOFS_BALANCE}    ${EXPECTED_BALANCE}

    ${MAINNET_BALANCE_AFTER} =      Get Mainnet Balance                   ${ADDR}
    ${MAINNET_BALANCE_DIFF} =       Evaluate    ${MAINNET_BALANCE_AFTER} - ${MAINNET_BALANCE}
    Should Be True          ${MAINNET_BALANCE_DIFF} < ${WITHDRAW_AMOUNT}
    ${WITHDRAW_FEE} =       Evaluate      ${WITHDRAW_AMOUNT} - ${MAINNET_BALANCE_DIFF}
    Log                     Withdraw fee is ${WITHDRAW_FEE}

    [Teardown]              Cleanup


*** Keywords ***

Cleanup
                            Cleanup Files
                            Get Docker Logs    withdraw
