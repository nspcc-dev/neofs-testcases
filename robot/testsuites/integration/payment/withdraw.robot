*** Settings ***
Variables   common.py

Library     neofs.py
Library     payment_neogo.py
Library     wallet_keywords.py
Library     rpc_call_keywords.py

Resource    setup_teardown.robot

*** Variables ***
${DEPOSIT_AMOUNT} =     ${10}
${WITHDRAW_AMOUNT} =    ${10}
${TRANSFER_AMOUNT} =    ${15}

*** Test cases ***
NeoFS Deposit and Withdraw
    [Documentation]         Testcase to validate NeoFS Withdraw operation.
    [Timeout]               10 min

    [Setup]                 Setup

    ${WALLET}   ${ADDR}    ${WIF} =   Init Wallet with Address    ${ASSETS_DIR}
    ${SCRIPT_HASH} =        Get ScriptHash                        ${WIF}

    ##########################################################
    # Transferring GAS from initial wallet to our test wallet
    ##########################################################
                            Transfer Mainnet Gas              ${WALLET}     ${TRANSFER_AMOUNT}
    ${MAINNET_BALANCE} =    Get Mainnet Balance               ${ADDR}
                            Should Be Equal As Numbers        ${MAINNET_BALANCE}  ${TRANSFER_AMOUNT}

    ############################
    # Making deposit into NeoFS
    ############################
                            NeoFS Deposit           ${WALLET}    ${DEPOSIT_AMOUNT}

    ${MAINNET_BALANCE} =    Get Mainnet Balance     ${ADDR}
    ${EXPECTED_BALANCE} =   Evaluate                ${TRANSFER_AMOUNT}-${DEPOSIT_AMOUNT}
                            Should Be True          ${MAINNET_BALANCE} < ${EXPECTED_BALANCE}

    ${NEOFS_BALANCE} =      Get NeoFS Balance       ${WALLET}
                            Should Be Equal As Numbers        ${NEOFS_BALANCE}    ${DEPOSIT_AMOUNT}

    # TODO: try to withdraw more than was deposited

    ###########################
    # Withdrawing deposit back
    ###########################
                            Withdraw Mainnet Gas          ${WALLET}              ${ADDR}    ${SCRIPT_HASH}    ${WITHDRAW_AMOUNT}
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

    ${NEOFS_BALANCE} =      Get NeoFS Balance             ${WALLET}
    ${EXPECTED_BALANCE} =   Evaluate                      ${DEPOSIT_AMOUNT} - ${WITHDRAW_AMOUNT}
                            Should Be Equal As numbers    ${NEOFS_BALANCE}    ${EXPECTED_BALANCE}

    ${MAINNET_BALANCE_AFTER} =      Get Mainnet Balance                   ${ADDR}
    ${MAINNET_BALANCE_DIFF} =       Evaluate    ${MAINNET_BALANCE_AFTER} - ${MAINNET_BALANCE}
                                    Should Be True          ${MAINNET_BALANCE_DIFF} < ${WITHDRAW_AMOUNT}

    [Teardown]              Teardown    withdraw
