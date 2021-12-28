*** Settings ***
Variables   common.py

Library     payment_neogo.py
Library     wallet_keywords.py
Library     rpc_call_keywords.py
Library     Process

Resource    setup_teardown.robot

*** Variables ***

${DEPOSIT_AMOUNT} =    ${25}
${DEPOSIT} =    ${60}
@{INCLUDE_SVC} =    ir
&{CONFIG_CHANGE} =    NEOFS_IR_EMIT_GAS_BALANCE_THRESHOLD=${10**16}

*** Test cases ***
IR GAS emission threshold value
    [Documentation]    Testcase to check sidechain balance when emission threshold is exceeded.
    [Tags]             GAS    Sidechain
    [Timeout]          5 min

    [Setup]             Setup

    ${WALLET}    ${ADDR}    ${WIF} =    Init Wallet with Address    ${ASSETS_DIR}

    ${SC_BALANCE} =     Get Sidechain Balance    ${ADDR}

    ${TX} =             Transfer Mainnet Gas                    ${MAINNET_WALLET_WIF}    ${ADDR}    ${DEPOSIT}
                        Wait Until Keyword Succeeds             ${MAINNET_TIMEOUT}    ${MAINNET_BLOCK_TIME}
                        ...    Transaction accepted in block    ${TX}
                        
##########################################################################################
# Threshold is set to default 0 and sidechain balance has changed after deposit operation
##########################################################################################

    ${TX_DEPOSIT} =     NeoFS Deposit                           ${WIF}          ${DEPOSIT_AMOUNT}
                        Wait Until Keyword Succeeds             ${MAINNET_TIMEOUT}    ${MAINNET_BLOCK_TIME}
                        ...    Transaction accepted in block    ${TX_DEPOSIT}

                        Sleep    ${MAINNET_BLOCK_TIME}

    ${BALANCE_CHANGED} =    Get Sidechain Balance    ${ADDR}
    Should Not Be Equal     ${SC_BALANCE}    ${BALANCE_CHANGED}

                        Make Down    ${INCLUDE_SVC}
                        Make Up      ${INCLUDE_SVC}    ${CONFIG_CHANGE}

######################################################################################
# Threshold is exceeded and sidechain balance has not changed after deposit operation
######################################################################################

    ${TX_DEPOSIT} =     NeoFS Deposit                           ${WIF}          ${DEPOSIT_AMOUNT}
                        Wait Until Keyword Succeeds             ${MAINNET_TIMEOUT}    ${MAINNET_BLOCK_TIME}
                        ...    Transaction accepted in block    ${TX_DEPOSIT}

                        Sleep    ${MAINNET_BLOCK_TIME}

    ${BALANCE_UNCHANGED} =    Get Sidechain Balance    ${ADDR}
    Should Be Equal     ${BALANCE_UNCHANGED}    ${BALANCE_CHANGED}

    [Teardown]    Teardown    emission_threshold
