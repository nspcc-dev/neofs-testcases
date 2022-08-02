*** Settings ***
Variables   common.py

Library     payment_neogo.py
Library     utility_keywords.py
Library     Process


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


    ${WALLET}    ${ADDR}    ${_} =    Generate Wallet

    ${SC_BALANCE} =     Get Sidechain Balance    ${ADDR}
                        Transfer Mainnet Gas                    ${WALLET}    ${DEPOSIT}

##########################################################################################
# Threshold is set to default 0 and sidechain balance has changed after deposit operation
##########################################################################################

                        NeoFS Deposit           ${WALLET}      ${DEPOSIT_AMOUNT}
                        Sleep    ${MAINNET_BLOCK_TIME}

    ${BALANCE_CHANGED} =    Get Sidechain Balance    ${ADDR}
    Should Not Be Equal     ${SC_BALANCE}    ${BALANCE_CHANGED}

                        Make Down    ${INCLUDE_SVC}
                        Make Up      ${INCLUDE_SVC}    ${CONFIG_CHANGE}

######################################################################################
# Threshold is exceeded and sidechain balance has not changed after deposit operation
######################################################################################

                        NeoFS Deposit           ${WALLET}      ${DEPOSIT_AMOUNT}
                        Sleep    ${MAINNET_BLOCK_TIME}

    ${BALANCE_UNCHANGED} =    Get Sidechain Balance    ${ADDR}
    Should Be Equal     ${BALANCE_UNCHANGED}    ${BALANCE_CHANGED}

