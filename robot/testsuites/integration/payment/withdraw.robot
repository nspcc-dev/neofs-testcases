*** Settings ***
Variables   ../../../variables/common.py

Library     ../${RESOURCES}/neofs.py
Library     ../${RESOURCES}/payment_neogo.py

*** Variables ***
${DEPOSIT_AMOUNT} =    10
${WITHDRAW_AMOUNT} =   10

*** Test cases ***
NeoFS Deposit and Withdraw
    [Documentation]         Testcase to validate NeoFS Withdraw operation.
    [Tags]                  Withdraw  NeoFS  NeoCLI
    [Timeout]               10 min

    ${WALLET} =             Init wallet
                            Generate wallet                       ${WALLET}
    ${ADDR} =               Dump Address                          ${WALLET}
    ${PRIV_KEY} =           Dump PrivKey                          ${WALLET}               ${ADDR}

    ${TX} =                 Transfer Mainnet Gas                  wallets/wallet.json     NTrezR3C4X8aMLVg7vozt5wguyNfFhwuFx      ${ADDR}     15
                            Wait Until Keyword Succeeds           1 min                   15 sec        
                            ...  Transaction accepted in block    ${TX}
                            Get Transaction                       ${TX}
    ${MAINNET_BALANCE} =    Expected Mainnet Balance              ${ADDR}                 15

    ${SCRIPT_HASH} =        Get ScripHash                         ${PRIV_KEY}

    
    ${TX_DEPOSIT} =         NeoFS Deposit                         ${WALLET}              ${ADDR}    ${SCRIPT_HASH}    ${DEPOSIT_AMOUNT}
                            Wait Until Keyword Succeeds           1 min                  15 sec        
                            ...  Transaction accepted in block    ${TX_DEPOSIT}
                            Get Transaction                       ${TX_DEPOSIT}
                            
                            Sleep                                 1 min

    # Expected amount diff will be formed from deposit amount and contract fee
    ${EXPECTED_DIFF} =      Evaluate                              -${DEPOSIT_AMOUNT}-${NEOFS_CONTRACT_DEPOSIT_GAS_FEE}
    ${DEPOSIT_BALANCE} =    Expected Mainnet Balance Diff         ${ADDR}                ${MAINNET_BALANCE}    ${EXPECTED_DIFF}

    ${NEOFS_BALANCE} =      Get Balance                           ${PRIV_KEY}            

    ${TX} =                 Withdraw Mainnet Gas                  ${WALLET}              ${ADDR}    ${SCRIPT_HASH}    ${WITHDRAW_AMOUNT}
                            Wait Until Keyword Succeeds           1 min                  15 sec        
                            ...  Transaction accepted in block    ${TX}

                            Sleep                                 1 min
                            Get Balance                           ${PRIV_KEY}   
                            Expected Balance                      ${PRIV_KEY}            ${NEOFS_BALANCE}    -${WITHDRAW_AMOUNT}

     # Expected amount diff will be formed from withdrawal amount and contract fee
     ${EXPECTED_DIFF_W} =   Evaluate                              ${WITHDRAW_AMOUNT}-${NEOFS_CONTRACT_WITHDRAW_GAS_FEE}
                            Expected Mainnet Balance Diff         ${ADDR}                ${DEPOSIT_BALANCE}    ${EXPECTED_DIFF_W}
    
    [Teardown]              Cleanup 
     
*** Keywords ***
    
Cleanup
                            Get Docker Logs    withdraw