*** Settings ***
Variables   ../../variables/common.py

Library     ${RESOURCES}/neofs.py
Library     ${RESOURCES}/payment_neogo.py

*** Test cases ***
NeoFS Deposit and Withdraw
    [Documentation]         Testcase to validate NeoFS Withdraw operation.
    [Tags]                  Withdraw  NeoFS  NeoCLI
    [Timeout]               10 min

    ${WALLET} =             Init wallet
                            Generate wallet                       ${WALLET}
    ${ADDR} =               Dump Address                          ${WALLET}
    ${PRIV_KEY} =           Dump PrivKey                          ${WALLET}               ${ADDR}

    ${TX} =                 Transfer Mainnet Gas                  wallets/wallet.json     NTrezR3C4X8aMLVg7vozt5wguyNfFhwuFx      ${ADDR}     55
                            Wait Until Keyword Succeeds           1 min                   15 sec        
                            ...  Transaction accepted in block    ${TX}
                            Get Transaction                       ${TX}
                            Expexted Mainnet Balance              ${ADDR}                 55

    ${SCRIPT_HASH} =        Get ScripHash                         ${PRIV_KEY}

    ${TX_DEPOSIT} =         NeoFS Deposit                         ${WALLET}              ${ADDR}                ${SCRIPT_HASH}    50
                            Wait Until Keyword Succeeds           1 min                  15 sec        
                            ...  Transaction accepted in block    ${TX_DEPOSIT}
                            Get Transaction                       ${TX_DEPOSIT}
                            
                            Sleep                                 1 min

                            Expexted Mainnet Balance              ${ADDR}                4.86192020
    ${NEOFS_BALANCE} =      Get Balance                           ${PRIV_KEY}            

    ${TX} =                 Withdraw Mainnet Gas                  ${WALLET}              ${ADDR}                ${SCRIPT_HASH}    50
                            Wait Until Keyword Succeeds           1 min                  15 sec        
                            ...  Transaction accepted in block    ${TX}

                            Sleep                                 1 min
                            Get Balance                           ${PRIV_KEY}   
                            Expected Balance                      ${PRIV_KEY}            ${NEOFS_BALANCE}       -50
                            Expexted Mainnet Balance              ${ADDR}                54.82554860
                            
                                     
