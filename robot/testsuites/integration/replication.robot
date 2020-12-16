*** Settings ***
Variables   ../../variables/common.py

Library     ${RESOURCES}/neofs.py
Library     ${RESOURCES}/payment_neogo.py

*** Test cases ***
NeoFS Object Replication
    [Documentation]         Testcase to validate NeoFS object replication.
    [Tags]                  Migration  Replication  NeoFS  NeoCLI
    [Timeout]               10 min

    ${WALLET} =             Init wallet
                            Generate wallet                       ${WALLET}
    ${ADDR} =               Dump Address                          ${WALLET}
    ${PRIV_KEY} =           Dump PrivKey                          ${WALLET}              ${ADDR}

    ${TX} =                 Transfer Mainnet Gas                  wallets/wallet.json    NTrezR3C4X8aMLVg7vozt5wguyNfFhwuFx      ${ADDR}     55
                            Wait Until Keyword Succeeds           1 min                  15 sec        
                            ...  Transaction accepted in block    ${TX}
                            Get Transaction                       ${TX}
                            Expexted Mainnet Balance              ${ADDR}                55

    ${SCRIPT_HASH} =        Get ScripHash                         ${PRIV_KEY}

    ${TX_DEPOSIT} =         NeoFS Deposit                         ${WALLET}              ${ADDR}                ${SCRIPT_HASH}    50
                            Wait Until Keyword Succeeds           1 min                  15 sec        
                            ...  Transaction accepted in block    ${TX_DEPOSIT}
                            Get Transaction                       ${TX_DEPOSIT}

    ${CID} =                Create container                      ${PRIV_KEY}    ${EMPTY}    REP 2 IN X CBF 1 SELECT 4 FROM * AS X
                            Container Existing                    ${PRIV_KEY}    ${CID}


    ${FILE} =               Generate file of bytes                1024
    ${FILE_HASH} =          Get file hash                         ${FILE}

    ${S_OID} =              Put object to NeoFS                   ${PRIV_KEY}    ${FILE}         ${CID}      ${EMPTY}    ${EMPTY} 
                            Validate storage policy for object    ${PRIV_KEY}    2               ${CID}      ${S_OID}   
    
    @{NODES_OBJ} =          Get nodes with object                 ${PRIV_KEY}    ${CID}          ${S_OID}  
    @{NODES_OBJ_STOPPED} =  Stop nodes                            1              @{NODES_OBJ}
    
                            Sleep                                 1 min
    
                            Validate storage policy for object    ${PRIV_KEY}    2               ${CID}      ${S_OID}
                            Start nodes                           @{NODES_OBJ_STOPPED}
  
    [Teardown]              Cleanup                               ${FILE}    @{NODES_OBJ_STOPPED}
    
    
*** Keywords ***
    
Cleanup
    [Arguments]             ${FILE}    @{NODES_OBJ_STOPPED}
                            Start nodes                           @{NODES_OBJ_STOPPED}
                            Cleanup Files                         ${FILE}


