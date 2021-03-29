*** Settings ***
Variables   ../../../variables/common.py

Library     ../${RESOURCES}/neofs.py
Library     ../${RESOURCES}/payment_neogo.py

*** Test cases ***
NeoFS Object Replication
    [Documentation]         Testcase to validate NeoFS object replication.
    [Tags]                  Migration  Replication  NeoFS  NeoCLI
    [Timeout]               15 min

    ${WALLET} =             Init wallet
                            Generate wallet                       ${WALLET}
    ${ADDR} =               Dump Address                          ${WALLET}
    ${PRIV_KEY} =           Dump PrivKey                          ${WALLET}              ${ADDR}

    ${TX} =                 Transfer Mainnet Gas                  wallets/wallet.json    NTrezR3C4X8aMLVg7vozt5wguyNfFhwuFx      ${ADDR}     11
                            Wait Until Keyword Succeeds           1 min                  15 sec        
                            ...  Transaction accepted in block    ${TX}
                            Get Transaction                       ${TX}
                            Expected Mainnet Balance              ${ADDR}                11

    ${SCRIPT_HASH} =        Get ScriptHash                         ${PRIV_KEY}

    ${TX_DEPOSIT} =         NeoFS Deposit                         ${WALLET}              ${ADDR}                ${SCRIPT_HASH}    10
                            Wait Until Keyword Succeeds           1 min                  15 sec        
                            ...  Transaction accepted in block    ${TX_DEPOSIT}
                            Get Transaction                       ${TX_DEPOSIT}

    ${CID} =                Create container                      ${PRIV_KEY}    ${EMPTY}    REP 2 IN X CBF 1 SELECT 4 FROM * AS X
                            Container Existing                    ${PRIV_KEY}    ${CID}


    ${FILE} =               Generate file of bytes                1024
    ${FILE_HASH} =          Get file hash                         ${FILE}

    ${S_OID} =              Put object                   ${PRIV_KEY}    ${FILE}         ${CID}      ${EMPTY}    ${EMPTY} 
                            Validate storage policy for object    ${PRIV_KEY}    2               ${CID}      ${S_OID}   
    
    @{NODES_OBJ} =          Get nodes with object                 ${PRIV_KEY}    ${CID}          ${S_OID}  

    ${NODES_LOG_TIME} =     Get Nodes Log Latest Timestamp

    @{NODES_OBJ_STOPPED} =  Stop nodes                            1              @{NODES_OBJ}
    
    ${state}  ${output}=    Run Keyword And Ignore Error
                            ...  Wait Until Keyword Succeeds           10 min                 2 min        
                            ...  Validate storage policy for object    ${PRIV_KEY}    2       ${CID}      ${S_OID}
                            
                            Run Keyword If  '${state}'!='PASS'  Log  Warning: Keyword failed: Validate storage policy for object ${S_OID} {\n}${output}  WARN

                            Find in Nodes Log                     object successfully replicated    ${NODES_LOG_TIME}

                            Start nodes                           @{NODES_OBJ_STOPPED}
                            Cleanup
    
    
*** Keywords ***
    
Cleanup
                            Cleanup Files                         
                            Get Docker Logs                       replication


