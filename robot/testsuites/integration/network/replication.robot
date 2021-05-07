*** Settings ***
Variables   ../../../variables/common.py

Library     ../${RESOURCES}/neofs.py
Library     ../${RESOURCES}/payment_neogo.py
Library     ../${RESOURCES}/utility_keywords.py
Library     ${KEYWORDS}/wallet_keywords.py
Library     ${KEYWORDS}/rpc_call_keywords.py

*** Variables ***
${PLACEMENT_RULE} =     "REP 2 IN X CBF 1 SELECT 4 FROM * AS X"
${TRANSFER_AMOUNT} =    ${11}

*** Test cases ***
NeoFS Object Replication
    [Documentation]         Testcase to validate NeoFS object replication.
    [Tags]                  Migration  Replication  NeoFS  NeoCLI
    [Timeout]               25 min

    [Setup]                 Create Temporary Directory

    ${WALLET}   ${ADDR}     ${PRIV_KEY} =   Init Wallet with Address    ${TEMP_DIR}
    ${TX} =                 Transfer Mainnet Gas                  ${MAINNET_WALLET_WIF}    ${ADDR}     ${TRANSFER_AMOUNT}
                            Wait Until Keyword Succeeds           1 min                  15 sec
                            ...  Transaction accepted in block    ${TX}

    ${MAINNET_BALANCE} =    Get Mainnet Balance                   ${ADDR}
    Should Be Equal As Numbers                                    ${MAINNET_BALANCE}  ${TRANSFER_AMOUNT}


    ${SCRIPT_HASH} =        Get ScriptHash                         ${PRIV_KEY}

    ${TX_DEPOSIT} =         NeoFS Deposit                         ${WALLET}              ${ADDR}                ${SCRIPT_HASH}    10
                            Wait Until Keyword Succeeds           1 min                  15 sec
                            ...  Transaction accepted in block    ${TX_DEPOSIT}
                            Get Transaction                       ${TX_DEPOSIT}

    ${CID} =                Create container                      ${PRIV_KEY}    ${EMPTY}   ${PLACEMENT_RULE}
                            Container Existing                    ${PRIV_KEY}    ${CID}


    ${FILE} =               Generate file of bytes                ${SIMPLE_OBJ_SIZE}
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

                            # We have 2 or 3 copies. Expected behaviour: after one epoch potential 3rd copy should be removed.

                            Sleep                                 ${NEOFS_EPOCH_TIMEOUT}

                            Validate storage policy for object    ${PRIV_KEY}    2       ${CID}      ${S_OID}

    [Teardown]              Cleanup


*** Keywords ***

Cleanup
                            Cleanup Files
                            Get Docker Logs                       replication
