*** Settings ***
Variables   ../../../variables/common.py

Library     ../${RESOURCES}/neofs.py
Library     ../${RESOURCES}/payment_neogo.py
Library     ../${RESOURCES}/utility_keywords.py
Library     ${KEYWORDS}/wallet_keywords.py
Library     ${KEYWORDS}/rpc_call_keywords.py

Resource    ../${RESOURCES}/payment_operations.robot

*** Variables ***
${PLACEMENT_RULE} =     REP 2 IN X CBF 1 SELECT 4 FROM * AS X

*** Test cases ***
NeoFS Object Replication
    [Documentation]         Testcase to validate NeoFS object replication.
    [Tags]                  Migration  Replication  NeoFS  NeoCLI
    [Timeout]               25 min

    [Setup]                 Create Temporary Directory

    ${WALLET}   ${ADDR}     ${WIF} =    Init Wallet with Address    ${TEMP_DIR}
    Payment Operations      ${ADDR}     ${WIF}

    ${CID} =                Create container                      ${WIF}    ${EMPTY}   ${PLACEMENT_RULE}
                            Container Existing                    ${WIF}    ${CID}

    ${FILE} =               Generate file of bytes                ${SIMPLE_OBJ_SIZE}
    ${FILE_HASH} =          Get file hash                         ${FILE}

    ${S_OID} =              Put object                  ${WIF}    ${FILE}         ${CID}      ${EMPTY}    ${EMPTY}
                            Validate storage policy for object    ${WIF}    2               ${CID}      ${S_OID}

    @{NODES_OBJ} =          Get nodes with object                 ${WIF}    ${CID}          ${S_OID}

    ${NODES_LOG_TIME} =     Get Nodes Log Latest Timestamp

    @{NODES_OBJ_STOPPED} =  Stop nodes                            1              @{NODES_OBJ}

    ${state}  ${output}=    Run Keyword And Ignore Error
                            ...  Wait Until Keyword Succeeds           10 min                 2 min
                            ...  Validate storage policy for object    ${WIF}    2       ${CID}      ${S_OID}

    Run Keyword If  '${state}'!='PASS'      Log  Warning: Keyword failed: Validate storage policy for object ${S_OID} {\n}${output}  WARN
    Find in Nodes Log                       object successfully replicated    ${NODES_LOG_TIME}
    Start nodes                             @{NODES_OBJ_STOPPED}

    # We have 2 or 3 copies. Expected behaviour: after one epoch potential 3rd copy should be removed.
    Sleep                                 ${NEOFS_EPOCH_TIMEOUT}
    Validate storage policy for object    ${WIF}    2       ${CID}      ${S_OID}

    [Teardown]              Cleanup


*** Keywords ***

Cleanup
                            Cleanup Files
                            Get Docker Logs                       replication
