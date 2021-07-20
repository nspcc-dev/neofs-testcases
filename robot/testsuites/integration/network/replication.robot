*** Settings ***
Variables   ../../../variables/common.py

Library     Collections
Library     ../${RESOURCES}/payment_neogo.py
Library     ../${RESOURCES}/neofs.py
Library     wallet_keywords.py
Library     rpc_call_keywords.py
Library     contract_keywords.py

Resource    ../${RESOURCES}/payment_operations.robot
Resource    ../${RESOURCES}/setup_teardown.robot

*** Variables ***
${PLACEMENT_RULE} =     REP 2 IN X CBF 1 SELECT 4 FROM * AS X
${EXPECTED_COPIES} =    ${2}
${CHECK_INTERVAL} =     1 min

*** Test cases ***
NeoFS Object Replication
    [Documentation]         Testcase to validate NeoFS object replication.
    [Tags]                  Migration  Replication  NeoFS  NeoCLI
    [Timeout]               25 min

    [Setup]                 Setup

    ${WALLET}   ${ADDR}     ${WIF} =    Init Wallet with Address    ${ASSETS_DIR}
    Payment Operations      ${ADDR}     ${WIF}

    ${CID} =                Create container                      ${WIF}    ${EMPTY}   ${PLACEMENT_RULE}
                            Container Existing                    ${WIF}    ${CID}

    ${FILE} =               Generate file of bytes                ${SIMPLE_OBJ_SIZE}
    ${FILE_HASH} =          Get file hash                         ${FILE}

    ${S_OID} =              Put object                  ${WIF}    ${FILE}       ${CID}      ${EMPTY}    ${EMPTY}
                            Validate storage policy for object    ${WIF}        ${EXPECTED_COPIES}      ${CID}      ${S_OID}

    @{NODES_OBJ} =          Get nodes with object                 ${WIF}    ${CID}          ${S_OID}
    ${NODES_LOG_TIME} =     Get Nodes Log Latest Timestamp

    @{NODES_OBJ_STOPPED} =  Stop nodes          1              @{NODES_OBJ}
    @{NETMAP} =             Convert To List     ${NEOFS_NETMAP}
                            Remove Values From List     ${NETMAP}   @{NODES_OBJ_STOPPED}

    # We expect that during two epochs the missed copy will be replicated.
    FOR    ${i}    IN RANGE   2
        ${PASSED} =     Run Keyword And Return Status
                        ...     Validate storage policy for object    ${WIF}    ${EXPECTED_COPIES}
                        ...     ${CID}      ${S_OID}    ${EMPTY}    ${NETMAP}
        Exit For Loop If    ${PASSED}
        Tick Epoch
        Sleep               ${CHECK_INTERVAL}
    END
    Run Keyword Unless      ${PASSED}     Fail   Keyword failed: Validate storage policy for object ${S_OID}

    Find in Nodes Log       object successfully replicated    ${NODES_LOG_TIME}
    Start nodes             @{NODES_OBJ_STOPPED}
    Tick Epoch

    # We have 2 or 3 copies. Expected behaviour: during two epochs potential 3rd copy should be removed.
    FOR    ${i}    IN RANGE   2
        ${PASSED} =     Run Keyword And Return Status
                        ...     Validate storage policy for object    ${WIF}    ${EXPECTED_COPIES}       ${CID}      ${S_OID}
        Exit For Loop If    ${PASSED}
        Tick Epoch
        Sleep               ${CHECK_INTERVAL}
    END
    Run Keyword Unless      ${PASSED}     Fail   Keyword failed: Validate storage policy for object ${S_OID}


    [Teardown]      Teardown    replication
