*** Settings ***
Variables   common.py
Variables   wellknown_acl.py

Library     container.py
Library     contract_keywords.py
Library     neofs.py
Library     neofs_verbs.py
Library     storage_policy.py
Library     utility_keywords.py

Library     Collections

Resource    payment_operations.robot
Resource    setup_teardown.robot

*** Variables ***
${EXPECTED_COPIES} =    ${2}

*** Test cases ***
NeoFS Object Replication
    [Documentation]         Testcase to validate NeoFS object replication.
    [Tags]                  Migration  Replication
    [Timeout]               25 min

    [Setup]                 Setup

    Log    Check replication mechanism
    Check Replication
    Log    Check Sticky Bit with SYSTEM Group via replication mechanism
    Check Replication    ${STICKYBIT_PUB_ACL}

    [Teardown]      Teardown    replication

*** Keywords ***
Check Replication
    [Arguments]    ${ACL}=${EMPTY}

    ${WALLET}   ${_}     ${_} =    Prepare Wallet And Deposit
    ${CID} =                Create Container    ${WALLET}    basic_acl=${ACL}

    ${FILE}    ${_} =       Generate file    ${SIMPLE_OBJ_SIZE}

    ${S_OID} =              Put Object    ${WALLET}    ${FILE}    ${CID}

    ${COPIES} =             Get Object Copies   Simple      ${WALLET}   ${CID}  ${S_OID}
                            Should Be Equal     ${EXPECTED_COPIES}  ${COPIES}

    @{NODES_OBJ} =          Get nodes with Object    ${WALLET}    ${CID}    ${S_OID}
    ${NODES_LOG_TIME} =     Get Nodes Log Latest Timestamp

    @{NODES_OBJ_STOPPED} =  Stop nodes          1       ${NODES_OBJ}
    @{NETMAP} =             Convert To List             ${NEOFS_NETMAP}
                            Remove Values From List     ${NETMAP}   ${NODES_OBJ_STOPPED}

    # We expect that during two epochs the missed copy will be replicated.
    FOR    ${i}    IN RANGE   2
        ${COPIES} =     Get Object Copies   Simple      ${WALLET}   ${CID}  ${S_OID}
        ${PASSED} =     Run Keyword And Return Status
                        ...     Should Be Equal     ${EXPECTED_COPIES}  ${COPIES}
        Exit For Loop If    ${PASSED}
        Tick Epoch
        Sleep               ${NEOFS_CONTRACT_CACHE_TIMEOUT}
    END
    Run Keyword Unless      ${PASSED}     Fail
    ...     Storage policy for object ${S_OID} in container ${CID} isn't valid

    Find in Nodes Log       object successfully replicated    ${NODES_LOG_TIME}
    Start nodes             ${NODES_OBJ_STOPPED}
    Tick Epoch

    # We have 2 or 3 copies. Expected behaviour: during two epochs potential 3rd copy should be removed.
    FOR    ${i}    IN RANGE   2
        ${COPIES} =     Get Object Copies   Simple      ${WALLET}   ${CID}  ${S_OID}
        ${PASSED} =     Run Keyword And Return Status
                        ...     Should Be Equal     ${EXPECTED_COPIES}  ${COPIES}
        Exit For Loop If    ${PASSED}
        Tick Epoch
        Sleep               ${NEOFS_CONTRACT_CACHE_TIMEOUT}
    END
    Run Keyword Unless      ${PASSED}     Fail
    ...     Storage policy for object ${S_OID} in container ${CID} isn't valid
