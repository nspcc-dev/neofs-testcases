*** Settings ***
Variables   common.py
Variables   wellknown_acl.py

Library     container.py
Library     neofs_verbs.py
Library     http_gate.py
Library     storage_policy.py
Library     utility_keywords.py

Resource    payment_operations.robot
Resource    setup_teardown.robot

*** Variables ***
${PLACEMENT_RULE} =     REP 1 IN X CBF 1 SELECT 1 FROM * AS X
@{INCLUDE_SVC} =    http_gate

*** Test cases ***

NeoFS HTTP Gateway
    [Documentation]     Creates container and does PUT, GET via HTTP Gate
    [Timeout]           5 min

    [Setup]             Setup
                        Make Up    ${INCLUDE_SVC}

    ${WALLET}   ${_}     ${_} =     Prepare Wallet And Deposit
    ${CID} =                        Create container                    ${WALLET}    rule=${PLACEMENT_RULE}  basic_acl=${PUBLIC_ACL}
    ${FILE}    ${HASH} =            Generate file    ${SIMPLE_OBJ_SIZE}
    ${FILE_L}    ${L_HASH} =        Generate file    ${COMPLEX_OBJ_SIZE}

    ${S_OID} =          Put object                 ${WALLET}    ${FILE}      ${CID}
    ${L_OID} =          Put object                 ${WALLET}    ${FILE_L}    ${CID}

    # By request from Service team - try to GET object from the node without object

    @{GET_NODE_LIST} =  Get nodes without object            ${WALLET}    ${CID}    ${S_OID}
    ${NODE} =           Evaluate                            random.choice($GET_NODE_LIST)    random

    ${GET_OBJ_S} =      Get object               ${WALLET}      ${CID}    ${S_OID}    ${EMPTY}    s_file_read    ${NODE}
    ${FILEPATH} =       Get via HTTP Gate        ${CID}         ${S_OID}

    ${PLAIN_FILE_HASH} =    Get file hash       ${GET_OBJ_S}
    ${GATE_FILE_HASH} =     Get file hash       ${FILEPATH}
                            Should Be Equal     ${HASH}      ${PLAIN_FILE_HASH}
                            Should Be Equal     ${HASH}      ${GATE_FILE_HASH}

    @{GET_NODE_LIST} =  Get nodes without object            ${WALLET}    ${CID}    ${L_OID}
    ${NODE} =           Evaluate                            random.choice($GET_NODE_LIST)    random

    ${GET_OBJ_L} =      Get object               ${WALLET}      ${CID}    ${L_OID}    ${EMPTY}    l_file_read    ${NODE}
    ${FILEPATH} =       Get via HTTP Gate        ${CID}         ${L_OID}

    ${PLAIN_FILE_HASH} =    Get file hash       ${GET_OBJ_L}
    ${GATE_FILE_HASH} =     Get file hash       ${FILEPATH}
                            Should Be Equal     ${L_HASH}      ${PLAIN_FILE_HASH}
                            Should Be Equal     ${L_HASH}      ${GATE_FILE_HASH}

    [Teardown]          Teardown    http_gate
