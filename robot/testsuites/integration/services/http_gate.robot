*** Settings ***
Variables   common.py
Variables   wellknown_acl.py

Library     neofs.py
Library     neofs_verbs.py
Library     http_gate.py

Resource    payment_operations.robot
Resource    setup_teardown.robot

*** Variables ***
${PLACEMENT_RULE} =     REP 1 IN X CBF 1 SELECT 1 FROM * AS X
${CONTAINER_WAIT_INTERVAL} =    1 min
@{INCLUDE_SVC} =    http_gate

*** Test cases ***

NeoFS HTTP Gateway
    [Documentation]     Creates container and does PUT, GET via HTTP Gate
    [Timeout]           5 min

    [Setup]             Setup
                        Make Up    ${INCLUDE_SVC}

    ${WALLET}   ${ADDR}     ${WIF} =   Prepare Wallet And Deposit

    ${CID} =            Create container                    ${WIF}    ${PUBLIC_ACL}    ${PLACEMENT_RULE}
                        Wait Until Keyword Succeeds         ${MORPH_BLOCK_TIME}     ${CONTAINER_WAIT_INTERVAL}
                        ...  Container Existing             ${WIF}    ${CID}

    ${FILE} =           Generate file of bytes              ${SIMPLE_OBJ_SIZE}
    ${FILE_L} =         Generate file of bytes              ${COMPLEX_OBJ_SIZE}
    ${FILE_HASH} =      Get file hash                       ${FILE}
    ${FILE_L_HASH} =    Get file hash                       ${FILE_L}

    ${S_OID} =          Put object                 ${WIF}    ${FILE}      ${CID}
    ${L_OID} =          Put object                 ${WIF}    ${FILE_L}    ${CID}

    # By request from Service team - try to GET object from the node without object

    @{GET_NODE_LIST} =  Get nodes without object            ${WIF}    ${CID}    ${S_OID}
    ${NODE} =           Evaluate                            random.choice($GET_NODE_LIST)    random

    ${GET_OBJ_S} =      Get object               ${WIF}     ${CID}    ${S_OID}    ${EMPTY}    s_file_read    ${NODE}
    ${FILEPATH} =       Get via HTTP Gate                   ${CID}    ${S_OID}

    ${PLAIN_FILE_HASH} =    Get file hash       ${GET_OBJ_S}
    ${GATE_FILE_HASH} =     Get file hash       ${FILEPATH}
                            Should Be Equal     ${FILE_HASH}      ${PLAIN_FILE_HASH}
                            Should Be Equal     ${FILE_HASH}      ${GATE_FILE_HASH}

    @{GET_NODE_LIST} =  Get nodes without object            ${WIF}    ${CID}    ${L_OID}
    ${NODE} =           Evaluate                            random.choice($GET_NODE_LIST)    random

    ${GET_OBJ_L} =      Get object               ${WIF}     ${CID}    ${L_OID}    ${EMPTY}    l_file_read    ${NODE}
    ${FILEPATH} =       Get via HTTP Gate                   ${CID}    ${L_OID}

    ${PLAIN_FILE_HASH} =    Get file hash       ${GET_OBJ_L}
    ${GATE_FILE_HASH} =     Get file hash       ${FILEPATH}
                            Should Be Equal     ${FILE_L_HASH}      ${PLAIN_FILE_HASH}
                            Should Be Equal     ${FILE_L_HASH}      ${GATE_FILE_HASH}

    [Teardown]          Teardown    http_gate
