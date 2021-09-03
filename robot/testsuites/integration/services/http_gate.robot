*** Settings ***
Variables   ../../../variables/common.py

Library     ../${RESOURCES}/neofs.py
Library     ../${RESOURCES}/payment_neogo.py
Library     ../${RESOURCES}/gates.py
Library     ${KEYWORDS}/wallet_keywords.py
Library     ${KEYWORDS}/rpc_call_keywords.py

Resource    ../${RESOURCES}/setup_teardown.robot

*** Variables ***
${PLACEMENT_RULE} =     REP 1 IN X CBF 1 SELECT 1 FROM * AS X
${TRANSFER_AMOUNT} =    ${6}
${DEPOSIT_AMOUNT} =     ${5}
${CONTAINER_WAIT_INTERVAL} =    1 min

*** Test cases ***

NeoFS HTTP Gateway
    [Documentation]     Creates container and does PUT, GET via HTTP Gate
    [Timeout]           5 min

    [Setup]             Setup
    ${WALLET}   ${ADDR}     ${WIF} =   Init Wallet with Address    ${ASSETS_DIR}
    ${TX} =             Transfer Mainnet Gas     ${MAINNET_WALLET_WIF}    ${ADDR}    ${TRANSFER_AMOUNT}

                        Wait Until Keyword Succeeds         ${MAINNET_TIMEOUT}    ${MAINNET_BLOCK_TIME}
                        ...  Transaction accepted in block  ${TX}

    ${MAINNET_BALANCE} =    Get Mainnet Balance             ${ADDR}
    Should Be Equal As Numbers                              ${MAINNET_BALANCE}      ${TRANSFER_AMOUNT}

    ${TX_DEPOSIT} =     NeoFS Deposit                       ${WIF}      ${DEPOSIT_AMOUNT}
                        Wait Until Keyword Succeeds         ${MAINNET_TIMEOUT}    ${MAINNET_BLOCK_TIME}
                        ...  Transaction accepted in block  ${TX_DEPOSIT}

    ${CID} =            Create container                    ${WIF}    0x0FFFFFFF    ${PLACEMENT_RULE}
                        Wait Until Keyword Succeeds         ${MORPH_BLOCK_TIME}     ${CONTAINER_WAIT_INTERVAL}
                        ...  Container Existing             ${WIF}    ${CID}

    ${FILE} =           Generate file of bytes              ${SIMPLE_OBJ_SIZE}
    ${FILE_L} =         Generate file of bytes              ${COMPLEX_OBJ_SIZE}
    ${FILE_HASH} =      Get file hash                       ${FILE}
    ${FILE_L_HASH} =    Get file hash                       ${FILE_L}

    ${S_OID} =          Put object                 ${WIF}    ${FILE}      ${CID}    ${EMPTY}    ${EMPTY}
    ${L_OID} =          Put object                 ${WIF}    ${FILE_L}    ${CID}    ${EMPTY}    ${EMPTY}

    # By request from Service team - try to GET object from the node without object

    @{GET_NODE_LIST} =  Get nodes without object            ${WIF}    ${CID}    ${S_OID}
    ${NODE} =           Evaluate                            random.choice($GET_NODE_LIST)    random

    ${GET_OBJ_S} =      Get object               ${WIF}     ${CID}    ${S_OID}    ${EMPTY}    s_file_read    ${NODE}
    ${FILEPATH} =       Get via HTTP Gate                   ${CID}    ${S_OID}

                        Verify file hash                    ${GET_OBJ_S}    ${FILE_HASH}
                        Verify file hash                    ${FILEPATH}    ${FILE_HASH}

    @{GET_NODE_LIST} =  Get nodes without object            ${WIF}    ${CID}    ${L_OID}
    ${NODE} =           Evaluate                            random.choice($GET_NODE_LIST)    random

    ${GET_OBJ_L} =      Get object               ${WIF}     ${CID}    ${L_OID}    ${EMPTY}    l_file_read    ${NODE}
    ${FILEPATH} =       Get via HTTP Gate                   ${CID}    ${L_OID}

                        Verify file hash                    ${GET_OBJ_L}    ${FILE_L_HASH}
                        Verify file hash                    ${FILEPATH}    ${FILE_L_HASH}

    [Teardown]          Teardown    http_gate
