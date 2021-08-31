*** Settings ***
Variables   ../../../variables/common.py

Library    Collections
Library     ../${RESOURCES}/neofs.py
Library     ../${RESOURCES}/payment_neogo.py
Library    String

Resource    ../${RESOURCES}/setup_teardown.robot
Resource    ../${RESOURCES}/payment_operations.robot
Resource    ../acl/common_steps_acl_bearer.robot

*** Variables ***
${POLICY} =       REP 2 IN X CBF 1 SELECT 2 FROM * AS X
${ATTR_TIME} =    Timestamp=new
${ATTR_DUPLICATE} =    Size=small, Size=big
${ATTR_NONE} =    NoAttribute=''
${ATTR_SINGLE} =    AttrNum=one
${CONTAINER_WAIT_INTERVAL} =    1 min

*** Test Cases ***
Duplicated Container Attributes
    [Documentation]             Testcase to check duplicated container attributes.
    [Tags]                      Container  NeoFS  NeoCLI
    [Timeout]                   10 min

    [Setup]                     Setup

    ${WALLET}   ${ADDR}     ${USER_KEY} =   Init Wallet with Address    ${ASSETS_DIR}
    Payment Operations      ${ADDR}         ${USER_KEY}

    ######################################################
    # Checking that container attributes cannot duplicate
    ######################################################

    Run Keyword And Expect Error    *
    ...    Create container        ${USER_KEY}    ${EMPTY}    ${POLICY}   ${ATTR_TIME}
    Run Keyword And Expect Error    *
    ...    Create container        ${USER_KEY}    ${EMPTY}    ${POLICY}    ${ATTR_DUPLICATE}

    ######################################################
    # Checking that container cannot have empty attribute
    ######################################################

    Run Keyword And Expect Error    *
    ...    Create container        ${USER_KEY}    ${EMPTY}    ${POLICY}    ${ATTR_NONE}

    #####################################################
    # Checking a successful step with a single attribute
    #####################################################

    ${CID} =                Create container    ${USER_KEY}    ${EMPTY}    ${POLICY}    ${ATTR_SINGLE}
                            Wait Until Keyword Succeeds    ${MORPH_BLOCK_TIME}       ${CONTAINER_WAIT_INTERVAL}
                            ...     Container Existing     ${USER_KEY}     ${CID}
    ${ATTRIBUTES} =         Get container attributes    ${USER_KEY}    ${CID}    ${EMPTY}    json_output=True
    ${ATTRIBUTES_DICT} =    Decode Container Attributes Json    ${ATTRIBUTES}
    Verify Head Attribute    ${ATTRIBUTES_DICT}    ${ATTR_SINGLE}

    [Teardown]              Teardown    container_attributes
