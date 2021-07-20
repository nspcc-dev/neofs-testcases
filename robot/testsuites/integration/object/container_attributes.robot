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

*** Test Cases ***
Duplicated Object Attributes
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

    #####################################################
    # Checking that container cannot have empty attibute
    #####################################################

    Run Keyword And Expect Error    *
    ...    Create container        ${USER_KEY}    ${EMPTY}    ${POLICY}    ${ATTR_NONE}

    #####################################################
    # Checking a successful step with a single attribute
    #####################################################

    ${CID} =                    Create container    ${USER_KEY}    ${EMPTY}    ${POLICY}    ${ATTR_SINGLE}
    ${HEAD} =                   Head container    ${USER_KEY}    ${CID}    ${EMPTY}    json_output=True
    ${ATTR} =                   Parse Header Attributes    ${HEAD}
    Should Be Equal    ${ATTR}    ${ATTR_SINGLE}

    [Teardown]              Teardown    container_attributes

*** Keywords ***

Parse Header Attributes

    [Arguments]    ${HEADER}
    &{HEADER_DIC} =             Evaluate    json.loads('''${HEADER}''')    json
    @{ATTR_DIC} =               Get From Dictionary    ${HEADER_DIC}    attributes
    &{ATTR_NUM_DIC} =           Get From List    ${ATTR_DIC}    0
    ${ATTR_KEY} =               Get From Dictionary    ${ATTR_NUM_DIC}    key
    ${ATTR_VALUE} =             Get From Dictionary    ${ATTR_NUM_DIC}    value
    ${ATTRIBUTE} =             Catenate    SEPARATOR=\=    ${ATTR_KEY}    ${ATTR_VALUE}
    [Return]    ${ATTRIBUTE}