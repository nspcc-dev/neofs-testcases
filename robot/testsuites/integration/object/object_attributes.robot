*** Settings ***
Variables   common.py
Variables   wellknown_acl.py

Library    Collections
Library    neofs.py
Library    payment_neogo.py
Library    String

Resource    setup_teardown.robot
Resource    payment_operations.robot

*** Variables ***
${POLICY} =       REP 2 IN X CBF 1 SELECT 2 FROM * AS X
${ATTR_FILENAME} =    FileName=new
${ATTR_DUPLICATE} =    FileType=jpg, FileType=png
${ATTR_NONE} =    NoAttribute=''
${ATTR_SINGLE} =    AttrNum=one

*** Test Cases ***

Duplicated Object Attributes
    [Documentation]             Testcase to check duplicated attributes.
    [Tags]                      Object  NeoFS  NeoCLI
    [Timeout]                   10 min

    [Setup]                     Setup

    ${WALLET}   ${ADDR}     ${USER_KEY} =   Init Wallet with Address    ${ASSETS_DIR}
    Payment Operations      ${ADDR}         ${USER_KEY}

    ${PUBLIC_CID} =             Create container       ${USER_KEY}    ${PUBLIC_ACL}    ${POLICY}    ${EMPTY}    
    ${FILE_S} =                 Generate file of bytes            ${SIMPLE_OBJ_SIZE}


    ###################################################
    # Checking that object attributes cannot duplicate
    ###################################################

    Run Keyword And Expect Error    *
    ...    Put object        ${USER_KEY}         ${FILE_S}    ${PUBLIC_CID}    ${EMPTY}    ${ATTR_FILENAME}
    Run Keyword And Expect Error    *
    ...    Put object        ${USER_KEY}         ${FILE_S}    ${PUBLIC_CID}    ${EMPTY}    ${ATTR_DUPLICATE}

    ##################################################
    # Checking that object cannot have empty attibute
    ##################################################

    Run Keyword And Expect Error    *
    ...    Put object        ${USER_KEY}         ${FILE_S}    ${PUBLIC_CID}    ${EMPTY}    ${ATTR_NONE}

    #####################################################
    # Checking a successful step with a single attribute
    #####################################################

    ${OID} =                    Put object    ${USER_KEY}         ${FILE_S}    ${PUBLIC_CID}    ${EMPTY}    ${ATTR_SINGLE}
    ${HEAD} =                   Head object              ${USER_KEY}         ${PUBLIC_CID}    ${OID}    json_output=True
    ${HEADER_58} =              Decode Object System Header Json   ${HEAD}
    Verify Head Attribute    ${HEADER_58}    ${ATTR_SINGLE}

    [Teardown]              Teardown    object_attributes
