*** Settings ***
Variables   common.py
Variables   wellknown_acl.py

Library     container.py
Library     neofs.py
Library     neofs_verbs.py
Library     payment_neogo.py
Library     String
Library     Collections

Resource    setup_teardown.robot
Resource    payment_operations.robot

*** Variables ***
&{ATTR_FILENAME} =      FileName=new
${ATTR_DUPLICATE} =     FileType=jpg,FileType=png
&{ATTR_NONE} =          NoAttribute=''
&{ATTR_SINGLE} =        AttrNum=one

*** Test Cases ***

Duplicated Object Attributes
    [Documentation]             Testcase to check duplicated attributes.
    [Tags]                      Object
    [Timeout]                   10 min

    [Setup]                     Setup

    ${WALLET}   ${_}     ${_} =    Prepare Wallet And Deposit

    ${PUBLIC_CID} =             Create Container       ${WALLET}    basic_acl=${PUBLIC_ACL_F}
    ${FILE_S} =                 Generate file of bytes            ${SIMPLE_OBJ_SIZE}


    ###################################################
    # Checking that object attributes cannot duplicate
    ###################################################

    Run Keyword And Expect Error    *
    ...    Put object        ${WALLET}         ${FILE_S}    ${PUBLIC_CID}    user_headers=${ATTR_FILENAME}
    # Robot doesn't allow to create a dictionary with the same keys, so using plain text option here
    Run Keyword And Expect Error    *
    ...    Put object        ${WALLET}         ${FILE_S}    ${PUBLIC_CID}    options=--attributes ${ATTR_DUPLICATE}

    ##################################################
    # Checking that object cannot have empty attibute
    ##################################################

    Run Keyword And Expect Error    *
    ...    Put object        ${WALLET}         ${FILE_S}    ${PUBLIC_CID}    user_headers=${ATTR_NONE}

    #####################################################
    # Checking a successful step with a single attribute
    #####################################################

    ${OID} =            Put object    ${WALLET}         ${FILE_S}    ${PUBLIC_CID}    user_headers=${ATTR_SINGLE}
    ${HEADER} =         Head object              ${WALLET}         ${PUBLIC_CID}    ${OID}
                        Dictionary Should Contain Sub Dictionary
                            ...     ${HEADER}[header][attributes]
                            ...     ${ATTR_SINGLE}
                            ...     msg="No expected User Attribute in HEAD response"

    [Teardown]          Teardown    object_attributes
