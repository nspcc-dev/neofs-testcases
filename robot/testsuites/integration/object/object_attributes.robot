*** Settings ***
Variables   common.py
Variables   wellknown_acl.py

Library     container.py
Library     neofs_verbs.py
Library     utility_keywords.py
Library     Collections

Resource    setup_teardown.robot
Resource    payment_operations.robot

*** Variables ***
&{ATTR_FILENAME} =      FileName=new
${ATTR_DUPLICATE} =     FileType=jpg,FileType=png
&{ATTR_NONE} =          NoAttribute=''
&{ATTR_SINGLE} =        AttrNum=one

*** Test Cases ***

Object Attrubutes
    [Timeout]           10 min
    [Setup]             Setup

                        Check Various Object Attributes     Simple
                        Check Various Object Attributes     Complex

    [Teardown]          Teardown    object_attributes


*** Keywords ***

Check Various Object Attributes
    [Arguments]         ${COMPLEXITY}

    ${WALLET}   ${_}     ${_} =    Prepare Wallet And Deposit

    ${PUBLIC_CID} =         Create Container       ${WALLET}    basic_acl=${PUBLIC_ACL_F}
    ${OBJ_SIZE} =           Run Keyword If  """${COMPLEXITY}""" == """Simple"""
                            ...     Set Variable    ${SIMPLE_OBJ_SIZE}
                            ...     ELSE
                            ...     Set Variable    ${COMPLEX_OBJ_SIZE}
    ${FILE}    ${_} =       Generate File           ${OBJ_SIZE}

    ###################################################
    # Checking that object attributes cannot duplicate
    ###################################################

    ${ERR} =    Run Keyword And Expect Error    *
                ...    Put object   ${WALLET}   ${FILE}     ${PUBLIC_CID}    user_headers=${ATTR_FILENAME}
                Should Contain      ${ERR}      code = 1024 message = duplication of attributes detected
    # Robot doesn't allow to create a dictionary with the same keys,
    # so using plain text option here
    ${ERR} =    Run Keyword And Expect Error    *
                ...    Put object   ${WALLET}   ${FILE}     ${PUBLIC_CID}    options=--attributes ${ATTR_DUPLICATE}
                Should Contain      ${ERR}      code = 1024 message = duplication of attributes detected

    ##################################################
    # Checking that object cannot have empty attibute
    ##################################################

    ${ERR} =    Run Keyword And Expect Error    *
                ...    Put object   ${WALLET}   ${FILE}     ${PUBLIC_CID}    user_headers=${ATTR_NONE}
                Should Contain      ${ERR}      code = 1024 message = empty attribute value

    #####################################################
    # Checking a successful step with a single attribute
    #####################################################

    ${OID} =            Put object      ${WALLET}     ${FILE}    ${PUBLIC_CID}    user_headers=${ATTR_SINGLE}
    ${HEADER} =         Head object     ${WALLET}     ${PUBLIC_CID}    ${OID}
                        Dictionary Should Contain Sub Dictionary
                            ...     ${HEADER}[header][attributes]
                            ...     ${ATTR_SINGLE}
                            ...     msg="No expected User Attribute in HEAD response"
    ${FOUND_OIDS} =     Search Object       ${WALLET}   ${PUBLIC_CID}  filters=${ATTR_SINGLE}
                        Should Be Equal     ${OID}      ${FOUND_OIDS}[0]
                            ...     msg="Cannot SEARCH an object by User Attribute"
