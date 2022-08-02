*** Settings ***
Variables   common.py

Library     container.py
Library     String
Library     Collections

Resource    setup_teardown.robot
Resource    payment_operations.robot
Resource    common_steps_acl_bearer.robot

*** Variables ***
&{ATTR_TIME} =          Timestamp=new
&{ATTR_NONE} =          NoAttribute=
&{ATTR_SINGLE} =        AttrNum=one
${ERROR_MSG} =          invalid container attribute

*** Test Cases ***
Duplicated Container Attributes
    [Documentation]             Testcase to check duplicated container attributes.
    [Tags]                      Container
    [Timeout]                   5 min


    ${WALLET}   ${_}     ${_} =   Prepare Wallet And Deposit

    ######################################################
    # Checking that container attributes cannot duplicate
    ######################################################

    # TODO: unstable case, the behaviour needs to be defined
    # https://github.com/nspcc-dev/neofs-node/issues/1339
    #Run Keyword And Expect Error    *
    #...    Create container        ${WALLET}    attributes=${ATTR_TIME}

    ${ERR} =    Run Keyword And Expect Error    *
                ...    Create Container        ${WALLET}    options=--attributes Size=small, Size=big
                Should Contain      ${ERR}      ${ERROR_MSG}

    ######################################################
    # Checking that container cannot have empty attribute
    ######################################################

    # TODO: the same unstable case, referenced in the above issue
    #${ERR} =    Run Keyword And Expect Error    *
    #            ...    Create Container        ${WALLET}    attributes=${ATTR_NONE}
    #            Should Contain      ${ERR}      ${ERROR_MSG}

    #####################################################
    # Checking a successful step with a single attribute
    #####################################################

    ${CID} =                Create Container    ${WALLET}    attributes=${ATTR_SINGLE}
    &{ATTRIBUTES} =         Get Container       ${WALLET}    ${CID}
                            Dictionary Should Contain Sub Dictionary
                                ...     ${ATTRIBUTES}[attributes]
                                ...     ${ATTR_SINGLE}
                                ...     msg="No expected container attributes found"

