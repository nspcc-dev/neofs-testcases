*** Settings ***
Variables    common.py
Variables    eacl_object_filters.py

Library     acl.py
Library     neofs.py
Library     Collections

Resource    common_steps_acl_extended.robot
Resource    common_steps_acl_basic.robot
Resource    payment_operations.robot
Resource    setup_teardown.robot

*** Variables ***
${OBJECT_PATH} =   testfile
${EACL_ERR_MSG} =    *

*** Test cases ***
Payload Length Object Filter for Extended ACL
    [Documentation]         Testcase to validate if $Object:payloadLength eACL filter is correctly handled.
    [Tags]                  ACL  eACL  NeoFS  NeoCLI
    [Timeout]               20 min

    [Setup]                 Setup

    Log    Check eACL payloadLength Filter with MatchType String Equal
    Check eACL Filters with MatchType String Equal    $Object:payloadLength
    Log    Check eACL payloadLength Filter with MatchType String Not Equal
    Check $Object:payloadLength Filter with MatchType String Not Equal    $Object:payloadLength

*** Keywords ***

Check $Object:payloadLength Filter with MatchType String Not Equal
    [Arguments]    ${FILTER}

    ${_}   ${_}    ${USER_KEY} =    Prepare Wallet And Deposit  
    ${_}   ${_}    ${OTHER_KEY} =    Prepare Wallet And Deposit

    ${CID} =            Create Container Public    ${USER_KEY}
    ${FILE_S}    ${_} =    Generate file    ${SIMPLE_OBJ_SIZE}
    ${FILE_0}    ${_} =    Generate file    ${0}

    ${S_OID_0} =        Put Object    ${USER_KEY}    ${FILE_0}    ${CID}    ${EMPTY}
    ${S_OID} =          Put Object    ${USER_KEY}    ${FILE_S}    ${CID}    ${EMPTY}

                        Get Object    ${USER_KEY}    ${CID}    ${S_OID}    ${EMPTY}    local_file_eacl
                        Head Object    ${USER_KEY}    ${CID}    ${S_OID}    ${EMPTY}

    &{HEADER_DICT} =    Object Header Decoded    ${USER_KEY}    ${CID}    ${S_OID}
    ${EACL_CUSTOM} =    Compose eACL Custom    ${HEADER_DICT}    STRING_NOT_EQUAL    ${FILTER}    DENY    OTHERS
                        Set eACL    ${USER_KEY}    ${CID}    ${EACL_CUSTOM}

 
    Run Keyword And Expect Error   ${EACL_ERR_MSG}    
    ...  Get object    ${OTHER_KEY}    ${CID}    ${S_OID_0}    ${EMPTY}    ${OBJECT_PATH}
    Get object    ${OTHER_KEY}    ${CID}    ${S_OID}     ${EMPTY}    ${OBJECT_PATH}
    Run Keyword And Expect error    ${EACL_ERR_MSG}
    ...  Head object    ${OTHER_KEY}    ${CID}    ${S_OID_0}    ${EMPTY}
    Head object    ${OTHER_KEY}    ${CID}    ${S_OID}    ${EMPTY}


    [Teardown]          Teardown    payload_length_filter
