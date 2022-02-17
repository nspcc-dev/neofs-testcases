*** Settings ***
Variables       common.py
Variables       eacl_object_filters.py

Library         acl.py
Library         neofs.py
Library         Collections

Resource        common_steps_acl_extended.robot
Resource        common_steps_acl_basic.robot
Resource        payment_operations.robot
Resource        setup_teardown.robot

*** Variables ***
${OBJECT_PATH} =   testfile
${EACL_ERR_MSG} =    *

*** Test cases ***
Object ID Object Filter for Extended ACL
    [Documentation]         Testcase to validate if $Object:objectID eACL filter is correctly handled.
    [Tags]                  ACL  eACL  NeoFS  NeoCLI
    [Timeout]               20 min

    [Setup]                 Setup
    
    Log    Check eACL objectID Filter with MatchType String Equal
    Check eACL Filters with MatchType String Equal    $Object:objectID
    Log    Check eACL objectID Filter with MatchType String Not Equal
    Check eACL Filters with MatchType String Not Equal   $Object:objectID

    #################################################################################
    # If the first eACL rule contradicts the second, the second one won't be applied
    #################################################################################
    Log    Check if the second rule that contradicts the first is not applied
    Check eACL Filters with MatchType String Equal with two contradicting filters    $Object:objectID

    ###########################################################################################################################
    # If both STRING_EQUAL and STRING_NOT_EQUAL matchTypes are applied for the same filter value, no object can be operated on
    ###########################################################################################################################
    Log    Check two matchTypes applied
    Check eACL Filters, two matchTypes    $Object:objectID

    [Teardown]          Teardown    object_id


*** Keywords ***
 
Check eACL Filters with MatchType String Equal with two contradicting filters
    [Arguments]    ${FILTER}

    ${_}   ${_}     ${USER_KEY} =    Prepare Wallet And Deposit  
    ${_}   ${_}     ${OTHER_KEY} =    Prepare Wallet And Deposit

    ${CID} =            Create Container Public    ${USER_KEY} 
    ${FILE_S_USER}    ${_} =    Generate file    ${SIMPLE_OBJ_SIZE}

    ${S_OID_USER} =     Put Object    ${USER_KEY}     ${FILE_S_USER}    ${CID}    ${EMPTY}
    &{HEADER_DICT_USER} =    Object Header Decoded    ${USER_KEY}    ${CID}    ${S_OID_USER}
   
                        Get Object    ${OTHER_KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}    ${OBJECT_PATH}

    ${filter_value} =    Get From Dictionary    ${HEADER_DICT_USER}    ${EACL_OBJ_FILTERS}[${FILTER}]
    ${filters} =        Set Variable    obj:${FILTER}=${filter_value}
    ${rule} =           Set Variable    allow get ${filters} others
    ${contradicting_filters} =     Set Variable    obj:$Object:payloadLength=${SIMPLE_OBJ_SIZE}
    ${contradicting_rule} =    Set Variable    deny get ${contradicting_filters} others
    ${eACL_gen} =       Create List    ${rule}    ${contradicting_rule}
    ${EACL_CUSTOM} =    Create eACL    ${CID}      ${eACL_gen}

                        Set eACL    ${USER_KEY}    ${CID}    ${EACL_CUSTOM}
                        Get object    ${OTHER_KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}    ${OBJECT_PATH}

Check eACL Filters, two matchTypes
    [Arguments]    ${FILTER}

    ${_}   ${_}    ${USER_KEY} =    Prepare Wallet And Deposit  
    ${_}   ${_}    ${OTHER_KEY} =    Prepare Wallet And Deposit

    ${CID} =            Create Container Public    ${USER_KEY}
    ${FILE_S}    ${_} =    Generate file    ${SIMPLE_OBJ_SIZE}

    ${S_OID_USER} =     Put Object    ${USER_KEY}    ${FILE_S}    ${CID}    ${EMPTY}
    ${S_OID_OTHER} =    Put Object    ${OTHER_KEY}    ${FILE_S}    ${CID}    ${EMPTY}
    &{HEADER_DICT_USER} =    Object Header Decoded    ${USER_KEY}    ${CID}    ${S_OID_USER}

                        Get Object    ${OTHER_KEY}    ${CID}    ${S_OID_USER}     ${EMPTY}    ${OBJECT_PATH}
                        Get Object    ${OTHER_KEY}    ${CID}    ${S_OID_OTHER}    ${EMPTY}    ${OBJECT_PATH}

    ${filter_value} =    Get From Dictionary    ${HEADER_DICT_USER}    ${EACL_OBJ_FILTERS}[${FILTER}]
    ${noneq_filters} =    Set Variable    obj:${FILTER}!=${filter_value}
    ${rule_noneq_filter} =    Set Variable    deny get ${noneq_filters} others
    ${eq_filters} =     Set Variable    obj:${FILTER}=${filter_value}
    ${rule_eq_filter} =    Set Variable    deny get ${eq_filters} others
    ${eACL_gen} =       Create List    ${rule_noneq_filter}    ${rule_eq_filter}
    ${EACL_CUSTOM} =    Create eACL    ${CID}      ${eACL_gen}

                        Set eACL    ${USER_KEY}    ${CID}    ${EACL_CUSTOM}
                        Run Keyword And Expect Error    *
                        ...  Get object      ${OTHER_KEY}    ${CID}    ${S_OID_OTHER}    ${EMPTY}    ${OBJECT_PATH}
                        Run Keyword And Expect Error    *
                        ...  Get Object    ${OTHER_KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}    ${OBJECT_PATH}
