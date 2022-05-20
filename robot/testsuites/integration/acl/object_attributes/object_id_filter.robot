*** Settings ***
Variables       common.py
Variables       eacl_object_filters.py

Library         acl.py
Library         container.py
Library         utility_keywords.py

Resource        common_steps_acl_extended.robot
Resource        payment_operations.robot
Resource        setup_teardown.robot

*** Variables ***
${OBJECT_PATH} =   testfile
${EACL_ERR_MSG} =    *

*** Test cases ***
Object ID Object Filter for Extended ACL
    [Documentation]         Testcase to validate if $Object:objectID eACL filter is correctly handled.
    [Tags]                  ACL  eACL
    [Timeout]               20 min

    [Setup]                 Setup

    Check eACL Filters with MatchType String Equal    $Object:objectID
    Check eACL Filters with MatchType String Not Equal   $Object:objectID

    #################################################################################
    # If the first eACL rule contradicts the second, the second one won't be applied
    #################################################################################
    Check eACL Filters with MatchType String Equal with two contradicting filters    $Object:objectID

    ###########################################################################################################################
    # If both STRING_EQUAL and STRING_NOT_EQUAL matchTypes are applied for the same filter value, no object can be operated on
    ###########################################################################################################################
    Check eACL Filters, two matchTypes    $Object:objectID

    [Teardown]          Teardown    object_id


*** Keywords ***

Check eACL Filters with MatchType String Equal with two contradicting filters
    [Arguments]    ${FILTER}

    ${WALLET}   ${_}     ${_} =    Prepare Wallet And Deposit
    ${WALLET_OTH}   ${_}     ${_} =    Prepare Wallet And Deposit

    ${CID} =            Create Container         ${WALLET}      basic_acl=eacl-public-read-write
    ${FILE_S_USER}    ${_} =    Generate file    ${SIMPLE_OBJ_SIZE}

    ${S_OID_USER} =     Put Object    ${WALLET}     ${FILE_S_USER}    ${CID}    ${EMPTY}

                        Get Object    ${WALLET_OTH}    ${CID}       ${S_OID_USER}    ${EMPTY}    ${OBJECT_PATH}

    &{HEADER} =         Head Object    ${WALLET}    ${CID}    ${S_OID_USER}
    ${filter_value} =    Get From Dictionary    ${HEADER}    ${EACL_OBJ_FILTERS}[${FILTER}]
    ${filters} =        Set Variable    obj:${FILTER}=${filter_value}
    ${rule} =           Set Variable    allow get ${filters} others
    ${contradicting_filters} =     Set Variable    obj:$Object:payloadLength=${SIMPLE_OBJ_SIZE}
    ${contradicting_rule} =    Set Variable    deny get ${contradicting_filters} others
    ${eACL_gen} =       Create List    ${rule}    ${contradicting_rule}
    ${EACL_CUSTOM} =    Create eACL    ${CID}      ${eACL_gen}

                        Set eACL    ${WALLET}    ${CID}    ${EACL_CUSTOM}
                        Get object    ${WALLET_OTH}    ${CID}    ${S_OID_USER}    ${EMPTY}    ${OBJECT_PATH}


Check eACL Filters, two matchTypes
    [Arguments]    ${FILTER}

    ${WALLET}   ${_}    ${_} =    Prepare Wallet And Deposit
    ${WALLET_OTH}   ${_}    ${_} =    Prepare Wallet And Deposit

    ${CID} =            Create Container    ${WALLET}       basic_acl=eacl-public-read-write
    ${FILE_S}    ${_} =    Generate file    ${SIMPLE_OBJ_SIZE}

    ${S_OID_USER} =     Put Object    ${WALLET}    ${FILE_S}    ${CID}    ${EMPTY}
    ${S_OID_OTHER} =    Put Object    ${WALLET_OTH}    ${FILE_S}    ${CID}    ${EMPTY}
    &{HEADER} =         Head Object   ${WALLET}    ${CID}    ${S_OID_USER}

                        Get Object    ${WALLET_OTH}    ${CID}    ${S_OID_USER}     ${EMPTY}    ${OBJECT_PATH}
                        Get Object    ${WALLET_OTH}    ${CID}    ${S_OID_OTHER}    ${EMPTY}    ${OBJECT_PATH}

    ${filter_value} =    Get From Dictionary    ${HEADER}    ${EACL_OBJ_FILTERS}[${FILTER}]
    ${noneq_filters} =    Set Variable    obj:${FILTER}!=${filter_value}
    ${rule_noneq_filter} =    Set Variable    deny get ${noneq_filters} others
    ${eq_filters} =     Set Variable    obj:${FILTER}=${filter_value}
    ${rule_eq_filter} =    Set Variable    deny get ${eq_filters} others
    ${eACL_gen} =       Create List    ${rule_noneq_filter}    ${rule_eq_filter}
    ${EACL_CUSTOM} =    Create eACL    ${CID}      ${eACL_gen}

                        Set eACL    ${WALLET}    ${CID}    ${EACL_CUSTOM}
                        Run Keyword And Expect Error    *
                        ...  Get object      ${WALLET_OTH}    ${CID}    ${S_OID_OTHER}    ${EMPTY}    ${OBJECT_PATH}
                        Run Keyword And Expect Error    *
                        ...  Get Object    ${WALLET_OTH}    ${CID}    ${S_OID_USER}    ${EMPTY}    ${OBJECT_PATH}
