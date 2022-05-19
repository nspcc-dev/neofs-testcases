*** Settings ***
Variables       common.py
Variables       eacl_object_filters.py

Library         acl.py
Library         container.py
Library         neofs.py
Library         Collections
Library         contract_keywords.py

Resource        common_steps_acl_extended.robot
Resource        common_steps_acl_basic.robot
Resource        payment_operations.robot
Resource        setup_teardown.robot

*** Variables ***
${OBJECT_PATH} =   testfile
${EACL_ERR_MSG} =    *

*** Test cases ***
Creation Epoch Object Filter for Extended ACL
    [Documentation]         Testcase to validate if $Object:creationEpoch eACL filter is correctly handled.
    [Tags]                  ACL  eACL
    [Timeout]               20 min

    [Setup]                 Setup

    Check eACL Filters with MatchType String Equal    $Object:creationEpoch
    Check $Object:creationEpoch Filter with MatchType String Not Equal    $Object:creationEpoch

    [Teardown]          Teardown    creation_epoch_filter

*** Keywords ***

Check $Object:creationEpoch Filter with MatchType String Not Equal
    [Arguments]    ${FILTER}

    ${WALLET}   ${_}    ${_} =    Prepare Wallet And Deposit
    ${WALLET_OTH}   ${_}    ${_} =    Prepare Wallet And Deposit

    ${CID} =            Create Container    ${WALLET}       basic_acl=eacl-public-read-write
    ${FILE_S}    ${_} =    Generate file    ${SIMPLE_OBJ_SIZE}

    ${S_OID} =          Put Object    ${WALLET}    ${FILE_S}    ${CID}
    ${S_OID_NEW} =      Put Object    ${WALLET}    ${FILE_S}    ${CID}

                        Get Object    ${WALLET}    ${CID}    ${S_OID_NEW}    ${EMPTY}    local_file_eacl
    &{HEADER} =         Head Object    ${WALLET}    ${CID}    ${S_OID_NEW}

    ${EACL_CUSTOM} =    Compose eACL Custom    ${CID}    ${HEADER}[header][creationEpoch]    !=    ${FILTER}    DENY    OTHERS
                        Set eACL    ${WALLET}    ${CID}    ${EACL_CUSTOM}

    Run Keyword And Expect Error   ${EACL_ERR_MSG}
    ...  Get object    ${WALLET_OTH}    ${CID}    ${S_OID}    ${EMPTY}    ${OBJECT_PATH}
    Get object    ${WALLET_OTH}    ${CID}    ${S_OID_NEW}     ${EMPTY}    ${OBJECT_PATH}
    Run Keyword And Expect error    ${EACL_ERR_MSG}
    ...  Head object    ${WALLET_OTH}    ${CID}    ${S_OID}
    Head object    ${WALLET_OTH}    ${CID}    ${S_OID_NEW}
