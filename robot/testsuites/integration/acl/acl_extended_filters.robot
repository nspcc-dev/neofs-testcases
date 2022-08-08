*** Settings ***
Variables       common.py

Library         acl.py
Library         container.py
Library         neofs_verbs.py
Library         Collections

Resource        common_steps_acl_extended.robot
Resource        payment_operations.robot
Resource        eacl_tables.robot

*** Test cases ***
Extended ACL Operations
    [Documentation]         Testcase to validate NeoFS operations with extended ACL.
    [Tags]                  ACL  eACL
    [Timeout]               5 min


    Check Filters    Simple
    Check Filters    Complex



*** Keywords ***

Check Filters
    [Arguments]    ${OBJ_COMPLEXITY}

    ${SIZE} =     Set Variable IF
    ...  """${OBJ_COMPLEXITY}""" == """Simple"""    ${SIMPLE_OBJ_SIZE}    ${COMPLEX_OBJ_SIZE}

    ${WALLET}        ${_}    ${_} =    Prepare Wallet And Deposit
    ${WALLET_OTH}    ${_}    ${_} =    Prepare Wallet And Deposit
    ${FILE_S}        ${_} =            Generate File    ${SIZE}

    Check eACL MatchType String Equal Request Deny     ${WALLET}    ${WALLET_OTH}    ${FILE_S}
    Check eACL MatchType String Equal Request Allow    ${WALLET}    ${WALLET_OTH}    ${FILE_S}


Check eACL MatchType String Equal Request Deny
    [Arguments]    ${USER_WALLET}    ${OTHER_WALLET}    ${FILE_S}
    ${CID} =                Create Container       ${USER_WALLET}    basic_acl=eacl-public-read-write
    ${S_OID_USER} =         Put object             ${USER_WALLET}    ${FILE_S}    ${CID}
                            Get object             ${USER_WALLET}    ${CID}       ${S_OID_USER}

                            Set eACL               ${USER_WALLET}    ${CID}    ${EACL_XHEADER_DENY_ALL}

                            # The current ACL cache lifetime is 30 sec
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                            Run Keyword And Expect Error    *
                            ...  Get object      ${OTHER_WALLET}    ${CID}    ${S_OID_USER}    options=--xhdr a=2
                            Get object           ${OTHER_WALLET}    ${CID}    ${S_OID_USER}    options=--xhdr a=256

                            Run Keyword And Expect Error    *
                            ...  Put object        ${OTHER_WALLET}    ${FILE_S}     ${CID}      options=--xhdr a=2
                            Run Keyword And Expect Error    *
                            ...  Get object      ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    options=--xhdr a=2
                            Run Keyword And Expect Error    *
                            ...   Search object             ${OTHER_WALLET}    ${CID}        keys=--oid ${S_OID_USER}    options=--xhdr a=2
                            Run Keyword And Expect Error    *
                            ...  Head object                ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    options=--xhdr a=2
                            Run Keyword And Expect Error    *
                            ...  Get Range                  ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    0:256         options=--xhdr a="2"
                            Run Keyword And Expect Error    *
                            ...  Get Range Hash             ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    bearer_token=${EMPTY}     range_cut=0:256      options=--xhdr a=2
                            Run Keyword And Expect Error    *
                            ...  Delete object              ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    options=--xhdr a=2

                            Put object                      ${OTHER_WALLET}    ${FILE_S}     ${CID}           options=--xhdr a=256
                            Get object                      ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    options=--xhdr a=*
                            Search object                   ${OTHER_WALLET}    ${CID}        keys=--oid ${S_OID_USER}    options=--xhdr a=
                            Head object                     ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    options=--xhdr a=.*
                            Get Range                       ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    0:256           options=--xhdr a="2 2"
                            Get Range Hash                  ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    bearer_token=${EMPTY}     range_cut=0:256      options=--xhdr a=256
                            Delete object                   ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    options=--xhdr a=22


Check eACL MatchType String Equal Request Allow
    [Arguments]    ${USER_WALLET}    ${OTHER_WALLET}    ${FILE_S}

    ${CID} =                Create Container    ${USER_WALLET}      basic_acl=eacl-public-read-write
    ${S_OID_USER} =         Put Object          ${USER_WALLET}      ${FILE_S}   ${CID}
                            Get Object          ${OTHER_WALLET}     ${CID}      ${S_OID_USER}

                            Set eACL    ${USER_WALLET}    ${CID}    ${EACL_XHEADER_ALLOW_ALL}

                            # The current ACL cache lifetime is 30 sec
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                            Get eACL                        ${USER_WALLET}    ${CID}

                            Run Keyword And Expect Error    *
                            ...  Get object                 ${OTHER_WALLET}    ${CID}    ${S_OID_USER}
                            Run Keyword And Expect Error    *
                            ...  Put object                 ${OTHER_WALLET}    ${FILE_S}     ${CID}
                            Run Keyword And Expect Error    *
                            ...  Get object                 ${OTHER_WALLET}    ${CID}        ${S_OID_USER}
                            Run Keyword And Expect Error    *
                            ...   Search object             ${OTHER_WALLET}    ${CID}        keys=--oid ${S_OID_USER}
                            Run Keyword And Expect Error    *
                            ...  Head object                ${OTHER_WALLET}    ${CID}        ${S_OID_USER}
                            Run Keyword And Expect Error    *
                            ...  Get Range                  ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    0:256
                            Run Keyword And Expect Error    *
                            ...  Get Range Hash             ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    bearer_token=${EMPTY}     range_cut=0:256
                            Run Keyword And Expect Error    *
                            ...  Delete object              ${OTHER_WALLET}    ${CID}        ${S_OID_USER}

                            Put object                      ${OTHER_WALLET}    ${FILE_S}     ${CID}           options=--xhdr a=2
                            Get object                      ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    options=--xhdr a=2
                            Search object                   ${OTHER_WALLET}    ${CID}        keys=--oid ${S_OID_USER}    options=--xhdr a=2
                            Head object                     ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    options=--xhdr a=2
                            Get Range                       ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    0:256           options=--xhdr a=2
                            Get Range Hash                  ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    bearer_token=${EMPTY}     range_cut=0:256       options=--xhdr a=2
                            Delete object                   ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    options=--xhdr a=2
