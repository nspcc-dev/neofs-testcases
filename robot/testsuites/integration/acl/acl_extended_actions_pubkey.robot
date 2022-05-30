*** Settings ***
Variables    common.py

Library      acl.py
Library      container.py
Library      neofs_verbs.py
Library      utility_keywords.py

Resource     common_steps_acl_extended.robot
Resource     payment_operations.robot
Resource     setup_teardown.robot
Resource     eacl_tables.robot

*** Variables ***
${EACL_KEY} =   L1FGTLE6shum3EC7mNTPArUqSCKnmtEweRzyuawtYRZwGjpeRuw1
&{USER_HEADER} =        key1=1      key2=abc
&{USER_HEADER_DEL} =    key1=del    key2=del
&{ANOTHER_HEADER} =     key1=oth    key2=oth


*** Test cases ***
Extended ACL Operations
    [Documentation]         Testcase to validate NeoFS operations with extended ACL.
    [Tags]                  ACL  eACL
    [Timeout]               20 min

    [Setup]                 Setup

    ${WALLET}   ${_}     ${_} =   Prepare Wallet And Deposit
    ${WALLET_OTH}   ${_}     ${_} =   Prepare Wallet And Deposit

                            Log    Check extended ACL with simple object
    ${FILE_S}    ${_} =     Generate file    ${SIMPLE_OBJ_SIZE}
                            Check eACL Deny All Other and Allow All Pubkey    ${WALLET}    ${FILE_S}    ${WALLET_OTH}

                            Log    Check extended ACL with complex object
    ${FILE_S}    ${_} =     Generate file    ${COMPLEX_OBJ_SIZE}
                            Check eACL Deny All Other and Allow All Pubkey    ${WALLET}    ${FILE_S}    ${WALLET_OTH}


    [Teardown]              Teardown    acl_extended_actions_pubkey


*** Keywords ***

Check eACL Deny All Other and Allow All Pubkey
    [Arguments]    ${USER_WALLET}    ${FILE_S}    ${WALLET_OTH}

    ${CID} =                Create Container           ${USER_WALLET}     basic_acl=eacl-public-read-write
    ${S_OID_USER} =         Put object                 ${USER_WALLET}     ${FILE_S}        ${CID}    user_headers=${USER_HEADER}
    ${D_OID_USER} =         Put object                 ${USER_WALLET}     ${FILE_S}        ${CID}    user_headers=${USER_HEADER_DEL}
    @{S_OBJ_H} =	    Create List	               ${S_OID_USER}

    ${WALLET_EACL}    ${_} =    Prepare Wallet with WIF And Deposit    ${EACL_KEY}

                            Put object                  ${WALLET_EACL}    ${FILE_S}     ${CID}       user_headers=${ANOTHER_HEADER}
                            Get object                  ${WALLET_EACL}    ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl
                            Search object               ${WALLET_EACL}    ${CID}        ${EMPTY}                 ${EMPTY}            ${USER_HEADER}        ${S_OBJ_H}
                            Head object                 ${WALLET_EACL}    ${CID}        ${S_OID_USER}
                            Get Range                   ${WALLET_EACL}    ${CID}        ${S_OID_USER}            s_get_range         ${EMPTY}            0:256
                            Get Range Hash              ${WALLET_EACL}    ${CID}        ${S_OID_USER}            ${EMPTY}            0:256
                            Delete object               ${WALLET_EACL}    ${CID}        ${D_OID_USER}

                            Set eACL                    ${USER_WALLET}    ${CID}        ${EACL_ALLOW_ALL_Pubkey}

                            # The current ACL cache lifetime is 30 sec
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                            Get eACL                            ${USER_WALLET}    ${CID}

                            Run Keyword And Expect Error        *
                            ...  Put object                          ${WALLET_OTH}    ${FILE_S}     ${CID}    user_headers=${USER_HEADER}
                            Run Keyword And Expect Error        *
                            ...  Get object                          ${WALLET_OTH}    ${CID}        ${S_OID_USER}     ${EMPTY}            local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Search object                       ${WALLET_OTH}    ${CID}        ${EMPTY}          ${EMPTY}            ${USER_HEADER}      ${S_OBJ_H}
                            Run Keyword And Expect Error        *
                            ...  Head object                         ${WALLET_OTH}    ${CID}        ${S_OID_USER}
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${WALLET_OTH}    ${CID}        ${S_OID_USER}     s_get_range     ${EMPTY}            0:256
                            Run Keyword And Expect Error        *
                            ...  Get Range Hash                      ${WALLET_OTH}    ${CID}        ${S_OID_USER}     ${EMPTY}        0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${WALLET_OTH}    ${CID}        ${S_OID_USER}

                            Put object              ${WALLET_EACL}    ${FILE_S}     ${CID}    user_headers=${ANOTHER_HEADER}
                            Get object              ${WALLET_EACL}    ${CID}        ${S_OID_USER}           ${EMPTY}            local_file_eacl
                            Search object           ${WALLET_EACL}    ${CID}        ${EMPTY}                ${EMPTY}            ${USER_HEADER}     ${S_OBJ_H}
                            Head object             ${WALLET_EACL}    ${CID}        ${S_OID_USER}
                            Get Range               ${WALLET_EACL}    ${CID}        ${S_OID_USER}           s_get_range         ${EMPTY}            0:256
                            Get Range Hash          ${WALLET_EACL}    ${CID}        ${S_OID_USER}           ${EMPTY}            0:256
                            Delete object           ${WALLET_EACL}    ${CID}        ${S_OID_USER}
