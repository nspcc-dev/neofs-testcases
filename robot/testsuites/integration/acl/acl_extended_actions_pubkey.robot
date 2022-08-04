*** Settings ***
Variables    common.py

Library      acl.py
Library      container.py
Library      neofs_verbs.py
Library      utility_keywords.py
Library    Collections

Resource     common_steps_acl_extended.robot
Resource     payment_operations.robot
Resource     setup_teardown.robot
Resource     eacl_tables.robot

*** Variables ***
&{USER_HEADER} =        key1=1      key2=abc
&{USER_HEADER_DEL} =    key1=del    key2=del
&{ANOTHER_HEADER} =     key1=oth    key2=oth


*** Test cases ***
Extended ACL Operations
    [Documentation]         Testcase to validate NeoFS operations with extended ACL.
    [Tags]                  ACL  eACL
    [Timeout]               20 min



    ${FILE_S}    ${_} =     Generate file    ${SIMPLE_OBJ_SIZE}
                            Check eACL Deny All Other and Allow All Pubkey    ${FILE_S}

    ${FILE_S}    ${_} =     Generate file    ${COMPLEX_OBJ_SIZE}
                            Check eACL Deny All Other and Allow All Pubkey    ${FILE_S}




*** Keywords ***

Check eACL Deny All Other and Allow All Pubkey
    [Arguments]    ${FILE_S}

    ${USER_WALLET}   ${_}     ${_} =   Prepare Wallet And Deposit
    ${WALLET_OTH}   ${_}     ${_} =   Prepare Wallet And Deposit

    ${CID} =                Create Container           ${USER_WALLET}     basic_acl=eacl-public-read-write
    ${S_OID_USER} =         Put object                 ${USER_WALLET}     ${FILE_S}        ${CID}    user_headers=${USER_HEADER}
    ${D_OID_USER} =         Put object                 ${USER_WALLET}     ${FILE_S}        ${CID}    user_headers=${USER_HEADER_DEL}
    @{S_OBJ_H} =	        Create List	               ${S_OID_USER}

    ${WALLET_ALLOW}    ${_}    ${_} =    Prepare Wallet And Deposit 

                            Put object                  ${WALLET_ALLOW}    ${FILE_S}     ${CID}       user_headers=${ANOTHER_HEADER}
                            Get object                  ${WALLET_ALLOW}    ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl
                            Search object               ${WALLET_ALLOW}    ${CID}        ${EMPTY}                 ${EMPTY}            ${USER_HEADER}        ${S_OBJ_H}
                            Head object                 ${WALLET_ALLOW}    ${CID}        ${S_OID_USER}
                            Get Range                   ${WALLET_ALLOW}    ${CID}        ${S_OID_USER}            0:256
                            Get Range Hash              ${WALLET_ALLOW}    ${CID}        ${S_OID_USER}            ${EMPTY}            0:256
                            Delete object               ${WALLET_ALLOW}    ${CID}        ${D_OID_USER}

    @{VERBS} =              Create List      get    head    put    delete    search    getrange    getrangehash
    ${RULES_OTH} =          EACL Rules       deny     ${VERBS}    others
    ${RULES_PUB} =          EACL Rules       allow    ${VERBS}    ${WALLET_ALLOW}
    ${eACL_gen} =           Combine Lists    ${RULES_PUB}    ${RULES_OTH}
    ${EACL_TABLE} =         Create eACL      ${CID}    ${eACL_gen}
                            Set EACL         ${USER_WALLET}    ${CID}    ${EACL_TABLE}

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
                            ...  Get Range                           ${WALLET_OTH}    ${CID}        ${S_OID_USER}     0:256
                            Run Keyword And Expect Error        *
                            ...  Get Range Hash                      ${WALLET_OTH}    ${CID}        ${S_OID_USER}     ${EMPTY}        0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${WALLET_OTH}    ${CID}        ${S_OID_USER}

                            Put object              ${WALLET_ALLOW}    ${FILE_S}     ${CID}    user_headers=${ANOTHER_HEADER}
                            Get object              ${WALLET_ALLOW}    ${CID}        ${S_OID_USER}           ${EMPTY}            local_file_eacl
                            Search object           ${WALLET_ALLOW}    ${CID}        ${EMPTY}                ${EMPTY}            ${USER_HEADER}     ${S_OBJ_H}
                            Head object             ${WALLET_ALLOW}    ${CID}        ${S_OID_USER}
                            Get Range               ${WALLET_ALLOW}    ${CID}        ${S_OID_USER}           0:256
                            Get Range Hash          ${WALLET_ALLOW}    ${CID}        ${S_OID_USER}           ${EMPTY}            0:256
                            Delete object           ${WALLET_ALLOW}    ${CID}        ${S_OID_USER}
