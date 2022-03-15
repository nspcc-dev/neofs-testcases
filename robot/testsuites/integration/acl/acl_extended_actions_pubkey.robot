*** Settings ***
Variables    common.py

Library      Collections
Library      acl.py
Library      neofs.py
Library      payment_neogo.py

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

    ${_}   ${_}     ${USER_KEY} =   Prepare Wallet And Deposit
    ${_}   ${_}     ${OTHER_KEY} =   Prepare Wallet And Deposit

                            Log    Check extended ACL with simple object
                            Generate files    ${SIMPLE_OBJ_SIZE}
                            Check eACL Deny All Other and Allow All Pubkey    ${USER_KEY}    ${FILE_S}    ${OTHER_KEY}

                            Log    Check extended ACL with complex object
                            Generate files    ${COMPLEX_OBJ_SIZE}
                            Check eACL Deny All Other and Allow All Pubkey    ${USER_KEY}    ${FILE_S}    ${OTHER_KEY}


    [Teardown]              Teardown    acl_extended_actions_pubkey


*** Keywords ***

Check eACL Deny All Other and Allow All Pubkey
    [Arguments]    ${USER_KEY}    ${FILE_S}    ${OTHER_KEY}

    ${CID} =                Create Container Public    ${USER_KEY}
    ${S_OID_USER} =         Put object                 ${USER_KEY}     ${FILE_S}        ${CID}    user_headers=${USER_HEADER}
    ${D_OID_USER} =         Put object                 ${USER_KEY}     ${FILE_S}        ${CID}    user_headers=${USER_HEADER_DEL}
    @{S_OBJ_H} =	    Create List	               ${S_OID_USER}

                            Put object                  ${EACL_KEY}    ${FILE_S}     ${CID}       user_headers=${ANOTHER_HEADER}
                            Get object                  ${EACL_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl
                            Search object               ${EACL_KEY}    ${CID}        ${EMPTY}                 ${EMPTY}            ${USER_HEADER}        ${S_OBJ_H}
                            Head object                 ${EACL_KEY}    ${CID}        ${S_OID_USER}
                            Get Range                   ${EACL_KEY}    ${CID}        ${S_OID_USER}            s_get_range         ${EMPTY}            0:256
                            Get Range Hash              ${EACL_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            0:256
                            Delete object               ${EACL_KEY}    ${CID}        ${D_OID_USER}

                            Set eACL                    ${USER_KEY}    ${CID}        ${EACL_ALLOW_ALL_Pubkey}

                            # The current ACL cache lifetime is 30 sec
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                            Get eACL                            ${USER_KEY}    ${CID}

                            Run Keyword And Expect Error        *
                            ...  Put object                          ${OTHER_KEY}    ${FILE_S}     ${CID}    user_headers=${USER_HEADER}
                            Run Keyword And Expect Error        *
                            ...  Get object                          ${OTHER_KEY}    ${CID}        ${S_OID_USER}     ${EMPTY}            local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Search object                       ${OTHER_KEY}    ${CID}        ${EMPTY}          ${EMPTY}            ${USER_HEADER}      ${S_OBJ_H}
                            Run Keyword And Expect Error        *
                            ...  Head object                         ${OTHER_KEY}    ${CID}        ${S_OID_USER}
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${OTHER_KEY}    ${CID}        ${S_OID_USER}     s_get_range     ${EMPTY}            0:256
                            Run Keyword And Expect Error        *
                            ...  Get Range Hash                      ${OTHER_KEY}    ${CID}        ${S_OID_USER}     ${EMPTY}        0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${OTHER_KEY}    ${CID}        ${S_OID_USER}

                            Put object              ${EACL_KEY}    ${FILE_S}     ${CID}    user_headers=${ANOTHER_HEADER}
                            Get object              ${EACL_KEY}    ${CID}        ${S_OID_USER}           ${EMPTY}            local_file_eacl
                            Search object           ${EACL_KEY}    ${CID}        ${EMPTY}                ${EMPTY}            ${USER_HEADER}     ${S_OBJ_H}
                            Head object             ${EACL_KEY}    ${CID}        ${S_OID_USER}
                            Get Range               ${EACL_KEY}    ${CID}        ${S_OID_USER}           s_get_range         ${EMPTY}            0:256
                            Get Range Hash          ${EACL_KEY}    ${CID}        ${S_OID_USER}           ${EMPTY}            0:256
                            Delete object           ${EACL_KEY}    ${CID}        ${S_OID_USER}
