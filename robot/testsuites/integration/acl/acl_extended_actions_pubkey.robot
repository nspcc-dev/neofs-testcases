*** Settings ***
Variables    ../../../variables/common.py
Library      ../${RESOURCES}/neofs.py
Library      ../${RESOURCES}/payment_neogo.py

Library      Collections

Resource     common_steps_acl_extended.robot
Resource     ../${RESOURCES}/payment_operations.robot
Resource     ../${RESOURCES}/setup_teardown.robot
Resource       ../../../variables/eacl_tables.robot

*** Variables ***
${EACL_KEY} =   L1FGTLE6shum3EC7mNTPArUqSCKnmtEweRzyuawtYRZwGjpeRuw1


*** Test cases ***
Extended ACL Operations
    [Documentation]         Testcase to validate NeoFS operations with extended ACL.
    [Tags]                  ACL  eACL  NeoFS  NeoCLI
    [Timeout]               20 min

    [Setup]                 Setup

                            Generate Keys

                            Log    Check extended ACL with simple object
                            Generate files    ${SIMPLE_OBJ_SIZE}
                            Check eACL Deny All Other and Allow All Pubkey

                            Log    Check extended ACL with complex object
                            Generate files    ${COMPLEX_OBJ_SIZE}
                            Check eACL Deny All Other and Allow All Pubkey


    [Teardown]              Teardown    acl_extended_actions_pubkey


*** Keywords ***

Check eACL Deny All Other and Allow All Pubkey

    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object                 ${USER_KEY}     ${FILE_S}            ${CID}            ${EMPTY}            ${FILE_USR_HEADER}
    ${D_OID_USER} =         Put object                 ${USER_KEY}     ${FILE_S}            ${CID}            ${EMPTY}            ${FILE_USR_HEADER_DEL}
    @{S_OBJ_H} =	        Create List	               ${S_OID_USER}

                            Put object                          ${EACL_KEY}    ${FILE_S}     ${CID}                   ${EMPTY}            ${FILE_OTH_HEADER}
                            Get object                          ${EACL_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl
                            Search object                       ${EACL_KEY}    ${CID}        ${EMPTY}                 ${EMPTY}            ${FILE_USR_HEADER}        ${S_OBJ_H}
                            Head object                         ${EACL_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}
                            Get Range                           ${EACL_KEY}    ${CID}        ${S_OID_USER}            s_get_range         ${EMPTY}            0:256
                            Get Range Hash                      ${EACL_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            0:256
                            Delete object                       ${EACL_KEY}    ${CID}        ${D_OID_USER}            ${EMPTY}

                            Set eACL                            ${USER_KEY}    ${CID}        ${EACL_ALLOW_ALL_Pubkey}

                            # The current ACL cache lifetime is 30 sec
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                            Get eACL                            ${USER_KEY}    ${CID}

                            Run Keyword And Expect Error        *
                            ...  Put object                          ${OTHER_KEY}    ${FILE_S}     ${CID}            ${EMPTY}            ${FILE_USR_HEADER}
                            Run Keyword And Expect Error        *
                            ...  Get object                          ${OTHER_KEY}    ${CID}        ${S_OID_USER}     ${EMPTY}            local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Search object                       ${OTHER_KEY}    ${CID}        ${EMPTY}          ${EMPTY}            ${FILE_USR_HEADER}      ${S_OBJ_H}
                            Run Keyword And Expect Error        *
                            ...  Head object                         ${OTHER_KEY}    ${CID}        ${S_OID_USER}     ${EMPTY}
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${OTHER_KEY}    ${CID}        ${S_OID_USER}     s_get_range     ${EMPTY}            0:256
                            Run Keyword And Expect Error        *
                            ...  Get Range Hash                      ${OTHER_KEY}    ${CID}        ${S_OID_USER}     ${EMPTY}        0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${OTHER_KEY}    ${CID}        ${S_OID_USER}     ${EMPTY}

                            Put object                          ${EACL_KEY}    ${FILE_S}     ${CID}                  ${EMPTY}            ${FILE_OTH_HEADER}
                            Get object                          ${EACL_KEY}    ${CID}        ${S_OID_USER}           ${EMPTY}            local_file_eacl
                            Search object                       ${EACL_KEY}    ${CID}        ${EMPTY}                ${EMPTY}            ${FILE_USR_HEADER}     ${S_OBJ_H}
                            Head object                         ${EACL_KEY}    ${CID}        ${S_OID_USER}           ${EMPTY}
                            Get Range                           ${EACL_KEY}    ${CID}        ${S_OID_USER}           s_get_range         ${EMPTY}            0:256
                            Get Range Hash                      ${EACL_KEY}    ${CID}        ${S_OID_USER}           ${EMPTY}            0:256
                            Delete object                       ${EACL_KEY}    ${CID}        ${S_OID_USER}           ${EMPTY}
