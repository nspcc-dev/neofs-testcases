*** Settings ***
Variables                   ../../../variables/common.py
Library                     Collections
Library                     ../${RESOURCES}/neofs.py
Library                     ../${RESOURCES}/payment_neogo.py
Library                     ../${RESOURCES}/utility_keywords.py

Resource                    common_steps_acl_extended.robot

*** Test cases ***
Extended ACL Operations
    [Documentation]         Testcase to validate NeoFS operations with extended ACL.
    [Tags]                  ACL  eACL  NeoFS  NeoCLI
    [Timeout]               20 min

    [Setup]                 Create Temporary Directory

                            Generate Keys
                            Prepare eACL Role rules

                            Log    Check extended ACL with simple object
                            Generate files    ${SIMPLE_OBJ_SIZE}
                            Check eACL Deny and Allow All System

                            Log    Check extended ACL with complex object
                            Generate files    ${COMPLEX_OBJ_SIZE}
                            Check eACL Deny and Allow All System

    [Teardown]              Cleanup


*** Keywords ***

Check eACL Deny and Allow All System
    ${CID} =                Create Container Public

    ${S_OID_USER} =         Put object      ${USER_KEY}    ${FILE_S}    ${CID}    ${EMPTY}    ${FILE_USR_HEADER}
    ${D_OID_USER_S} =       Put object      ${USER_KEY}    ${FILE_S}    ${CID}    ${EMPTY}    ${FILE_USR_HEADER_DEL}
    ${D_OID_USER_SN} =      Put object      ${USER_KEY}    ${FILE_S}    ${CID}    ${EMPTY}    ${FILE_USR_HEADER_DEL}

    @{S_OBJ_H} =	        Create List	             ${S_OID_USER}

                            Put object      ${SYSTEM_KEY}       ${FILE_S}    ${CID}    ${EMPTY}    ${FILE_OTH_HEADER}
                            Put object      ${SYSTEM_KEY_SN}    ${FILE_S}    ${CID}    ${EMPTY}    ${FILE_OTH_HEADER}

                            Get object    ${SYSTEM_KEY}       ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl
                            Get object    ${SYSTEM_KEY_SN}    ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl

                            Search object            ${SYSTEM_KEY}       ${CID}    ${EMPTY}    ${EMPTY}    ${FILE_USR_HEADER}    ${S_OBJ_H}
                            Search object            ${SYSTEM_KEY_SN}    ${CID}    ${EMPTY}    ${EMPTY}    ${FILE_USR_HEADER}    ${S_OBJ_H}

                            Head object              ${SYSTEM_KEY}       ${CID}    ${S_OID_USER}    ${EMPTY}
                            Head object              ${SYSTEM_KEY_SN}    ${CID}    ${S_OID_USER}    ${EMPTY}

                            Get Range                ${SYSTEM_KEY}       ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Get Range                ${SYSTEM_KEY_SN}    ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256

                            Get Range Hash           ${SYSTEM_KEY}       ${CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Get Range Hash           ${SYSTEM_KEY_SN}    ${CID}    ${S_OID_USER}    ${EMPTY}    0:256

                            Delete object            ${SYSTEM_KEY}       ${CID}    ${D_OID_USER_S}     ${EMPTY}
                            Delete object            ${SYSTEM_KEY_SN}    ${CID}    ${D_OID_USER_SN}    ${EMPTY}

                            Set eACL                 ${USER_KEY}     ${CID}        ${EACL_DENY_ALL_SYSTEM}    --await

                            # The current ACL cache lifetime is 30 sec
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                            Run Keyword And Expect Error    *
                            ...  Put object        ${SYSTEM_KEY}       ${FILE_S}    ${CID}    ${EMPTY}    ${FILE_OTH_HEADER}
                            Run Keyword And Expect Error    *
                            ...  Put object        ${SYSTEM_KEY_SN}    ${FILE_S}    ${CID}    ${EMPTY}    ${FILE_OTH_HEADER}

                            Run Keyword And Expect Error    *
                            ...  Get object      ${SYSTEM_KEY}       ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl
                            Run Keyword And Expect Error    *
                            ...  Get object      ${SYSTEM_KEY_SN}    ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl

                            Run Keyword And Expect Error    *
                            ...  Search object              ${SYSTEM_KEY}       ${CID}    ${EMPTY}    ${EMPTY}    ${FILE_USR_HEADER}    ${S_OBJ_H}
                            Run Keyword And Expect Error    *
                            ...  Search object              ${SYSTEM_KEY_SN}    ${CID}    ${EMPTY}    ${EMPTY}    ${FILE_USR_HEADER}    ${S_OBJ_H}


                            Run Keyword And Expect Error        *
                            ...  Head object                         ${SYSTEM_KEY}       ${CID}    ${S_OID_USER}    ${EMPTY}
                            Run Keyword And Expect Error        *
                            ...  Head object                         ${SYSTEM_KEY_SN}    ${CID}    ${S_OID_USER}    ${EMPTY}

                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${SYSTEM_KEY}       ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${SYSTEM_KEY_SN}    ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256


                            Run Keyword And Expect Error        *
                            ...  Get Range Hash                      ${SYSTEM_KEY}       ${CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Run Keyword And Expect Error        *
                            ...  Get Range Hash                      ${SYSTEM_KEY_SN}    ${CID}    ${S_OID_USER}    ${EMPTY}    0:256


                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${SYSTEM_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}
                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${SYSTEM_KEY_SN}    ${CID}        ${S_OID_USER}            ${EMPTY}


                            Set eACL                            ${USER_KEY}     ${CID}        ${EACL_ALLOW_ALL_SYSTEM}    --await

                            # The current ACL cache lifetime is 30 sec
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

    ${D_OID_USER_S} =       Put object                 ${USER_KEY}     ${FILE_S}            ${CID}            ${EMPTY}            ${FILE_USR_HEADER_DEL}
    ${D_OID_USER_SN} =      Put object                 ${USER_KEY}     ${FILE_S}            ${CID}            ${EMPTY}            ${FILE_USR_HEADER_DEL}


                            Put object                 ${SYSTEM_KEY}       ${FILE_S}     ${CID}            ${EMPTY}                   ${FILE_OTH_HEADER}
                            Put object                 ${SYSTEM_KEY_SN}    ${FILE_S}     ${CID}            ${EMPTY}                   ${FILE_OTH_HEADER}

                            Get object               ${SYSTEM_KEY}       ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl
                            Get object               ${SYSTEM_KEY_SN}    ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl

                            Search object                       ${SYSTEM_KEY}       ${CID}        ${EMPTY}            ${EMPTY}                 ${FILE_USR_HEADER}       ${S_OBJ_H}
                            Search object                       ${SYSTEM_KEY_SN}    ${CID}        ${EMPTY}            ${EMPTY}                 ${FILE_USR_HEADER}       ${S_OBJ_H}

                            Head object                         ${SYSTEM_KEY}       ${CID}        ${S_OID_USER}            ${EMPTY}
                            Head object                         ${SYSTEM_KEY_SN}    ${CID}        ${S_OID_USER}            ${EMPTY}

                            Get Range                           ${SYSTEM_KEY}       ${CID}        ${S_OID_USER}            s_get_range      ${EMPTY}            0:256
                            Get Range                           ${SYSTEM_KEY_SN}    ${CID}        ${S_OID_USER}            s_get_range      ${EMPTY}            0:256

                            Get Range Hash                      ${SYSTEM_KEY}       ${CID}        ${S_OID_USER}            ${EMPTY}            0:256
                            Get Range Hash                      ${SYSTEM_KEY_SN}    ${CID}        ${S_OID_USER}            ${EMPTY}            0:256

                            Delete object                       ${SYSTEM_KEY}       ${CID}        ${D_OID_USER_S}            ${EMPTY}
                            Delete object                       ${SYSTEM_KEY_SN}    ${CID}        ${D_OID_USER_SN}           ${EMPTY}


Cleanup
                            Cleanup Files
                            Get Docker Logs    acl_extended
