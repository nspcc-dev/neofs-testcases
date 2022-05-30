*** Settings ***
Variables    common.py

Library     acl.py
Library     container.py
Library     neofs_verbs.py
Library     utility_keywords.py

Resource     common_steps_acl_extended.robot
Resource     payment_operations.robot
Resource     setup_teardown.robot
Resource     eacl_tables.robot

*** Variables ***
${SYSTEM_KEY} =     ${NEOFS_IR_WIF}
&{USER_HEADER} =        key1=1      key2=abc
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
                            Check Сompound Operations    ${WALLET}    ${WALLET_OTH}    ${FILE_S}

                            Log    Check extended ACL with complex object
    ${FILE_S}    ${_} =     Generate file    ${COMPLEX_OBJ_SIZE}
                            Check Сompound Operations    ${WALLET}    ${WALLET_OTH}    ${FILE_S}

    [Teardown]      Teardown    acl_extended_compound


*** Keywords ***


Check Сompound Operations
    [Arguments]             ${WALLET}    ${WALLET_OTH}    ${FILE_S}

    ${WALLET_SYS}    ${ADDR_SYS} =     Prepare Wallet with WIF And Deposit    ${SYSTEM_KEY}

    Check eACL Сompound Get    ${WALLET_OTH}    ${EACL_COMPOUND_GET_OTHERS}    ${FILE_S}    ${WALLET}    ${WALLET_SYS}
    Check eACL Сompound Get    ${WALLET}        ${EACL_COMPOUND_GET_USER}      ${FILE_S}    ${WALLET}    ${WALLET_SYS}
    Check eACL Сompound Get    ${WALLET_SYS}    ${EACL_COMPOUND_GET_SYSTEM}    ${FILE_S}    ${WALLET}    ${WALLET_SYS}

    Check eACL Сompound Delete    ${WALLET_OTH}     ${EACL_COMPOUND_DELETE_OTHERS}    ${FILE_S}    ${WALLET}    ${WALLET_SYS}
    Check eACL Сompound Delete    ${WALLET}         ${EACL_COMPOUND_DELETE_USER}      ${FILE_S}    ${WALLET}    ${WALLET_SYS}
    Check eACL Сompound Delete    ${WALLET_SYS}     ${EACL_COMPOUND_DELETE_SYSTEM}    ${FILE_S}    ${WALLET}    ${WALLET_SYS}

    Check eACL Сompound Get Range Hash    ${WALLET_OTH}     ${EACL_COMPOUND_GET_HASH_OTHERS}    ${FILE_S}    ${WALLET}    ${WALLET_SYS}
    Check eACL Сompound Get Range Hash    ${WALLET}         ${EACL_COMPOUND_GET_HASH_USER}      ${FILE_S}    ${WALLET}    ${WALLET_SYS}
    Check eACL Сompound Get Range Hash    ${WALLET_SYS}     ${EACL_COMPOUND_GET_HASH_SYSTEM}    ${FILE_S}    ${WALLET}    ${WALLET_SYS}

Check eACL Сompound Get
    [Arguments]             ${WALLET}    ${DENY_EACL}    ${FILE_S}    ${USER_WALLET}    ${WALLET_SYS}

    ${CID} =                Create Container    ${USER_WALLET}  basic_acl=eacl-public-read-write

    ${S_OID_USER} =         Put object    ${USER_WALLET}    ${FILE_S}    ${CID}           user_headers=${USER_HEADER}
                            Put object    ${WALLET}         ${FILE_S}    ${CID}           user_headers=${ANOTHER_HEADER}
                            Get object    ${WALLET}         ${CID}       ${S_OID_USER}    ${EMPTY}    local_file_eacl
                            Set eACL      ${USER_WALLET}    ${CID}       ${DENY_EACL}

                            # The current ACL cache lifetime is 30 sec
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                            Run Keyword And Expect Error    *
                            ...  Head object    ${WALLET}    ${CID}    ${S_OID_USER}

                            Get object        ${WALLET}    ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl
                            IF    "${WALLET}" == "${WALLET_SYS}"
                                Run Keyword And Expect Error    *
                                ...    Get Range    ${WALLET}    ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            ELSE
                                Get Range     ${WALLET}    ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            END
                            Get Range Hash    ${WALLET}    ${CID}    ${S_OID_USER}    ${EMPTY}       0:256


Check eACL Сompound Delete
    [Arguments]             ${WALLET}    ${DENY_EACL}    ${FILE_S}    ${USER_WALLET}    ${WALLET_SYS}

    ${CID} =                Create Container       ${USER_WALLET}   basic_acl=eacl-public-read-write

    ${S_OID_USER} =         Put object             ${USER_WALLET}    ${FILE_S}    ${CID}    user_headers=${USER_HEADER}
    ${D_OID_USER} =         Put object             ${USER_WALLET}    ${FILE_S}    ${CID}
                            Put object             ${WALLET}         ${FILE_S}    ${CID}    user_headers=${ANOTHER_HEADER}
                            IF    "${WALLET}" == "${WALLET_SYS}"
                                Run Keyword And Expect Error    *
                                ...    Delete object                   ${WALLET}    ${CID}       ${D_OID_USER}
                            ELSE
                                Delete object                   ${WALLET}    ${CID}       ${D_OID_USER}
                            END

                            Set eACL    ${USER_WALLET}    ${CID}       ${DENY_EACL}

                            # The current ACL cache lifetime is 30 sec
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                            Run Keyword And Expect Error    *
                            ...  Head object                ${WALLET}    ${CID}       ${S_OID_USER}
                            Run Keyword And Expect Error    *
                            ...  Put object        ${WALLET}    ${FILE_S}    ${CID}    user_headers=${ANOTHER_HEADER}
                            IF    "${WALLET}" == "${WALLET_SYS}"
                                Run Keyword And Expect Error    *
                                ...    Delete object                   ${WALLET}    ${CID}       ${S_OID_USER}
                            ELSE
                                Delete object                   ${WALLET}    ${CID}       ${S_OID_USER}
                            END


Check eACL Сompound Get Range Hash
    [Arguments]             ${WALLET}    ${DENY_EACL}    ${FILE_S}    ${USER_WALLET}    ${WALLET_SYS}

    ${CID} =                Create Container       ${USER_WALLET}   basic_acl=eacl-public-read-write

    ${S_OID_USER} =         Put object             ${USER_WALLET}          ${FILE_S}    ${CID}    user_headers=${USER_HEADER}
                            Put object             ${WALLET}               ${FILE_S}    ${CID}    user_headers=${ANOTHER_HEADER}
                            Get Range Hash         ${WALLET_SYS}      ${CID}       ${S_OID_USER}    ${EMPTY}    0:256

                            Set eACL               ${USER_WALLET}         ${CID}       ${DENY_EACL}

                            # The current ACL cache lifetime is 30 sec
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                            Run Keyword And Expect Error    *
                            ...  Get Range     ${WALLET}    ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Run Keyword And Expect Error    *
                            ...  Get object    ${WALLET}    ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl

                            Get Range Hash     ${WALLET}    ${CID}    ${S_OID_USER}    ${EMPTY}    0:256
