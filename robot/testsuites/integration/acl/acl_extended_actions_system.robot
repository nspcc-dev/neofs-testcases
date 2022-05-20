*** Settings ***
Variables    common.py

Library     acl.py
Library     container.py
Library     neofs_verbs.py
Library     utility_keywords.py

Resource    common_steps_acl_extended.robot
Resource    payment_operations.robot
Resource    setup_teardown.robot
Resource    eacl_tables.robot

*** Variables ***
&{USER_HEADER} =        key1=1      key2=abc
&{USER_HEADER_DEL} =    key1=del    key2=del
&{ANOTHER_USER_HEADER} =        key1=oth    key2=oth

*** Test cases ***
Extended ACL Operations
    [Documentation]         Testcase to validate NeoFS operations with extended ACL.
    [Tags]                  ACL  eACL
    [Timeout]               20 min

    [Setup]                 Setup

    ${WALLET}   ${_}     ${_} =   Prepare Wallet And Deposit

                            Log    Check extended ACL with simple object
    ${FILE_S}    ${_} =     Generate file    ${SIMPLE_OBJ_SIZE}
                            Check eACL Deny and Allow All System    ${WALLET}    ${FILE_S}

                            Log    Check extended ACL with complex object
    ${FILE_S}    ${_} =     Generate file    ${COMPLEX_OBJ_SIZE}
                            Check eACL Deny and Allow All System    ${WALLET}    ${FILE_S}

    [Teardown]              Teardown    acl_extended_actions_system


*** Keywords ***

Check eACL Deny and Allow All System
    [Arguments]     ${WALLET}      ${FILE_S}

    ${WALLET_SN}    ${_} =     Prepare Wallet with WIF And Deposit    ${NEOFS_SN_WIF}
    ${WALLET_IR}    ${_} =     Prepare Wallet with WIF And Deposit    ${NEOFS_IR_WIF}

    ${CID} =            Create Container    ${WALLET}   basic_acl=eacl-public-read-write

    ${S_OID_USER} =     Put object      ${WALLET}    ${FILE_S}    ${CID}    user_headers=${USER_HEADER}
    ${D_OID_USER_S} =   Put object      ${WALLET}    ${FILE_S}    ${CID}    user_headers=${USER_HEADER_DEL}
    ${D_OID_USER_SN} =  Put object      ${WALLET}    ${FILE_S}    ${CID}    user_headers=${USER_HEADER_DEL}

    @{S_OBJ_H} =	Create List     ${S_OID_USER}

                        Put object      ${WALLET_IR}    ${FILE_S}    ${CID}    user_headers=${ANOTHER_USER_HEADER}
                        Put object      ${WALLET_SN}    ${FILE_S}    ${CID}    user_headers=${ANOTHER_USER_HEADER}

                        Get object    ${WALLET_IR}    ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl
                        Get object    ${WALLET_SN}    ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl

                        Search object        ${WALLET_IR}    ${CID}    ${EMPTY}    ${EMPTY}    ${USER_HEADER}    ${S_OBJ_H}
                        Search object        ${WALLET_SN}    ${CID}    ${EMPTY}    ${EMPTY}    ${USER_HEADER}    ${S_OBJ_H}

                        Head object          ${WALLET_IR}    ${CID}    ${S_OID_USER}
                        Head object          ${WALLET_SN}    ${CID}    ${S_OID_USER}

                        Run Keyword And Expect Error    *
                        ...    Get Range            ${WALLET_IR}    ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                        Run Keyword And Expect Error    *
                        ...    Get Range            ${WALLET_SN}    ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256

                        Get Range Hash       ${WALLET_IR}    ${CID}    ${S_OID_USER}    ${EMPTY}    0:256
                        Get Range Hash       ${WALLET_SN}    ${CID}    ${S_OID_USER}    ${EMPTY}    0:256

                        Run Keyword And Expect Error    *
                        ...    Delete object        ${WALLET_IR}    ${CID}    ${D_OID_USER_S}
                        Run Keyword And Expect Error    *
                        ...    Delete object        ${WALLET_SN}    ${CID}    ${D_OID_USER_SN}

                        Set eACL             ${WALLET}     ${CID}       ${EACL_DENY_ALL_SYSTEM}

                        # The current ACL cache lifetime is 30 sec
                        Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                        Run Keyword And Expect Error    *
                        ...  Put object        ${WALLET_IR}    ${FILE_S}    ${CID}    user_headers=${ANOTHER_USER_HEADER}
                        Run Keyword And Expect Error    *
                        ...  Put object        ${WALLET_SN}    ${FILE_S}    ${CID}    user_headers=${ANOTHER_USER_HEADER}

                        Run Keyword And Expect Error    *
                        ...  Get object      ${WALLET_IR}      ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl
                        Run Keyword And Expect Error    *
                        ...  Get object      ${WALLET_SN}    ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl

                        Run Keyword And Expect Error    *
                        ...  Search object              ${WALLET_IR}    ${CID}    ${EMPTY}    ${EMPTY}    ${USER_HEADER}    ${S_OBJ_H}
                        Run Keyword And Expect Error    *
                        ...  Search object              ${WALLET_SN}    ${CID}    ${EMPTY}    ${EMPTY}    ${USER_HEADER}    ${S_OBJ_H}


                        Run Keyword And Expect Error        *
                        ...  Head object                ${WALLET_IR}    ${CID}    ${S_OID_USER}
                        Run Keyword And Expect Error        *
                        ...  Head object                ${WALLET_SN}    ${CID}    ${S_OID_USER}

                        Run Keyword And Expect Error        *
                        ...  Get Range                  ${WALLET_IR}    ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                        Run Keyword And Expect Error        *
                        ...  Get Range                  ${WALLET_SN}    ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256


                        Run Keyword And Expect Error        *
                        ...  Get Range Hash             ${WALLET_IR}    ${CID}    ${S_OID_USER}    ${EMPTY}    0:256
                        Run Keyword And Expect Error        *
                        ...  Get Range Hash             ${WALLET_SN}    ${CID}    ${S_OID_USER}    ${EMPTY}    0:256


                        Run Keyword And Expect Error        *
                        ...  Delete object              ${WALLET_IR}    ${CID}        ${S_OID_USER}
                        Run Keyword And Expect Error        *
                        ...  Delete object              ${WALLET_SN}    ${CID}        ${S_OID_USER}


                        Set eACL                        ${WALLET}     ${CID}        ${EACL_ALLOW_ALL_SYSTEM}

                        # The current ACL cache lifetime is 30 sec
                        Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                        Delete object        ${WALLET}    ${CID}    ${D_OID_USER_S}
                        Delete object        ${WALLET}    ${CID}    ${D_OID_USER_SN}

    ${D_OID_USER_S} =   Put object     ${WALLET}     ${FILE_S}    ${CID}    user_headers=${USER_HEADER_DEL}
    ${D_OID_USER_SN} =  Put object     ${WALLET}     ${FILE_S}    ${CID}    user_headers=${USER_HEADER_DEL}

                        Put object     ${WALLET_IR}    ${FILE_S}     ${CID}    user_headers=${ANOTHER_USER_HEADER}
                        Put object     ${WALLET_SN}    ${FILE_S}     ${CID}    user_headers=${ANOTHER_USER_HEADER}

                        Get object       ${WALLET_IR}    ${CID}        ${S_OID_USER}      ${EMPTY}    local_file_eacl
                        Get object       ${WALLET_SN}    ${CID}        ${S_OID_USER}      ${EMPTY}    local_file_eacl

                        Search object        ${WALLET_IR}    ${CID}    ${EMPTY}        ${EMPTY}     ${USER_HEADER}       ${S_OBJ_H}
                        Search object        ${WALLET_SN}    ${CID}    ${EMPTY}        ${EMPTY}     ${USER_HEADER}       ${S_OBJ_H}

                        Head object          ${WALLET_IR}    ${CID}    ${S_OID_USER}
                        Head object          ${WALLET_SN}    ${CID}    ${S_OID_USER}

                        Run Keyword And Expect Error        *
                        ...  Get Range            ${WALLET_IR}    ${CID}    ${S_OID_USER}    s_get_range      ${EMPTY}    0:256
                        Run Keyword And Expect Error        *
                        ...  Get Range            ${WALLET_SN}    ${CID}    ${S_OID_USER}    s_get_range      ${EMPTY}    0:256

                        Get Range Hash       ${WALLET_IR}    ${CID}    ${S_OID_USER}    ${EMPTY}    0:256
                        Get Range Hash       ${WALLET_SN}    ${CID}    ${S_OID_USER}    ${EMPTY}    0:256

                        Run Keyword And Expect Error        *
                        ...  Delete object        ${WALLET_IR}    ${CID}    ${D_OID_USER_S}
                        Run Keyword And Expect Error        *
                        ...  Delete object        ${WALLET_SN}    ${CID}    ${D_OID_USER_SN}
