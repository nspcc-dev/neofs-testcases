*** Settings ***
Variables    common.py

Library     acl.py
Library     container.py
Library     neofs_verbs.py
Library     utility_keywords.py

Resource    common_steps_acl_extended.robot
Resource    payment_operations.robot
Resource    eacl_tables.robot

*** Variables ***
&{USER_HEADER} =        key1=1      key2=abc
&{USER_HEADER_DEL} =    key1=del    key2=del
&{ANOTHER_USER_HEADER} =        key1=oth    key2=oth
${DEPOSIT} =            ${30}
${EACL_ERROR_MSG} =     code = 2048 message = access to object operation denied

*** Test cases ***
Extended ACL Operations
    [Documentation]         Testcase to validate NeoFS operations with extended ACL.
    [Tags]                  ACL  eACL
    [Timeout]               20 min


    ${WALLET}   ${_}     ${_} =   Prepare Wallet And Deposit

                            Log    Check extended ACL with simple object
    ${FILE_S}    ${_} =     Generate file    ${SIMPLE_OBJ_SIZE}
                            Check eACL Deny and Allow All System    ${WALLET}    ${FILE_S}

                            Log    Check extended ACL with complex object
    ${FILE_S}    ${_} =     Generate file    ${COMPLEX_OBJ_SIZE}
                            Check eACL Deny and Allow All System    ${WALLET}    ${FILE_S}



*** Keywords ***

Check eACL Deny and Allow All System
    [Arguments]     ${WALLET}      ${FILE_S}

                        Transfer Mainnet Gas    ${STORAGE_WALLET_PATH}  ${DEPOSIT + 1}
                        NeoFS Deposit           ${STORAGE_WALLET_PATH}  ${DEPOSIT}
                        Transfer Mainnet Gas    ${IR_WALLET_PATH}       ${DEPOSIT + 1}  wallet_password=${IR_WALLET_PASS}
                        NeoFS Deposit           ${IR_WALLET_PATH}       ${DEPOSIT}      wallet_password=${IR_WALLET_PASS}

    ${CID} =            Create Container    ${WALLET}   basic_acl=eacl-public-read-write

    ${S_OID_USER} =     Put object      ${WALLET}    ${FILE_S}    ${CID}    user_headers=${USER_HEADER}
    ${D_OID_USER_S} =   Put object      ${WALLET}    ${FILE_S}    ${CID}    user_headers=${USER_HEADER_DEL}
    ${D_OID_USER_SN} =  Put object      ${WALLET}    ${FILE_S}    ${CID}    user_headers=${USER_HEADER_DEL}

    @{S_OBJ_H} =	Create List     ${S_OID_USER}

    ${ERR} =            Run Keyword And Expect Error    *
                        ...    Put object      ${IR_WALLET_PATH}    ${FILE_S}    ${CID}    user_headers=${ANOTHER_USER_HEADER}     wallet_config=${IR_WALLET_CONFIG}
                        Should Contain          ${ERR}    ${EACL_ERROR_MSG}

                        Put object      ${STORAGE_WALLET_PATH}    ${FILE_S}    ${CID}    user_headers=${ANOTHER_USER_HEADER}

                        Get object    ${IR_WALLET_PATH}    ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl     wallet_config=${IR_WALLET_CONFIG}
                        Get object    ${STORAGE_WALLET_PATH}    ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl

                        Search object        ${IR_WALLET_PATH}    ${CID}    ${EMPTY}    ${EMPTY}    ${USER_HEADER}    ${S_OBJ_H}     wallet_config=${IR_WALLET_CONFIG}
                        Search object        ${STORAGE_WALLET_PATH}    ${CID}    ${EMPTY}    ${EMPTY}    ${USER_HEADER}    ${S_OBJ_H}

                        Head object          ${IR_WALLET_PATH}    ${CID}    ${S_OID_USER}     wallet_config=${IR_WALLET_CONFIG}
                        Head object          ${STORAGE_WALLET_PATH}    ${CID}    ${S_OID_USER}

                        Run Keyword And Expect Error    *
                        ...    Get Range            ${IR_WALLET_PATH}    ${CID}    ${S_OID_USER}    0:256     wallet_config=${IR_WALLET_CONFIG}
                        Run Keyword And Expect Error    *
                        ...    Get Range            ${STORAGE_WALLET_PATH}    ${CID}    ${S_OID_USER}    0:256

                        #Get Range Hash       ${IR_WALLET_PATH}    ${CID}    ${S_OID_USER}    ${EMPTY}    0:256     wallet_config=${IR_WALLET_CONFIG}
                        #Get Range Hash       ${STORAGE_WALLET_PATH}    ${CID}    ${S_OID_USER}    ${EMPTY}    0:256

                        Run Keyword And Expect Error    *
                        ...    Delete object        ${IR_WALLET_PATH}    ${CID}    ${D_OID_USER_S}     wallet_config=${IR_WALLET_CONFIG}
                        Run Keyword And Expect Error    *
                        ...    Delete object        ${STORAGE_WALLET_PATH}    ${CID}    ${D_OID_USER_SN}

                        Set eACL             ${WALLET}     ${CID}       ${EACL_DENY_ALL_SYSTEM}

                        # The current ACL cache lifetime is 30 sec
                        Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                        Run Keyword And Expect Error    *
                        ...  Put object        ${IR_WALLET_PATH}    ${FILE_S}    ${CID}    user_headers=${ANOTHER_USER_HEADER}     wallet_config=${IR_WALLET_CONFIG}
                        Run Keyword And Expect Error    *
                        ...  Put object        ${STORAGE_WALLET_PATH}    ${FILE_S}    ${CID}    user_headers=${ANOTHER_USER_HEADER}

                        Run Keyword And Expect Error    *
                        ...  Get object      ${IR_WALLET_PATH}      ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl     wallet_config=${IR_WALLET_CONFIG}
                        Run Keyword And Expect Error    *
                        ...  Get object      ${STORAGE_WALLET_PATH}    ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl

                        Run Keyword And Expect Error    *
                        ...  Search object              ${IR_WALLET_PATH}    ${CID}    ${EMPTY}    ${EMPTY}    ${USER_HEADER}    ${S_OBJ_H}     wallet_config=${IR_WALLET_CONFIG}
                        Run Keyword And Expect Error    *
                        ...  Search object              ${STORAGE_WALLET_PATH}    ${CID}    ${EMPTY}    ${EMPTY}    ${USER_HEADER}    ${S_OBJ_H}


                        Run Keyword And Expect Error        *
                        ...  Head object                ${IR_WALLET_PATH}    ${CID}    ${S_OID_USER}     wallet_config=${IR_WALLET_CONFIG}
                        Run Keyword And Expect Error        *
                        ...  Head object                ${STORAGE_WALLET_PATH}    ${CID}    ${S_OID_USER}

                        Run Keyword And Expect Error        *
                        ...  Get Range                  ${IR_WALLET_PATH}    ${CID}    ${S_OID_USER}    0:256     wallet_config=${IR_WALLET_CONFIG}
                        Run Keyword And Expect Error        *
                        ...  Get Range                  ${STORAGE_WALLET_PATH}    ${CID}    ${S_OID_USER}    0:256


                        Run Keyword And Expect Error        *
                        ...  Get Range Hash             ${IR_WALLET_PATH}    ${CID}    ${S_OID_USER}    ${EMPTY}    0:256     wallet_config=${IR_WALLET_CONFIG}
                        Run Keyword And Expect Error        *
                        ...  Get Range Hash             ${STORAGE_WALLET_PATH}    ${CID}    ${S_OID_USER}    ${EMPTY}    0:256


                        Run Keyword And Expect Error        *
                        ...  Delete object              ${IR_WALLET_PATH}    ${CID}        ${S_OID_USER}     wallet_config=${IR_WALLET_CONFIG}
                        Run Keyword And Expect Error        *
                        ...  Delete object              ${STORAGE_WALLET_PATH}    ${CID}        ${S_OID_USER}


                        Set eACL                        ${WALLET}     ${CID}        ${EACL_ALLOW_ALL_SYSTEM}

                        # The current ACL cache lifetime is 30 sec
                        Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                        Delete object        ${WALLET}    ${CID}    ${D_OID_USER_S}
                        Delete object        ${WALLET}    ${CID}    ${D_OID_USER_SN}

    ${D_OID_USER_S} =   Put object     ${WALLET}     ${FILE_S}    ${CID}    user_headers=${USER_HEADER_DEL}
    ${D_OID_USER_SN} =  Put object     ${WALLET}     ${FILE_S}    ${CID}    user_headers=${USER_HEADER_DEL}
                        Put object     ${STORAGE_WALLET_PATH}    ${FILE_S}     ${CID}    user_headers=${ANOTHER_USER_HEADER}
    ${ERR} =            Run Keyword And Expect Error        *
                        ...    Put object     ${IR_WALLET_PATH}    ${FILE_S}     ${CID}    user_headers=${ANOTHER_USER_HEADER}     wallet_config=${IR_WALLET_CONFIG}
                        Should Contain          ${ERR}    ${EACL_ERROR_MSG}

                        Get object       ${IR_WALLET_PATH}    ${CID}        ${S_OID_USER}      ${EMPTY}    local_file_eacl     wallet_config=${IR_WALLET_CONFIG}
                        Get object       ${STORAGE_WALLET_PATH}    ${CID}        ${S_OID_USER}      ${EMPTY}    local_file_eacl

                        Search object        ${IR_WALLET_PATH}    ${CID}    ${EMPTY}        ${EMPTY}     ${USER_HEADER}       ${S_OBJ_H}     wallet_config=${IR_WALLET_CONFIG}
                        Search object        ${STORAGE_WALLET_PATH}    ${CID}    ${EMPTY}        ${EMPTY}     ${USER_HEADER}       ${S_OBJ_H}

                        Head object          ${IR_WALLET_PATH}    ${CID}    ${S_OID_USER}     wallet_config=${IR_WALLET_CONFIG}
                        Head object          ${STORAGE_WALLET_PATH}    ${CID}    ${S_OID_USER}

                        Run Keyword And Expect Error        *
                        ...  Get Range            ${IR_WALLET_PATH}    ${CID}    ${S_OID_USER}    0:256     wallet_config=${IR_WALLET_CONFIG}
                        Run Keyword And Expect Error        *
                        ...  Get Range            ${STORAGE_WALLET_PATH}    ${CID}    ${S_OID_USER}    0:256

                        #Get Range Hash       ${IR_WALLET_PATH}    ${CID}    ${S_OID_USER}    ${EMPTY}    0:256     wallet_config=${IR_WALLET_CONFIG}
                        #Get Range Hash       ${STORAGE_WALLET_PATH}    ${CID}    ${S_OID_USER}    ${EMPTY}    0:256

                        Run Keyword And Expect Error        *
                        ...  Delete object        ${IR_WALLET_PATH}    ${CID}    ${D_OID_USER_S}     wallet_config=${IR_WALLET_CONFIG}
                        Run Keyword And Expect Error        *
                        ...  Delete object        ${STORAGE_WALLET_PATH}    ${CID}    ${D_OID_USER_SN}
