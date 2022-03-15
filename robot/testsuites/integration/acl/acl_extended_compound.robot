*** Settings ***
Variables    common.py

Library     Collections
Library     neofs.py
Library     payment_neogo.py
Library     acl.py

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

    ${_}   ${_}     ${USER_KEY} =   Prepare Wallet And Deposit
    ${_}   ${_}     ${OTHER_KEY} =   Prepare Wallet And Deposit

                    Log    Check extended ACL with simple object
                    Generate files    ${SIMPLE_OBJ_SIZE}
                    Check Сompound Operations    ${USER_KEY}    ${OTHER_KEY}

                    Log    Check extended ACL with complex object
                    Generate files    ${COMPLEX_OBJ_SIZE}
                    Check Сompound Operations    ${USER_KEY}    ${OTHER_KEY}

    [Teardown]      Teardown    acl_extended_compound


*** Keywords ***



Check Сompound Operations
    [Arguments]             ${USER_KEY}    ${OTHER_KEY}
                            Check eACL Сompound Get    ${OTHER_KEY}     ${EACL_COMPOUND_GET_OTHERS}    ${USER_KEY}
                            Check eACL Сompound Get    ${USER_KEY}      ${EACL_COMPOUND_GET_USER}    ${USER_KEY}
                            Check eACL Сompound Get    ${SYSTEM_KEY}    ${EACL_COMPOUND_GET_SYSTEM}    ${USER_KEY}

                            Check eACL Сompound Delete    ${OTHER_KEY}     ${EACL_COMPOUND_DELETE_OTHERS}    ${USER_KEY}
                            Check eACL Сompound Delete    ${USER_KEY}      ${EACL_COMPOUND_DELETE_USER}    ${USER_KEY}
                            Check eACL Сompound Delete    ${SYSTEM_KEY}    ${EACL_COMPOUND_DELETE_SYSTEM}    ${USER_KEY}

                            Check eACL Сompound Get Range Hash    ${OTHER_KEY}     ${EACL_COMPOUND_GET_HASH_OTHERS}    ${USER_KEY}
                            Check eACL Сompound Get Range Hash    ${USER_KEY}      ${EACL_COMPOUND_GET_HASH_USER}    ${USER_KEY}
                            Check eACL Сompound Get Range Hash    ${SYSTEM_KEY}    ${EACL_COMPOUND_GET_HASH_SYSTEM}    ${USER_KEY}

Check eACL Сompound Get
    [Arguments]             ${KEY}    ${DENY_EACL}    ${USER_KEY}

    ${CID} =                Create Container Public    ${USER_KEY}

    ${S_OID_USER} =         Put object             ${USER_KEY}    ${FILE_S}    ${CID}    user_headers=${USER_HEADER}
                            Put object             ${KEY}         ${FILE_S}    ${CID}    user_headers=${ANOTHER_HEADER}
                            Get object           ${KEY}         ${CID}       ${S_OID_USER}    ${EMPTY}    local_file_eacl
                            Set eACL                        ${USER_KEY}    ${CID}       ${DENY_EACL}

                            # The current ACL cache lifetime is 30 sec
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                            Run Keyword And Expect Error    *
                            ...  Head object                ${KEY}    ${CID}    ${S_OID_USER}

                            Get object           ${KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}       local_file_eacl
                            IF    "${KEY}" == "${NEOFS_IR_WIF}"
                                Run Keyword And Expect Error    *
                                ...    Get Range                       ${KEY}    ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}           0:256
                            ELSE
                                Get Range                       ${KEY}    ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}           0:256
                            END
                            Get Range Hash                  ${KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}       0:256


Check eACL Сompound Delete
    [Arguments]             ${KEY}    ${DENY_EACL}    ${USER_KEY}

    ${CID} =                Create Container Public    ${USER_KEY}

    ${S_OID_USER} =         Put object             ${USER_KEY}    ${FILE_S}    ${CID}    user_headers=${USER_HEADER}
    ${D_OID_USER} =         Put object             ${USER_KEY}    ${FILE_S}    ${CID}
                            Put object             ${KEY}         ${FILE_S}    ${CID}    user_headers=${ANOTHER_HEADER}
                            IF    "${KEY}" == "${NEOFS_IR_WIF}"
                                Run Keyword And Expect Error    *
                                ...    Delete object                   ${KEY}    ${CID}       ${D_OID_USER}
                            ELSE
                                Delete object                   ${KEY}    ${CID}       ${D_OID_USER}
                            END

                            Set eACL                        ${USER_KEY}    ${CID}       ${DENY_EACL}

                            # The current ACL cache lifetime is 30 sec
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                            Run Keyword And Expect Error    *
                            ...  Head object                ${KEY}    ${CID}       ${S_OID_USER}
                            Run Keyword And Expect Error    *
                            ...  Put object        ${KEY}    ${FILE_S}    ${CID}    user_headers=${ANOTHER_HEADER}
                            IF    "${KEY}" == "${NEOFS_IR_WIF}"
                                Run Keyword And Expect Error    *
                                ...    Delete object                   ${KEY}    ${CID}       ${S_OID_USER}
                            ELSE
                                Delete object                   ${KEY}    ${CID}       ${S_OID_USER}
                            END



Check eACL Сompound Get Range Hash
    [Arguments]             ${KEY}    ${DENY_EACL}    ${USER_KEY}

    ${CID} =                Create Container Public    ${USER_KEY}

    ${S_OID_USER} =         Put object             ${USER_KEY}          ${FILE_S}    ${CID}    user_headers=${USER_HEADER}
                            Put object             ${KEY}               ${FILE_S}    ${CID}    user_headers=${ANOTHER_HEADER}
                            Get Range Hash         ${NEOFS_SN_WIF}      ${CID}       ${S_OID_USER}    ${EMPTY}    0:256

                            Set eACL               ${USER_KEY}         ${CID}       ${DENY_EACL}

                            # The current ACL cache lifetime is 30 sec
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                            Run Keyword And Expect Error    *
                            ...  Get Range                  ${KEY}    ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Run Keyword And Expect Error    *
                            ...  Get object      ${KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}       local_file_eacl

                            Get Range Hash                  ${KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}       0:256
