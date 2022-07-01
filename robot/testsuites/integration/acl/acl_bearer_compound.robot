*** Settings ***
Variables   common.py

Library     acl.py
Library     container.py
Library     neofs_verbs.py
Library     utility_keywords.py

Resource    eacl_tables.robot
Resource    common_steps_acl_bearer.robot
Resource    payment_operations.robot
Resource    setup_teardown.robot

*** Variables ***
${DEPOSIT} =            ${30}
&{USER_HEADER} =        key1=1      key2=abc
&{ANOTHER_HEADER} =     key1=oth    key2=oth

*** Test cases ***
BearerToken Operations for Сompound Operations
    [Documentation]         Testcase to validate NeoFS operations with BearerToken for Сompound Operations.
    [Tags]                  ACL   BearerToken
    [Timeout]               20 min

    [Setup]                 Setup

    ${WALLET}   ${_}     ${_} =   Prepare Wallet And Deposit
    ${WALLET_OTH}   ${_}     ${_} =   Prepare Wallet And Deposit

                            Log    Check Bearer token with simple object
    ${FILE_S}    ${_} =     Generate file    ${SIMPLE_OBJ_SIZE}
                            Check Сompound Operations    ${WALLET}    ${WALLET_OTH}    ${FILE_S}

                            Log    Check Bearer token with complex object
    ${FILE_S}    ${_} =     Generate file    ${COMPLEX_OBJ_SIZE}
                            Check Сompound Operations    ${WALLET}    ${WALLET_OTH}    ${FILE_S}

    [Teardown]              Teardown    acl_bearer_compound


*** Keywords ***

Check Сompound Operations
    [Arguments]         ${USER_WALLET}    ${OTHER_WALLET}    ${FILE_S}

                        Transfer Mainnet Gas    ${IR_WALLET_PATH}       ${DEPOSIT + 1}  wallet_password=${IR_WALLET_PASS}
                        NeoFS Deposit           ${IR_WALLET_PATH}       ${DEPOSIT}      wallet_password=${IR_WALLET_PASS}

                        Check Bearer Сompound Get    ${OTHER_WALLET}     OTHERS    ${EACL_DENY_ALL_OTHERS}    ${FILE_S}    ${USER_WALLET}
                        Check Bearer Сompound Get    ${USER_WALLET}      USER      ${EACL_DENY_ALL_USER}    ${FILE_S}     ${USER_WALLET}
                        #Check Bearer Сompound Get    ${IR_WALLET_PATH}    SYSTEM    ${EACL_DENY_ALL_SYSTEM}    ${FILE_S}     ${USER_WALLET}

                        Check Bearer Сompound Delete    ${OTHER_WALLET}     OTHERS    ${EACL_DENY_ALL_OTHERS}    ${FILE_S}     ${USER_WALLET}
                        Check Bearer Сompound Delete    ${USER_WALLET}      USER      ${EACL_DENY_ALL_USER}    ${FILE_S}     ${USER_WALLET}
                        #Check Bearer Сompound Delete    ${IR_WALLET_PATH}    SYSTEM    ${EACL_DENY_ALL_SYSTEM}    ${FILE_S}    ${USER_WALLET}

                        Check Bearer Сompound Get Range Hash    ${OTHER_WALLET}     OTHERS    ${EACL_DENY_ALL_OTHERS}    ${USER_WALLET}    ${FILE_S}
                        Check Bearer Сompound Get Range Hash    ${USER_WALLET}      USER      ${EACL_DENY_ALL_USER}    ${USER_WALLET}    ${FILE_S}
                        #Check Bearer Сompound Get Range Hash    ${IR_WALLET_PATH}    SYSTEM    ${EACL_DENY_ALL_SYSTEM}    ${USER_WALLET}    ${FILE_S}
Check Bearer Сompound Get
    [Arguments]         ${WALLET}    ${DENY_GROUP}    ${DENY_EACL}    ${FILE_S}    ${USER_WALLET}

    ${CID} =            Create Container           ${USER_WALLET}   basic_acl=eacl-public-read-write
                        Prepare eACL Role rules    ${CID}
    ${S_OID_USER} =     Put object                 ${USER_WALLET}     ${FILE_S}   ${CID}  user_headers=${USER_HEADER}
    @{S_OBJ_H} =        Create List	           ${S_OID_USER}

    ${S_OID_USER} =     Put object     ${USER_WALLET}     ${FILE_S}    ${CID}           user_headers=${USER_HEADER}
                        Put object     ${WALLET}    ${FILE_S}    ${CID}           user_headers=${ANOTHER_HEADER}
                        Get object     ${WALLET}    ${CID}       ${S_OID_USER}    ${EMPTY}    local_file_eacl
                        Set eACL       ${USER_WALLET}     ${CID}       ${DENY_EACL}

                        # The current ACL cache lifetime is 30 sec
                        Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

    ${rule1}=           Create Dictionary    Operation=GET             Access=ALLOW    Role=${DENY_GROUP}
    ${rule2}=           Create Dictionary    Operation=GETRANGE        Access=ALLOW    Role=${DENY_GROUP}
    ${rule3}=           Create Dictionary    Operation=GETRANGEHASH    Access=ALLOW    Role=${DENY_GROUP}
    ${eACL_gen}=        Create List    ${rule1}    ${rule2}    ${rule3}
    ${EACL_TOKEN} =     Form BearerToken File      ${USER_WALLET}    ${CID}    ${eACL_gen}

                        Run Keyword And Expect Error    *
                        ...  Head object     ${WALLET}    ${CID}    ${S_OID_USER}    bearer_token=${EACL_TOKEN}

                        Get Object    ${WALLET}    ${CID}    ${S_OID_USER}    ${EACL_TOKEN}      local_file_eacl
                        IF    "${WALLET}" == "${IR_WALLET_PATH}"
                            Run Keyword And Expect Error    *
                            ...    Get Range    ${WALLET}    ${CID}    ${S_OID_USER}    s_get_range        ${EACL_TOKEN}       0:256
                        ELSE
                            Get Range    ${WALLET}    ${CID}    ${S_OID_USER}    s_get_range        ${EACL_TOKEN}       0:256
                        END
                        Get Range Hash    ${WALLET}    ${CID}    ${S_OID_USER}    ${EACL_TOKEN}      0:256


Check Bearer Сompound Delete
    [Arguments]         ${WALLET}    ${DENY_GROUP}    ${DENY_EACL}    ${FILE_S}    ${USER_WALLET}

    ${CID} =            Create Container           ${USER_WALLET}   basic_acl=eacl-public-read-write
                        Prepare eACL Role rules    ${CID}
    ${S_OID_USER} =     Put object         ${USER_WALLET}    ${FILE_S}    ${CID}    user_headers=${USER_HEADER}
    ${D_OID_USER} =     Put object         ${USER_WALLET}    ${FILE_S}    ${CID}
                        Put object         ${WALLET}    ${FILE_S}    ${CID}    user_headers=${ANOTHER_HEADER}
                        IF    "${WALLET}" == "${IR_WALLET_PATH}"
                            Run Keyword And Expect Error    *
                            ...    Delete object    ${WALLET}    ${CID}       ${D_OID_USER}    ${EMPTY}
                        ELSE
                            Delete object    ${WALLET}    ${CID}       ${D_OID_USER}    ${EMPTY}
                        END

                        Set eACL    ${USER_WALLET}    ${CID}       ${DENY_EACL}

                        # The current ACL cache lifetime is 30 sec
                        Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

    ${rule1} =          Create Dictionary    Operation=DELETE          Access=ALLOW    Role=${DENY_GROUP}
    ${rule2} =          Create Dictionary    Operation=PUT             Access=DENY     Role=${DENY_GROUP}
    ${rule3} =          Create Dictionary    Operation=HEAD            Access=DENY     Role=${DENY_GROUP}
    ${eACL_gen} =       Create List    ${rule1}    ${rule2}    ${rule3}
    ${EACL_TOKEN} =     Form BearerToken File           ${USER_WALLET}    ${CID}    ${eACL_gen}

                        Run Keyword And Expect Error    *
                        ...  Head object   ${WALLET}    ${CID}       ${S_OID_USER}    bearer_token=${EACL_TOKEN}
                        Run Keyword And Expect Error    *
                        ...  Put object    ${WALLET}    ${FILE_S}    ${CID}   bearer=${EACL_TOKEN}    user_headers=${ANOTHER_HEADER}

                        Delete object      ${USER_WALLET}    ${CID}       ${S_OID_USER}    bearer=${EACL_TOKEN}



Check Bearer Сompound Get Range Hash
    [Arguments]         ${WALLET}    ${DENY_GROUP}    ${DENY_EACL}    ${USER_WALLET}    ${FILE_S}

    ${CID} =            Create Container           ${USER_WALLET}   basic_acl=eacl-public-read-write
                        Prepare eACL Role rules    ${CID}

    ${S_OID_USER} =     Put object              ${USER_WALLET}      ${FILE_S}    ${CID}    user_headers=${USER_HEADER}
                        Put object              ${WALLET}           ${FILE_S}    ${CID}    user_headers=${ANOTHER_HEADER}
                        Get Range hash          ${IR_WALLET_PATH}       ${CID}       ${S_OID_USER}    ${EMPTY}    0:256
                        Set eACL                ${USER_WALLET}      ${CID}       ${DENY_EACL}

                        # The current ACL cache lifetime is 30 sec
                        Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

    ${rule1} =          Create Dictionary    Operation=GETRANGEHASH    Access=ALLOW    Role=${DENY_GROUP}
    ${rule2} =          Create Dictionary    Operation=GETRANGE        Access=DENY     Role=${DENY_GROUP}
    ${rule3} =          Create Dictionary    Operation=GET             Access=DENY     Role=${DENY_GROUP}
    ${eACL_gen} =       Create List    ${rule1}    ${rule2}    ${rule3}
    ${EACL_TOKEN} =     Form BearerToken File      ${USER_WALLET}    ${CID}    ${eACL_gen}

                        Run Keyword And Expect Error    *
                        ...  Get Range      ${WALLET}    ${CID}    ${S_OID_USER}    s_get_range    ${EACL_TOKEN}    0:256
                        Run Keyword And Expect Error    *
                        ...  Get object     ${WALLET}    ${CID}    ${S_OID_USER}    ${EACL_TOKEN}    local_file_eacl

                        Get Range Hash      ${WALLET}    ${CID}    ${S_OID_USER}    ${EACL_TOKEN}    0:256
