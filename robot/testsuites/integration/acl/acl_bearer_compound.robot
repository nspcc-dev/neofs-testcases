*** Settings ***
Variables   ../../../variables/common.py

Library     Collections
Library     acl.py
Library     neofs.py
Library     payment_neogo.py

Resource    ../../../variables/eacl_tables.robot
Resource    common_steps_acl_bearer.robot
Resource    ../${RESOURCES}/payment_operations.robot
Resource    ../${RESOURCES}/setup_teardown.robot

*** Variables ***
${SYSTEM_KEY} =     ${NEOFS_IR_WIF}

*** Test cases ***
BearerToken Operations for Сompound Operations
    [Documentation]         Testcase to validate NeoFS operations with BearerToken for Сompound Operations.
    [Tags]                  ACL  NeoFSCLI BearerToken
    [Timeout]               20 min

    [Setup]                 Setup
    
    ${WALLET}   ${ADDR}     ${USER_KEY} =   Prepare Wallet And Deposit 
    ${WALLET_OTH}   ${ADDR_OTH}     ${OTHER_KEY} =   Prepare Wallet And Deposit 
                            Prepare eACL Role rules

                            Log    Check Bearer token with simple object
    ${FILE_S} =             Generate file    ${SIMPLE_OBJ_SIZE}
                            Check Сompound Operations    ${USER_KEY}    ${OTHER_KEY}    ${FILE_S}

                            Log    Check Bearer token with complex object
    ${FILE_S} =             Generate file    ${COMPLEX_OBJ_SIZE}
                            Check Сompound Operations    ${USER_KEY}    ${OTHER_KEY}    ${FILE_S}

    [Teardown]              Teardown    acl_bearer_compound


*** Keywords ***

Check Сompound Operations
    [Arguments]    ${USER_KEY}    ${OTHER_KEY}    ${FILE_S}
                            Check Bearer Сompound Get    ${OTHER_KEY}     OTHERS    ${EACL_DENY_ALL_OTHERS}    ${FILE_S}    ${USER_KEY} 
                            Check Bearer Сompound Get    ${USER_KEY}      USER      ${EACL_DENY_ALL_USER}    ${FILE_S}     ${USER_KEY}
                            Check Bearer Сompound Get    ${SYSTEM_KEY}    SYSTEM    ${EACL_DENY_ALL_SYSTEM}    ${FILE_S}     ${USER_KEY}

                            Check Bearer Сompound Delete    ${OTHER_KEY}     OTHERS    ${EACL_DENY_ALL_OTHERS}    ${FILE_S}     ${USER_KEY}
                            Check Bearer Сompound Delete    ${USER_KEY}      USER      ${EACL_DENY_ALL_USER}    ${FILE_S}     ${USER_KEY}
                            Check Bearer Сompound Delete    ${SYSTEM_KEY}    SYSTEM    ${EACL_DENY_ALL_SYSTEM}    ${FILE_S}    ${USER_KEY} 

                            Check Bearer Сompound Get Range Hash    ${OTHER_KEY}     OTHERS    ${EACL_DENY_ALL_OTHERS}    ${FILE_S}    ${USER_KEY}    
                            Check Bearer Сompound Get Range Hash    ${USER_KEY}      USER      ${EACL_DENY_ALL_USER}    ${FILE_S}    ${USER_KEY}
                            Check Bearer Сompound Get Range Hash    ${SYSTEM_KEY}    SYSTEM    ${EACL_DENY_ALL_SYSTEM}    ${FILE_S}    ${USER_KEY}
Check Bearer Сompound Get
    [Arguments]             ${KEY}    ${DENY_GROUP}    ${DENY_EACL}    ${FILE_S}    ${USER_KEY}

    ${CID} =                Create Container Public    ${USER_KEY}
    ${S_OID_USER} =         Put object                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER}
    @{S_OBJ_H} =	        Create List	                        ${S_OID_USER}

    ${S_OID_USER} =     Put object     ${USER_KEY}    ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_USR_HEADER}
                        Put object     ${KEY}         ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_OTH_HEADER}
                        Get object     ${KEY}         ${CID}       ${S_OID_USER}    ${EMPTY}    local_file_eacl
                        Set eACL       ${USER_KEY}    ${CID}       ${DENY_EACL}

                        # The current ACL cache lifetime is 30 sec
                        Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

    ${rule1}=           Create Dictionary    Operation=GET             Access=ALLOW    Role=${DENY_GROUP}
    ${rule2}=           Create Dictionary    Operation=GETRANGE        Access=ALLOW    Role=${DENY_GROUP}
    ${rule3}=           Create Dictionary    Operation=GETRANGEHASH    Access=ALLOW    Role=${DENY_GROUP}
    ${eACL_gen}=        Create List    ${rule1}    ${rule2}    ${rule3}
    ${EACL_TOKEN} =     Form BearerToken File      ${USER_KEY}    ${CID}    ${eACL_gen}

                        Run Keyword And Expect Error    *
                        ...  Head object     ${KEY}    ${CID}    ${S_OID_USER}    ${EACL_TOKEN}

                        Get object           ${KEY}    ${CID}    ${S_OID_USER}    ${EACL_TOKEN}      local_file_eacl
                        Get Range            ${KEY}    ${CID}    ${S_OID_USER}    s_get_range        ${EACL_TOKEN}       0:256
                        Get Range Hash       ${KEY}    ${CID}    ${S_OID_USER}    ${EACL_TOKEN}      0:256


Check Bearer Сompound Delete
    [Arguments]             ${KEY}    ${DENY_GROUP}    ${DENY_EACL}    ${FILE_S}    ${USER_KEY}    

    ${CID} =                Create Container Public    ${USER_KEY}
    ${S_OID_USER} =     Put object         ${USER_KEY}    ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_USR_HEADER}
    ${D_OID_USER} =     Put object         ${USER_KEY}    ${FILE_S}    ${CID}           ${EMPTY}    ${EMPTY}
                        Put object         ${KEY}         ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_OTH_HEADER}
                        Delete object      ${KEY}         ${CID}       ${D_OID_USER}    ${EMPTY}

                        Set eACL           ${USER_KEY}    ${CID}       ${DENY_EACL}

                        # The current ACL cache lifetime is 30 sec
                        Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

    ${rule1} =          Create Dictionary    Operation=DELETE          Access=ALLOW    Role=${DENY_GROUP}
    ${rule2} =          Create Dictionary    Operation=PUT             Access=DENY     Role=${DENY_GROUP}
    ${rule3} =          Create Dictionary    Operation=HEAD            Access=DENY     Role=${DENY_GROUP}
    ${eACL_gen} =       Create List    ${rule1}    ${rule2}    ${rule3}
    ${EACL_TOKEN} =     Form BearerToken File           ${USER_KEY}    ${CID}    ${eACL_gen}

                        Run Keyword And Expect Error    *
                        ...  Head object   ${KEY}    ${CID}       ${S_OID_USER}    ${EACL_TOKEN}
                        Run Keyword And Expect Error    *
                        ...  Put object    ${KEY}    ${FILE_S}    ${CID}           ${EACL_TOKEN}    ${FILE_OTH_HEADER}

                        Delete object      ${KEY}    ${CID}       ${S_OID_USER}    ${EACL_TOKEN}



Check Bearer Сompound Get Range Hash
    [Arguments]             ${KEY}    ${DENY_GROUP}    ${DENY_EACL}    ${FILE_S}    ${USER_KEY}

    ${CID} =                Create Container Public    ${USER_KEY}

    ${S_OID_USER} =         Put object             ${USER_KEY}         ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_USR_HEADER}
                            Put object             ${KEY}              ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_OTH_HEADER}
                            Get Range Hash                  ${NEOFS_SN_WIF}    ${CID}       ${S_OID_USER}    ${EMPTY}    0:256
                        Set eACL           ${USER_KEY}         ${CID}       ${DENY_EACL}

                        # The current ACL cache lifetime is 30 sec
                        Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

    ${rule1} =          Create Dictionary    Operation=GETRANGEHASH    Access=ALLOW    Role=${DENY_GROUP}
    ${rule2} =          Create Dictionary    Operation=GETRANGE        Access=DENY     Role=${DENY_GROUP}
    ${rule3} =          Create Dictionary    Operation=GET             Access=DENY     Role=${DENY_GROUP}
    ${eACL_gen} =       Create List    ${rule1}    ${rule2}    ${rule3}
    ${EACL_TOKEN} =     Form BearerToken File      ${USER_KEY}    ${CID}    ${eACL_gen}

                        Run Keyword And Expect Error    *
                        ...  Get Range      ${KEY}    ${CID}    ${S_OID_USER}    s_get_range    ${EACL_TOKEN}    0:256
                        Run Keyword And Expect Error    *
                        ...  Get object     ${KEY}    ${CID}    ${S_OID_USER}    ${EACL_TOKEN}    local_file_eacl

                        Get Range Hash      ${KEY}    ${CID}    ${S_OID_USER}    ${EACL_TOKEN}    0:256
