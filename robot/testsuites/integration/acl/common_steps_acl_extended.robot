*** Settings ***
Variables   ../../../variables/common.py

*** Variables ***
${FILE_USR_HEADER} =        key1=1,key2=abc
${FILE_USR_HEADER_DEL} =    key1=del,key2=del
${FILE_OTH_HEADER} =        key1=oth,key2=oth
${RULE_FOR_ALL} =           REP 2 IN X CBF 1 SELECT 4 FROM * AS X


*** Keywords ***

Generate eACL Keys
    ${EACL_KEY_GEN} =	    Form WIF from String    782676b81a35c5f07325ec523e8521ee4946b6e5d4c6cd652dd0c3ba51ce03de
                            Set Global Variable     ${EACL_KEY}         ${EACL_KEY_GEN}
                            Set Global Variable     ${SYSTEM_KEY}       ${NEOFS_IR_WIF}

Create Container Public
                            Log	                Create Public Container
    ${PUBLIC_CID_GEN} =     Create container    ${USER_KEY}    0x4FFFFFFF    ${RULE_FOR_ALL}
    [Return]                ${PUBLIC_CID_GEN}


Generate files
    [Arguments]             ${SIZE}
    ${FILE_S_GEN_1} =       Generate file of bytes    ${SIZE}
    ${FILE_S_GEN_2} =       Generate file of bytes    ${SIZE}
                            Set Global Variable       ${FILE_S}      ${FILE_S_GEN_1}
                            Set Global Variable       ${FILE_S_2}    ${FILE_S_GEN_2}



Check eACL Deny and Allow All
    [Arguments]     ${KEY}       ${DENY_EACL}    ${ALLOW_EACL}

    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object                 ${USER_KEY}     ${FILE_S}            ${CID}            ${EMPTY}            ${FILE_USR_HEADER}
    ${D_OID_USER} =         Put object                 ${USER_KEY}     ${FILE_S}            ${CID}            ${EMPTY}            ${FILE_USR_HEADER_DEL}
    @{S_OBJ_H} =	        Create List	               ${S_OID_USER}

                            Put object                 ${KEY}    ${FILE_S}            ${CID}            ${EMPTY}            ${FILE_OTH_HEADER}

                            Get object                 ${KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl
                            Search object              ${KEY}    ${CID}        ${EMPTY}                 ${EMPTY}            ${FILE_USR_HEADER}    ${S_OBJ_H}
                            Head object                ${KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}

                            Get Range                  ${KEY}    ${CID}        ${S_OID_USER}            s_get_range       ${EMPTY}            0:256
                            Get Range Hash             ${KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}          0:256
                            Delete object              ${KEY}    ${CID}        ${D_OID_USER}            ${EMPTY}

                            Set eACL                   ${USER_KEY}     ${CID}        ${DENY_EACL}

                            # The current ACL cache lifetime is 30 sec
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                            Run Keyword And Expect Error        *
                            ...  Put object                          ${KEY}    ${FILE_S}    ${CID}           ${EMPTY}            ${FILE_USR_HEADER}
                            Run Keyword And Expect Error        *
                            ...  Get object                          ${KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}            local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Search object                       ${KEY}    ${CID}       ${EMPTY}         ${EMPTY}            ${FILE_USR_HEADER}       ${S_OBJ_H}
                            Run Keyword And Expect Error        *
                            ...  Head object                         ${KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${KEY}    ${CID}       ${S_OID_USER}    s_get_range         ${EMPTY}            0:256
                            Run Keyword And Expect Error        *
                            ...  Get Range Hash                      ${KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}            0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}

                            Set eACL                            ${USER_KEY}    ${CID}       ${ALLOW_EACL}

                            # The current ACL cache lifetime is 30 sec
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                            Put object                 ${KEY}    ${FILE_S}     ${CID}              ${EMPTY}            ${FILE_OTH_HEADER}
                            Get object               ${KEY}    ${CID}        ${S_OID_USER}       ${EMPTY}            local_file_eacl
                            Search object                       ${KEY}    ${CID}        ${EMPTY}            ${EMPTY}            ${FILE_USR_HEADER}     ${S_OBJ_H}
                            Head object                         ${KEY}    ${CID}        ${S_OID_USER}       ${EMPTY}
                            Get Range                           ${KEY}    ${CID}        ${S_OID_USER}       s_get_range          ${EMPTY}            0:256
                            Get Range Hash                      ${KEY}    ${CID}        ${S_OID_USER}       ${EMPTY}             0:256
                            Delete object                       ${KEY}    ${CID}        ${S_OID_USER}       ${EMPTY}

