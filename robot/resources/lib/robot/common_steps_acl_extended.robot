*** Settings ***
Variables   common.py
Variables   eacl_object_filters.py

Library     acl.py
Library     neofs.py
Library     Collections

Resource    common_steps_acl_basic.robot
Resource    payment_operations.robot

*** Variables ***
${FILE_USR_HEADER} =        key1=1,key2=abc
${FILE_USR_HEADER_DEL} =    key1=del,key2=del
${FILE_OTH_HEADER} =        key1=oth,key2=oth
${OBJECT_PATH} =   testfile
${EACL_ERR_MSG} =    *

*** Keywords ***

Create Container Public
    [Arguments]             ${USER_KEY}
                            Log	                Create Public Container
    ${PUBLIC_CID_GEN} =     Create container    ${USER_KEY}    0x4FFFFFFF    ${COMMON_PLACEMENT_RULE}
                            Wait Until Keyword Succeeds        ${MORPH_BLOCK_TIME}       ${CONTAINER_WAIT_INTERVAL}
                            ...     Container Existing     ${USER_KEY}       ${PUBLIC_CID_GEN}
    [Return]                ${PUBLIC_CID_GEN}


Generate files
    [Arguments]             ${SIZE}

    ${FILE_S_GEN_1} =       Generate file of bytes    ${SIZE}
    ${FILE_S_GEN_2} =       Generate file of bytes    ${SIZE}
                            Set Global Variable       ${FILE_S}      ${FILE_S_GEN_1}
                            Set Global Variable       ${FILE_S_2}    ${FILE_S_GEN_2}



Check eACL Deny and Allow All
    [Arguments]     ${KEY}    ${DENY_EACL}    ${ALLOW_EACL}    ${USER_KEY}

    ${CID} =                Create Container Public    ${USER_KEY}
    ${S_OID_USER} =         Put object                 ${USER_KEY}     ${FILE_S}            ${CID}            ${EMPTY}            ${FILE_USR_HEADER}
    ${D_OID_USER} =         Put object                 ${USER_KEY}     ${FILE_S}            ${CID}            ${EMPTY}            ${FILE_USR_HEADER_DEL}
    @{S_OBJ_H} =	    Create List	               ${S_OID_USER}

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

Compose eACL Custom
    [Arguments]    ${HEADER_DICT}    ${MATCH_TYPE}    ${FILTER}    ${ACCESS}    ${ROLE}

    ${filter_value} =    Get From dictionary    ${HEADER_DICT}    ${EACL_OBJ_FILTERS}[${FILTER}]

    ${filters} =        Create Dictionary    headerType=OBJECT    matchType=${MATCH_TYPE}    key=${FILTER}    value=${filter_value}
    ${rule_get}=        Create Dictionary    Operation=GET             Access=${ACCESS}    Role=${ROLE}    Filters=${filters}
    ${rule_head}=       Create Dictionary    Operation=HEAD            Access=${ACCESS}    Role=${ROLE}    Filters=${filters}
    ${rule_put}=        Create Dictionary    Operation=PUT             Access=${ACCESS}    Role=${ROLE}    Filters=${filters}
    ${rule_del}=        Create Dictionary    Operation=DELETE          Access=${ACCESS}    Role=${ROLE}    Filters=${filters}
    ${rule_search}=     Create Dictionary    Operation=SEARCH          Access=${ACCESS}    Role=${ROLE}    Filters=${filters}
    ${rule_range}=      Create Dictionary    Operation=GETRANGE        Access=${ACCESS}    Role=${ROLE}    Filters=${filters}
    ${rule_rangehash}=    Create Dictionary    Operation=GETRANGEHASH    Access=${ACCESS}    Role=${ROLE}    Filters=${filters}

    ${eACL_gen}=        Create List    ${rule_get}    ${rule_head}    ${rule_put}    ${rule_del}    
    ...  ${rule_search}    ${rule_range}    ${rule_rangehash}
    ${EACL_CUSTOM} =    Form eACL JSON Common File    ${eACL_gen}

    [Return]    ${EACL_CUSTOM}

Object Header Decoded
    [Arguments]    ${USER_KEY}    ${CID}    ${S_OID_USER}

    ${HEADER} =         Head Object    ${USER_KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}    json_output=True
    &{HEADER_DICT} =    Decode Object System Header Json    ${HEADER}

    [Return]    &{HEADER_DICT}

Check eACL Filters with MatchType String Equal
    [Arguments]    ${FILTER}

    ${_}   ${_}    ${USER_KEY} =    Prepare Wallet And Deposit  
    ${_}   ${_}    ${OTHER_KEY} =    Prepare Wallet And Deposit

    ${CID} =            Create Container Public    ${USER_KEY} 
    ${FILE_S}    ${_} =    Generate file    ${SIMPLE_OBJ_SIZE}

    ${S_OID_USER} =     Put Object    ${USER_KEY}    ${FILE_S}    ${CID}    ${EMPTY}    ${FILE_USR_HEADER}
    ${D_OID_USER} =     Put object    ${USER_KEY}    ${FILE_S}    ${CID}    ${EMPTY}
    @{S_OBJ_H} =	    Create List    ${S_OID_USER}

                        Get Object    ${OTHER_KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl
                        Search Object    ${OTHER_KEY}    ${CID}    ${EMPTY}    ${EMPTY}    ${FILE_USR_HEADER}    ${S_OBJ_H}
                        Head Object    ${OTHER_KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}
                        Get Range    ${OTHER_KEY}    ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                        Get Range Hash    ${OTHER_KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}    0:256
                        Delete Object    ${OTHER_KEY}    ${CID}    ${D_OID_USER}    ${EMPTY}

    &{HEADER_DICT} =    Object Header Decoded    ${USER_KEY}    ${CID}    ${S_OID_USER}
    ${EACL_CUSTOM} =    Compose eACL Custom    ${HEADER_DICT}    STRING_EQUAL    ${FILTER}    DENY    OTHERS
                        Set eACL    ${USER_KEY}    ${CID}    ${EACL_CUSTOM}

    IF    'GET' in ${VERB_FILTER_DEP}[${FILTER}]  
        Run Keyword And Expect Error   ${EACL_ERR_MSG}    
        ...  Get object    ${OTHER_KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}    ${OBJECT_PATH}
    END
    IF    'HEAD' in ${VERB_FILTER_DEP}[${FILTER}]
        Run Keyword And Expect error    ${EACL_ERR_MSG}
        ...  Head object    ${OTHER_KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}
    END
    IF    'RANGE' in ${VERB_FILTER_DEP}[${FILTER}]
        Run Keyword And Expect error    ${EACL_ERR_MSG}
        ...  Get Range    ${OTHER_KEY}    ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256   
    END
    IF    'SEARCH' in ${VERB_FILTER_DEP}[${FILTER}]  
        Run Keyword And Expect Error   ${EACL_ERR_MSG}    
        ...  Search Object    ${OTHER_KEY}    ${CID}    ${EMPTY}    ${EMPTY}    ${FILE_USR_HEADER}    ${S_OBJ_H}
    END
    IF    'RANGEHASH' in ${VERB_FILTER_DEP}[${FILTER}]
        Run Keyword And Expect error    ${EACL_ERR_MSG}
        ...  Get Range Hash    ${OTHER_KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}    0:256
    END
    IF    'DELETE' in ${VERB_FILTER_DEP}[${FILTER}]
        Run Keyword And Expect error    ${EACL_ERR_MSG}
        ...  Delete Object    ${OTHER_KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}   
    END
    
Check eACL Filters with MatchType String Not Equal
    [Arguments]    ${FILTER}

    ${_}   ${_}    ${USER_KEY} =    Prepare Wallet And Deposit  
    ${_}   ${_}    ${OTHER_KEY} =    Prepare Wallet And Deposit

    ${CID} =            Create Container Public    ${USER_KEY}
    ${FILE_S}    ${_} =    Generate file    ${SIMPLE_OBJ_SIZE}

    ${S_OID_OTH} =      Put Object    ${OTHER_KEY}    ${FILE_S}    ${CID}    ${EMPTY}    ${FILE_OTH_HEADER}
    ${S_OID_USER} =     Put Object    ${USER_KEY}    ${FILE_S}    ${CID}    ${EMPTY}    ${FILE_USR_HEADER}
    ${D_OID_USER} =     Put object    ${USER_KEY}    ${FILE_S}    ${CID}    ${EMPTY}
    @{S_OBJ_H} =	    Create List    ${S_OID_USER}

                        Get Object    ${USER_KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl
                        Head Object    ${USER_KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}
                        Search Object    ${USER_KEY}    ${CID}    ${EMPTY}    ${EMPTY}    ${FILE_USR_HEADER}    ${S_OBJ_H}
                        Get Range    ${USER_KEY}    ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                        Get Range Hash    ${USER_KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}    0:256

    &{HEADER_DICT} =    Object Header Decoded    ${USER_KEY}    ${CID}    ${S_OID_USER}
    ${EACL_CUSTOM} =    Compose eACL Custom    ${HEADER_DICT}    STRING_NOT_EQUAL    ${FILTER}    DENY    OTHERS
                        Set eACL    ${USER_KEY}    ${CID}    ${EACL_CUSTOM}

    IF    'GET' in ${VERB_FILTER_DEP}[${FILTER}]  
        Run Keyword And Expect Error   ${EACL_ERR_MSG}    
        ...  Get object    ${OTHER_KEY}    ${CID}    ${S_OID_OTH}    ${EMPTY}    ${OBJECT_PATH}
        Get object    ${OTHER_KEY}    ${CID}    ${S_OID_USER}     ${EMPTY}    ${OBJECT_PATH}
    END
    IF    'HEAD' in ${VERB_FILTER_DEP}[${FILTER}]
        Run Keyword And Expect error    ${EACL_ERR_MSG}
        ...  Head object    ${OTHER_KEY}    ${CID}    ${S_OID_OTH}    ${EMPTY}
        Head object    ${OTHER_KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}
    END
    IF    'SEARCH' in ${VERB_FILTER_DEP}[${FILTER}]
        Run Keyword And Expect error    ${EACL_ERR_MSG}
        ...  Search object    ${OTHER_KEY}    ${CID}    ${EMPTY}    ${EMPTY}    ${FILE_OTH_HEADER}    ${S_OBJ_H}
        Search object    ${OTHER_KEY}    ${CID}    ${EMPTY}    ${EMPTY}    ${FILE_USR_HEADER}    ${S_OBJ_H}
    END
    IF    'RANGE' in ${VERB_FILTER_DEP}[${FILTER}]
        Run Keyword And Expect error    ${EACL_ERR_MSG}
        ...  Get Range    ${OTHER_KEY}    ${CID}    ${S_OID_OTH}    s_get_range    ${EMPTY}    0:256
        Get Range    ${OTHER_KEY}    ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256  
    END
    IF    'RANGEHASH' in ${VERB_FILTER_DEP}[${FILTER}]
        Run Keyword And Expect error    ${EACL_ERR_MSG}
        ...  Get Range Hash    ${OTHER_KEY}    ${CID}    ${S_OID_OTH}    ${EMPTY}    0:256
        Get Range Hash    ${OTHER_KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}    0:256
    END
    IF    'DELETE' in ${VERB_FILTER_DEP}[${FILTER}]
        Run Keyword And Expect error    ${EACL_ERR_MSG}
        ...  Delete Object    ${OTHER_KEY}    ${CID}    ${S_OID_OTH}    ${EMPTY}
        Delete Object    ${OTHER_KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}   
    END
