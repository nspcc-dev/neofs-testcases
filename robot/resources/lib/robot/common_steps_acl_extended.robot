*** Settings ***
Variables   common.py
Variables   eacl_object_filters.py

Library     acl.py
Library     container.py
Library     neofs.py
Library     neofs_verbs.py

Library     Collections

Resource    common_steps_acl_basic.robot
Resource    payment_operations.robot

*** Variables ***
&{USER_HEADER} =        key1=1          key2=abc
&{USER_HEADER_DEL} =    key1=del        key2=del
&{ANOTHER_HEADER} =     key1=oth        key2=oth
${OBJECT_PATH} =        testfile
${EACL_ERR_MSG} =       *

*** Keywords ***

Create Container Public
    [Arguments]             ${WALLET}
    ${PUBLIC_CID_GEN} =     Create container    ${WALLET}    basic_acl=${PUBLIC_ACL}
    [Return]                ${PUBLIC_CID_GEN}

Generate files
    [Arguments]             ${SIZE}

    ${FILE_S_GEN_1} =       Generate file of bytes    ${SIZE}
    ${FILE_S_GEN_2} =       Generate file of bytes    ${SIZE}
                            Set Global Variable       ${FILE_S}      ${FILE_S_GEN_1}
                            Set Global Variable       ${FILE_S_2}    ${FILE_S_GEN_2}


Check eACL Deny and Allow All
    [Arguments]     ${WALLET}    ${DENY_EACL}    ${ALLOW_EACL}    ${USER_WALLET}

    ${CID} =                Create Container Public    ${USER_WALLET}
    ${FILE_S}    ${_} =    Generate file    ${SIMPLE_OBJ_SIZE}
    ${S_OID_USER} =         Put object                 ${USER_WALLET}     ${FILE_S}    ${CID}      user_headers=${USER_HEADER}
    ${D_OID_USER} =         Put object                 ${USER_WALLET}     ${FILE_S}    ${CID}      user_headers=${USER_HEADER_DEL}
    @{S_OBJ_H} =	        Create List	               ${S_OID_USER}

                            Put object                 ${WALLET}    ${FILE_S}    ${CID}            user_headers=${ANOTHER_HEADER}

                            Get object                 ${WALLET}    ${CID}        ${S_OID_USER}    ${EMPTY}    local_file_eacl
                            Search object              ${WALLET}    ${CID}        ${EMPTY}         ${EMPTY}    ${USER_HEADER}    ${S_OBJ_H}
                            Head object                ${WALLET}    ${CID}        ${S_OID_USER}

                            Get Range                  ${WALLET}    ${CID}        ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Get Range Hash             ${WALLET}    ${CID}        ${S_OID_USER}    ${EMPTY}    0:256
                            Delete object              ${WALLET}    ${CID}        ${D_OID_USER}

                            Set eACL                   ${USER_WALLET}     ${CID}        ${DENY_EACL}

                            # The current ACL cache lifetime is 30 sec
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                            Run Keyword And Expect Error        *
                            ...  Put object                          ${WALLET}    ${FILE_S}    ${CID}           user_headers=${USER_HEADER}
                            Run Keyword And Expect Error        *
                            ...  Get object                          ${WALLET}    ${CID}       ${S_OID_USER}    ${EMPTY}    local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Search object                       ${WALLET}    ${CID}       ${EMPTY}         ${EMPTY}    ${USER_HEADER}    ${S_OBJ_H}
                            Run Keyword And Expect Error        *
                            ...  Head object                         ${WALLET}    ${CID}       ${S_OID_USER}
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${WALLET}    ${CID}       ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Run Keyword And Expect Error        *
                            ...  Get Range Hash                      ${WALLET}    ${CID}       ${S_OID_USER}    ${EMPTY}    0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${WALLET}    ${CID}       ${S_OID_USER}

                            Set eACL                            ${USER_WALLET}    ${CID}       ${ALLOW_EACL}

                            # The current ACL cache lifetime is 30 sec
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                            Put object        ${WALLET}    ${FILE_S}     ${CID}            user_headers=${ANOTHER_HEADER}
                            Get object        ${WALLET}    ${CID}        ${S_OID_USER}     ${EMPTY}    local_file_eacl
                            Search object     ${WALLET}    ${CID}        ${EMPTY}          ${EMPTY}    ${USER_HEADER}    ${S_OBJ_H}
                            Head object       ${WALLET}    ${CID}        ${S_OID_USER}
                            Get Range         ${WALLET}    ${CID}        ${S_OID_USER}     s_get_range    ${EMPTY}    0:256
                            Get Range Hash    ${WALLET}    ${CID}        ${S_OID_USER}     ${EMPTY}    0:256
                            Delete object     ${WALLET}    ${CID}        ${S_OID_USER}

Compose eACL Custom
    [Arguments]    ${CID}    ${HEADER_DICT}    ${MATCH_TYPE}    ${FILTER}    ${ACCESS}    ${ROLE}

    ${filter_value} =    Get From dictionary    ${HEADER_DICT}   ${EACL_OBJ_FILTERS}[${FILTER}]

    ${filters} =        Set Variable    obj:${FILTER}${MATCH_TYPE}${filter_value}
    ${rule_get}=        Set Variable    ${ACCESS} get ${filters} ${ROLE}
    ${rule_head}=       Set Variable    ${ACCESS} head ${filters} ${ROLE}
    ${rule_put}=        Set Variable    ${ACCESS} put ${filters} ${ROLE}
    ${rule_del}=        Set Variable    ${ACCESS} delete ${filters} ${ROLE}
    ${rule_search}=     Set Variable    ${ACCESS} search ${filters} ${ROLE}
    ${rule_range}=      Set Variable    ${ACCESS} getrange ${filters} ${ROLE}
    ${rule_rangehash}=    Set Variable    ${ACCESS} getrangehash ${filters} ${ROLE}

    ${eACL_gen}=        Create List    ${rule_get}    ${rule_head}    ${rule_put}    ${rule_del}
    ...  ${rule_search}    ${rule_range}    ${rule_rangehash}
    ${EACL_CUSTOM} =    Create eACL    ${CID}    ${eACL_gen}

    [Return]    ${EACL_CUSTOM}

Object Header Decoded
    [Arguments]    ${WALLET}    ${CID}    ${OID}

    &{HEADER} =         Head Object    ${WALLET}    ${CID}    ${OID}
    # FIXME
    # 'objectID' key repositioning in dictionary for the calling keyword might
    # work uniformly with any key from 'header'
                        Set To Dictionary   ${HEADER}[header]   objectID     ${HEADER}[objectID]

    [Return]    &{HEADER}[header]

Check eACL Filters with MatchType String Equal
    [Arguments]    ${FILTER}

    ${WALLET}   ${_}    ${_} =    Prepare Wallet And Deposit
    ${WALLET_OTH}   ${_}    ${_} =    Prepare Wallet And Deposit

    ${CID} =                Create Container Public    ${WALLET}
    ${FILE_S}    ${_} =     Generate file    ${SIMPLE_OBJ_SIZE}

    ${S_OID_USER} =     Put Object    ${WALLET}    ${FILE_S}    ${CID}  user_headers=${USER_HEADER}
    ${D_OID_USER} =     Put object    ${WALLET}    ${FILE_S}    ${CID}
    @{S_OBJ_H} =	    Create List    ${S_OID_USER}

                        Get Object    ${WALLET_OTH}    ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl
                        Search Object    ${WALLET_OTH}    ${CID}    ${EMPTY}    ${EMPTY}    ${USER_HEADER}    ${S_OBJ_H}
                        Head Object    ${WALLET_OTH}    ${CID}    ${S_OID_USER}
                        Get Range    ${WALLET_OTH}    ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                        Get Range Hash    ${WALLET_OTH}    ${CID}    ${S_OID_USER}    ${EMPTY}    0:256
                        Delete Object    ${WALLET_OTH}    ${CID}    ${D_OID_USER}

    &{HEADER_DICT} =    Object Header Decoded    ${WALLET}    ${CID}    ${S_OID_USER}
    ${EACL_CUSTOM} =    Compose eACL Custom    ${CID}    ${HEADER_DICT}    =    ${FILTER}    DENY    OTHERS
                        Set eACL    ${WALLET}    ${CID}    ${EACL_CUSTOM}

                        # The current ACL cache lifetime is 30 sec
                        Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

    IF    'GET' in ${VERB_FILTER_DEP}[${FILTER}]
        Run Keyword And Expect Error   ${EACL_ERR_MSG}
        ...  Get object    ${WALLET_OTH}    ${CID}    ${S_OID_USER}    ${EMPTY}    ${OBJECT_PATH}
    END
    IF    'HEAD' in ${VERB_FILTER_DEP}[${FILTER}]
        Run Keyword And Expect error    ${EACL_ERR_MSG}
        ...  Head object    ${WALLET_OTH}    ${CID}    ${S_OID_USER}
    END
    IF    'RANGE' in ${VERB_FILTER_DEP}[${FILTER}]
        Run Keyword And Expect error    ${EACL_ERR_MSG}
        ...  Get Range    ${WALLET_OTH}    ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
    END
    IF    'SEARCH' in ${VERB_FILTER_DEP}[${FILTER}]
        Run Keyword And Expect Error   ${EACL_ERR_MSG}
        ...  Search Object    ${WALLET_OTH}    ${CID}    ${EMPTY}    ${EMPTY}    ${USER_HEADER}    ${S_OBJ_H}
    END
    IF    'RANGEHASH' in ${VERB_FILTER_DEP}[${FILTER}]
        Run Keyword And Expect error    ${EACL_ERR_MSG}
        ...  Get Range Hash    ${WALLET_OTH}    ${CID}    ${S_OID_USER}    ${EMPTY}    0:256
    END
    IF    'DELETE' in ${VERB_FILTER_DEP}[${FILTER}]
        Run Keyword And Expect error    ${EACL_ERR_MSG}
        ...  Delete Object    ${WALLET_OTH}    ${CID}    ${S_OID_USER}
    END

Check eACL Filters with MatchType String Not Equal
    [Arguments]    ${FILTER}

    ${WALLET}   ${_}    ${_} =    Prepare Wallet And Deposit
    ${WALLET_OTH}   ${_}    ${_} =    Prepare Wallet And Deposit

    ${CID} =            Create Container Public    ${WALLET}
    ${FILE_S}    ${_} =    Generate file    ${SIMPLE_OBJ_SIZE}

    ${S_OID_OTH} =      Put Object    ${WALLET_OTH}    ${FILE_S}    ${CID}    user_headers=${ANOTHER_HEADER}
    ${S_OID_USER} =     Put Object    ${WALLET}    ${FILE_S}    ${CID}    user_headers=${USER_HEADER}
    ${D_OID_USER} =     Put object    ${WALLET}    ${FILE_S}    ${CID}
    @{S_OBJ_H} =	    Create List    ${S_OID_USER}

                        Get Object    ${WALLET}    ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl
                        Head Object    ${WALLET}    ${CID}    ${S_OID_USER}
                        Search Object    ${WALLET}    ${CID}    ${EMPTY}    ${EMPTY}    ${FILE_USR_HEADER}    ${S_OBJ_H}
                        Get Range    ${WALLET}    ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                        Get Range Hash    ${WALLET}    ${CID}    ${S_OID_USER}    ${EMPTY}    0:256

    &{HEADER_DICT} =    Object Header Decoded    ${WALLET}    ${CID}    ${S_OID_USER}
    ${EACL_CUSTOM} =    Compose eACL Custom    ${CID}    ${HEADER_DICT}    !=    ${FILTER}    deny    others
                        Set eACL    ${WALLET}    ${CID}    ${EACL_CUSTOM}

                        # The current ACL cache lifetime is 30 sec
                        Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

    IF    'GET' in ${VERB_FILTER_DEP}[${FILTER}]
        Run Keyword And Expect Error   ${EACL_ERR_MSG}
        ...  Get object    ${WALLET_OTH}    ${CID}    ${S_OID_OTH}    ${EMPTY}    ${OBJECT_PATH}
        Get object    ${WALLET_OTH}    ${CID}    ${S_OID_USER}     ${EMPTY}    ${OBJECT_PATH}
    END
    IF    'HEAD' in ${VERB_FILTER_DEP}[${FILTER}]
        Run Keyword And Expect error    ${EACL_ERR_MSG}
        ...  Head object    ${WALLET_OTH}    ${CID}    ${S_OID_OTH}
        Head object    ${WALLET_OTH}    ${CID}    ${S_OID_USER}
    END
    IF    'SEARCH' in ${VERB_FILTER_DEP}[${FILTER}]
        Run Keyword And Expect error    ${EACL_ERR_MSG}
        ...  Search object    ${WALLET_OTH}    ${CID}    ${EMPTY}    ${EMPTY}    ${ANOTHER_HEADER}    ${S_OBJ_H}
        Search object    ${WALLET_OTH}    ${CID}    ${EMPTY}    ${EMPTY}    ${USER_HEADER}    ${S_OBJ_H}
    END
    IF    'RANGE' in ${VERB_FILTER_DEP}[${FILTER}]
        Run Keyword And Expect error    ${EACL_ERR_MSG}
        ...  Get Range    ${WALLET_OTH}    ${CID}    ${S_OID_OTH}    s_get_range    ${EMPTY}    0:256
        Get Range    ${WALLET_OTH}    ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
    END
    IF    'RANGEHASH' in ${VERB_FILTER_DEP}[${FILTER}]
        Run Keyword And Expect error    ${EACL_ERR_MSG}
        ...  Get Range Hash    ${WALLET_OTH}    ${CID}    ${S_OID_OTH}    ${EMPTY}    0:256
        Get Range Hash    ${WALLET_OTH}    ${CID}    ${S_OID_USER}    ${EMPTY}    0:256
    END
    IF    'DELETE' in ${VERB_FILTER_DEP}[${FILTER}]
        Run Keyword And Expect error    ${EACL_ERR_MSG}
        ...  Delete Object    ${WALLET_OTH}    ${CID}    ${S_OID_OTH}
        Delete Object    ${WALLET_OTH}    ${CID}    ${S_OID_USER}
    END
