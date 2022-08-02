*** Settings ***
Variables   common.py

Library     acl.py
Library     container.py
Library     neofs_verbs.py
Library     utility_keywords.py

Resource    eacl_tables.robot
Resource    common_steps_acl_bearer.robot
Resource    payment_operations.robot

*** Variables ***
&{USER_HEADER} =        key1=1      key2=abc
&{USER_HEADER_DEL} =    key1=del    key2=del
&{ANOTHER_HEADER} =     key1=oth    key2=oth

*** Test cases ***
BearerToken Operations with Filter UserHeader Equal
    [Documentation]         Testcase to validate NeoFS operations with BearerToken with Filter UserHeader Equal.
    [Tags]                  ACL   BearerToken
    [Timeout]               10 min


    ${WALLET}   ${_}     ${_} =   Prepare Wallet And Deposit

                            Log    Check Bearer token with simple object
    ${FILE_S}    ${_} =     Generate file    ${SIMPLE_OBJ_SIZE}
                            Check eACL Deny and Allow All Bearer Filter UserHeader Equal    ${WALLET}    ${FILE_S}


                            Log    Check Bearer token with complex object
    ${FILE_S}    ${_} =     Generate file    ${COMPLEX_OBJ_SIZE}
                            Check eACL Deny and Allow All Bearer Filter UserHeader Equal    ${WALLET}    ${FILE_S}


*** Keywords ***
Check eACL Deny and Allow All Bearer Filter UserHeader Equal
    [Arguments]    ${WALLET}    ${FILE_S}
    ${CID} =                Create Container           ${WALLET}    basic_acl=eacl-public-read-write
                            Prepare eACL Role rules    ${CID}
    ${S_OID_USER} =         Put object                 ${WALLET}     ${FILE_S}   ${CID}  user_headers=${USER_HEADER}
    ${S_OID_USER_2} =       Put object                 ${WALLET}     ${FILE_S}   ${CID}
    ${D_OID_USER} =         Put object                 ${WALLET}     ${FILE_S}   ${CID}  user_headers=${USER_HEADER_DEL}
    @{S_OBJ_H} =	    Create List	               ${S_OID_USER}

                            Put object       ${WALLET}    ${FILE_S}    ${CID}               user_headers=${ANOTHER_HEADER}
                            Get object       ${WALLET}    ${CID}       ${S_OID_USER}        ${EMPTY}      local_file_eacl
                            Search object    ${WALLET}    ${CID}       ${EMPTY}             ${EMPTY}      ${USER_HEADER}     ${S_OBJ_H}
                            Head object      ${WALLET}    ${CID}       ${S_OID_USER}
                            Get Range        ${WALLET}    ${CID}       ${S_OID_USER}        0:256
                            Delete object    ${WALLET}    ${CID}       ${D_OID_USER}

                            Set eACL         ${WALLET}    ${CID}       ${EACL_DENY_ALL_USER}

                            # The current ACL cache lifetime is 30 sec
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

    ${filters}=         Create Dictionary    headerType=OBJECT    matchType=STRING_EQUAL    key=key2    value=abc

    ${rule1}=           Create Dictionary    Operation=GET             Access=ALLOW    Role=USER    Filters=${filters}
    ${rule2}=           Create Dictionary    Operation=HEAD            Access=ALLOW    Role=USER    Filters=${filters}
    ${rule3}=           Create Dictionary    Operation=PUT             Access=ALLOW    Role=USER    Filters=${filters}
    ${rule4}=           Create Dictionary    Operation=DELETE          Access=ALLOW    Role=USER    Filters=${filters}
    ${rule5}=           Create Dictionary    Operation=SEARCH          Access=ALLOW    Role=USER    Filters=${filters}
    ${rule6}=           Create Dictionary    Operation=GETRANGE        Access=ALLOW    Role=USER    Filters=${filters}
    ${rule7}=           Create Dictionary    Operation=GETRANGEHASH    Access=ALLOW    Role=USER    Filters=${filters}

    ${eACL_gen}=        Create List    ${rule1}    ${rule2}    ${rule3}    ${rule4}    ${rule5}    ${rule6}    ${rule7}

    ${EACL_TOKEN} =     Form BearerToken File      ${WALLET}    ${CID}    ${eACL_gen}

                        Run Keyword And Expect Error        *
                        ...  Put object    ${WALLET}    ${FILE_S}    ${CID}    user_headers=${USER_HEADER}
                        Run Keyword And Expect Error        *
                        ...  Get object    ${WALLET}    ${CID}       ${S_OID_USER}    ${EMPTY}      local_file_eacl
                        Run Keyword And Expect Error        *
                        ...  Search object  ${WALLET}   ${CID}       ${EMPTY}         ${EMPTY}      ${USER_HEADER}      ${S_OBJ_H}
                        Run Keyword And Expect Error        *
                        ...  Head object    ${WALLET}    ${CID}      ${S_OID_USER}
                        Run Keyword And Expect Error        *
                        ...  Get Range      ${WALLET}    ${CID}      ${S_OID_USER}    0:256
                        Run Keyword And Expect Error        *
                        ...  Delete object  ${WALLET}    ${CID}      ${S_OID_USER}
                        Run Keyword And Expect Error        *
                        ...  Search object  ${WALLET}    ${CID}      ${EMPTY}         ${EACL_TOKEN}   ${USER_HEADER}     ${S_OBJ_H}

                        Run Keyword And Expect Error        *
                        ...  Put object     ${WALLET}    ${FILE_S}     ${CID}    bearer=${EACL_TOKEN}    user_headers=${ANOTHER_HEADER}

                        Get object          ${WALLET}    ${CID}        ${S_OID_USER}        ${EACL_TOKEN}       local_file_eacl
                        Run Keyword And Expect Error        *
                        ...  Get object     ${WALLET}    ${CID}        ${S_OID_USER_2}      ${EACL_TOKEN}       local_file_eacl

                        Run Keyword And Expect Error        *
                        ...  Get Range      ${WALLET}    ${CID}        ${S_OID_USER}        0:256    bearer=${EACL_TOKEN}

                        Run Keyword And Expect Error        *
                        ...  Get Range Hash     ${WALLET}    ${CID}    ${S_OID_USER}        ${EACL_TOKEN}   0:256

                        Head object         ${WALLET}    ${CID}        ${S_OID_USER}        bearer_token=${EACL_TOKEN}
                        Run Keyword And Expect Error        *
                        ...  Head object    ${WALLET}    ${CID}        ${S_OID_USER_2}      bearer_token=${EACL_TOKEN}
                        # Delete can not be filtered by UserHeader.
                        Run Keyword And Expect Error        *
                        ...  Delete object      ${WALLET}    ${CID}    ${S_OID_USER}        bearer=${EACL_TOKEN}
                        Run Keyword And Expect Error        *
                        ...  Delete object      ${WALLET}    ${CID}    ${S_OID_USER_2}      bearer=${EACL_TOKEN}
