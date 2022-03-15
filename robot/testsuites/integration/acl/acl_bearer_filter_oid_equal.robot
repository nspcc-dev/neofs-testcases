*** Settings ***
Variables   common.py

Library     Collections
Library     neofs.py
Library     neofs_verbs.py
Library     acl.py
Library     payment_neogo.py

Resource    eacl_tables.robot
Resource    common_steps_acl_bearer.robot
Resource    payment_operations.robot
Resource    setup_teardown.robot

*** Variables ***
&{USER_HEADER} =        key1=1      key2=abc
&{USER_HEADER_DEL} =    key1=del    key2=del
&{ANOTHER_HEADER} =     key1=oth    key2=oth

*** Test cases ***
BearerToken Operations with Filter OID Equal
    [Documentation]         Testcase to validate NeoFS operations with BearerToken with Filter OID Equal.
    [Tags]                  ACL   BearerToken
    [Timeout]               20 min

    [Setup]                 Setup

    ${WALLET}   ${ADDR}     ${USER_KEY} =   Prepare Wallet And Deposit
    ${WALLET_OTH}   ${ADDR_OTH}     ${OTHER_KEY} =   Prepare Wallet And Deposit

                            Log    Check Bearer token with simple object
    ${FILE_S} =             Generate file    ${SIMPLE_OBJ_SIZE}
                            Check eACL Deny and Allow All Bearer Filter OID Equal    ${USER_KEY}    ${FILE_S}

                            Log    Check Bearer token with complex object

    ${FILE_S} =             Generate file    ${COMPLEX_OBJ_SIZE}
                            Check eACL Deny and Allow All Bearer Filter OID Equal    ${USER_KEY}    ${FILE_S}

    [Teardown]              Teardown    acl_bearer_filter_oid_equal



*** Keywords ***

Check eACL Deny and Allow All Bearer Filter OID Equal
    [Arguments]    ${USER_KEY}    ${FILE_S}

    ${CID} =                Create Container Public    ${USER_KEY}
                            Prepare eACL Role rules    ${CID}
    ${S_OID_USER} =         Put object         ${USER_KEY}     ${FILE_S}   ${CID}  user_headers=${USER_HEADER}
    ${S_OID_USER_2} =       Put object         ${USER_KEY}     ${FILE_S}   ${CID}
    ${D_OID_USER} =         Put object         ${USER_KEY}     ${FILE_S}   ${CID}  user_headers=${USER_HEADER_DEL}
    @{S_OBJ_H} =	    Create List	       ${S_OID_USER}

                            Put object         ${USER_KEY}    ${FILE_S}     ${CID}               user_headers=${ANOTHER_HEADER}
                            Get object         ${USER_KEY}    ${CID}        ${S_OID_USER}        ${EMPTY}      local_file_eacl
                            Search object      ${USER_KEY}    ${CID}        ${EMPTY}             ${EMPTY}      ${USER_HEADER}    ${S_OBJ_H}
                            Head object        ${USER_KEY}    ${CID}        ${S_OID_USER}
                            Get Range          ${USER_KEY}    ${CID}        ${S_OID_USER}        s_get_range       ${EMPTY}          0:256
                            Delete object      ${USER_KEY}    ${CID}        ${D_OID_USER}

                            Set eACL           ${USER_KEY}    ${CID}        ${EACL_DENY_ALL_USER}

                            # The current ACL cache lifetime is 30 sec
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

    ${filters}=         Create Dictionary    headerType=OBJECT    matchType=STRING_EQUAL    key=$Object:objectID    value=${S_OID_USER}

    ${rule1}=           Create Dictionary    Operation=GET             Access=ALLOW    Role=USER    Filters=${filters}
    ${rule2}=           Create Dictionary    Operation=HEAD            Access=ALLOW    Role=USER    Filters=${filters}
    ${rule3}=           Create Dictionary    Operation=PUT             Access=ALLOW    Role=USER    Filters=${filters}
    ${rule4}=           Create Dictionary    Operation=DELETE          Access=ALLOW    Role=USER    Filters=${filters}
    ${rule5}=           Create Dictionary    Operation=SEARCH          Access=ALLOW    Role=USER    Filters=${filters}
    ${rule6}=           Create Dictionary    Operation=GETRANGE        Access=ALLOW    Role=USER    Filters=${filters}
    ${rule7}=           Create Dictionary    Operation=GETRANGEHASH    Access=ALLOW    Role=USER    Filters=${filters}

    ${eACL_gen}=        Create List    ${rule1}    ${rule2}    ${rule3}    ${rule4}    ${rule5}    ${rule6}    ${rule7}

    ${EACL_TOKEN} =     Form BearerToken File      ${USER_KEY}    ${CID}    ${eACL_gen}

                        Run Keyword And Expect Error        *
                        ...  Put object     ${USER_KEY}    ${FILE_S}     ${CID}       user_headers=${USER_HEADER}
                        Run Keyword And Expect Error        *
                        ...  Get object     ${USER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}      local_file_eacl
                        Run Keyword And Expect Error        *
                        ...  Search object  ${USER_KEY}    ${CID}    ${EMPTY}     ${EMPTY}      ${USER_HEADER}      ${S_OBJ_H}
                        Run Keyword And Expect Error        *
                        ...  Head object    ${USER_KEY}    ${CID}    ${S_OID_USER}
                        Run Keyword And Expect Error        *
                        ...  Get Range      ${USER_KEY}    ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}      0:256
                        Run Keyword And Expect Error        *
                        ...  Delete object  ${USER_KEY}    ${CID}    ${S_OID_USER}
                        Run Keyword And Expect Error        *
                        ...  Search object  ${USER_KEY}    ${CID}    ${EMPTY}    ${EACL_TOKEN}    ${USER_HEADER}    ${S_OBJ_H}
                        Run Keyword And Expect Error        *
                        ...  Put object     ${USER_KEY}    ${FILE_S}     ${CID}     bearer=${EACL_TOKEN}     user_headers=${ANOTHER_HEADER}
                        Run Keyword And Expect Error        *
                        ...  Get object     ${USER_KEY}    ${CID}      ${S_OID_USER_2}    ${EACL_TOKEN}       local_file_eacl

                        Get object       ${USER_KEY}    ${CID}      ${S_OID_USER}      ${EACL_TOKEN}       local_file_eacl
                        Get Range        ${USER_KEY}    ${CID}      ${S_OID_USER}      s_get_range     ${EACL_TOKEN}   0:256

                        Head object      ${USER_KEY}    ${CID}      ${S_OID_USER}      bearer_token=${EACL_TOKEN}
                        Delete object    ${USER_KEY}    ${CID}      ${S_OID_USER}      bearer=${EACL_TOKEN}
                        Run Keyword And Expect Error        *
                        ...  Delete object              ${USER_KEY}    ${CID}        ${D_OID_USER}        bearer=${EACL_TOKEN}
