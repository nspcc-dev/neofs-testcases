*** Settings ***
Variables   common.py

Library     Collections
Library     neofs.py
Library     acl.py
Library     payment_neogo.py

Resource    eacl_tables.robot
Resource    common_steps_acl_bearer.robot
Resource    payment_operations.robot
Resource    setup_teardown.robot

*** Test cases ***
BearerToken Operations
    [Documentation]         Testcase to validate NeoFS operations with BearerToken.
    [Tags]                  ACL  NeoFS  NeoCLI BearerToken
    [Timeout]               20 min

    [Setup]                 Setup

    ${WALLET}   ${ADDR}     ${USER_KEY} =   Prepare Wallet And Deposit
                            Prepare eACL Role rules

                            Log    Check Bearer token with simple object
    ${FILE_S} =             Generate file    ${SIMPLE_OBJ_SIZE}
                            Check eACL Allow All Bearer Filter Requst Equal Deny    ${USER_KEY}    ${FILE_S}

                            Log    Check Bearer token with complex object
    ${FILE_S} =             Generate file    ${COMPLEX_OBJ_SIZE}
                            Check eACL Allow All Bearer Filter Requst Equal Deny    ${USER_KEY}    ${FILE_S}

    [Teardown]              Teardown    acl_bearer_request_filter_xheader_deny



*** Keywords ***

Check eACL Allow All Bearer Filter Requst Equal Deny
    [Arguments]    ${USER_KEY}    ${FILE_S}

    ${CID} =                Create Container Public    ${USER_KEY}
    ${S_OID_USER} =         Put object                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER}
    ${S_OID_USER_2} =       Put object                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${EMPTY}
    ${D_OID_USER} =         Put object                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER_DEL}
    @{S_OBJ_H} =	    Create List	               ${S_OID_USER}


    ${filters}=             Create Dictionary    headerType=REQUEST    matchType=STRING_EQUAL    key=a    value=256
    ${rule1}=               Create Dictionary    Operation=GET             Access=DENY    Role=USER    Filters=${filters}
    ${rule2}=               Create Dictionary    Operation=HEAD            Access=DENY    Role=USER    Filters=${filters}
    ${rule3}=               Create Dictionary    Operation=PUT             Access=DENY    Role=USER    Filters=${filters}
    ${rule4}=               Create Dictionary    Operation=DELETE          Access=DENY    Role=USER    Filters=${filters}
    ${rule5}=               Create Dictionary    Operation=SEARCH          Access=DENY    Role=USER    Filters=${filters}
    ${rule6}=               Create Dictionary    Operation=GETRANGE        Access=DENY    Role=USER    Filters=${filters}
    ${rule7}=               Create Dictionary    Operation=GETRANGEHASH    Access=DENY    Role=USER    Filters=${filters}
    ${eACL_gen}=            Create List    ${rule1}    ${rule2}    ${rule3}    ${rule4}    ${rule5}    ${rule6}    ${rule7}

    ${EACL_TOKEN} =         Form BearerToken File       ${USER_KEY}    ${CID}    ${eACL_gen}

                        Put object      ${USER_KEY}    ${FILE_S}     ${CID}           ${EACL_TOKEN}    ${FILE_OTH_HEADER}   ${EMPTY}      --xhdr a=2
                        Get object      ${USER_KEY}    ${CID}        ${S_OID_USER}    ${EACL_TOKEN}    local_file_eacl      ${EMPTY}      --xhdr a=2
                        Search object   ${USER_KEY}    ${CID}        ${EMPTY}         ${EACL_TOKEN}    ${FILE_USR_HEADER}   ${S_OBJ_H}    --xhdr a=2
                        Head object     ${USER_KEY}    ${CID}        ${S_OID_USER}    ${EACL_TOKEN}    ${EMPTY}         --xhdr a=2
                        Get Range       ${USER_KEY}    ${CID}        ${S_OID_USER}    s_get_range      ${EACL_TOKEN}    0:256         --xhdr a=2
                        Get Range Hash  ${USER_KEY}    ${CID}        ${S_OID_USER}    ${EACL_TOKEN}    0:256            --xhdr a=2
                        Delete object   ${USER_KEY}    ${CID}        ${D_OID_USER}    ${EACL_TOKEN}    --xhdr a=2

                        Run Keyword And Expect Error    *
                        ...  Put object     ${USER_KEY}    ${FILE_S}    ${CID}       ${EACL_TOKEN}    ${FILE_USR_HEADER}       ${EMPTY}   --xhdr a=256
                        Run Keyword And Expect Error    *
                        ...  Get object     ${USER_KEY}    ${CID}       ${S_OID_USER}    ${EACL_TOKEN}    local_file_eacl      ${EMPTY}   --xhdr a=256
                        Run Keyword And Expect Error    *
                        ...  Search object   ${USER_KEY}    ${CID}       ${EMPTY}     ${EACL_TOKEN}    ${FILE_USR_HEADER}   ${EMPTY}   --xhdr a=256
                        Run Keyword And Expect Error    *
                        ...  Head object     ${USER_KEY}    ${CID}       ${S_OID_USER}    ${EACL_TOKEN}    ${EMPTY}     --xhdr a=256
                        Run Keyword And Expect Error    *
                        ...  Get Range       ${USER_KEY}    ${CID}       ${S_OID_USER}    s_get_range      ${EACL_TOKEN}    0:256      --xhdr a=256
                        Run Keyword And Expect Error    *
                        ...  Get Range Hash  ${USER_KEY}    ${CID}       ${S_OID_USER}    ${EACL_TOKEN}    0:256    --xhdr a=256
                        Run Keyword And Expect Error    *
                        ...  Delete object   ${USER_KEY}    ${CID}       ${S_OID_USER}    ${EACL_TOKEN}    --xhdr a=256
