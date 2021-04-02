*** Settings ***
Variables   ../../../variables/common.py

Library     Collections

Library     ../${RESOURCES}/neofs.py
Library     ../${RESOURCES}/payment_neogo.py

Resource    common_steps_acl_bearer.robot



*** Test cases ***
BearerToken Operations
    [Documentation]         Testcase to validate NeoFS operations with BearerToken.
    [Tags]                  ACL  NeoFS  NeoCLI BearerToken
    [Timeout]               20 min

                            Generate Keys
                            Prepare eACL Role rules
    
                            Log    Check Bearer token with simple object
                            Generate file    ${SIMPLE_OBJ_SIZE}
                            Check eACL Allow All Bearer Filter Requst Equal Deny

                            Log    Check Bearer token with complex object
                            
                            Generate file    ${COMPLEX_OBJ_SIZE}
                            Check eACL Allow All Bearer Filter Requst Equal Deny

    [Teardown]              Cleanup   
    
    
 
*** Keywords ***

Check eACL Allow All Bearer Filter Requst Equal Deny
    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER} 
    ${S_OID_USER_2} =       Put object                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${EMPTY}
    ${D_OID_USER} =         Put object                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER_DEL} 
    @{S_OBJ_H} =	        Create List	                        ${S_OID_USER}


    ${filters}=             Create Dictionary    headerType=REQUEST    matchType=STRING_EQUAL    key=a    value=256 
    ${rule1}=               Create Dictionary    Operation=GET             Access=DENY    Role=USER    Filters=${filters}
    ${rule2}=               Create Dictionary    Operation=HEAD            Access=DENY    Role=USER    Filters=${filters}
    ${rule3}=               Create Dictionary    Operation=PUT             Access=DENY    Role=USER    Filters=${filters}
    ${rule4}=               Create Dictionary    Operation=DELETE          Access=DENY    Role=USER    Filters=${filters}
    ${rule5}=               Create Dictionary    Operation=SEARCH          Access=DENY    Role=USER    Filters=${filters}
    ${rule6}=               Create Dictionary    Operation=GETRANGE        Access=DENY    Role=USER    Filters=${filters}
    ${rule7}=               Create Dictionary    Operation=GETRANGEHASH    Access=DENY    Role=USER    Filters=${filters}
    ${eACL_gen}=            Create List    ${rule1}    ${rule2}    ${rule3}    ${rule4}    ${rule5}    ${rule6}    ${rule7}
                            Form BearerToken file               ${USER_KEY}    ${CID}    bearer_allow_all_user   ${eACL_gen}   100500 

                            Put object      ${USER_KEY}    ${FILE_S}     ${CID}           bearer_allow_all_user    ${FILE_OTH_HEADER}       ${EMPTY}      --xhdr a=2
                            Get object    ${USER_KEY}    ${CID}        ${S_OID_USER}    bearer_allow_all_user    local_file_eacl          ${EMPTY}      --xhdr a=2
                            Search object            ${USER_KEY}    ${CID}        ${EMPTY}         bearer_allow_all_user    ${FILE_USR_HEADER}       ${S_OBJ_H}    --xhdr a=2     
                            Head object              ${USER_KEY}    ${CID}        ${S_OID_USER}    bearer_allow_all_user    ${EMPTY}                 --xhdr a=2
                            Get Range                ${USER_KEY}    ${CID}        ${S_OID_USER}    s_get_range              bearer_allow_all_user    0:256         --xhdr a=2
                            Get Range Hash           ${USER_KEY}    ${CID}        ${S_OID_USER}    bearer_allow_all_user    0:256                    --xhdr a=2
                            Delete object            ${USER_KEY}    ${CID}        ${D_OID_USER}    bearer_allow_all_user    --xhdr a=2
        
                            Run Keyword And Expect Error    *
                            ...  Put object             ${USER_KEY}    ${FILE_S}    ${CID}           bearer_allow_all_user    ${FILE_USR_HEADER}       ${EMPTY}       --xhdr a=256
                            Run Keyword And Expect Error    *
                            ...  Get object           ${USER_KEY}    ${CID}       ${S_OID_USER}    bearer_allow_all_user    local_file_eacl          ${EMPTY}       --xhdr a=256
                            Run Keyword And Expect Error    *
                            ...  Search object                   ${USER_KEY}    ${CID}       ${EMPTY}         bearer_allow_all_user    ${FILE_USR_HEADER}       ${EMPTY}       --xhdr a=256
                            Run Keyword And Expect Error    *
                            ...  Head object                     ${USER_KEY}    ${CID}       ${S_OID_USER}    bearer_allow_all_user    ${EMPTY}                 --xhdr a=256
                            Run Keyword And Expect Error    *
                            ...  Get Range                       ${USER_KEY}    ${CID}       ${S_OID_USER}    s_get_range              bearer_allow_all_user    0:256          --xhdr a=256
                            Run Keyword And Expect Error    *
                            ...  Get Range Hash                  ${USER_KEY}    ${CID}       ${S_OID_USER}    bearer_allow_all_user    0:256                    --xhdr a=256
                            Run Keyword And Expect Error    *
                            ...  Delete object                   ${USER_KEY}    ${CID}       ${S_OID_USER}    bearer_allow_all_user    --xhdr a=256


Cleanup
                            Cleanup Files
                            Get Docker Logs    acl_bearer