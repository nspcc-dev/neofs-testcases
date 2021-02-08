*** Settings ***

Variables   ../../../variables/common.py
Library     ../${RESOURCES}/neofs.py
Library     ../${RESOURCES}/payment_neogo.py

Library     Collections
Resource    common_steps_acl_bearer.robot

*** Test cases ***
BearerToken Operations for Inaccessible Container
    [Documentation]         Testcase to validate NeoFS operations with BearerToken for Inaccessible Container.
    [Tags]                  ACL  NeoFS  NeoCLI BearerToken
    [Timeout]               20 min

                            Generate Keys
                            Prepare eACL Role rules
    
                            Log    Check Bearer token with simple object
                            Generate file    1024
                            Check Container Inaccessible and Allow All Bearer

                            Log    Check Bearer token with complex object
                            Cleanup Files    ${FILE_S}
                            Generate file    70e+6
                            Check Container Inaccessible and Allow All Bearer

    [Teardown]              Cleanup   
    
    
 
*** Keywords ***

Check Container Inaccessible and Allow All Bearer
    ${CID} =                Create Container Inaccessible

                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS            ${USER_KEY}    ${FILE_S}     ${CID}           ${EMPTY}       ${FILE_USR_HEADER} 
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${USER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Search object                  ${USER_KEY}    ${CID}        ${EMPTY}         ${EMPTY}       ${FILE_USR_HEADER}    
                            Run Keyword And Expect Error        *
                            ...  Head object                    ${USER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}               
                            Run Keyword And Expect Error        *
                            ...  Get Range                      ${USER_KEY}    ${CID}        ${S_OID_USER}    s_get_range    ${EMPTY}              0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                  ${USER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}
 
    ${rule1}=               Create Dictionary    Operation=PUT             Access=ALLOW    Role=USER   
    ${rule2}=               Create Dictionary    Operation=SEARCH          Access=ALLOW    Role=USER  

    ${eACL_gen}=            Create List    ${rule1}    ${rule2}

                            Form BearerToken file               ${USER_KEY}    ${CID}    bearer_allow_all_user   ${eACL_gen}   100500

                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS            ${USER_KEY}    ${FILE_S}     ${CID}           bearer_allow_all_user       ${FILE_USR_HEADER} 
                            Run Keyword And Expect Error        *
                            ...  Search object                  ${USER_KEY}    ${CID}        ${EMPTY}         bearer_allow_all_user       ${FILE_USR_HEADER}    
            

Cleanup
    @{CLEANUP_FILES} =      Create List	       ${FILE_S}       
                            ...                bearer_allow_all_user   gen_eacl_deny_all_USER           
                            Cleanup Files      @{CLEANUP_FILES}
                            Get Docker Logs    acl_bearer