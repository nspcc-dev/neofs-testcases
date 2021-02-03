*** Settings ***
Variables   ../../../variables/common.py
Library     ../${RESOURCES}/neofs.py
Library     ../${RESOURCES}/payment_neogo.py

Library     Collections
Resource    common_steps_acl_bearer.robot

*** Test cases ***
BearerToken Operations for Сompound Operations
    [Documentation]         Testcase to validate NeoFS operations with BearerToken for Сompound Operations.
    [Tags]                  ACL  NeoFS  NeoCLI BearerToken
    [Timeout]               20 min

                            Generate Keys
                            Prepare eACL Role rules
    
                            Log    Check Bearer token with simple object
                            Generate file    1024
                            Check Сompound Operations

                            Log    Check Bearer token with complex object
                            Cleanup Files    ${FILE_S}
                            Generate file    70e+6
                            Check Сompound Operations

    [Teardown]              Cleanup   
    
 
*** Keywords ***

Check Сompound Operations
                            Check Bearer Сompound Get    ${OTHER_KEY}     OTHERS    ${EACL_DENY_ALL_OTHERS}
                            Check Bearer Сompound Get    ${USER_KEY}      USER      ${EACL_DENY_ALL_USER}
                            Check Bearer Сompound Get    ${SYSTEM_KEY}    SYSTEM    ${EACL_DENY_ALL_SYSTEM}

                            Check Bearer Сompound Delete    ${OTHER_KEY}     OTHERS    ${EACL_DENY_ALL_OTHERS}
                            Check Bearer Сompound Delete    ${USER_KEY}      USER      ${EACL_DENY_ALL_USER}
                            Check Bearer Сompound Delete    ${SYSTEM_KEY}    SYSTEM    ${EACL_DENY_ALL_SYSTEM}

                            Check Bearer Сompound Get Range Hash    ${OTHER_KEY}     OTHERS    ${EACL_DENY_ALL_OTHERS}
                            Check Bearer Сompound Get Range Hash    ${USER_KEY}      USER      ${EACL_DENY_ALL_USER}
                            Check Bearer Сompound Get Range Hash    ${SYSTEM_KEY}    SYSTEM    ${EACL_DENY_ALL_SYSTEM}


Check Bearer Сompound Get
    [Arguments]             ${KEY}    ${DENY_GROUP}    ${DENY_EACL}

    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER} 
    @{S_OBJ_H} =	        Create List	                        ${S_OID_USER}

    ${S_OID_USER} =         Put object to NeoFS             ${USER_KEY}    ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_USR_HEADER} 
                            Put object to NeoFS             ${KEY}         ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_OTH_HEADER} 
                            Get object from NeoFS           ${KEY}         ${CID}       ${S_OID_USER}    ${EMPTY}    local_file_eacl
                            Set eACL                        ${USER_KEY}    ${CID}       ${DENY_EACL}     --await
                            
    ${rule1}=               Create Dictionary    Operation=GET             Access=ALLOW    Role=${DENY_GROUP}  
    ${rule2}=               Create Dictionary    Operation=GETRANGE        Access=ALLOW    Role=${DENY_GROUP}  
    ${rule3}=               Create Dictionary    Operation=GETRANGEHASH    Access=ALLOW    Role=${DENY_GROUP}    
    ${eACL_gen}=            Create List    ${rule1}    ${rule2}    ${rule3}
                            Form BearerToken file           ${USER_KEY}    ${CID}    bearer_allow   ${eACL_gen}   100500 

                            Run Keyword And Expect Error    *
                            ...  Head object                ${KEY}    ${CID}    ${S_OID_USER}    bearer_allow  

                            Get object from NeoFS           ${KEY}    ${CID}    ${S_OID_USER}    bearer_allow       local_file_eacl
                            Get Range                       ${KEY}    ${CID}    ${S_OID_USER}    s_get_range        bearer_allow       0:256
                            Get Range Hash                  ${KEY}    ${CID}    ${S_OID_USER}    bearer_allow       0:256


Check Bearer Сompound Delete
    [Arguments]             ${KEY}    ${DENY_GROUP}    ${DENY_EACL}

    ${CID} =                Create Container Public

    ${S_OID_USER} =         Put object to NeoFS             ${USER_KEY}    ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_USR_HEADER}
    ${D_OID_USER} =         Put object to NeoFS             ${USER_KEY}    ${FILE_S}    ${CID}           ${EMPTY}    ${EMPTY}
                            Put object to NeoFS             ${KEY}         ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_OTH_HEADER} 
                            Delete object                   ${KEY}         ${CID}       ${D_OID_USER}    ${EMPTY}
                            
                            Set eACL                        ${USER_KEY}    ${CID}       ${DENY_EACL}     --await
                            
    ${rule1} =              Create Dictionary    Operation=DELETE          Access=ALLOW    Role=${DENY_GROUP}  
    ${rule2} =              Create Dictionary    Operation=PUT             Access=DENY     Role=${DENY_GROUP}   
    ${rule3} =              Create Dictionary    Operation=HEAD            Access=DENY     Role=${DENY_GROUP}  
    ${eACL_gen} =           Create List    ${rule1}    ${rule2}    ${rule3}
                            Form BearerToken file           ${USER_KEY}    ${CID}    bearer_allow   ${eACL_gen}   100500 

                            Run Keyword And Expect Error    *
                            ...  Head object                ${KEY}    ${CID}       ${S_OID_USER}    bearer_allow     
                            Run Keyword And Expect Error    *
                            ...  Put object to NeoFS        ${KEY}    ${FILE_S}    ${CID}           bearer_allow    ${FILE_OTH_HEADER}   

                            Delete object                   ${KEY}    ${CID}       ${S_OID_USER}    bearer_allow



Check Bearer Сompound Get Range Hash
    [Arguments]             ${KEY}    ${DENY_GROUP}    ${DENY_EACL}    

    ${CID} =                Create Container Public

    ${S_OID_USER} =         Put object to NeoFS             ${USER_KEY}         ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_USR_HEADER}
                            Put object to NeoFS             ${KEY}              ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_OTH_HEADER} 
                            Get Range Hash                  ${SYSTEM_KEY_SN}    ${CID}       ${S_OID_USER}    ${EMPTY}    0:256

                            Set eACL                        ${USER_KEY}         ${CID}       ${DENY_EACL}     --await
                            
        ${rule1} =          Create Dictionary    Operation=GETRANGEHASH    Access=ALLOW    Role=${DENY_GROUP} 
        ${rule2} =          Create Dictionary    Operation=GETRANGE        Access=DENY     Role=${DENY_GROUP}  
        ${rule3} =          Create Dictionary    Operation=GET             Access=DENY     Role=${DENY_GROUP} 
        ${eACL_gen} =       Create List    ${rule1}    ${rule2}    ${rule3}
                            Form BearerToken file          ${USER_KEY}    ${CID}    bearer_allow   ${eACL_gen}   100500 

                            Run Keyword And Expect Error    *
                            ...  Get Range                  ${KEY}    ${CID}    ${S_OID_USER}    s_get_range    bearer_allow    0:256
                            Run Keyword And Expect Error    *
                            ...  Get object from NeoFS      ${KEY}    ${CID}    ${S_OID_USER}    bearer_allow    local_file_eacl
                            
                            Get Range Hash                  ${KEY}    ${CID}    ${S_OID_USER}    bearer_allow    0:256
                     

Cleanup
    @{CLEANUP_FILES} =      Create List	       ${FILE_S}    local_file_eacl    s_get_range    
                            ...                gen_eacl_deny_all_USER    bearer_allow          
                            Cleanup Files      @{CLEANUP_FILES}
                            Get Docker Logs    acl_bearer