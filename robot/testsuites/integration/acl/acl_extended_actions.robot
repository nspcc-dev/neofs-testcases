*** Settings ***
Variables                   ../../../variables/common.py
Library                     Collections
Library                     ../${RESOURCES}/neofs.py
Library                     ../${RESOURCES}/payment_neogo.py

Resource                    common_steps_acl_extended.robot
     
*** Test cases ***
Extended ACL Operations
    [Documentation]         Testcase to validate NeoFS operations with extended ACL.
    [Tags]                  ACL  eACL  NeoFS  NeoCLI
    [Timeout]               20 min

                            Generate Keys
                            Prepare eACL Role rules

                            Log    Check extended ACL with simple object

                            Generate files    1024
                            
                            Check Actions
                            

                            Cleanup Files    ${FILE_S}    ${FILE_S_2}
                            
                            Log    Check extended ACL with complex object
                            Generate files    10e+6
                            Check Actions
                             
                             
    [Teardown]              Cleanup  

    
*** Keywords ***

Check Actions
                            Check eACL Deny and Allow All Other
                            Check eACL Deny and Allow All User
                            Check eACL Deny and Allow All System
                            Check eACL Deny All Other and Allow All Pubkey



Check eACL Deny and Allow All User
                            Check eACL Deny and Allow All    ${USER_KEY}    ${EACL_DENY_ALL_USER}    ${EACL_ALLOW_ALL_USER}                  


Check eACL Deny and Allow All Other
                            Check eACL Deny and Allow All    ${OTHER_KEY}    ${EACL_DENY_ALL_OTHER}    ${EACL_ALLOW_ALL_OTHER} 


Check eACL Deny and Allow All System
    ${CID} =                Create Container Public

    ${S_OID_USER} =         Put object to NeoFS      ${USER_KEY}    ${FILE_S}    ${CID}    ${EMPTY}    ${FILE_USR_HEADER} 
    ${D_OID_USER_S} =       Put object to NeoFS      ${USER_KEY}    ${FILE_S}    ${CID}    ${EMPTY}    ${FILE_USR_HEADER_DEL} 
    ${D_OID_USER_SN} =      Put object to NeoFS      ${USER_KEY}    ${FILE_S}    ${CID}    ${EMPTY}    ${FILE_USR_HEADER_DEL} 

    @{S_OBJ_H} =	        Create List	             ${S_OID_USER}

                            Put object to NeoFS      ${SYSTEM_KEY}       ${FILE_S}    ${CID}    ${EMPTY}    ${FILE_OTH_HEADER} 
                            Put object to NeoFS      ${SYSTEM_KEY_SN}    ${FILE_S}    ${CID}    ${EMPTY}    ${FILE_OTH_HEADER} 
                            
                            Get object from NeoFS    ${SYSTEM_KEY}       ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl
                            Get object from NeoFS    ${SYSTEM_KEY_SN}    ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl

                            Search object            ${SYSTEM_KEY}       ${CID}    ${EMPTY}    ${EMPTY}    ${FILE_USR_HEADER}    ${S_OBJ_H}            
                            Search object            ${SYSTEM_KEY_SN}    ${CID}    ${EMPTY}    ${EMPTY}    ${FILE_USR_HEADER}    ${S_OBJ_H}            

                            Head object              ${SYSTEM_KEY}       ${CID}    ${S_OID_USER}    ${EMPTY}             
                            Head object              ${SYSTEM_KEY_SN}    ${CID}    ${S_OID_USER}    ${EMPTY}             

                            Get Range                ${SYSTEM_KEY}       ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Get Range                ${SYSTEM_KEY_SN}    ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256

                            Get Range Hash           ${SYSTEM_KEY}       ${CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Get Range Hash           ${SYSTEM_KEY_SN}    ${CID}    ${S_OID_USER}    ${EMPTY}    0:256
                             
                            Delete object            ${SYSTEM_KEY}       ${CID}    ${D_OID_USER_S}     ${EMPTY}
                            Delete object            ${SYSTEM_KEY_SN}    ${CID}    ${D_OID_USER_SN}    ${EMPTY}

                            Set eACL                 ${USER_KEY}     ${CID}        ${EACL_DENY_ALL_SYSTEM}    --await

                            Run Keyword And Expect Error    *
                            ...  Put object to NeoFS        ${SYSTEM_KEY}       ${FILE_S}    ${CID}    ${EMPTY}    ${FILE_OTH_HEADER} 
                            Run Keyword And Expect Error    *
                            ...  Put object to NeoFS        ${SYSTEM_KEY_SN}    ${FILE_S}    ${CID}    ${EMPTY}    ${FILE_OTH_HEADER} 

                            Run Keyword And Expect Error    *
                            ...  Get object from NeoFS      ${SYSTEM_KEY}       ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl
                            Run Keyword And Expect Error    *
                            ...  Get object from NeoFS      ${SYSTEM_KEY_SN}    ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl
                            
                            Run Keyword And Expect Error    *
                            ...  Search object              ${SYSTEM_KEY}       ${CID}    ${EMPTY}    ${EMPTY}    ${FILE_USR_HEADER}    ${S_OBJ_H}            
                            Run Keyword And Expect Error    *
                            ...  Search object              ${SYSTEM_KEY_SN}    ${CID}    ${EMPTY}    ${EMPTY}    ${FILE_USR_HEADER}    ${S_OBJ_H}            

                            
                            Run Keyword And Expect Error        *
                            ...  Head object                         ${SYSTEM_KEY}       ${CID}    ${S_OID_USER}    ${EMPTY}             
                            Run Keyword And Expect Error        *
                            ...  Head object                         ${SYSTEM_KEY_SN}    ${CID}    ${S_OID_USER}    ${EMPTY}             
                            
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${SYSTEM_KEY}       ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${SYSTEM_KEY_SN}    ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            

                            Run Keyword And Expect Error        *
                            ...  Get Range Hash                      ${SYSTEM_KEY}       ${CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Run Keyword And Expect Error        *
                            ...  Get Range Hash                      ${SYSTEM_KEY_SN}    ${CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            

                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${SYSTEM_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}
                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${SYSTEM_KEY_SN}    ${CID}        ${S_OID_USER}            ${EMPTY}


                            Set eACL                            ${USER_KEY}     ${CID}        ${EACL_ALLOW_ALL_SYSTEM}    --await


    ${D_OID_USER_S} =       Put object to NeoFS                 ${USER_KEY}     ${FILE_S}            ${CID}            ${EMPTY}            ${FILE_USR_HEADER_DEL} 
    ${D_OID_USER_SN} =      Put object to NeoFS                 ${USER_KEY}     ${FILE_S}            ${CID}            ${EMPTY}            ${FILE_USR_HEADER_DEL} 


                            Put object to NeoFS                 ${SYSTEM_KEY}       ${FILE_S}     ${CID}            ${EMPTY}                   ${FILE_OTH_HEADER} 
                            Put object to NeoFS                 ${SYSTEM_KEY_SN}    ${FILE_S}     ${CID}            ${EMPTY}                   ${FILE_OTH_HEADER} 
                            
                            Get object from NeoFS               ${SYSTEM_KEY}       ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl
                            Get object from NeoFS               ${SYSTEM_KEY_SN}    ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl

                            Search object                       ${SYSTEM_KEY}       ${CID}        ${EMPTY}            ${EMPTY}                 ${FILE_USR_HEADER}       ${S_OBJ_H}            
                            Search object                       ${SYSTEM_KEY_SN}    ${CID}        ${EMPTY}            ${EMPTY}                 ${FILE_USR_HEADER}       ${S_OBJ_H}            
                            
                            Head object                         ${SYSTEM_KEY}       ${CID}        ${S_OID_USER}            ${EMPTY}             
                            Head object                         ${SYSTEM_KEY_SN}    ${CID}        ${S_OID_USER}            ${EMPTY}            

                            Get Range                           ${SYSTEM_KEY}       ${CID}        ${S_OID_USER}            s_get_range      ${EMPTY}            0:256
                            Get Range                           ${SYSTEM_KEY_SN}    ${CID}        ${S_OID_USER}            s_get_range      ${EMPTY}            0:256

                            Get Range Hash                      ${SYSTEM_KEY}       ${CID}        ${S_OID_USER}            ${EMPTY}            0:256
                            Get Range Hash                      ${SYSTEM_KEY_SN}    ${CID}        ${S_OID_USER}            ${EMPTY}            0:256

                            Delete object                       ${SYSTEM_KEY}       ${CID}        ${D_OID_USER_S}            ${EMPTY}
                            Delete object                       ${SYSTEM_KEY_SN}    ${CID}        ${D_OID_USER_SN}           ${EMPTY}



Check eACL Deny All Other and Allow All Pubkey

    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}            ${CID}            ${EMPTY}            ${FILE_USR_HEADER} 
    ${D_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}            ${CID}            ${EMPTY}            ${FILE_USR_HEADER_DEL} 
    @{S_OBJ_H} =	        Create List	                        ${S_OID_USER}

                            Put object to NeoFS                 ${EACL_KEY}    ${FILE_S}     ${CID}                   ${EMPTY}            ${FILE_OTH_HEADER} 
                            Get object from NeoFS               ${EACL_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl
                            Search object                       ${EACL_KEY}    ${CID}        ${EMPTY}                 ${EMPTY}            ${FILE_USR_HEADER}        ${S_OBJ_H}            
                            Head object                         ${EACL_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}             
                            Get Range                           ${EACL_KEY}    ${CID}        ${S_OID_USER}            s_get_range         ${EMPTY}            0:256
                            Get Range Hash                      ${EACL_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            0:256
                            Delete object                       ${EACL_KEY}    ${CID}        ${D_OID_USER}            ${EMPTY}

                            Set eACL                            ${USER_KEY}    ${CID}        ${EACL_ALLOW_ALL_Pubkey}    --await
                            Get eACL                            ${USER_KEY}    ${CID}

                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS                 ${OTHER_KEY}    ${FILE_S}     ${CID}            ${EMPTY}            ${FILE_USR_HEADER} 
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS               ${OTHER_KEY}    ${CID}        ${S_OID_USER}     ${EMPTY}            local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Search object                       ${OTHER_KEY}    ${CID}        ${EMPTY}          ${EMPTY}            ${FILE_USR_HEADER}      ${S_OBJ_H}            
                            Run Keyword And Expect Error        *
                            ...  Head object                         ${OTHER_KEY}    ${CID}        ${S_OID_USER}     ${EMPTY}            
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${OTHER_KEY}    ${CID}        ${S_OID_USER}     s_get_range     ${EMPTY}            0:256
                            Run Keyword And Expect Error        *
                            ...  Get Range Hash                      ${OTHER_KEY}    ${CID}        ${S_OID_USER}     ${EMPTY}        0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${OTHER_KEY}    ${CID}        ${S_OID_USER}     ${EMPTY}

                            Put object to NeoFS                 ${EACL_KEY}    ${FILE_S}     ${CID}                  ${EMPTY}            ${FILE_OTH_HEADER} 
                            Get object from NeoFS               ${EACL_KEY}    ${CID}        ${S_OID_USER}           ${EMPTY}            local_file_eacl
                            Search object                       ${EACL_KEY}    ${CID}        ${EMPTY}                ${EMPTY}            ${FILE_USR_HEADER}     ${S_OBJ_H}
                            Head object                         ${EACL_KEY}    ${CID}        ${S_OID_USER}           ${EMPTY}            
                            Get Range                           ${EACL_KEY}    ${CID}        ${S_OID_USER}           s_get_range         ${EMPTY}            0:256
                            Get Range Hash                      ${EACL_KEY}    ${CID}        ${S_OID_USER}           ${EMPTY}            0:256
                            Delete object                       ${EACL_KEY}    ${CID}        ${S_OID_USER}           ${EMPTY}


Check eACL Deny and Allow All
    [Arguments]     ${KEY}       ${DENY_EACL}    ${ALLOW_EACL}

    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}            ${CID}            ${EMPTY}            ${FILE_USR_HEADER} 
    ${D_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}            ${CID}            ${EMPTY}            ${FILE_USR_HEADER_DEL} 
    @{S_OBJ_H} =	        Create List	                        ${S_OID_USER}

                            Put object to NeoFS                 ${KEY}    ${FILE_S}            ${CID}            ${EMPTY}            ${FILE_OTH_HEADER} 
                                            
                            Get object from NeoFS               ${KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl
                            Search object                       ${KEY}    ${CID}        ${EMPTY}                 ${EMPTY}            ${FILE_USR_HEADER}    ${S_OBJ_H}            
                            Head object                         ${KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}           
                            
                            Get Range                           ${KEY}    ${CID}        ${S_OID_USER}            s_get_range       ${EMPTY}            0:256
                            Get Range Hash                      ${KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}          0:256
                            Delete object                       ${KEY}    ${CID}        ${D_OID_USER}            ${EMPTY}

                            Set eACL                            ${USER_KEY}     ${CID}        ${DENY_EACL}    --await

                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS                 ${KEY}    ${FILE_S}            ${CID}            ${EMPTY}            ${FILE_USR_HEADER} 
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS               ${KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Search object                       ${KEY}    ${CID}        ${EMPTY}                 ${EMPTY}            ${FILE_USR_HEADER}       ${S_OBJ_H}            
                            Run Keyword And Expect Error        *
                            ...  Head object                         ${KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}             
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${KEY}    ${CID}        ${S_OID_USER}            s_get_range         ${EMPTY}            0:256
                            Run Keyword And Expect Error        *
                            ...  Get Range Hash                      ${KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}

                            Set eACL                            ${USER_KEY}     ${CID}        ${ALLOW_EACL}    --await


                            Put object to NeoFS                 ${KEY}    ${FILE_S}     ${CID}              ${EMPTY}            ${FILE_OTH_HEADER} 
                            Get object from NeoFS               ${KEY}    ${CID}        ${S_OID_USER}       ${EMPTY}            local_file_eacl
                            Search object                       ${KEY}    ${CID}        ${EMPTY}            ${EMPTY}            ${FILE_USR_HEADER}     ${S_OBJ_H}            
                            Head object                         ${KEY}    ${CID}        ${S_OID_USER}       ${EMPTY}             
                            Get Range                           ${KEY}    ${CID}        ${S_OID_USER}       s_get_range          ${EMPTY}            0:256
                            Get Range Hash                      ${KEY}    ${CID}        ${S_OID_USER}       ${EMPTY}             0:256
                            Delete object                       ${KEY}    ${CID}        ${S_OID_USER}       ${EMPTY}



Cleanup
    @{CLEANUP_FILES} =      Create List	       ${FILE_S}    ${FILE_S_2}    local_file_eacl    s_get_range
                            ...                gen_eacl_allow_all_OTHERS    gen_eacl_deny_all_USER    gen_eacl_allow_all_USER
                            ...                gen_eacl_deny_all_SYSTEM    gen_eacl_allow_all_SYSTEM    gen_eacl_allow_pubkey_deny_OTHERS
                            ...                gen_eacl_deny_all_OTHERS    
                            ...                gen_eacl_compound_del_SYSTEM    gen_eacl_compound_del_USER    gen_eacl_compound_del_OTHERS
                            ...                gen_eacl_compound_get_hash_OTHERS    gen_eacl_compound_get_hash_SYSTEM    gen_eacl_compound_get_hash_USER
                            ...                gen_eacl_compound_get_OTHERS    gen_eacl_compound_get_SYSTEM    gen_eacl_compound_get_USER
                            Cleanup Files      @{CLEANUP_FILES}
                            Get Docker Logs    acl_extended
