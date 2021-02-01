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
                            Check Сompound Operations  

                            Cleanup Files    ${FILE_S}    ${FILE_S_2}
                            
                            Log    Check extended ACL with complex object
                            Generate files    10e+6
                            Check Сompound Operations
                             
    [Teardown]              Cleanup  

    
*** Keywords ***


                            
Check Сompound Operations         
                            Check eACL Сompound Get    ${OTHER_KEY}     ${EACL_COMPOUND_GET_OTHERS}     
                            Check eACL Сompound Get    ${USER_KEY}      ${EACL_COMPOUND_GET_USER}       
                            Check eACL Сompound Get    ${SYSTEM_KEY}    ${EACL_COMPOUND_GET_SYSTEM}     

                            Check eACL Сompound Delete    ${OTHER_KEY}     ${EACL_COMPOUND_DELETE_OTHERS}     
                            Check eACL Сompound Delete    ${USER_KEY}      ${EACL_COMPOUND_DELETE_USER}       
                            Check eACL Сompound Delete    ${SYSTEM_KEY}    ${EACL_COMPOUND_DELETE_SYSTEM}   

                            Check eACL Сompound Get Range Hash    ${OTHER_KEY}     ${EACL_COMPOUND_GET_HASH_OTHERS}     
                            Check eACL Сompound Get Range Hash    ${USER_KEY}      ${EACL_COMPOUND_GET_HASH_USER}       
                            Check eACL Сompound Get Range Hash    ${SYSTEM_KEY}    ${EACL_COMPOUND_GET_HASH_SYSTEM} 

Check eACL Сompound Get
    [Arguments]             ${KEY}    ${DENY_EACL}     

    ${CID} =                Create Container Public

    ${S_OID_USER} =         Put object to NeoFS             ${USER_KEY}    ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_USR_HEADER} 
                            Put object to NeoFS             ${KEY}         ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_OTH_HEADER} 
                            Get object from NeoFS           ${KEY}         ${CID}       ${S_OID_USER}    ${EMPTY}    local_file_eacl
                            Set eACL                        ${USER_KEY}    ${CID}       ${DENY_EACL}     --await
                            
                            Run Keyword And Expect Error    *
                            ...  Head object                ${KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}             
                            
                            Get object from NeoFS           ${KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}       local_file_eacl
                            Get Range                       ${KEY}    ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}           0:256
                            Get Range Hash                  ${KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}       0:256


Check eACL Сompound Delete
    [Arguments]             ${KEY}    ${DENY_EACL}    

    ${CID} =                Create Container Public

    ${S_OID_USER} =         Put object to NeoFS             ${USER_KEY}    ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_USR_HEADER}
    ${D_OID_USER} =         Put object to NeoFS             ${USER_KEY}    ${FILE_S}    ${CID}           ${EMPTY}    ${EMPTY}
                            Put object to NeoFS             ${KEY}         ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_OTH_HEADER} 
                            Delete object                   ${KEY}         ${CID}       ${D_OID_USER}    ${EMPTY}
                            
                            Set eACL                        ${USER_KEY}    ${CID}       ${DENY_EACL}     --await
                            
                            Run Keyword And Expect Error    *
                            ...  Head object                ${KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}     
                            Run Keyword And Expect Error    *
                            ...  Put object to NeoFS        ${KEY}    ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_OTH_HEADER}   

                            Delete object                   ${KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}



Check eACL Сompound Get Range Hash
    [Arguments]             ${KEY}    ${DENY_EACL}    

    ${CID} =                Create Container Public

    ${S_OID_USER} =         Put object to NeoFS             ${USER_KEY}         ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_USR_HEADER}
                            Put object to NeoFS             ${KEY}              ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_OTH_HEADER} 
                            Get Range Hash                  ${SYSTEM_KEY_SN}    ${CID}       ${S_OID_USER}    ${EMPTY}    0:256

                            Set eACL                        ${USER_KEY}         ${CID}       ${DENY_EACL}     --await
                            
                            Run Keyword And Expect Error    *
                            ...  Get Range                  ${KEY}    ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Run Keyword And Expect Error    *
                            ...  Get object from NeoFS      ${KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}       local_file_eacl
                            
                            Get Range Hash                  ${KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}       0:256
                                                       
  

Cleanup
    @{CLEANUP_FILES} =      Create List	       ${FILE_S}    ${FILE_S_2}    local_file_eacl    eacl_custom    s_get_range
                            ...                gen_eacl_allow_all_OTHERS    gen_eacl_deny_all_USER    gen_eacl_allow_all_USER
                            ...                gen_eacl_deny_all_SYSTEM    gen_eacl_allow_all_SYSTEM    gen_eacl_allow_pubkey_deny_OTHERS
                            ...                gen_eacl_deny_all_OTHERS    
                            ...                gen_eacl_compound_del_SYSTEM    gen_eacl_compound_del_USER    gen_eacl_compound_del_OTHERS
                            ...                gen_eacl_compound_get_hash_OTHERS    gen_eacl_compound_get_hash_SYSTEM    gen_eacl_compound_get_hash_USER
                            ...                gen_eacl_compound_get_OTHERS    gen_eacl_compound_get_SYSTEM    gen_eacl_compound_get_USER
                            Cleanup Files      @{CLEANUP_FILES}
                            Get Docker Logs    acl_extended
