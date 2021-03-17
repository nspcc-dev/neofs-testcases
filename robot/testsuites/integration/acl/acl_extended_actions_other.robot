*** Settings ***
Variables                   ../../../variables/common.py
Library                     Collections
Library                     ../${RESOURCES}/neofs.py
Library                     ../${RESOURCES}/payment_neogo.py

Resource                    common_steps_acl_extended.robot
     
*** Test cases ***
Extended ACL Operations
    [Documentation]         Testcase to validate NeoFS operations with extended ACL with Other group key.
    [Tags]                  ACL  eACL  NeoFS  NeoCLI
    [Timeout]               20 min

                            Generate Keys
                            Prepare eACL Role rules

                            Log    Check extended ACL with simple object

                            Generate files    1024
                            Check eACL Deny and Allow All Other
                            
                            Cleanup Files    ${FILE_S}    ${FILE_S_2}
                            
                            Log    Check extended ACL with complex object
                            Generate files    70e+6
                            Check eACL Deny and Allow All Other
                             
                             
    [Teardown]              Cleanup  

    
*** Keywords ***

Check eACL Deny and Allow All Other
                            Check eACL Deny and Allow All    ${OTHER_KEY}    ${EACL_DENY_ALL_OTHER}    ${EACL_ALLOW_ALL_OTHER} 




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
