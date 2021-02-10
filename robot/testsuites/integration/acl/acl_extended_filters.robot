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
                            Check Filters

                            Cleanup Files    ${FILE_S}    ${FILE_S_2}
                            
                            Log    Check extended ACL with complex object
                            Generate files    70e+6
                            Check Filters
                             
    [Teardown]              Cleanup  

    
*** Keywords ***


Check Filters
                            Check eACL MatchType String Equal Object
                            Check eACL MatchType String Not Equal Object
                            Check eACL MatchType String Equal Request Deny
                            Check eACL MatchType String Equal Request Allow
   

Check eACL MatchType String Equal Request Deny
    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object             ${USER_KEY}     ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_USR_HEADER} 

    ${HEADER} =             Head object                     ${USER_KEY}     ${CID}       ${S_OID_USER}    ${EMPTY}     
    &{HEADER_DICT} =        Parse Object System Header      ${HEADER}                             
                            Get object           ${OTHER_KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}    local_file_eacl

                            
    ${ID_value} =	        Get From Dictionary	            ${HEADER_DICT}    ID   

                            Set eACL                        ${USER_KEY}    ${CID}    ${EACL_XHEADER_DENY_ALL}    --await
                                                        
                            Run Keyword And Expect Error    *
                            ...  Get object      ${OTHER_KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl    ${EMPTY}    --xhdr a=2
                            Get object           ${OTHER_KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl    ${EMPTY}    --xhdr a=256

                            Run Keyword And Expect Error    *
                            ...  Put object        ${OTHER_KEY}    ${FILE_S}     ${CID}           ${EMPTY}       ${FILE_OTH_HEADER}    ${EMPTY}      --xhdr a=2
                            Run Keyword And Expect Error    *
                            ...  Get object      ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       local_file_eacl       ${EMPTY}      --xhdr a=2
                            Run Keyword And Expect Error    *
                            ...   Search object             ${OTHER_KEY}    ${CID}        ${EMPTY}         ${EMPTY}       ${FILE_USR_HEADER}    ${EMPTY}      --xhdr a=2     
                            Run Keyword And Expect Error    *
                            ...  Head object                ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       ${EMPTY}              --xhdr a=2         
                            Run Keyword And Expect Error    *
                            ...  Get Range                  ${OTHER_KEY}    ${CID}        ${S_OID_USER}    s_get_range    ${EMPTY}              0:256         --xhdr a="2"
                            Run Keyword And Expect Error    *
                            ...  Get Range Hash             ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       0:256                 --xhdr a=2
                            Run Keyword And Expect Error    *
                            ...  Delete object              ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       --xhdr a=2

                            Put object             ${OTHER_KEY}    ${FILE_S}     ${CID}           ${EMPTY}       ${FILE_OTH_HEADER}    ${EMPTY}        --xhdr a=256
                            Get object           ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       local_file_eacl       ${EMPTY}        --xhdr a=*
                            Search object                   ${OTHER_KEY}    ${CID}        ${EMPTY}         ${EMPTY}       ${FILE_USR_HEADER}    ${EMPTY}        --xhdr a=
                            Head object                     ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       ${EMPTY}              --xhdr a=.*
                            Get Range                       ${OTHER_KEY}    ${CID}        ${S_OID_USER}    s_get_range    ${EMPTY}              0:256           --xhdr a="2 2"
                            Get Range Hash                  ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       0:256                 --xhdr a=256
                            Delete object                   ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       --xhdr a=22



Check eACL MatchType String Equal Request Allow
    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object             ${USER_KEY}     ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_USR_HEADER} 

    ${HEADER} =             Head object                     ${USER_KEY}     ${CID}       ${S_OID_USER}    ${EMPTY}     
    &{HEADER_DICT} =        Parse Object System Header      ${HEADER}                             
                            Get object           ${OTHER_KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}    local_file_eacl

                           
    ${ID_value} =	        Get From Dictionary	            ${HEADER_DICT}    ID   

                            Set eACL                        ${USER_KEY}    ${CID}    ${EACL_XHEADER_ALLOW_ALL}    --await
                            Get eACL                        ${USER_KEY}    ${CID}
                                                        
                            Run Keyword And Expect Error    *
                            ...  Get object      ${OTHER_KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl    ${EMPTY}    
                            Run Keyword And Expect Error    *
                            ...  Put object        ${OTHER_KEY}    ${FILE_S}     ${CID}           ${EMPTY}       ${FILE_OTH_HEADER}    ${EMPTY}       
                            Run Keyword And Expect Error    *
                            ...  Get object      ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       local_file_eacl       ${EMPTY}      
                            Run Keyword And Expect Error    *
                            ...   Search object             ${OTHER_KEY}    ${CID}        ${EMPTY}         ${EMPTY}       ${FILE_USR_HEADER}    ${EMPTY}         
                            Run Keyword And Expect Error    *
                            ...  Head object                ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       ${EMPTY}                    
                            Run Keyword And Expect Error    *
                            ...  Get Range                  ${OTHER_KEY}    ${CID}        ${S_OID_USER}    s_get_range    ${EMPTY}              0:256         
                            Run Keyword And Expect Error    *
                            ...  Get Range Hash             ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       0:256                  
                            Run Keyword And Expect Error    *
                            ...  Delete object              ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}        

                            Put object             ${OTHER_KEY}    ${FILE_S}     ${CID}           ${EMPTY}       ${FILE_OTH_HEADER}    ${EMPTY}        --xhdr a=2
                            Get object           ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       local_file_eacl       ${EMPTY}        --xhdr a=2
                            Search object                   ${OTHER_KEY}    ${CID}        ${EMPTY}         ${EMPTY}       ${FILE_USR_HEADER}    ${EMPTY}        --xhdr a=2
                            Head object                     ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       ${EMPTY}              --xhdr a=2
                            Get Range                       ${OTHER_KEY}    ${CID}        ${S_OID_USER}    s_get_range    ${EMPTY}              0:256           --xhdr a=2
                            Get Range Hash                  ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       0:256                 --xhdr a=2
                            Delete object                   ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       --xhdr a=2


Check eACL MatchType String Equal Object
    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object             ${USER_KEY}     ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_USR_HEADER} 

    ${HEADER} =             Head object                     ${USER_KEY}     ${CID}       ${S_OID_USER}    ${EMPTY}     
    &{HEADER_DICT} =        Parse Object System Header      ${HEADER}                             
                            Get object           ${OTHER_KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}    local_file_eacl


                            Log	                            Set eACL for Deny GET operation with StringEqual Object ID
    ${ID_value} =	        Get From Dictionary	            ${HEADER_DICT}    ID   

    ${filters} =            Create Dictionary    headerType=OBJECT    matchType=STRING_EQUAL    key=$Object:objectID    value=${ID_value}
    ${rule1} =              Create Dictionary    Operation=GET        Access=DENY               Role=OTHERS             Filters=${filters}
    ${eACL_gen} =           Create List    ${rule1}
    ${EACL_CUSTOM} =        Form eACL json common file    eacl_custom    ${eACL_gen}
                                        
                            Set eACL                        ${USER_KEY}       ${CID}    ${EACL_CUSTOM}    --await
                            Run Keyword And Expect Error    *
                            ...  Get object      ${OTHER_KEY}      ${CID}    ${S_OID_USER}     ${EMPTY}        local_file_eacl


                            Log	                            Set eACL for Deny GET operation with StringEqual Object Extended User Header     
    ${S_OID_USER_OTH} =     Put object             ${USER_KEY}     ${FILE_S}    ${CID}               ${EMPTY}        ${FILE_OTH_HEADER} 

    ${filters} =            Create Dictionary    headerType=OBJECT    matchType=STRING_EQUAL    key=key1    value=1
    ${rule1} =              Create Dictionary    Operation=GET        Access=DENY               Role=OTHERS             Filters=${filters}
    ${eACL_gen} =           Create List    ${rule1}
    ${EACL_CUSTOM} =        Form eACL json common file    eacl_custom    ${eACL_gen}
    
    
                            Set eACL                        ${USER_KEY}     ${CID}       ${EACL_CUSTOM}       --await                         
                            Run Keyword And Expect Error    *
                            ...  Get object      ${OTHER_KEY}    ${CID}       ${S_OID_USER}        ${EMPTY}        local_file_eacl
                            Get object           ${OTHER_KEY}    ${CID}       ${S_OID_USER_OTH}    ${EMPTY}        local_file_eacl
                            


Check eACL MatchType String Not Equal Object
    ${CID} =                Create Container Public
    
    ${S_OID_USER} =         Put object             ${USER_KEY}     ${FILE_S}      ${CID}    ${EMPTY}    ${FILE_USR_HEADER} 
    ${S_OID_OTHER} =        Put object             ${OTHER_KEY}    ${FILE_S_2}    ${CID}    ${EMPTY}    ${FILE_OTH_HEADER} 
    
    ${HEADER} =             Head object                     ${USER_KEY}    ${CID}    ${S_OID_USER}     ${EMPTY}     
                            Head object                     ${USER_KEY}    ${CID}    ${S_OID_OTHER}    ${EMPTY} 

    &{HEADER_DICT} =        Parse Object System Header      ${HEADER} 
                            
                            Get object           ${OTHER_KEY}    ${CID}    ${S_OID_USER}     ${EMPTY}    local_file_eacl
                            Get object           ${OTHER_KEY}    ${CID}    ${S_OID_OTHER}    ${EMPTY}    local_file_eacl
    
                            Log	                            Set eACL for Deny GET operation with StringNotEqual Object ID
    ${ID_value} =	        Get From Dictionary	            ${HEADER_DICT}    ID   

    ${filters} =            Create Dictionary    headerType=OBJECT    matchType=STRING_NOT_EQUAL    key=$Object:objectID    value=${ID_value}
    ${rule1} =              Create Dictionary    Operation=GET        Access=DENY                   Role=OTHERS             Filters=${filters}
    ${eACL_gen} =           Create List    ${rule1}
    ${EACL_CUSTOM} =        Form eACL json common file    eacl_custom    ${eACL_gen}
    
                            Set eACL                        ${USER_KEY}       ${CID}    ${EACL_CUSTOM}    --await
                            Run Keyword And Expect Error    *
                            ...  Get object      ${OTHER_KEY}      ${CID}    ${S_OID_OTHER}    ${EMPTY}            local_file_eacl
                            Get object           ${OTHER_KEY}      ${CID}    ${S_OID_USER}     ${EMPTY}            local_file_eacl


                            Log	                            Set eACL for Deny GET operation with StringEqual Object Extended User Header     
    ${S_OID_USER_OTH} =     Put object             ${USER_KEY}    ${FILE_S}    ${CID}               ${EMPTY}            ${FILE_OTH_HEADER} 

    ${filters} =            Create Dictionary    headerType=OBJECT    matchType=STRING_NOT_EQUAL    key=key1       value=1
    ${rule1} =              Create Dictionary    Operation=GET        Access=DENY                   Role=OTHERS    Filters=${filters}
    ${eACL_gen} =           Create List    ${rule1}
    ${EACL_CUSTOM} =        Form eACL json common file    eacl_custom    ${eACL_gen}                        
                            
                            Set eACL                        ${USER_KEY}    ${CID}       ${EACL_CUSTOM}       --await                         
                            Run Keyword And Expect Error    *
                            ...  Get object      ${OTHER_KEY}    ${CID}      ${S_OID_USER_OTH}    ${EMPTY}            local_file_eacl
                            Get object           ${OTHER_KEY}    ${CID}      ${S_OID_USER}        ${EMPTY}            local_file_eacl



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
