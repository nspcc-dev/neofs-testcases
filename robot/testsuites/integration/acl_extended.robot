*** Settings ***
Variables                   ../../variables/common.py
Library                     Collections
Library                     ${RESOURCES}/neofs.py
Library                     ${RESOURCES}/payment_neogo.py

*** Variables ***
${FILE_USR_HEADER} =        key1=1,key2=abc
${FILE_USR_HEADER_DEL} =    key1=del,key2=del
${FILE_OTH_HEADER} =        key1=oth,key2=oth
${RULE_FOR_ALL} =           REP 2 IN X CBF 1 SELECT 4 FROM * AS X

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
                            Check Filters
                            Check Сompound Operations  

                            
                            Cleanup Files    ${FILE_S}    ${FILE_S_2}
                            
                    #        Log    Check extended ACL with complex object
                    #        Generate files    20e+6
                    #        Check Actions
                    #        Check Filters
                             
   # [Teardown]              Cleanup  

    
*** Keywords ***

Check Actions
                            Check eACL Deny and Allow All Other
                            Check eACL Deny and Allow All User
                            Check eACL Deny and Allow All System
                            Check eACL Deny All Other and Allow All Pubkey

    
Check Filters
                            Check eACL MatchType String Equal Object
                            Check eACL MatchType String Not Equal Object
                            #Check eACL MatchType String Equal Request
                            
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
                                                       
  


Check eACL MatchType String Equal Request
    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object to NeoFS             ${USER_KEY}     ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_USR_HEADER} 

    ${HEADER} =             Head object                     ${USER_KEY}     ${CID}       ${S_OID_USER}    ${EMPTY}     
    &{HEADER_DICT} =        Parse Object System Header      ${HEADER}                             
                            Get object from NeoFS           ${OTHER_KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}    local_file_eacl


                            Log	                            Set eACL for Deny GET operation with StringEqual Object ID
    ${ID_value} =	        Get From Dictionary	            ${HEADER_DICT}    ID   
    ${EACL_CUSTOM} =        Form eACL json filter file             eacl_custom       GET       DENY              STRING_EQUAL    $Request:ttl    2    OTHERS    REQUEST
                            Set eACL                        ${USER_KEY}       ${CID}    ${EACL_CUSTOM}    --await
                            Run Keyword And Expect Error    *
                            ...  Get object from NeoFS      ${OTHER_KEY}      ${CID}    ${S_OID_USER}     ${EMPTY}        local_file_eacl


                       



Check eACL MatchType String Equal Object
    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object to NeoFS             ${USER_KEY}     ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_USR_HEADER} 

    ${HEADER} =             Head object                     ${USER_KEY}     ${CID}       ${S_OID_USER}    ${EMPTY}     
    &{HEADER_DICT} =        Parse Object System Header      ${HEADER}                             
                            Get object from NeoFS           ${OTHER_KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}    local_file_eacl


                            Log	                            Set eACL for Deny GET operation with StringEqual Object ID
    ${ID_value} =	        Get From Dictionary	            ${HEADER_DICT}    ID   
    ${EACL_CUSTOM} =        Form eACL json filter file             eacl_custom       GET       DENY              STRING_EQUAL    $Object:objectID    ${ID_value}    OTHERS
                            Set eACL                        ${USER_KEY}       ${CID}    ${EACL_CUSTOM}    --await
                            Run Keyword And Expect Error    *
                            ...  Get object from NeoFS      ${OTHER_KEY}      ${CID}    ${S_OID_USER}     ${EMPTY}        local_file_eacl


                            Log	                            Set eACL for Deny GET operation with StringEqual Object Extended User Header     
    ${S_OID_USER_OTH} =     Put object to NeoFS             ${USER_KEY}     ${FILE_S}    ${CID}               ${EMPTY}        ${FILE_OTH_HEADER} 
    ${EACL_CUSTOM} =        Form eACL json filter file             eacl_custom     GET          DENY                 STRING_EQUAL    key1                  1    OTHERS
                            Set eACL                        ${USER_KEY}     ${CID}       ${EACL_CUSTOM}       --await                         
                            Run Keyword And Expect Error    *
                            ...  Get object from NeoFS      ${OTHER_KEY}    ${CID}       ${S_OID_USER}        ${EMPTY}        local_file_eacl
                            Get object from NeoFS           ${OTHER_KEY}    ${CID}       ${S_OID_USER_OTH}    ${EMPTY}        local_file_eacl
                            


Check eACL MatchType String Not Equal Object
    ${CID} =                Create Container Public
    
    ${S_OID_USER} =         Put object to NeoFS             ${USER_KEY}     ${FILE_S}      ${CID}    ${EMPTY}    ${FILE_USR_HEADER} 
    ${S_OID_OTHER} =        Put object to NeoFS             ${OTHER_KEY}    ${FILE_S_2}    ${CID}    ${EMPTY}    ${FILE_OTH_HEADER} 
    
    ${HEADER} =             Head object                     ${USER_KEY}    ${CID}    ${S_OID_USER}     ${EMPTY}     
                            Head object                     ${USER_KEY}    ${CID}    ${S_OID_OTHER}    ${EMPTY} 

    &{HEADER_DICT} =        Parse Object System Header      ${HEADER} 
                            
                            Get object from NeoFS           ${OTHER_KEY}    ${CID}    ${S_OID_USER}     ${EMPTY}    local_file_eacl
                            Get object from NeoFS           ${OTHER_KEY}    ${CID}    ${S_OID_OTHER}    ${EMPTY}    local_file_eacl
    
                            Log	                            Set eACL for Deny GET operation with StringNotEqual Object ID
    ${ID_value} =	        Get From Dictionary	            ${HEADER_DICT}    ID   
    ${EACL_CUSTOM} =        Form eACL json filter file             eacl_custom       GET       DENY              STRING_NOT_EQUAL    $Object:objectID    ${ID_value}    OTHERS
                            Set eACL                        ${USER_KEY}       ${CID}    ${EACL_CUSTOM}    --await
                            Run Keyword And Expect Error    *
                            ...  Get object from NeoFS      ${OTHER_KEY}      ${CID}    ${S_OID_OTHER}    ${EMPTY}            local_file_eacl
                            Get object from NeoFS           ${OTHER_KEY}      ${CID}    ${S_OID_USER}     ${EMPTY}            local_file_eacl


                            Log	                            Set eACL for Deny GET operation with StringEqual Object Extended User Header     
    ${S_OID_USER_OTH} =     Put object to NeoFS             ${USER_KEY}    ${FILE_S}    ${CID}               ${EMPTY}            ${FILE_OTH_HEADER} 
    ${EACL_CUSTOM} =        Form eACL json filter file             eacl_custom    GET          DENY                 STRING_NOT_EQUAL    key1                  1    OTHERS
                            Set eACL                        ${USER_KEY}    ${CID}       ${EACL_CUSTOM}       --await                         
                            Run Keyword And Expect Error    *
                            ...  Get object from NeoFS      ${OTHER_KEY}    ${CID}      ${S_OID_USER_OTH}    ${EMPTY}            local_file_eacl
                            Get object from NeoFS           ${OTHER_KEY}    ${CID}      ${S_OID_USER}        ${EMPTY}            local_file_eacl




Generate Keys
    ${WALLET} =             Init wallet
                            Generate wallet    ${WALLET}
    ${ADDR} =               Dump Address       ${WALLET}
    ${USER_KEY_GEN} =       Dump PrivKey       ${WALLET}    ${ADDR}            

    ${WALLET_OTH} =         Init wallet
                            Generate wallet    ${WALLET_OTH}
    ${ADDR_OTH} =           Dump Address       ${WALLET_OTH}
    ${OTHER_KEY_GEN} =      Dump PrivKey       ${WALLET_OTH}       ${ADDR_OTH}      
    

    ${EACL_KEY_GEN} =	    Form WIF from String    782676b81a35c5f07325ec523e8521ee4946b6e5d4c6cd652dd0c3ba51ce03de
    ${SYSTEM_KEY_GEN} =	    Form WIF from String    c428b4a06f166fde9f8afcf918194acdde35aa2612ecf42fe0c94273425ded21    
    ${SYSTEM_KEY_GEN_SN} =  Form WIF from String    0fa21a94be2227916284e4b3495180d9c93d04f095fe9d5a86f22044f5c411d2

                            Set Global Variable     ${USER_KEY}         ${USER_KEY_GEN}
                            Set Global Variable     ${OTHER_KEY}        ${OTHER_KEY_GEN}
                            Set Global Variable     ${SYSTEM_KEY}       ${SYSTEM_KEY_GEN}
                            Set Global Variable     ${SYSTEM_KEY_SN}    ${SYSTEM_KEY_GEN_SN}
                            Set Global Variable     ${EACL_KEY}         ${EACL_KEY_GEN}

                            Payment Operations      ${WALLET}           ${ADDR}        ${USER_KEY}  
                            Payment Operations      ${WALLET_OTH}       ${ADDR_OTH}    ${OTHER_KEY}
 

Payment Operations
    [Arguments]    ${WALLET}    ${ADDR}    ${KEY}
    
    ${TX} =                 Transfer Mainnet Gas                  wallets/wallet.json    NTrezR3C4X8aMLVg7vozt5wguyNfFhwuFx    ${ADDR}    55
                            
                            Wait Until Keyword Succeeds           1 min                  15 sec        
                            ...  Transaction accepted in block    ${TX}
                            Get Transaction                       ${TX}
                            Expexted Mainnet Balance              ${ADDR}                55

    ${SCRIPT_HASH} =        Get ScripHash                         ${KEY}

    ${TX_DEPOSIT} =         NeoFS Deposit                         ${WALLET}              ${ADDR}    ${SCRIPT_HASH}    50
                            Wait Until Keyword Succeeds           1 min                  15 sec        
                            ...  Transaction accepted in block    ${TX_DEPOSIT}
                            Get Transaction                       ${TX_DEPOSIT}


Create Container Public
                            Log	                Create Public Container
    ${PUBLIC_CID_GEN} =     Create container    ${USER_KEY}    0x4FFFFFFF    ${RULE_FOR_ALL}
    [Return]                ${PUBLIC_CID_GEN}
                            
 
Generate files
    [Arguments]             ${SIZE}
    ${FILE_S_GEN_1} =       Generate file of bytes    ${SIZE}
    ${FILE_S_GEN_2} =       Generate file of bytes    ${SIZE}
                            Set Global Variable       ${FILE_S}      ${FILE_S_GEN_1}
                            Set Global Variable       ${FILE_S_2}    ${FILE_S_GEN_2}
 

Prepare eACL Role rules
                            Log	                   Set eACL for different Role cases

    # eACL rules for all operations and similar permissions
    @{Roles} =	        Create List    OTHERS    USER    SYSTEM
    FOR	${role}	IN	@{Roles}
        ${rule1}=               Create Dictionary    Opration=GET             Access=DENY    Role=${role} 
        ${rule2}=               Create Dictionary    Opration=HEAD            Access=DENY    Role=${role} 
        ${rule3}=               Create Dictionary    Opration=PUT             Access=DENY    Role=${role}  
        ${rule4}=               Create Dictionary    Opration=DELETE          Access=DENY    Role=${role} 
        ${rule5}=               Create Dictionary    Opration=SEARCH          Access=DENY    Role=${role}
        ${rule6}=               Create Dictionary    Opration=GETRANGE        Access=DENY    Role=${role}
        ${rule7}=               Create Dictionary    Opration=GETRANGEHASH    Access=DENY    Role=${role}

        ${eACL_gen}=            Create List    ${rule1}    ${rule2}    ${rule3}    ${rule4}    ${rule5}    ${rule6}    ${rule7}
                                Form eACL json common file    gen_eacl_deny_all_${role}    ${eACL_gen}
    END


    FOR	${role}	IN	@{Roles}
        ${rule1}=               Create Dictionary    Opration=GET             Access=ALLOW    Role=${role} 
        ${rule2}=               Create Dictionary    Opration=HEAD            Access=ALLOW    Role=${role} 
        ${rule3}=               Create Dictionary    Opration=PUT             Access=ALLOW    Role=${role}  
        ${rule4}=               Create Dictionary    Opration=DELETE          Access=ALLOW    Role=${role} 
        ${rule5}=               Create Dictionary    Opration=SEARCH          Access=ALLOW    Role=${role}
        ${rule6}=               Create Dictionary    Opration=GETRANGE        Access=ALLOW    Role=${role}
        ${rule7}=               Create Dictionary    Opration=GETRANGEHASH    Access=ALLOW    Role=${role}

        ${eACL_gen}=            Create List    ${rule1}    ${rule2}    ${rule3}    ${rule4}    ${rule5}    ${rule6}    ${rule7}
                                Form eACL json common file    gen_eacl_allow_all_${role}    ${eACL_gen}
    END


    ${rule1}=               Create Dictionary    Opration=GET             Access=ALLOW    Role=A9tDy6Ye+UimXCCzJrlAmRE0FDZHjf3XRyya9rELtgAA 
    ${rule2}=               Create Dictionary    Opration=HEAD            Access=ALLOW    Role=A9tDy6Ye+UimXCCzJrlAmRE0FDZHjf3XRyya9rELtgAA 
    ${rule3}=               Create Dictionary    Opration=PUT             Access=ALLOW    Role=A9tDy6Ye+UimXCCzJrlAmRE0FDZHjf3XRyya9rELtgAA 
    ${rule4}=               Create Dictionary    Opration=DELETE          Access=ALLOW    Role=A9tDy6Ye+UimXCCzJrlAmRE0FDZHjf3XRyya9rELtgAA 
    ${rule5}=               Create Dictionary    Opration=SEARCH          Access=ALLOW    Role=A9tDy6Ye+UimXCCzJrlAmRE0FDZHjf3XRyya9rELtgAA 
    ${rule6}=               Create Dictionary    Opration=GETRANGE        Access=ALLOW    Role=A9tDy6Ye+UimXCCzJrlAmRE0FDZHjf3XRyya9rELtgAA 
    ${rule7}=               Create Dictionary    Opration=GETRANGEHASH    Access=ALLOW    Role=A9tDy6Ye+UimXCCzJrlAmRE0FDZHjf3XRyya9rELtgAA 
    ${rule8}=               Create Dictionary    Opration=GET             Access=DENY     Role=OTHERS
    ${rule9}=               Create Dictionary    Opration=HEAD            Access=DENY     Role=OTHERS
    ${rule10}=              Create Dictionary    Opration=PUT             Access=DENY     Role=OTHERS 
    ${rule11}=              Create Dictionary    Opration=DELETE          Access=DENY     Role=OTHERS 
    ${rule12}=              Create Dictionary    Opration=SEARCH          Access=DENY     Role=OTHERS
    ${rule13}=              Create Dictionary    Opration=GETRANGE        Access=DENY     Role=OTHERS
    ${rule14}=              Create Dictionary    Opration=GETRANGEHASH    Access=DENY     Role=OTHERS


    ${eACL_gen}=            Create List    ${rule1}    ${rule2}    ${rule3}     ${rule4}     ${rule5}     ${rule6}     ${rule7}
                            ...            ${rule8}    ${rule9}    ${rule10}    ${rule11}    ${rule12}    ${rule13}    ${rule14}
                            Form eACL json common file    gen_eacl_allow_pubkey_deny_OTHERS    ${eACL_gen}

                            Set Global Variable    ${EACL_DENY_ALL_OTHER}      gen_eacl_deny_all_OTHERS
                            Set Global Variable    ${EACL_ALLOW_ALL_OTHER}     gen_eacl_allow_all_OTHERS
                                                                                  
                            Set Global Variable    ${EACL_DENY_ALL_USER}       gen_eacl_deny_all_USER
                            Set Global Variable    ${EACL_ALLOW_ALL_USER}      gen_eacl_allow_all_USER

                            Set Global Variable    ${EACL_DENY_ALL_SYSTEM}     gen_eacl_deny_all_SYSTEM
                            Set Global Variable    ${EACL_ALLOW_ALL_SYSTEM}    gen_eacl_allow_all_SYSTEM
                            
                            Set Global Variable    ${EACL_ALLOW_ALL_Pubkey}    gen_eacl_allow_pubkey_deny_OTHERS


    # eACL rules for Compound operations: GET/GetRange/GetRangeHash
    @{Roles} =	        Create List    OTHERS    USER    SYSTEM
    FOR	${role}	IN	@{Roles}
        ${rule1}=               Create Dictionary    Opration=GET             Access=ALLOW    Role=${role} 
        ${rule2}=               Create Dictionary    Opration=GETRANGE        Access=ALLOW    Role=${role} 
        ${rule3}=               Create Dictionary    Opration=GETRANGEHASH    Access=ALLOW    Role=${role}  
        ${rule4}=               Create Dictionary    Opration=HEAD            Access=DENY     Role=${role}
        ${eACL_gen}=            Create List    ${rule1}    ${rule2}    ${rule3}    ${rule4}
                                Form eACL json common file    gen_eacl_compound_get_${role}    ${eACL_gen}
                                Set Global Variable    ${EACL_COMPOUND_GET_${role}}    gen_eacl_compound_get_${role}
    END

    # eACL rules for Compound operations: Delete
    @{Roles} =	        Create List    OTHERS    USER    SYSTEM
    FOR	${role}	IN	@{Roles}
        ${rule1}=               Create Dictionary    Opration=DELETE          Access=ALLOW    Role=${role}  
        ${rule2}=               Create Dictionary    Opration=PUT             Access=DENY     Role=${role}   
        ${rule3}=               Create Dictionary    Opration=HEAD            Access=DENY     Role=${role}  
        ${eACL_gen}=            Create List    ${rule1}    ${rule2}    ${rule3}   
                                Form eACL json common file    gen_eacl_compound_del_${role}    ${eACL_gen}
                                Set Global Variable    ${EACL_COMPOUND_DELETE_${role}}    gen_eacl_compound_del_${role}
    END

    # eACL rules for Compound operations: Delete
    @{Roles} =	        Create List    OTHERS    USER    SYSTEM
    FOR	${role}	IN	@{Roles}
        ${rule1}=               Create Dictionary    Opration=GETRANGEHASH    Access=ALLOW    Role=${role}  
        ${rule2}=               Create Dictionary    Opration=GETRANGE        Access=DENY     Role=${role}   
        ${rule3}=               Create Dictionary    Opration=GET             Access=DENY     Role=${role}  
        ${eACL_gen}=            Create List    ${rule1}    ${rule2}    ${rule3}   
                                Form eACL json common file    gen_eacl_compound_get_hash_${role}    ${eACL_gen}
                                Set Global Variable    ${EACL_COMPOUND_GET_HASH_${role}}    gen_eacl_compound_get_hash_${role}
    END




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

                            Search object            ${SYSTEM_KEY}       ${CID}    ${EMPTY}    ${EMPTY}    ${FILE_USR_HEADER}    @{S_OBJ_H}            
                            Search object            ${SYSTEM_KEY_SN}    ${CID}    ${EMPTY}    ${EMPTY}    ${FILE_USR_HEADER}    @{S_OBJ_H}            

                            Head object              ${SYSTEM_KEY}       ${CID}    ${S_OID_USER}    ${EMPTY}             
                            Head object              ${SYSTEM_KEY_SN}    ${CID}    ${S_OID_USER}    ${EMPTY}             

                            Get Range                ${SYSTEM_KEY}       ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Get Range                ${SYSTEM_KEY_SN}    ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256

                            Get Range Hash           ${SYSTEM_KEY}       ${CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Get Range Hash           ${SYSTEM_KEY_SN}    ${CID}    ${S_OID_USER}    ${EMPTY}    0:256
                             
                            Delete object            ${SYSTEM_KEY}       ${CID}    ${D_OID_USER_S}     ${EMPTY}
                            Delete object            ${SYSTEM_KEY_SN}    ${CID}    ${D_OID_USER_SN}    ${EMPTY}


                            Set eACL                 ${USER_KEY}     ${CID}        ${EACL_DENY_ALL_SYSTEM}
                            Sleep                    ${MORPH_BLOCK_TIMEOUT}
 


                            Run Keyword And Expect Error    *
                            ...  Put object to NeoFS        ${SYSTEM_KEY}       ${FILE_S}    ${CID}    ${EMPTY}    ${FILE_OTH_HEADER} 
                            Run Keyword And Expect Error    *
                            ...  Put object to NeoFS        ${SYSTEM_KEY_SN}    ${FILE_S}    ${CID}    ${EMPTY}    ${FILE_OTH_HEADER} 

                            Run Keyword And Expect Error    *
                            ...  Get object from NeoFS      ${SYSTEM_KEY}       ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl
                            Run Keyword And Expect Error    *
                            ...  Get object from NeoFS      ${SYSTEM_KEY_SN}    ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl
                            
                            Run Keyword And Expect Error    *
                            ...  Search object              ${SYSTEM_KEY}       ${CID}    ${EMPTY}    ${EMPTY}    ${FILE_USR_HEADER}    @{S_OBJ_H}            
                            Run Keyword And Expect Error    *
                            ...  Search object              ${SYSTEM_KEY_SN}    ${CID}    ${EMPTY}    ${EMPTY}    ${FILE_USR_HEADER}    @{S_OBJ_H}            

                            
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


                            Set eACL                            ${USER_KEY}     ${CID}        ${EACL_ALLOW_ALL_SYSTEM}
                            Sleep                               ${MORPH_BLOCK_TIMEOUT}

    ${D_OID_USER_S} =       Put object to NeoFS                 ${USER_KEY}     ${FILE_S}            ${CID}            ${EMPTY}            ${FILE_USR_HEADER_DEL} 
    ${D_OID_USER_SN} =      Put object to NeoFS                 ${USER_KEY}     ${FILE_S}            ${CID}            ${EMPTY}            ${FILE_USR_HEADER_DEL} 


                            Put object to NeoFS                 ${SYSTEM_KEY}       ${FILE_S}     ${CID}            ${EMPTY}                   ${FILE_OTH_HEADER} 
                            Put object to NeoFS                 ${SYSTEM_KEY_SN}    ${FILE_S}     ${CID}            ${EMPTY}                   ${FILE_OTH_HEADER} 
                            
                            Get object from NeoFS               ${SYSTEM_KEY}       ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl
                            Get object from NeoFS               ${SYSTEM_KEY_SN}    ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl

                            Search object                       ${SYSTEM_KEY}       ${CID}        ${EMPTY}            ${EMPTY}                 ${FILE_USR_HEADER}       @{S_OBJ_H}            
                            Search object                       ${SYSTEM_KEY_SN}    ${CID}        ${EMPTY}            ${EMPTY}                 ${FILE_USR_HEADER}       @{S_OBJ_H}            
                            
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
                            Search object                       ${EACL_KEY}    ${CID}        ${EMPTY}                 ${EMPTY}            ${FILE_USR_HEADER}        @{S_OBJ_H}            
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
                            ...  Search object                       ${OTHER_KEY}    ${CID}        ${EMPTY}          ${EMPTY}            ${FILE_USR_HEADER}      @{S_OBJ_H}            
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
                            Search object                       ${EACL_KEY}    ${CID}        ${EMPTY}                ${EMPTY}            ${FILE_USR_HEADER}     @{S_OBJ_H}
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
                            Search object                       ${KEY}    ${CID}        ${EMPTY}            ${EMPTY}                 ${FILE_USR_HEADER}    @{S_OBJ_H}            
                            Head object                         ${KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}           
                            
                            Get Range                           ${KEY}    ${CID}        ${S_OID_USER}            s_get_range       ${EMPTY}            0:256
                            Get Range Hash                      ${KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}          0:256
                            Delete object                       ${KEY}    ${CID}        ${D_OID_USER}            ${EMPTY}

                            Set eACL                            ${USER_KEY}     ${CID}        ${DENY_EACL}
                            Sleep                               ${MORPH_BLOCK_TIMEOUT}

                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS                 ${KEY}    ${FILE_S}            ${CID}            ${EMPTY}            ${FILE_USR_HEADER} 
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS               ${KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Search object                       ${KEY}    ${CID}        ${EMPTY}            ${EMPTY}                 ${FILE_USR_HEADER}       @{S_OBJ_H}            
                            Run Keyword And Expect Error        *
                            ...  Head object                         ${KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}             
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${KEY}    ${CID}        ${S_OID_USER}            s_get_range         ${EMPTY}            0:256
                            Run Keyword And Expect Error        *
                            ...  Get Range Hash                      ${KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}

                            Set eACL                            ${USER_KEY}     ${CID}        ${ALLOW_EACL}
                            Sleep                               ${MORPH_BLOCK_TIMEOUT}

                            Put object to NeoFS                 ${KEY}    ${FILE_S}     ${CID}              ${EMPTY}            ${FILE_OTH_HEADER} 
                            Get object from NeoFS               ${KEY}    ${CID}        ${S_OID_USER}       ${EMPTY}            local_file_eacl
                            Search object                       ${KEY}    ${CID}        ${EMPTY}            ${EMPTY}            ${FILE_USR_HEADER}     @{S_OBJ_H}            
                            Head object                         ${KEY}    ${CID}        ${S_OID_USER}       ${EMPTY}             
                            Get Range                           ${KEY}    ${CID}        ${S_OID_USER}       s_get_range          ${EMPTY}            0:256
                            Get Range Hash                      ${KEY}    ${CID}        ${S_OID_USER}       ${EMPTY}             0:256
                            Delete object                       ${KEY}    ${CID}        ${S_OID_USER}       ${EMPTY}

Cleanup
    @{CLEANUP_FILES} =      Create List	       ${FILE_S}    ${FILE_S_2}    local_file_eacl    eacl_custom    s_get_range
                            Cleanup Files      @{CLEANUP_FILES}
                            Get Docker Logs    acl_extended