*** Settings ***
Variables   ../../variables/common.py

Library     Collections

Library     ${RESOURCES}/neofs.py
Library     ${RESOURCES}/payment_neogo.py

*** Variables ***
${FILE_USR_HEADER} =        key1=1,key2=abc
${FILE_USR_HEADER_DEL} =    key1=del,key2=del
${FILE_OTH_HEADER} =        key1=oth,key2=oth
${RULE_FOR_ALL} =           REP 2 IN X CBF 1 SELECT 4 FROM * AS X

*** Test cases ***
Extended ACL Operations
    [Documentation]     Testcase to validate NeoFS operations with extended ACL.
    [Tags]              ACL  eACL  NeoFS  NeoCLI
    [Timeout]           20 min

    Generate Keys
    Generate file
    Prepare eACL Role rules
    
    Check Actions
    Check Filters
    
    
 
*** Keywords ***

Check Actions
    Check eACL Deny and Allow All Other
    Check eACL Deny and Allow All User
    Check eACL Deny and Allow All System
    # Issue https://github.com/nspcc-dev/neofs-node/issues/224
    # Check eACL Deny All Other and Allow All Pubkey

    
Check Filters
    Check eACL MatchType String Equal
    Check eACL MatchType String Not Equal


Check eACL MatchType String Equal
    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object to NeoFS             ${USER_KEY}     ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_USR_HEADER} 

    ${HEADER} =             Head object                     ${USER_KEY}     ${CID}       ${S_OID_USER}    ${EMPTY}     
    &{HEADER_DICT} =        Parse Object System Header      ${HEADER}                             
                            Get object from NeoFS           ${OTHER_KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}    local_file_eacl


                            Log	                            Set eACL for Deny GET operation with StringEqual Object ID
    ${ID_value} =	        Get From Dictionary	            ${HEADER_DICT}    ID   
    ${EACL_CUSTOM} =        Form eACL json file             eacl_custom       GET       DENY              STRING_EQUAL    $Object:objectID    ${ID_value}    OTHERS
                            Set eACL                        ${USER_KEY}       ${CID}    ${EACL_CUSTOM}    --await
                            Run Keyword And Expect Error    *
                            ...  Get object from NeoFS      ${OTHER_KEY}      ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl


                            Log	                            Set eACL for Deny GET operation with StringEqual Object Extended User Header     
    ${S_OID_USER_OTH} =     Put object to NeoFS             ${USER_KEY}     ${FILE_S}    ${CID}               ${EMPTY}        ${FILE_OTH_HEADER} 
    ${EACL_CUSTOM} =        Form eACL json file             eacl_custom     GET          DENY                 STRING_EQUAL    key1                  1    OTHERS
                            Set eACL                        ${USER_KEY}     ${CID}       ${EACL_CUSTOM}       --await                         
                            Run Keyword And Expect Error    *
                            ...  Get object from NeoFS      ${OTHER_KEY}    ${CID}       ${S_OID_USER}        ${EMPTY}        local_file_eacl
                            Get object from NeoFS           ${OTHER_KEY}    ${CID}       ${S_OID_USER_OTH}    ${EMPTY}        local_file_eacl



Check eACL MatchType String Not Equal
    ${CID} =                Create Container Public
    ${FILE_S_2} =           Generate file of bytes          2048
    
    ${S_OID_USER} =         Put object to NeoFS             ${USER_KEY}     ${FILE_S}      ${CID}    ${EMPTY}    ${FILE_USR_HEADER} 
    ${S_OID_OTHER} =        Put object to NeoFS             ${OTHER_KEY}    ${FILE_S_2}    ${CID}    ${EMPTY}    ${FILE_OTH_HEADER} 
    
    ${HEADER} =             Head object                     ${USER_KEY}    ${CID}    ${S_OID_USER}     ${EMPTY}     
                            Head object                     ${USER_KEY}    ${CID}    ${S_OID_OTHER}    ${EMPTY} 

    &{HEADER_DICT} =  Parse Object System Header      ${HEADER} 
                            
                            Get object from NeoFS           ${OTHER_KEY}    ${CID}    ${S_OID_USER}     ${EMPTY}    local_file_eacl
                            Get object from NeoFS           ${OTHER_KEY}    ${CID}    ${S_OID_OTHER}    ${EMPTY}    local_file_eacl
    
                            Log	                            Set eACL for Deny GET operation with StringNotEqual Object ID
    ${ID_value} =	        Get From Dictionary	            ${HEADER_DICT}    ID   
    ${EACL_CUSTOM} =        Form eACL json file             eacl_custom       GET       DENY              STRING_NOT_EQUAL    $Object:objectID    ${ID_value}    OTHERS
                            Set eACL                        ${USER_KEY}       ${CID}    ${EACL_CUSTOM}    --await
                            Run Keyword And Expect Error    *
                            ...  Get object from NeoFS      ${OTHER_KEY}      ${CID}    ${S_OID_OTHER}    ${EMPTY}            local_file_eacl
                            Get object from NeoFS           ${OTHER_KEY}      ${CID}    ${S_OID_USER}     ${EMPTY}            local_file_eacl


                            Log	                            Set eACL for Deny GET operation with StringEqual Object Extended User Header     
    ${S_OID_USER_OTH} =     Put object to NeoFS             ${USER_KEY}    ${FILE_S}    ${CID}               ${EMPTY}            ${FILE_OTH_HEADER} 
    ${EACL_CUSTOM} =        Form eACL json file             eacl_custom    GET          DENY                 STRING_NOT_EQUAL    key1                  1    OTHERS
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
                            
 
Generate file
    ${FILE_S_GEN} =         Generate file of bytes    1024
                            Set Global Variable       ${FILE_S}    ${FILE_S_GEN}
 

Prepare eACL Role rules
                            Log	                   Set eACL for different Role cases
                            Set Global Variable    ${EACL_DENY_ALL_OTHER}      robot/resources/lib/eacl/eacl_encoded_deny_all
                            Set Global Variable    ${EACL_ALLOW_ALL_OTHER}     robot/resources/lib/eacl/eacl_encoded_allow_all
                                                                                  
                            Set Global Variable    ${EACL_DENY_ALL_USER}       robot/resources/lib/eacl/eacl_encoded_deny_all_user
                            Set Global Variable    ${EACL_ALLOW_ALL_USER}      robot/resources/lib/eacl/eacl_encoded_allow_all_user

                            Set Global Variable    ${EACL_DENY_ALL_SYSTEM}     robot/resources/lib/eacl/eacl_encoded_deny_all_sys
                            Set Global Variable    ${EACL_ALLOW_ALL_SYSTEM}    robot/resources/lib/eacl/eacl_encoded_allow_all_sys
                            
                            Set Global Variable    ${EACL_ALLOW_ALL_Pubkey}    robot/resources/lib/eacl/eacl_encoded_allow_all_pubkey
 

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
                            ...  Head object                         ${SYSTEM_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}             
                            Run Keyword And Expect Error        *
                            ...  Head object                         ${SYSTEM_KEY_SN}    ${CID}        ${S_OID_USER}            ${EMPTY}             
                            
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${SYSTEM_KEY}    ${CID}        ${S_OID_USER}           s_get_range      ${EMPTY}            0:256
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${SYSTEM_KEY_SN}    ${CID}        ${S_OID_USER}        s_get_range    ${EMPTY}            0:256
                            
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

                            Delete object                       ${SYSTEM_KEY}       ${CID}        ${D_OID_USER_S}            ${EMPTY}
                            Delete object                       ${SYSTEM_KEY_SN}    ${CID}        ${D_OID_USER_SN}            ${EMPTY}



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
                            ...  Delete object                       ${OTHER_KEY}    ${CID}        ${S_OID_USER}     ${EMPTY}

                            Put object to NeoFS                 ${EACL_KEY}    ${FILE_S}     ${CID}                  ${EMPTY}            ${FILE_OTH_HEADER} 
                            Get object from NeoFS               ${EACL_KEY}    ${CID}        ${S_OID_USER}           ${EMPTY}            local_file_eacl
                            Search object                       ${EACL_KEY}    ${CID}        ${EMPTY}                ${EMPTY}            ${FILE_USR_HEADER}     @{S_OBJ_H}
                            Head object                         ${EACL_KEY}    ${CID}        ${S_OID_USER}           ${EMPTY}            
                            Get Range                           ${EACL_KEY}    ${CID}        ${S_OID_USER}           s_get_range         ${EMPTY}            0:256
                            Delete object                       ${EACL_KEY}    ${CID}        ${D_OID_USER}           ${EMPTY}


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
                            ...  Delete object                       ${KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}


                            Set eACL                            ${USER_KEY}     ${CID}        ${ALLOW_EACL}
                            Sleep                               ${MORPH_BLOCK_TIMEOUT}


                            Put object to NeoFS                 ${KEY}    ${FILE_S}     ${CID}              ${EMPTY}            ${FILE_OTH_HEADER} 
                            Get object from NeoFS               ${KEY}    ${CID}        ${S_OID_USER}       ${EMPTY}            local_file_eacl
                            Search object                       ${KEY}    ${CID}        ${EMPTY}            ${EMPTY}            ${FILE_USR_HEADER}     @{S_OBJ_H}            
                            Head object                         ${KEY}    ${CID}        ${S_OID_USER}       ${EMPTY}             
                            Get Range                           ${KEY}    ${CID}        ${S_OID_USER}       s_get_range          ${EMPTY}            0:256
                            Delete object                       ${KEY}    ${CID}        ${S_OID_USER}       ${EMPTY}

