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
BearerToken Operations
    [Documentation]         Testcase to validate NeoFS operations with BearerToken.
    [Tags]                  ACL  NeoFS  NeoCLI BearerToken
    [Timeout]               20 min

                            Generate Keys
                            Prepare eACL Role rules
    

                            Log    Check Bearer token with simple object
                            Generate file    1024
                            Check Container Inaccessible and Allow All Bearer
                            Check eACL Deny and Allow All Bearer
                            Check eACL Deny and Allow All Bearer Filter OID Equal
                            Check eACL Deny and Allow All Bearer Filter OID NotEqual
                            Check eACL Deny and Allow All Bearer Filter UserHeader Equal

                            
                            Log    Check Bearer token with complex object
                            Cleanup Files    ${FILE_S}
                            Generate file    20e+6
                            Check Container Inaccessible and Allow All Bearer
                            Check eACL Deny and Allow All Bearer
                            Check eACL Deny and Allow All Bearer Filter OID Equal
                            Check eACL Deny and Allow All Bearer Filter OID NotEqual
                            Check eACL Deny and Allow All Bearer Filter UserHeader Equal

    [Teardown]              Cleanup   
    
    
 
*** Keywords ***

Generate Keys
    ${WALLET} =             Init wallet
                            Generate wallet         ${WALLET}
    ${ADDR} =               Dump Address            ${WALLET}
    ${USER_KEY_GEN} =       Dump PrivKey            ${WALLET}           ${ADDR}            

    ${WALLET_OTH} =         Init wallet
                            Generate wallet         ${WALLET_OTH}
    ${ADDR_OTH} =           Dump Address            ${WALLET_OTH}
    ${OTHER_KEY_GEN} =      Dump PrivKey            ${WALLET_OTH}       ${ADDR_OTH}      
    

    ${EACL_KEY_GEN} =	    Form WIF from String            782676b81a35c5f07325ec523e8521ee4946b6e5d4c6cd652dd0c3ba51ce03de
    ${SYSTEM_KEY_GEN} =	    Form WIF from String            c428b4a06f166fde9f8afcf918194acdde35aa2612ecf42fe0c94273425ded21    
    ${SYSTEM_KEY_GEN_SN} =  Form WIF from String            0fa21a94be2227916284e4b3495180d9c93d04f095fe9d5a86f22044f5c411d2

                            Set Global Variable     ${USER_KEY}                  ${USER_KEY_GEN}
                            Set Global Variable     ${OTHER_KEY}                 ${OTHER_KEY_GEN}
                            Set Global Variable     ${SYSTEM_KEY}                ${SYSTEM_KEY_GEN}
                            Set Global Variable     ${SYSTEM_KEY_SN}             ${SYSTEM_KEY_GEN_SN}
                            Set Global Variable     ${EACL_KEY}                  ${EACL_KEY_GEN}

                            Payment Operations      ${WALLET}       ${ADDR}      ${USER_KEY}  
                            Payment Operations      ${WALLET_OTH}   ${ADDR_OTH}  ${OTHER_KEY}
 

Payment Operations
    [Arguments]    ${WALLET}   ${ADDR}   ${KEY}
    
    ${TX} =                 Transfer Mainnet Gas    wallets/wallet.json     NTrezR3C4X8aMLVg7vozt5wguyNfFhwuFx      ${ADDR}     55
                            Wait Until Keyword Succeeds         1 min       15 sec        
                            ...  Transaction accepted in block  ${TX}
                            Get Transaction                     ${TX}
                            Expexted Mainnet Balance            ${ADDR}     55

    ${SCRIPT_HASH} =        Get ScripHash           ${KEY}

    ${TX_DEPOSIT} =         NeoFS Deposit           ${WALLET}               ${ADDR}     ${SCRIPT_HASH}      50
                            Wait Until Keyword Succeeds         1 min          15 sec        
                            ...  Transaction accepted in block  ${TX_DEPOSIT}
                            Get Transaction                     ${TX_DEPOSIT}




Create Container Public
                            Log	                                Create Public Container
    ${PUBLIC_CID_GEN} =     Create container                    ${USER_KEY}     0x0FFFFFFF
    [Return]                ${PUBLIC_CID_GEN}


Create Container Inaccessible
                            Log	                                Create Inaccessible Container
    ${PUBLIC_CID_GEN} =     Create container                    ${USER_KEY}     0x40000000
    [Return]                ${PUBLIC_CID_GEN}



Generate file
    [Arguments]             ${SIZE}
            
    ${FILE_S_GEN} =         Generate file of bytes    ${SIZE}
                            Set Global Variable       ${FILE_S}    ${FILE_S_GEN}
 

Prepare eACL Role rules
                            Log	                    Set eACL for different Role cases
                            Set Global Variable     ${EACL_DENY_ALL_OTHER}        robot/resources/lib/eacl/eacl_encoded_deny_all
                            Set Global Variable     ${EACL_ALLOW_ALL_OTHER}       robot/resources/lib/eacl/eacl_encoded_allow_all
                                                                                  
                            Set Global Variable     ${EACL_DENY_ALL_USER}         robot/resources/lib/eacl/eacl_encoded_deny_all_user
                            Set Global Variable     ${EACL_ALLOW_ALL_USER}        robot/resources/lib/eacl/eacl_encoded_allow_all_user

                            Set Global Variable     ${EACL_DENY_ALL_SYSTEM}       robot/resources/lib/eacl/eacl_encoded_deny_all_sys
                            Set Global Variable     ${EACL_ALLOW_ALL_SYSTEM}      robot/resources/lib/eacl/eacl_encoded_allow_all_sys
                            
                            Set Global Variable     ${EACL_ALLOW_ALL_Pubkey}      robot/resources/lib/eacl/eacl_encoded_allow_all_pubkey
 


Check Container Inaccessible and Allow All Bearer
    ${CID} =                Create Container Inaccessible

                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS                 ${USER_KEY}    ${FILE_S}     ${CID}                   ${EMPTY}              ${FILE_USR_HEADER} 
                            Run Keyword And Expect Error        *
                            ...  Search object                       ${USER_KEY}    ${CID}        ${EMPTY}                 ${EMPTY}              ${FILE_USR_HEADER}
                    
                            Form BearerToken file for all ops        bearer_allow_all_user     ${USER_KEY}     ${CID}    ALLOW     USER  100500

                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS                 ${USER_KEY}    ${FILE_S}     ${CID}                   bearer_allow_all_user              ${FILE_USR_HEADER} 
                            Run Keyword And Expect Error        *
                            ...  Search object                       ${USER_KEY}    ${CID}        ${EMPTY}                 bearer_allow_all_user              ${FILE_USR_HEADER}            
                            


Check eACL Deny and Allow All Bearer
    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER} 
    ${D_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER_DEL} 
    @{S_OBJ_H} =	        Create List	                        ${S_OID_USER}

 
                            Put object to NeoFS                 ${USER_KEY}    ${FILE_S}     ${CID}                   ${EMPTY}              ${FILE_OTH_HEADER} 
                            Get object from NeoFS               ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}              local_file_eacl
                            Search object                       ${USER_KEY}    ${CID}        ${EMPTY}                 ${EMPTY}              ${FILE_USR_HEADER}         @{S_OBJ_H}            
                            Head object                         ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}               
                            Get Range                           ${USER_KEY}    ${CID}        ${S_OID_USER}            s_get_range            ${EMPTY}              0:256
                            Delete object                       ${USER_KEY}    ${CID}        ${D_OID_USER}            ${EMPTY}

                            Set eACL                            ${USER_KEY}     ${CID}        ${EACL_DENY_ALL_USER}   --await

                            Form BearerToken file for all ops        bearer_allow_all_user    ${USER_KEY}    ${CID}    ALLOW     USER  100500

                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS                 ${USER_KEY}    ${FILE_S}     ${CID}                   ${EMPTY}              ${FILE_USR_HEADER} 
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS               ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}              local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Search object                       ${USER_KEY}    ${CID}        ${EMPTY}                 ${EMPTY}              ${FILE_USR_HEADER}          @{S_OBJ_H}
                            Run Keyword And Expect Error        *
                            ...  Head object                         ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}               
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${USER_KEY}    ${CID}        ${S_OID_USER}            s_get_range            ${EMPTY}              0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}


                            Put object to NeoFS                 ${USER_KEY}    ${FILE_S}     ${CID}                   bearer_allow_all_user               ${FILE_OTH_HEADER} 
                            Get object from NeoFS               ${USER_KEY}    ${CID}        ${S_OID_USER}            bearer_allow_all_user               local_file_eacl
                            Search object                       ${USER_KEY}    ${CID}        ${EMPTY}                 bearer_allow_all_user               ${FILE_USR_HEADER}             @{S_OBJ_H}
                            Head object                         ${USER_KEY}    ${CID}        ${S_OID_USER}            bearer_allow_all_user               
                            Get Range                           ${USER_KEY}    ${CID}        ${S_OID_USER}            s_get_range            bearer_allow_all_user               0:256     
                            Delete object                       ${USER_KEY}    ${CID}        ${S_OID_USER}            bearer_allow_all_user





Check eACL Deny and Allow All Bearer Filter OID Equal
    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER} 
    ${S_OID_USER_2} =       Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${EMPTY}
    ${D_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER_DEL} 
    @{S_OBJ_H} =	        Create List	                        ${S_OID_USER}

 
                            Put object to NeoFS                 ${USER_KEY}    ${FILE_S}     ${CID}                   ${EMPTY}              ${FILE_OTH_HEADER} 
                            Get object from NeoFS               ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}              local_file_eacl
                            Search object                       ${USER_KEY}    ${CID}        ${EMPTY}                 ${EMPTY}              ${FILE_USR_HEADER}         @{S_OBJ_H}            
                            Head object                         ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}               
                            Get Range                           ${USER_KEY}    ${CID}        ${S_OID_USER}            s_get_range            ${EMPTY}              0:256
                            Delete object                       ${USER_KEY}    ${CID}        ${D_OID_USER}            ${EMPTY}

                            Set eACL                            ${USER_KEY}     ${CID}        ${EACL_DENY_ALL_USER}   --await

                            Form BearerToken file filter for all ops        bearer_allow_all_user    ${USER_KEY}    ${CID}    ALLOW     USER  100500   STRING_EQUAL   $Object:objectID  ${S_OID_USER}  

                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS                 ${USER_KEY}    ${FILE_S}     ${CID}                   ${EMPTY}              ${FILE_USR_HEADER} 
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS               ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}              local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Search object                       ${USER_KEY}    ${CID}        ${EMPTY}                 ${EMPTY}              ${FILE_USR_HEADER}          @{S_OBJ_H}
                            Run Keyword And Expect Error        *
                            ...  Head object                         ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}               
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${USER_KEY}    ${CID}        ${S_OID_USER}            s_get_range            ${EMPTY}              0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}

                            # Search is allowed without filter condition.
                            Search object                       ${USER_KEY}    ${CID}        ${EMPTY}                 bearer_allow_all_user               ${FILE_USR_HEADER}             @{S_OBJ_H}

                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS            ${USER_KEY}    ${FILE_S}     ${CID}                   bearer_allow_all_user               ${FILE_OTH_HEADER} 
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${USER_KEY}    ${CID}        ${S_OID_USER_2}          bearer_allow_all_user               local_file_eacl

                            Get object from NeoFS               ${USER_KEY}    ${CID}        ${S_OID_USER}            bearer_allow_all_user               local_file_eacl                                                                        
                            Get Range                           ${USER_KEY}    ${CID}        ${S_OID_USER}            s_get_range                         bearer_allow_all_user               0:256     
                            
                            Head object                         ${USER_KEY}    ${CID}        ${S_OID_USER}            bearer_allow_all_user               
                            Delete object                       ${USER_KEY}    ${CID}        ${D_OID_USER}            bearer_allow_all_user




Check eACL Deny and Allow All Bearer Filter OID NotEqual
    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER} 
    ${S_OID_USER_2} =       Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${EMPTY}
    ${D_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER_DEL} 
    @{S_OBJ_H} =	        Create List	                        ${S_OID_USER}

 
                            Put object to NeoFS                 ${USER_KEY}    ${FILE_S}     ${CID}                   ${EMPTY}              ${FILE_OTH_HEADER} 
                            Get object from NeoFS               ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}              local_file_eacl
                            Search object                       ${USER_KEY}    ${CID}        ${EMPTY}                 ${EMPTY}              ${FILE_USR_HEADER}         @{S_OBJ_H}            
                            Head object                         ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}               
                            Get Range                           ${USER_KEY}    ${CID}        ${S_OID_USER}            s_get_range            ${EMPTY}              0:256
                            Delete object                       ${USER_KEY}    ${CID}        ${D_OID_USER}            ${EMPTY}

                            Set eACL                            ${USER_KEY}     ${CID}        ${EACL_DENY_ALL_USER}   --await

                            Form BearerToken file filter for all ops        bearer_allow_all_user    ${USER_KEY}    ${CID}    ALLOW     USER  100500   STRING_NOT_EQUAL   $Object:objectID  ${S_OID_USER_2}  

                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS                 ${USER_KEY}    ${FILE_S}     ${CID}                   ${EMPTY}              ${FILE_USR_HEADER} 
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS               ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}              local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Search object                       ${USER_KEY}    ${CID}        ${EMPTY}                 ${EMPTY}              ${FILE_USR_HEADER}          @{S_OBJ_H}
                            Run Keyword And Expect Error        *
                            ...  Head object                         ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}               
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${USER_KEY}    ${CID}        ${S_OID_USER}            s_get_range            ${EMPTY}              0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}

                            # Search is allowed without filter condition.
                            Search object                       ${USER_KEY}    ${CID}        ${EMPTY}                 bearer_allow_all_user               ${FILE_USR_HEADER}             @{S_OBJ_H}

                            Put object to NeoFS                 ${USER_KEY}    ${FILE_S}     ${CID}                   bearer_allow_all_user               ${FILE_OTH_HEADER} 
                            
                            Get object from NeoFS               ${USER_KEY}    ${CID}        ${S_OID_USER}            bearer_allow_all_user               local_file_eacl                
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${USER_KEY}    ${CID}        ${S_OID_USER_2}          bearer_allow_all_user               local_file_eacl
                            
                            Get Range                           ${USER_KEY}    ${CID}        ${S_OID_USER}            s_get_range            bearer_allow_all_user               0:256     
                            Run Keyword And Expect Error        *
                            ...  Get Range                      ${USER_KEY}    ${CID}        ${S_OID_USER_2}          s_get_range            bearer_allow_all_user               0:256     
                            
                            Head object                         ${USER_KEY}    ${CID}        ${S_OID_USER}            bearer_allow_all_user               
                            Run Keyword And Expect Error        *
                            ...  Head object                    ${USER_KEY}    ${CID}        ${S_OID_USER_2}          bearer_allow_all_user               

                            Delete object                       ${USER_KEY}    ${CID}        ${D_OID_USER}            bearer_allow_all_user
                            
                            Run Keyword And Expect Error        *
                            ...  Delete object                  ${USER_KEY}    ${CID}        ${D_OID_USER_2}          bearer_allow_all_user



Check eACL Deny and Allow All Bearer Filter UserHeader Equal
    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER} 
    ${S_OID_USER_2} =       Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${EMPTY}
    ${D_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER_DEL} 
    @{S_OBJ_H} =	        Create List	                        ${S_OID_USER}

 
                            Put object to NeoFS                 ${USER_KEY}    ${FILE_S}     ${CID}                   ${EMPTY}              ${FILE_OTH_HEADER} 
                            Get object from NeoFS               ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}              local_file_eacl
                            Search object                       ${USER_KEY}    ${CID}        ${EMPTY}                 ${EMPTY}              ${FILE_USR_HEADER}         @{S_OBJ_H}            
                            Head object                         ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}               
                            Get Range                           ${USER_KEY}    ${CID}        ${S_OID_USER}            s_get_range            ${EMPTY}              0:256
                            Delete object                       ${USER_KEY}    ${CID}        ${D_OID_USER}            ${EMPTY}

                            Set eACL                            ${USER_KEY}     ${CID}        ${EACL_DENY_ALL_USER}   --await

                            Form BearerToken file filter for all ops        bearer_allow_all_user    ${USER_KEY}    ${CID}    ALLOW     USER  100500   STRING_EQUAL   key2    abc

                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS                 ${USER_KEY}    ${FILE_S}     ${CID}                   ${EMPTY}              ${FILE_USR_HEADER} 
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS               ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}              local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Search object                       ${USER_KEY}    ${CID}        ${EMPTY}                 ${EMPTY}              ${FILE_USR_HEADER}          @{S_OBJ_H}
                            Run Keyword And Expect Error        *
                            ...  Head object                         ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}               
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${USER_KEY}    ${CID}        ${S_OID_USER}            s_get_range            ${EMPTY}              0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}

                            # Search is allowed without filter condition.
                            Search object                       ${USER_KEY}    ${CID}        ${EMPTY}                 bearer_allow_all_user               ${FILE_USR_HEADER}             @{S_OBJ_H}

                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS            ${USER_KEY}    ${FILE_S}     ${CID}                   bearer_allow_all_user               ${FILE_OTH_HEADER} 
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${USER_KEY}    ${CID}        ${S_OID_USER_2}          bearer_allow_all_user               local_file_eacl

                            Get object from NeoFS               ${USER_KEY}    ${CID}        ${S_OID_USER}            bearer_allow_all_user               local_file_eacl                                                                        
                                
                            Run Keyword And Expect Error        *
                            ...  Get Range                       ${USER_KEY}    ${CID}        ${S_OID_USER}            s_get_range                         bearer_allow_all_user               0:256     
                            
                            Head object                         ${USER_KEY}    ${CID}        ${S_OID_USER}            bearer_allow_all_user               
                            Delete object                       ${USER_KEY}    ${CID}        ${D_OID_USER}            bearer_allow_all_user

# Check eACL Deny and Allow All Bearer Filter UserHeader NotEqual


Cleanup
    @{CLEANUP_FILES} =      Create List	     ${FILE_S}    local_file_eacl    s_get_range    bearer_allow_all_user
                            Cleanup Files    @{CLEANUP_FILES}