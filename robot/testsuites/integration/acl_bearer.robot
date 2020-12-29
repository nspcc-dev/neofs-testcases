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
                            Check eACL Deny and Allow All Bearer Filter UserHeader NotEqual
                            Check eACL Allow All Bearer Filter Requst Equal Deny
                            Check eACL Deny and Allow All Bearer Filter Requst Equal
                            Check eACL Deny and Allow All Bearer Filter Requst NotEqual
                            Check Сompound Operations

                            # TODO:

                            Log    Check Bearer token with complex object
                            Cleanup Files    ${FILE_S}
                            Generate file    10e+6
                            Check Container Inaccessible and Allow All Bearer
                            Check eACL Deny and Allow All Bearer
                            Check eACL Deny and Allow All Bearer Filter OID Equal
                            Check eACL Deny and Allow All Bearer Filter OID NotEqual
                            Check eACL Deny and Allow All Bearer Filter UserHeader Equal
                            Check eACL Deny and Allow All Bearer Filter UserHeader NotEqual
                            Check eACL Deny and Allow All Bearer Filter Requst Equal
                            Check eACL Deny and Allow All Bearer Filter Requst NotEqual
                            Check Сompound Operations

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
    

    ${EACL_KEY_GEN} =	    Form WIF from String    782676b81a35c5f07325ec523e8521ee4946b6e5d4c6cd652dd0c3ba51ce03de
    ${SYSTEM_KEY_GEN} =	    Form WIF from String    c428b4a06f166fde9f8afcf918194acdde35aa2612ecf42fe0c94273425ded21    
    ${SYSTEM_KEY_GEN_SN} =  Form WIF from String    0fa21a94be2227916284e4b3495180d9c93d04f095fe9d5a86f22044f5c411d2

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
                            Log	                  Create Public Container
    ${PUBLIC_CID_GEN} =     Create container      ${USER_KEY}    0x0FFFFFFF
    [Return]                ${PUBLIC_CID_GEN} 


Create Container Inaccessible
                            Log	                  Create Inaccessible Container
    ${PUBLIC_CID_GEN} =     Create container      ${USER_KEY}     0x40000000
    [Return]                ${PUBLIC_CID_GEN}


Generate file
    [Arguments]             ${SIZE}
            
    ${FILE_S_GEN} =         Generate file of bytes    ${SIZE}
                            Set Global Variable       ${FILE_S}    ${FILE_S_GEN}
 

Prepare eACL Role rules
                            Log	                    Set eACL for different Role cases

    # eACL rules for all operations and similar permissions
    @{Roles} =	        Create List    OTHERS    USER    SYSTEM
    FOR	${role}	IN	@{Roles}
        ${rule1} =              Create Dictionary    Operation=GET             Access=DENY    Role=${role} 
        ${rule2} =              Create Dictionary    Operation=HEAD            Access=DENY    Role=${role} 
        ${rule3} =              Create Dictionary    Operation=PUT             Access=DENY    Role=${role}  
        ${rule4} =              Create Dictionary    Operation=DELETE          Access=DENY    Role=${role} 
        ${rule5} =              Create Dictionary    Operation=SEARCH          Access=DENY    Role=${role}
        ${rule6} =              Create Dictionary    Operation=GETRANGE        Access=DENY    Role=${role}
        ${rule7} =              Create Dictionary    Operation=GETRANGEHASH    Access=DENY    Role=${role}

        ${eACL_gen} =           Create List    ${rule1}    ${rule2}    ${rule3}    ${rule4}    ${rule5}    ${rule6}    ${rule7}
                                Form eACL json common file    gen_eacl_deny_all_${role}    ${eACL_gen}
                                Set Global Variable    ${EACL_DENY_ALL_${role}}       gen_eacl_deny_all_${role}
    END


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
            

Check eACL Deny and Allow All Bearer
    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object to NeoFS                 ${USER_KEY}    ${FILE_S}    ${CID}    ${EMPTY}    ${FILE_USR_HEADER} 
    ${D_OID_USER} =         Put object to NeoFS                 ${USER_KEY}    ${FILE_S}    ${CID}    ${EMPTY}    ${FILE_USR_HEADER_DEL} 
    @{S_OBJ_H} =	        Create List	                        ${S_OID_USER}

 
                            Put object to NeoFS                 ${USER_KEY}    ${FILE_S}     ${CID}           ${EMPTY}       ${FILE_OTH_HEADER} 
                            Get object from NeoFS               ${USER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       local_file_eacl
                            Search object                       ${USER_KEY}    ${CID}        ${EMPTY}         ${EMPTY}       ${FILE_USR_HEADER}    ${S_OBJ_H}            
                            Head object                         ${USER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}               
                            Get Range                           ${USER_KEY}    ${CID}        ${S_OID_USER}    s_get_range    ${EMPTY}              0:256
                            Delete object                       ${USER_KEY}    ${CID}        ${D_OID_USER}    ${EMPTY}

                            Set eACL                            ${USER_KEY}    ${CID}        ${EACL_DENY_ALL_USER}    --await


    ${rule1}=               Create Dictionary    Operation=GET             Access=ALLOW    Role=USER 
    ${rule2}=               Create Dictionary    Operation=HEAD            Access=ALLOW    Role=USER 
    ${rule3}=               Create Dictionary    Operation=PUT             Access=ALLOW    Role=USER  
    ${rule4}=               Create Dictionary    Operation=DELETE          Access=ALLOW    Role=USER  
    ${rule5}=               Create Dictionary    Operation=SEARCH          Access=ALLOW    Role=USER 
    ${rule6}=               Create Dictionary    Operation=GETRANGE        Access=ALLOW    Role=USER  
    ${rule7}=               Create Dictionary    Operation=GETRANGEHASH    Access=ALLOW    Role=USER 

    ${eACL_gen}=            Create List    ${rule1}    ${rule2}    ${rule3}    ${rule4}    ${rule5}    ${rule6}    ${rule7}

                            Form BearerToken file               ${USER_KEY}    ${CID}    bearer_allow_all_user   ${eACL_gen}   100500

                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS            ${USER_KEY}    ${FILE_S}    ${CID}           ${EMPTY}       ${FILE_USR_HEADER} 
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${USER_KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}       local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Search object                  ${USER_KEY}    ${CID}       ${EMPTY}         ${EMPTY}       ${FILE_USR_HEADER}    ${S_OBJ_H}
                            Run Keyword And Expect Error        *
                            ...  Head object                    ${USER_KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}               
                            Run Keyword And Expect Error        *
                            ...  Get Range                      ${USER_KEY}    ${CID}       ${S_OID_USER}    s_get_range    ${EMPTY}              0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                  ${USER_KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}

                            Put object to NeoFS                 ${USER_KEY}    ${FILE_S}    ${CID}           bearer_allow_all_user    ${FILE_OTH_HEADER} 
                            Get object from NeoFS               ${USER_KEY}    ${CID}       ${S_OID_USER}    bearer_allow_all_user    local_file_eacl
                            Search object                       ${USER_KEY}    ${CID}       ${EMPTY}         bearer_allow_all_user    ${FILE_USR_HEADER}       ${S_OBJ_H}
                            Head object                         ${USER_KEY}    ${CID}       ${S_OID_USER}    bearer_allow_all_user               
                            Get Range                           ${USER_KEY}    ${CID}       ${S_OID_USER}    s_get_range              bearer_allow_all_user    0:256     
                            Delete object                       ${USER_KEY}    ${CID}       ${S_OID_USER}    bearer_allow_all_user


Check eACL Deny and Allow All Bearer Filter OID Equal
    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER} 
    ${S_OID_USER_2} =       Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${EMPTY}
    ${D_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER_DEL} 
    @{S_OBJ_H} =	        Create List	                        ${S_OID_USER}

                            Put object to NeoFS                 ${USER_KEY}    ${FILE_S}     ${CID}                   ${EMPTY}              ${FILE_OTH_HEADER} 
                            Get object from NeoFS               ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}              local_file_eacl
                            Search object                       ${USER_KEY}    ${CID}        ${EMPTY}                 ${EMPTY}              ${FILE_USR_HEADER}         ${S_OBJ_H}            
                            Head object                         ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}               
                            Get Range                           ${USER_KEY}    ${CID}        ${S_OID_USER}            s_get_range            ${EMPTY}              0:256
                            Delete object                       ${USER_KEY}    ${CID}        ${D_OID_USER}            ${EMPTY}

                            Set eACL                            ${USER_KEY}    ${CID}        ${EACL_DENY_ALL_USER}    --await

    ${filters}=             Create Dictionary    headerType=OBJECT    matchType=STRING_EQUAL    key=$Object:objectID    value=${S_OID_USER} 

    ${rule1}=               Create Dictionary    Operation=GET             Access=ALLOW    Role=USER    Filters=${filters}
    ${rule2}=               Create Dictionary    Operation=HEAD            Access=ALLOW    Role=USER    Filters=${filters}
    ${rule3}=               Create Dictionary    Operation=PUT             Access=ALLOW    Role=USER    Filters=${filters}
    ${rule4}=               Create Dictionary    Operation=DELETE          Access=ALLOW    Role=USER    Filters=${filters}
    ${rule5}=               Create Dictionary    Operation=SEARCH          Access=ALLOW    Role=USER    Filters=${filters}
    ${rule6}=               Create Dictionary    Operation=GETRANGE        Access=ALLOW    Role=USER    Filters=${filters}
    ${rule7}=               Create Dictionary    Operation=GETRANGEHASH    Access=ALLOW    Role=USER    Filters=${filters}

    ${eACL_gen}=            Create List    ${rule1}    ${rule2}    ${rule3}    ${rule4}    ${rule5}    ${rule6}    ${rule7}

                            Form BearerToken file               ${USER_KEY}    ${CID}    bearer_allow_all_user   ${eACL_gen}   100500 

                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS            ${USER_KEY}    ${FILE_S}     ${CID}                   ${EMPTY}              ${FILE_USR_HEADER} 
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}              local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Search object                  ${USER_KEY}    ${CID}        ${EMPTY}                 ${EMPTY}              ${FILE_USR_HEADER}          ${S_OBJ_H}
                            Run Keyword And Expect Error        *
                            ...  Head object                    ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}               
                            Run Keyword And Expect Error        *
                            ...  Get Range                      ${USER_KEY}    ${CID}        ${S_OID_USER}            s_get_range            ${EMPTY}              0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                  ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}
                            Run Keyword And Expect Error        *
                            ...  Search object                  ${USER_KEY}    ${CID}        ${EMPTY}                 bearer_allow_all_user               ${FILE_USR_HEADER}             ${S_OBJ_H}
                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS            ${USER_KEY}    ${FILE_S}     ${CID}                   bearer_allow_all_user               ${FILE_OTH_HEADER} 
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${USER_KEY}    ${CID}        ${S_OID_USER_2}          bearer_allow_all_user               local_file_eacl

                            Get object from NeoFS               ${USER_KEY}    ${CID}        ${S_OID_USER}            bearer_allow_all_user               local_file_eacl                                                                        
                            Get Range                           ${USER_KEY}    ${CID}        ${S_OID_USER}            s_get_range                         bearer_allow_all_user               0:256     
                            
                            Head object                         ${USER_KEY}    ${CID}        ${S_OID_USER}            bearer_allow_all_user               
                            Delete object                       ${USER_KEY}    ${CID}        ${S_OID_USER}            bearer_allow_all_user
                            Run Keyword And Expect Error        *
                            ...  Delete object                  ${USER_KEY}    ${CID}        ${D_OID_USER}            bearer_allow_all_user



Check eACL Deny and Allow All Bearer Filter OID NotEqual
    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER} 
    ${S_OID_USER_2} =       Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${EMPTY}
    ${D_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER_DEL} 
    @{S_OBJ_H} =	        Create List	                        ${S_OID_USER}

 
                            Put object to NeoFS                 ${USER_KEY}    ${FILE_S}     ${CID}                   ${EMPTY}              ${FILE_OTH_HEADER} 
                            Get object from NeoFS               ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}              local_file_eacl
                            Search object                       ${USER_KEY}    ${CID}        ${EMPTY}                 ${EMPTY}              ${FILE_USR_HEADER}         ${S_OBJ_H}            
                            Head object                         ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}               
                            Get Range                           ${USER_KEY}    ${CID}        ${S_OID_USER}            s_get_range            ${EMPTY}              0:256
                            Delete object                       ${USER_KEY}    ${CID}        ${D_OID_USER}            ${EMPTY}

                            Set eACL                            ${USER_KEY}    ${CID}        ${EACL_DENY_ALL_USER}    --await

    ${filters}=             Create Dictionary    headerType=OBJECT    matchType=STRING_NOT_EQUAL    key=$Object:objectID    value=${S_OID_USER_2} 

    ${rule1}=               Create Dictionary    Operation=GET             Access=ALLOW    Role=USER    Filters=${filters}
    ${rule2}=               Create Dictionary    Operation=HEAD            Access=ALLOW    Role=USER    Filters=${filters}
    ${rule3}=               Create Dictionary    Operation=PUT             Access=ALLOW    Role=USER    Filters=${filters}
    ${rule4}=               Create Dictionary    Operation=DELETE          Access=ALLOW    Role=USER    Filters=${filters}
    ${rule5}=               Create Dictionary    Operation=SEARCH          Access=ALLOW    Role=USER    Filters=${filters}
    ${rule6}=               Create Dictionary    Operation=GETRANGE        Access=ALLOW    Role=USER    Filters=${filters}
    ${rule7}=               Create Dictionary    Operation=GETRANGEHASH    Access=ALLOW    Role=USER    Filters=${filters}

    ${eACL_gen}=            Create List    ${rule1}    ${rule2}    ${rule3}    ${rule4}    ${rule5}    ${rule6}    ${rule7}

                            Form BearerToken file               ${USER_KEY}    ${CID}    bearer_allow_all_user   ${eACL_gen}   100500 

                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS            ${USER_KEY}    ${FILE_S}     ${CID}                   ${EMPTY}              ${FILE_USR_HEADER} 
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}              local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Search object                  ${USER_KEY}    ${CID}        ${EMPTY}                 ${EMPTY}              ${FILE_USR_HEADER}          ${S_OBJ_H}
                            Run Keyword And Expect Error        *
                            ...  Head object                    ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}               
                            Run Keyword And Expect Error        *
                            ...  Get Range                      ${USER_KEY}    ${CID}        ${S_OID_USER}            s_get_range            ${EMPTY}              0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                  ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}
                            Run Keyword And Expect Error        *
                            ...  Search object                  ${USER_KEY}    ${CID}        ${EMPTY}                 bearer_allow_all_user               ${FILE_USR_HEADER}             ${S_OBJ_H}

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

                            Delete object                       ${USER_KEY}    ${CID}        ${S_OID_USER}            bearer_allow_all_user
                            
                            Run Keyword And Expect Error        *
                            ...  Delete object                  ${USER_KEY}    ${CID}        ${D_OID_USER_2}          bearer_allow_all_user




Check eACL Allow All Bearer Filter Requst Equal Deny
    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER} 
    ${S_OID_USER_2} =       Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${EMPTY}
    ${D_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER_DEL} 
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

                            Put object to NeoFS      ${USER_KEY}    ${FILE_S}     ${CID}           bearer_allow_all_user    ${FILE_OTH_HEADER}       ${EMPTY}      --xhdr a=2
                            Get object from NeoFS    ${USER_KEY}    ${CID}        ${S_OID_USER}    bearer_allow_all_user    local_file_eacl          ${EMPTY}      --xhdr a=2
                            Search object            ${USER_KEY}    ${CID}        ${EMPTY}         bearer_allow_all_user    ${FILE_USR_HEADER}       ${S_OBJ_H}    --xhdr a=2     
                            Head object              ${USER_KEY}    ${CID}        ${S_OID_USER}    bearer_allow_all_user    ${EMPTY}                 --xhdr a=2
                            Get Range                ${USER_KEY}    ${CID}        ${S_OID_USER}    s_get_range              bearer_allow_all_user    0:256         --xhdr a=2
                            Get Range Hash           ${USER_KEY}    ${CID}        ${S_OID_USER}    bearer_allow_all_user    0:256                    --xhdr a=2
                            Delete object            ${USER_KEY}    ${CID}        ${D_OID_USER}    bearer_allow_all_user    --xhdr a=2
        
                            Run Keyword And Expect Error    *
                            ...  Put object to NeoFS             ${USER_KEY}    ${FILE_S}    ${CID}           bearer_allow_all_user    ${FILE_USR_HEADER}       ${EMPTY}       --xhdr a=256
                            Run Keyword And Expect Error    *
                            ...  Get object from NeoFS           ${USER_KEY}    ${CID}       ${S_OID_USER}    bearer_allow_all_user    local_file_eacl          ${EMPTY}       --xhdr a=256
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


Check eACL Deny and Allow All Bearer Filter Requst NotEqual
    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER} 
    ${S_OID_USER_2} =       Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${EMPTY}
    ${D_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER_DEL} 
    @{S_OBJ_H} =	        Create List	                        ${S_OID_USER}

                            Put object to NeoFS                 ${USER_KEY}    ${FILE_S}     ${CID}                   ${EMPTY}              ${FILE_OTH_HEADER} 
                            Get object from NeoFS               ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}              local_file_eacl
                            Search object                       ${USER_KEY}    ${CID}        ${EMPTY}                 ${EMPTY}              ${FILE_USR_HEADER}         ${S_OBJ_H}            
                            Head object                         ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}               
                            Get Range                           ${USER_KEY}    ${CID}        ${S_OID_USER}            s_get_range            ${EMPTY}              0:256
                            Delete object                       ${USER_KEY}    ${CID}        ${D_OID_USER}            ${EMPTY}

                            Set eACL                            ${USER_KEY}    ${CID}        ${EACL_DENY_ALL_USER}    --await

    ${filters}=             Create Dictionary    headerType=REQUEST    matchType=STRING_NOT_EQUAL    key=a    value=256 
    ${rule1}=               Create Dictionary    Operation=GET             Access=ALLOW    Role=USER    Filters=${filters}
    ${rule2}=               Create Dictionary    Operation=HEAD            Access=ALLOW    Role=USER    Filters=${filters}
    ${rule3}=               Create Dictionary    Operation=PUT             Access=ALLOW    Role=USER    Filters=${filters}
    ${rule4}=               Create Dictionary    Operation=DELETE          Access=ALLOW    Role=USER    Filters=${filters}
    ${rule5}=               Create Dictionary    Operation=SEARCH          Access=ALLOW    Role=USER    Filters=${filters}
    ${rule6}=               Create Dictionary    Operation=GETRANGE        Access=ALLOW    Role=USER    Filters=${filters}
    ${rule7}=               Create Dictionary    Operation=GETRANGEHASH    Access=ALLOW    Role=USER    Filters=${filters}
    ${eACL_gen}=            Create List    ${rule1}    ${rule2}    ${rule3}    ${rule4}    ${rule5}    ${rule6}    ${rule7}
                            Form BearerToken file               ${USER_KEY}    ${CID}    bearer_allow_all_user   ${eACL_gen}   100500 

                            Run Keyword And Expect Error    *
                            ...  Put object to NeoFS        ${USER_KEY}    ${FILE_S}    ${CID}           ${EMPTY}       ${FILE_USR_HEADER} 
                            Run Keyword And Expect Error    *
                            ...  Get object from NeoFS      ${USER_KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}       local_file_eacl
                            #Run Keyword And Expect Error    *
                            #...  Search object              ${USER_KEY}    ${CID}       ${EMPTY}         ${EMPTY}       ${FILE_USR_HEADER}    ${S_OBJ_H}
                            Run Keyword And Expect Error    *
                            ...  Head object                ${USER_KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}               
                            Run Keyword And Expect Error    *
                            ...  Get Range                  ${USER_KEY}    ${CID}       ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Run Keyword And Expect Error    *
                            ...  Delete object              ${USER_KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}
                            
                            Put object to NeoFS             ${USER_KEY}    ${FILE_S}    ${CID}           bearer_allow_all_user    ${FILE_USR_HEADER}      ${EMPTY}       --xhdr a=2
                            Get object from NeoFS           ${USER_KEY}    ${CID}       ${S_OID_USER}    bearer_allow_all_user    local_file_eacl          ${EMPTY}       --xhdr a=2
                            Search object                   ${USER_KEY}    ${CID}       ${EMPTY}         bearer_allow_all_user    ${FILE_USR_HEADER}       ${EMPTY}       --xhdr a=2
                            Head object                     ${USER_KEY}    ${CID}       ${S_OID_USER}    bearer_allow_all_user    ${EMPTY}                 --xhdr a=2
                            Get Range                       ${USER_KEY}    ${CID}       ${S_OID_USER}    s_get_range              bearer_allow_all_user    0:256          --xhdr a=2
                            Get Range Hash                  ${USER_KEY}    ${CID}       ${S_OID_USER}    bearer_allow_all_user    0:256                    --xhdr a=2
                            Delete object                   ${USER_KEY}    ${CID}       ${S_OID_USER}    bearer_allow_all_user    --xhdr a=2


Check eACL Deny and Allow All Bearer Filter Requst Equal
    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER} 
    ${S_OID_USER_2} =       Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${EMPTY}
    ${D_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER_DEL} 
    @{S_OBJ_H} =	        Create List	                        ${S_OID_USER}

                            Put object to NeoFS                 ${USER_KEY}    ${FILE_S}     ${CID}                   ${EMPTY}              ${FILE_OTH_HEADER} 
                            Get object from NeoFS               ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}              local_file_eacl
                            Search object                       ${USER_KEY}    ${CID}        ${EMPTY}                 ${EMPTY}              ${FILE_USR_HEADER}         ${S_OBJ_H}            
                            Head object                         ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}               
                            Get Range                           ${USER_KEY}    ${CID}        ${S_OID_USER}            s_get_range            ${EMPTY}              0:256
                            Delete object                       ${USER_KEY}    ${CID}        ${D_OID_USER}            ${EMPTY}

                            Set eACL                            ${USER_KEY}    ${CID}        ${EACL_DENY_ALL_USER}    --await

    ${filters}=             Create Dictionary    headerType=REQUEST    matchType=STRING_EQUAL    key=a    value=256 
    ${rule1}=               Create Dictionary    Operation=GET             Access=ALLOW    Role=USER    Filters=${filters}
    ${rule2}=               Create Dictionary    Operation=HEAD            Access=ALLOW    Role=USER    Filters=${filters}
    ${rule3}=               Create Dictionary    Operation=PUT             Access=ALLOW    Role=USER    Filters=${filters}
    ${rule4}=               Create Dictionary    Operation=DELETE          Access=ALLOW    Role=USER    Filters=${filters}
    ${rule5}=               Create Dictionary    Operation=SEARCH          Access=ALLOW    Role=USER    Filters=${filters}
    ${rule6}=               Create Dictionary    Operation=GETRANGE        Access=ALLOW    Role=USER    Filters=${filters}
    ${rule7}=               Create Dictionary    Operation=GETRANGEHASH    Access=ALLOW    Role=USER    Filters=${filters}
    ${eACL_gen}=            Create List    ${rule1}    ${rule2}    ${rule3}    ${rule4}    ${rule5}    ${rule6}    ${rule7}
                            Form BearerToken file               ${USER_KEY}    ${CID}    bearer_allow_all_user   ${eACL_gen}   100500 

                            Run Keyword And Expect Error    *
                            ...  Put object to NeoFS        ${USER_KEY}    ${FILE_S}    ${CID}           ${EMPTY}       ${FILE_USR_HEADER} 
                            Run Keyword And Expect Error    *
                            ...  Get object from NeoFS      ${USER_KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}       local_file_eacl
                            Run Keyword And Expect Error    *
                            ...  Search object              ${USER_KEY}    ${CID}       ${EMPTY}         ${EMPTY}       ${FILE_USR_HEADER}    ${S_OBJ_H}
                            Run Keyword And Expect Error    *
                            ...  Head object                ${USER_KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}               
                            Run Keyword And Expect Error    *
                            ...  Get Range                  ${USER_KEY}    ${CID}       ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Run Keyword And Expect Error    *
                            ...  Delete object              ${USER_KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}
                            
                            Put object to NeoFS             ${USER_KEY}    ${FILE_S}    ${CID}           bearer_allow_all_user    ${FILE_USR_HEADER}       ${EMPTY}       --xhdr a=256
                            Get object from NeoFS           ${USER_KEY}    ${CID}       ${S_OID_USER}    bearer_allow_all_user    local_file_eacl          ${EMPTY}       --xhdr a=256
                            Search object                   ${USER_KEY}    ${CID}       ${EMPTY}         bearer_allow_all_user    ${FILE_USR_HEADER}       ${EMPTY}       --xhdr a=256
                            Head object                     ${USER_KEY}    ${CID}       ${S_OID_USER}    bearer_allow_all_user    ${EMPTY}                 --xhdr a=256
                            Get Range                       ${USER_KEY}    ${CID}       ${S_OID_USER}    s_get_range              bearer_allow_all_user    0:256          --xhdr a=256
                            Get Range Hash                  ${USER_KEY}    ${CID}       ${S_OID_USER}    bearer_allow_all_user    0:256                    --xhdr a=256
                            Delete object                   ${USER_KEY}    ${CID}       ${S_OID_USER}    bearer_allow_all_user    --xhdr a=256


Check eACL Deny and Allow All Bearer Filter UserHeader Equal
    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER} 
    ${S_OID_USER_2} =       Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${EMPTY}
    ${D_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER_DEL} 
    @{S_OBJ_H} =	        Create List	                        ${S_OID_USER}

 
                            Put object to NeoFS                 ${USER_KEY}    ${FILE_S}     ${CID}                   ${EMPTY}              ${FILE_OTH_HEADER} 
                            Get object from NeoFS               ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}              local_file_eacl
                            Search object                       ${USER_KEY}    ${CID}        ${EMPTY}                 ${EMPTY}              ${FILE_USR_HEADER}         ${S_OBJ_H}            
                            Head object                         ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}               
                            Get Range                           ${USER_KEY}    ${CID}        ${S_OID_USER}            s_get_range            ${EMPTY}              0:256
                            Delete object                       ${USER_KEY}    ${CID}        ${D_OID_USER}            ${EMPTY}

                            Set eACL                            ${USER_KEY}    ${CID}        ${EACL_DENY_ALL_USER}    --await


    ${filters}=             Create Dictionary    headerType=OBJECT    matchType=STRING_EQUAL    key=key2    value=abc 

    ${rule1}=               Create Dictionary    Operation=GET             Access=ALLOW    Role=USER    Filters=${filters}
    ${rule2}=               Create Dictionary    Operation=HEAD            Access=ALLOW    Role=USER    Filters=${filters}
    ${rule3}=               Create Dictionary    Operation=PUT             Access=ALLOW    Role=USER    Filters=${filters}
    ${rule4}=               Create Dictionary    Operation=DELETE          Access=ALLOW    Role=USER    Filters=${filters}
    ${rule5}=               Create Dictionary    Operation=SEARCH          Access=ALLOW    Role=USER    Filters=${filters}
    ${rule6}=               Create Dictionary    Operation=GETRANGE        Access=ALLOW    Role=USER    Filters=${filters}
    ${rule7}=               Create Dictionary    Operation=GETRANGEHASH    Access=ALLOW    Role=USER    Filters=${filters}

    ${eACL_gen}=            Create List    ${rule1}    ${rule2}    ${rule3}    ${rule4}    ${rule5}    ${rule6}    ${rule7}

                            Form BearerToken file               ${USER_KEY}    ${CID}    bearer_allow_all_user   ${eACL_gen}   100500 

                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS            ${USER_KEY}    ${FILE_S}     ${CID}                   ${EMPTY}              ${FILE_USR_HEADER} 
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}              local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Search object                  ${USER_KEY}    ${CID}        ${EMPTY}                 ${EMPTY}              ${FILE_USR_HEADER}          ${S_OBJ_H}
                            Run Keyword And Expect Error        *
                            ...  Head object                    ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}               
                            Run Keyword And Expect Error        *
                            ...  Get Range                      ${USER_KEY}    ${CID}        ${S_OID_USER}            s_get_range            ${EMPTY}              0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                  ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}
                            Run Keyword And Expect Error        *
                            ...  Search object                  ${USER_KEY}    ${CID}        ${EMPTY}                 bearer_allow_all_user               ${FILE_USR_HEADER}             ${S_OBJ_H}

                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS            ${USER_KEY}    ${FILE_S}     ${CID}                   bearer_allow_all_user               ${FILE_OTH_HEADER} 
                            
                            Get object from NeoFS               ${USER_KEY}    ${CID}        ${S_OID_USER}            bearer_allow_all_user               local_file_eacl                                                                         
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${USER_KEY}    ${CID}        ${S_OID_USER_2}          bearer_allow_all_user               local_file_eacl

                            Run Keyword And Expect Error        *
                            ...  Get Range                      ${USER_KEY}    ${CID}        ${S_OID_USER}            s_get_range                         bearer_allow_all_user               0:256     
                            
                            Run Keyword And Expect Error        *
                            ...  Get Range Hash                 ${USER_KEY}    ${CID}        ${S_OID_USER}            bearer_allow_all_user               0:256     
                            
                            Head object                         ${USER_KEY}    ${CID}        ${S_OID_USER}            bearer_allow_all_user               
                            Run Keyword And Expect Error        *
                            ...  Head object                    ${USER_KEY}    ${CID}        ${S_OID_USER_2}          bearer_allow_all_user               

                            # Delete can not be filtered by UserHeader.
                            Run Keyword And Expect Error        *
                            ...  Delete object                  ${USER_KEY}    ${CID}        ${S_OID_USER}            bearer_allow_all_user
                            Run Keyword And Expect Error        *
                            ...  Delete object                  ${USER_KEY}    ${CID}        ${S_OID_USER_2}          bearer_allow_all_user



Check eACL Deny and Allow All Bearer Filter UserHeader NotEqual
    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_OTH_HEADER}
    ${S_OID_USER_2} =       Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER} 
    ${D_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}   ${CID}  ${EMPTY}  ${FILE_USR_HEADER_DEL} 
    @{S_OBJ_H} =	        Create List	                        ${S_OID_USER_2}

 
                            Put object to NeoFS                 ${USER_KEY}    ${FILE_S}     ${CID}                   ${EMPTY}              ${FILE_OTH_HEADER} 
                            Get object from NeoFS               ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}              local_file_eacl
                            Search object                       ${USER_KEY}    ${CID}        ${EMPTY}                 ${EMPTY}              ${FILE_USR_HEADER}         ${S_OBJ_H}            
                            Head object                         ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}               
                            Get Range                           ${USER_KEY}    ${CID}        ${S_OID_USER}            s_get_range            ${EMPTY}              0:256
                            Delete object                       ${USER_KEY}    ${CID}        ${D_OID_USER}            ${EMPTY}

                            Set eACL                            ${USER_KEY}    ${CID}        ${EACL_DENY_ALL_USER}    --await


    ${filters}=             Create Dictionary    headerType=OBJECT    matchType=STRING_NOT_EQUAL    key=key2    value=abc 

    ${rule1}=               Create Dictionary    Operation=GET             Access=ALLOW    Role=USER    Filters=${filters}
    ${rule2}=               Create Dictionary    Operation=HEAD            Access=ALLOW    Role=USER    Filters=${filters}
    ${rule3}=               Create Dictionary    Operation=PUT             Access=ALLOW    Role=USER    Filters=${filters}
    ${rule4}=               Create Dictionary    Operation=DELETE          Access=ALLOW    Role=USER    Filters=${filters}
    ${rule5}=               Create Dictionary    Operation=SEARCH          Access=ALLOW    Role=USER    Filters=${filters}
    ${rule6}=               Create Dictionary    Operation=GETRANGE        Access=ALLOW    Role=USER    Filters=${filters}
    ${rule7}=               Create Dictionary    Operation=GETRANGEHASH    Access=ALLOW    Role=USER    Filters=${filters}

    ${eACL_gen}=            Create List    ${rule1}    ${rule2}    ${rule3}    ${rule4}      ${rule6}    ${rule7}

                            Form BearerToken file               ${USER_KEY}    ${CID}    bearer_allow_all_user   ${eACL_gen}   100500 

                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS            ${USER_KEY}    ${FILE_S}     ${CID}             ${EMPTY}    ${FILE_USR_HEADER} 
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${USER_KEY}    ${CID}        ${S_OID_USER}      ${EMPTY}    local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Search object                  ${USER_KEY}    ${CID}        ${EMPTY}           ${EMPTY}    ${FILE_USR_HEADER}    ${S_OBJ_H}
                            Run Keyword And Expect Error        *
                            ...  Head object                    ${USER_KEY}    ${CID}        ${S_OID_USER}      ${EMPTY}               
                            Run Keyword And Expect Error        *
                            ...  Get Range                      ${USER_KEY}    ${CID}        ${S_OID_USER}      s_get_range    ${EMPTY}    0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                  ${USER_KEY}    ${CID}        ${S_OID_USER}      ${EMPTY}
                            
                            # Search can not use filter by headers
                            Run Keyword And Expect Error        *
                            ...  Search object                  ${USER_KEY}    ${CID}        ${EMPTY}           bearer_allow_all_user    ${FILE_USR_HEADER}    ${S_OBJ_H}
                            
                            # Different behaviour for big and small objects!
                            # Put object to NeoFS                 ${USER_KEY}    ${FILE_S}     ${CID}             bearer_allow_all_user    ${FILE_OTH_HEADER}
                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS            ${USER_KEY}    ${FILE_S}     ${CID}             bearer_allow_all_user    ${EMPTY}  
                            
                            Get object from NeoFS               ${USER_KEY}    ${CID}        ${S_OID_USER}      bearer_allow_all_user    local_file_eacl                                                                         
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${USER_KEY}    ${CID}        ${S_OID_USER_2}    bearer_allow_all_user    local_file_eacl

                            Run Keyword And Expect Error        *
                            ...  Get Range                      ${USER_KEY}    ${CID}        ${S_OID_USER}      s_get_range    bearer_allow_all_user    0:256     
                            
                            Run Keyword And Expect Error        *
                            ...  Get Range Hash                 ${USER_KEY}    ${CID}        ${S_OID_USER}      bearer_allow_all_user    0:256     
                            
                            Head object                         ${USER_KEY}    ${CID}        ${S_OID_USER}      bearer_allow_all_user               
                            Run Keyword And Expect Error        *
                            ...  Head object                    ${USER_KEY}    ${CID}        ${S_OID_USER_2}    bearer_allow_all_user               

                            # Delete can not be filtered by UserHeader.
                            Run Keyword And Expect Error        *
                            ...  Delete object                  ${USER_KEY}    ${CID}        ${S_OID_USER}      bearer_allow_all_user
                            Run Keyword And Expect Error        *
                            ...  Delete object                  ${USER_KEY}    ${CID}        ${S_OID_USER_2}    bearer_allow_all_user


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
                            ...                bearer_allow_all_user   gen_eacl_deny_all_USER    bearer_allow          
                            Cleanup Files      @{CLEANUP_FILES}
                            Get Docker Logs    acl_bearer