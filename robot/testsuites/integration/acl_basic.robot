*** Settings ***
Variables                   ../../variables/common.py
  
Library                     ${RESOURCES}/neofs.py
Library                     ${RESOURCES}/payment_neogo.py


*** Variables ***
${RULE_FOR_ALL} =           REP 2 IN X CBF 1 SELECT 4 FROM * AS X


*** Test cases ***
Basic ACL Operations
    [Documentation]         Testcase to validate NeoFS operations with ACL.
    [Tags]                  ACL  NeoFS  NeoCLI
    [Timeout]               20 min

                            Generate Keys
                            Create Containers
    
                            Generate file
                            Check Private Container
                            Check Public Container
                            Check Read-Only Container

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

    ${SYSTEM_KEY_GEN} =	    Form WIF from String            c428b4a06f166fde9f8afcf918194acdde35aa2612ecf42fe0c94273425ded21    
    ${SYSTEM_KEY_GEN_SN} =  Form WIF from String            0fa21a94be2227916284e4b3495180d9c93d04f095fe9d5a86f22044f5c411d2

                            Set Global Variable     ${USER_KEY}                  ${USER_KEY_GEN}
                            Set Global Variable     ${OTHER_KEY}                 ${OTHER_KEY_GEN}
                            Set Global Variable     ${SYSTEM_KEY_IR}             ${SYSTEM_KEY_GEN}
                            Set Global Variable     ${SYSTEM_KEY_SN}             ${SYSTEM_KEY_GEN_SN}

                            Payment Operations      ${WALLET}       ${ADDR}      ${USER_KEY}  
                            Payment Operations      ${WALLET_OTH}   ${ADDR_OTH}  ${OTHER_KEY}
                            
    # Basic ACL manual page: https://neospcc.atlassian.net/wiki/spaces/NEOF/pages/362348545/NeoFS+ACL
    # TODO: X - Sticky bit validation on public container


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




Create Containers
    # Create containers:

    

                            Log	                   Create Private Container    
    ${PRIV_CID_GEN} =       Create container       ${USER_KEY}        0x18888888              ${RULE_FOR_ALL}
                            Container Existing     ${USER_KEY}        ${PRIV_CID_GEN}    

                            Log	                   Create Public Container
    ${PUBLIC_CID_GEN} =     Create container       ${USER_KEY}        0x1FFFFFFF              ${RULE_FOR_ALL}
                            Container Existing     ${USER_KEY}        ${PUBLIC_CID_GEN}      

                            Log	                   Create Read-Only Container          
    ${READONLY_CID_GEN} =   Create container       ${USER_KEY}        0x1FFF88FF              ${RULE_FOR_ALL}
                            Container Existing     ${USER_KEY}        ${READONLY_CID_GEN}       

                            Set Global Variable    ${PRIV_CID}        ${PRIV_CID_GEN}
                            Set Global Variable    ${PUBLIC_CID}      ${PUBLIC_CID_GEN}
                            Set Global Variable    ${READONLY_CID}    ${READONLY_CID_GEN}


Generate file
    # Generate small file
    ${FILE_S_GEN} =         Generate file of bytes    1024
    ${FILE_S_HASH_GEN} =    Get file hash             ${FILE_S_GEN}

                            Set Global Variable       ${FILE_S}         ${FILE_S_GEN}
                            Set Global Variable       ${FILE_S_HASH}    ${FILE_S_HASH_GEN}

Check Private Container
    # Check Private:
    # Expected: User - pass, Other - fail, System(IR) - pass (+ System(Container node) - pass, Non-container node - fail). 

    # Put
    ${S_OID_USER} =         Put object to NeoFS                 ${USER_KEY}         ${FILE_S}    ${PRIV_CID}    ${EMPTY}    ${EMPTY} 
                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS            ${OTHER_KEY}        ${FILE_S}    ${PRIV_CID}    ${EMPTY}    ${EMPTY}         
                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS            ${SYSTEM_KEY_IR}    ${FILE_S}    ${PRIV_CID}    ${EMPTY}    ${EMPTY} 
    ${S_OID_SYS_SN} =       Put object to NeoFS                 ${SYSTEM_KEY_SN}    ${FILE_S}    ${PRIV_CID}    ${EMPTY}    ${EMPTY} 

                        


    # Get
                            Get object from NeoFS               ${USER_KEY}         ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}      s_file_read
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${OTHER_KEY}        ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}      s_file_read
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${SYSTEM_KEY_IR}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}      s_file_read
                            Get object from NeoFS               ${SYSTEM_KEY_SN}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}      s_file_read 

    # Get Range
                            Get Range                           ${USER_KEY}         ${PRIV_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Run Keyword And Expect Error        *
                            ...  Get Range                      ${OTHER_KEY}        ${PRIV_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Run Keyword And Expect Error        *
                            ...  Get Range                      ${SYSTEM_KEY_IR}    ${PRIV_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Run Keyword And Expect Error        *
                            ...  Get Range                      ${SYSTEM_KEY_SN}    ${PRIV_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256

    # Get Range Hash
                            Get Range Hash                      ${USER_KEY}         ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Run Keyword And Expect Error        *
                            ...  Get Range Hash                 ${OTHER_KEY}        ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Get Range Hash                      ${SYSTEM_KEY_IR}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Get Range Hash                      ${SYSTEM_KEY_SN}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    0:256

    # Search
    @{S_OBJ_PRIV} =	        Create List	                        ${S_OID_USER}       ${S_OID_SYS_SN}    
                            Search object                       ${USER_KEY}         ${PRIV_CID}    ${EMPTY}    ${EMPTY}    ${EMPTY}    @{S_OBJ_PRIV}
                            Run Keyword And Expect Error        *
                            ...  Search object                  ${OTHER_KEY}        ${PRIV_CID}    ${EMPTY}    ${EMPTY}    ${EMPTY}    @{S_OBJ_PRIV}
                            Search object                       ${SYSTEM_KEY_IR}    ${PRIV_CID}    ${EMPTY}    ${EMPTY}    ${EMPTY}    @{S_OBJ_PRIV}
                            Search object                       ${SYSTEM_KEY_SN}    ${PRIV_CID}    ${EMPTY}    ${EMPTY}    ${EMPTY}    @{S_OBJ_PRIV}

 
    # Head
                            Head object                         ${USER_KEY}         ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                            Run Keyword And Expect Error        *
                            ...  Head object                    ${OTHER_KEY}        ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                            Head object                         ${SYSTEM_KEY_IR}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                            Head object                         ${SYSTEM_KEY_SN}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                        

    # Delete  
                            Run Keyword And Expect Error        *
                            ...  Delete object                  ${OTHER_KEY}        ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}  
                            Run Keyword And Expect Error        *
                            ...  Delete object                  ${SYSTEM_KEY_IR}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}   
                            Run Keyword And Expect Error        *
                            ...  Delete object                  ${SYSTEM_KEY_SN}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}
                            Delete object                       ${USER_KEY}         ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}   
 

Check Public Container

    # Put
    ${S_OID_USER} =         Put object to NeoFS                 ${USER_KEY}         ${FILE_S}    ${PUBLIC_CID}    ${EMPTY}    ${EMPTY} 
    ${S_OID_OTHER} =        Put object to NeoFS                 ${OTHER_KEY}        ${FILE_S}    ${PUBLIC_CID}    ${EMPTY}    ${EMPTY} 
    # https://github.com/nspcc-dev/neofs-node/issues/178
    ${S_OID_SYS_IR} =       Put object to NeoFS                 ${SYSTEM_KEY_IR}    ${FILE_S}    ${PUBLIC_CID}    ${EMPTY}    ${EMPTY} 
    ${S_OID_SYS_SN} =       Put object to NeoFS                 ${SYSTEM_KEY_SN}    ${FILE_S}    ${PUBLIC_CID}    ${EMPTY}    ${EMPTY} 

    # Get
                            Get object from NeoFS               ${USER_KEY}         ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read
                            Get object from NeoFS               ${OTHER_KEY}        ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read
                            Get object from NeoFS               ${SYSTEM_KEY_IR}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read
                            Get object from NeoFS               ${SYSTEM_KEY_SN}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read 

    # Get Range
                            Get Range                           ${USER_KEY}         ${PUBLIC_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Get Range                           ${OTHER_KEY}        ${PUBLIC_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Get Range                           ${SYSTEM_KEY_IR}    ${PUBLIC_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Get Range                           ${SYSTEM_KEY_SN}    ${PUBLIC_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256


    # Get Range Hash
                            Get Range Hash                      ${USER_KEY}         ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Get Range Hash                      ${OTHER_KEY}        ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Get Range Hash                      ${SYSTEM_KEY_IR}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Get Range Hash                      ${SYSTEM_KEY_SN}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    0:256

    # Search
    @{S_OBJ_PRIV} =	        Create List	                        ${S_OID_USER}       ${S_OID_OTHER}    ${S_OID_SYS_SN}    ${S_OID_SYS_IR}
                            Search object                       ${USER_KEY}         ${PUBLIC_CID}     ${EMPTY}    ${EMPTY}    ${EMPTY}    @{S_OBJ_PRIV}
                            Search object                       ${OTHER_KEY}        ${PUBLIC_CID}     ${EMPTY}    ${EMPTY}    ${EMPTY}    @{S_OBJ_PRIV}
                            Search object                       ${SYSTEM_KEY_IR}    ${PUBLIC_CID}     ${EMPTY}    ${EMPTY}    ${EMPTY}    @{S_OBJ_PRIV}
                            Search object                       ${SYSTEM_KEY_SN}    ${PUBLIC_CID}     ${EMPTY}    ${EMPTY}    ${EMPTY}    @{S_OBJ_PRIV}

    # Head
                            Head object                         ${USER_KEY}         ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                            Head object                         ${OTHER_KEY}        ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                            Head object                         ${SYSTEM_KEY_IR}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                            Head object                         ${SYSTEM_KEY_SN}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}

                            Head object                         ${USER_KEY}         ${PUBLIC_CID}    ${S_OID_OTHER}    ${EMPTY}    ${EMPTY}
                            Head object                         ${OTHER_KEY}        ${PUBLIC_CID}    ${S_OID_OTHER}    ${EMPTY}    ${EMPTY}
                            Head object                         ${SYSTEM_KEY_IR}    ${PUBLIC_CID}    ${S_OID_OTHER}    ${EMPTY}    ${EMPTY}
                            Head object                         ${SYSTEM_KEY_SN}    ${PUBLIC_CID}    ${S_OID_OTHER}    ${EMPTY}    ${EMPTY}

                            Head object                         ${USER_KEY}         ${PUBLIC_CID}    ${S_OID_SYS_SN}    ${EMPTY}    ${EMPTY}
                            Head object                         ${OTHER_KEY}        ${PUBLIC_CID}    ${S_OID_SYS_SN}    ${EMPTY}    ${EMPTY}
                            Head object                         ${SYSTEM_KEY_IR}    ${PUBLIC_CID}    ${S_OID_SYS_SN}    ${EMPTY}    ${EMPTY}
                            Head object                         ${SYSTEM_KEY_SN}    ${PUBLIC_CID}    ${S_OID_SYS_SN}    ${EMPTY}    ${EMPTY}


    # Delete
                            # https://github.com/nspcc-dev/neofs-node/issues/178
                            Delete object                       ${USER_KEY}         ${PUBLIC_CID}    ${S_OID_SYS_IR}    ${EMPTY}     
                            Delete object                       ${OTHER_KEY}        ${PUBLIC_CID}    ${S_OID_SYS_SN}    ${EMPTY}
                            Delete object                       ${SYSTEM_KEY_IR}    ${PUBLIC_CID}    ${S_OID_USER}      ${EMPTY}  
                            Delete object                       ${SYSTEM_KEY_SN}    ${PUBLIC_CID}    ${S_OID_OTHER}     ${EMPTY}


Check Read-Only Container
    # Check Read Only container:

    # Put
    ${S_OID_USER} =         Put object to NeoFS                 ${USER_KEY}         ${FILE_S}    ${READONLY_CID}    ${EMPTY}    ${EMPTY}
                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS            ${OTHER_KEY}        ${FILE_S}    ${READONLY_CID}    ${EMPTY}    ${EMPTY}
                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS            ${SYSTEM_KEY_IR}    ${FILE_S}    ${READONLY_CID}    ${EMPTY}    ${EMPTY}
    ${S_OID_SYS_SN} =       Put object to NeoFS                 ${SYSTEM_KEY_SN}    ${FILE_S}    ${READONLY_CID}    ${EMPTY}    ${EMPTY}

    # Get
                            Get object from NeoFS               ${USER_KEY}         ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read
                            Get object from NeoFS               ${OTHER_KEY}        ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read
                            Get object from NeoFS               ${SYSTEM_KEY_IR}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read 
                            Get object from NeoFS               ${SYSTEM_KEY_SN}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read

    # Get Range
                            Get Range                           ${USER_KEY}         ${READONLY_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Get Range                           ${OTHER_KEY}        ${READONLY_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Get Range                           ${SYSTEM_KEY_IR}    ${READONLY_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Get Range                           ${SYSTEM_KEY_SN}    ${READONLY_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256


    # Get Range Hash
                            Get Range Hash                      ${USER_KEY}         ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Get Range Hash                      ${OTHER_KEY}        ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Get Range Hash                      ${SYSTEM_KEY_IR}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Get Range Hash                      ${SYSTEM_KEY_SN}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    0:256

    # Search
    @{S_OBJ_RO} =	        Create List	                        ${S_OID_USER}       ${S_OID_SYS_SN}     
                            Search object                       ${USER_KEY}         ${READONLY_CID}    ${EMPTY}    ${EMPTY}    ${EMPTY}    @{S_OBJ_RO}
                            Search object                       ${OTHER_KEY}        ${READONLY_CID}    ${EMPTY}    ${EMPTY}    ${EMPTY}    @{S_OBJ_RO}
                            Search object                       ${SYSTEM_KEY_IR}    ${READONLY_CID}    ${EMPTY}    ${EMPTY}    ${EMPTY}    @{S_OBJ_RO}
                            Search object                       ${SYSTEM_KEY_SN}    ${READONLY_CID}    ${EMPTY}    ${EMPTY}    ${EMPTY}    @{S_OBJ_RO}

 
    # Head
                            Head object                         ${USER_KEY}         ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                            Head object                         ${OTHER_KEY}        ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                            Head object                         ${SYSTEM_KEY_IR}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                            Head object                         ${SYSTEM_KEY_SN}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}

    # Delete
                            Run Keyword And Expect Error        *       
                            ...  Delete object                  ${OTHER_KEY}        ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}
                            Run Keyword And Expect Error        *  
                            ...  Delete object                  ${SYSTEM_KEY_IR}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}
                            Run Keyword And Expect Error        *  
                            ...  Delete object                  ${SYSTEM_KEY_SN}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}
                            Delete object                       ${USER_KEY}         ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}



Cleanup
    @{CLEANUP_FILES} =      Create List	     ${FILE_S}    s_file_read    s_get_range  
                            Cleanup Files    @{CLEANUP_FILES}