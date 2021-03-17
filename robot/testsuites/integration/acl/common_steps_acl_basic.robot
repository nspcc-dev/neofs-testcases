*** Variables ***
${RULE_FOR_ALL} =           REP 2 IN X CBF 1 SELECT 4 FROM * AS X


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
    
    ${TX} =                 Transfer Mainnet Gas    wallets/wallet.json     NVUzCUvrbuWadAm6xBoyZ2U7nCmS9QBZtb      ${ADDR}     3
                            Wait Until Keyword Succeeds         1 min       15 sec        
                            ...  Transaction accepted in block  ${TX}
                            Get Transaction                     ${TX}
                            Expected Mainnet Balance            ${ADDR}     3

    ${SCRIPT_HASH} =        Get ScriptHash           ${KEY}

    ${TX_DEPOSIT} =         NeoFS Deposit           ${WALLET}               ${ADDR}     ${SCRIPT_HASH}      2
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
    [Arguments]             ${SIZE}
    ${FILE_S_GEN} =         Generate file of bytes    ${SIZE}
    ${FILE_S_HASH_GEN} =    Get file hash             ${FILE_S_GEN}

                            Set Global Variable       ${FILE_S}         ${FILE_S_GEN}
                            Set Global Variable       ${FILE_S_HASH}    ${FILE_S_HASH_GEN}
