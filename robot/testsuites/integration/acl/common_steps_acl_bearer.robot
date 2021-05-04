*** Settings ***
Variables   ../../../variables/common.py

Library     ${KEYWORDS}/wallet.py

*** Variables ***
${FILE_USR_HEADER} =        key1=1,key2=abc
${FILE_USR_HEADER_DEL} =    key1=del,key2=del
${FILE_OTH_HEADER} =        key1=oth,key2=oth
${RULE_FOR_ALL} =           REP 2 IN X CBF 1 SELECT 4 FROM * AS X


*** Keywords ***

Generate Keys
    # Generate new wallets
    ${WALLET}       ${ADDR}         ${USER_KEY_GEN} =   Init Wallet with Address    ${TEMP_DIR}
    ${WALLET_OTH}   ${ADDR_OTH}     ${OTHER_KEY_GEN} =  Init Wallet with Address    ${TEMP_DIR}

    # Get pre-defined keys
    ${EACL_KEY_GEN} =	    Form WIF from String    782676b81a35c5f07325ec523e8521ee4946b6e5d4c6cd652dd0c3ba51ce03de
    ${SYSTEM_KEY_GEN} =     Set Variable            ${NEOFS_IR_WIF}
    ${SYSTEM_KEY_GEN_SN} =  Set Variable            ${NEOFS_SN_WIF}

    # Set global variables for keys for each role
                            Set Global Variable     ${USER_KEY}                  ${USER_KEY_GEN}
                            Set Global Variable     ${OTHER_KEY}                 ${OTHER_KEY_GEN}
                            Set Global Variable     ${SYSTEM_KEY}                ${SYSTEM_KEY_GEN}
                            Set Global Variable     ${SYSTEM_KEY_SN}             ${SYSTEM_KEY_GEN_SN}
                            Set Global Variable     ${EACL_KEY}                  ${EACL_KEY_GEN}

                            Payment Operations      ${WALLET}       ${ADDR}      ${USER_KEY}
                            Payment Operations      ${WALLET_OTH}   ${ADDR_OTH}  ${OTHER_KEY}


Payment Operations
    [Arguments]    ${WALLET}   ${ADDR}   ${KEY}

    ${TX} =                 Transfer Mainnet Gas    wallets/wallet.json     ${DEF_WALLET_ADDR}      ${ADDR}     3
                            Wait Until Keyword Succeeds         1 min       15 sec
                            ...  Transaction accepted in block  ${TX}
                            Get Transaction                     ${TX}
                            Expected Mainnet Balance            ${ADDR}     3

    ${SCRIPT_HASH} =        Get ScriptHash           ${KEY}

    ${TX_DEPOSIT} =         NeoFS Deposit           ${WALLET}               ${ADDR}     ${SCRIPT_HASH}      2
                            Wait Until Keyword Succeeds         1 min          15 sec
                            ...  Transaction accepted in block  ${TX_DEPOSIT}
                            Get Transaction                     ${TX_DEPOSIT}


Create Container Public
                            Log	                  Create Public Container
    ${PUBLIC_CID_GEN} =     Create container      ${USER_KEY}    0x0FFFFFFF     ${COMMON_PLACEMENT_RULE}
    [Return]                ${PUBLIC_CID_GEN}


Create Container Inaccessible
                            Log	                  Create Inaccessible Container
    ${PUBLIC_CID_GEN} =     Create container      ${USER_KEY}     0x40000000     ${COMMON_PLACEMENT_RULE}
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
