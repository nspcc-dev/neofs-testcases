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
    ${WALLET}       ${ADDR}         ${USER_KEY_GEN}  =   Init Wallet with Address    ${TEMP_DIR}
    ${WALLET_OTH}   ${ADDR_OTH}     ${OTHER_KEY_GEN} =   Init Wallet with Address    ${TEMP_DIR}


    ${EACL_KEY_GEN} =	    Form WIF from String    782676b81a35c5f07325ec523e8521ee4946b6e5d4c6cd652dd0c3ba51ce03de
    ${SYSTEM_KEY_GEN} =     Set Variable            ${NEOFS_IR_WIF}
    ${SYSTEM_KEY_GEN_SN} =  Set Variable            ${NEOFS_SN_WIF}

                            Set Global Variable     ${USER_KEY}         ${USER_KEY_GEN}
                            Set Global Variable     ${OTHER_KEY}        ${OTHER_KEY_GEN}
                            Set Global Variable     ${SYSTEM_KEY}       ${SYSTEM_KEY_GEN}
                            Set Global Variable     ${SYSTEM_KEY_SN}    ${SYSTEM_KEY_GEN_SN}
                            Set Global Variable     ${EACL_KEY}         ${EACL_KEY_GEN}

                            Payment Operations      ${WALLET}           ${ADDR}        ${USER_KEY}
                            Payment Operations      ${WALLET_OTH}       ${ADDR_OTH}    ${OTHER_KEY}


Payment Operations
    [Arguments]    ${WALLET}    ${ADDR}    ${KEY}

    ${TX} =                 Transfer Mainnet Gas                  wallets/wallet.json    ${DEF_WALLET_ADDR}    ${ADDR}    3

                            Wait Until Keyword Succeeds           1 min                  15 sec
                            ...  Transaction accepted in block    ${TX}
                            Get Transaction                       ${TX}
                            Expected Mainnet Balance              ${ADDR}                3

    ${SCRIPT_HASH} =        Get ScriptHash                         ${KEY}

    ${TX_DEPOSIT} =         NeoFS Deposit                         ${WALLET}              ${ADDR}    ${SCRIPT_HASH}    2
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
        ${rule1}=               Create Dictionary    Operation=GET             Access=DENY    Role=${role}
        ${rule2}=               Create Dictionary    Operation=HEAD            Access=DENY    Role=${role}
        ${rule3}=               Create Dictionary    Operation=PUT             Access=DENY    Role=${role}
        ${rule4}=               Create Dictionary    Operation=DELETE          Access=DENY    Role=${role}
        ${rule5}=               Create Dictionary    Operation=SEARCH          Access=DENY    Role=${role}
        ${rule6}=               Create Dictionary    Operation=GETRANGE        Access=DENY    Role=${role}
        ${rule7}=               Create Dictionary    Operation=GETRANGEHASH    Access=DENY    Role=${role}

        ${eACL_gen}=            Create List    ${rule1}    ${rule2}    ${rule3}    ${rule4}    ${rule5}    ${rule6}    ${rule7}
                                Form eACL json common file    gen_eacl_deny_all_${role}    ${eACL_gen}
    END


    FOR	${role}	IN	@{Roles}
        ${rule1}=               Create Dictionary    Operation=GET             Access=ALLOW    Role=${role}
        ${rule2}=               Create Dictionary    Operation=HEAD            Access=ALLOW    Role=${role}
        ${rule3}=               Create Dictionary    Operation=PUT             Access=ALLOW    Role=${role}
        ${rule4}=               Create Dictionary    Operation=DELETE          Access=ALLOW    Role=${role}
        ${rule5}=               Create Dictionary    Operation=SEARCH          Access=ALLOW    Role=${role}
        ${rule6}=               Create Dictionary    Operation=GETRANGE        Access=ALLOW    Role=${role}
        ${rule7}=               Create Dictionary    Operation=GETRANGEHASH    Access=ALLOW    Role=${role}

        ${eACL_gen}=            Create List    ${rule1}    ${rule2}    ${rule3}    ${rule4}    ${rule5}    ${rule6}    ${rule7}
                                Form eACL json common file    gen_eacl_allow_all_${role}    ${eACL_gen}
    END


    ${rule1}=               Create Dictionary    Operation=GET             Access=ALLOW    Role=A9tDy6Ye+UimXCCzJrlAmRE0FDZHjf3XRyya9rELtgAA
    ${rule2}=               Create Dictionary    Operation=HEAD            Access=ALLOW    Role=A9tDy6Ye+UimXCCzJrlAmRE0FDZHjf3XRyya9rELtgAA
    ${rule3}=               Create Dictionary    Operation=PUT             Access=ALLOW    Role=A9tDy6Ye+UimXCCzJrlAmRE0FDZHjf3XRyya9rELtgAA
    ${rule4}=               Create Dictionary    Operation=DELETE          Access=ALLOW    Role=A9tDy6Ye+UimXCCzJrlAmRE0FDZHjf3XRyya9rELtgAA
    ${rule5}=               Create Dictionary    Operation=SEARCH          Access=ALLOW    Role=A9tDy6Ye+UimXCCzJrlAmRE0FDZHjf3XRyya9rELtgAA
    ${rule6}=               Create Dictionary    Operation=GETRANGE        Access=ALLOW    Role=A9tDy6Ye+UimXCCzJrlAmRE0FDZHjf3XRyya9rELtgAA
    ${rule7}=               Create Dictionary    Operation=GETRANGEHASH    Access=ALLOW    Role=A9tDy6Ye+UimXCCzJrlAmRE0FDZHjf3XRyya9rELtgAA
    ${rule8}=               Create Dictionary    Operation=GET             Access=DENY     Role=OTHERS
    ${rule9}=               Create Dictionary    Operation=HEAD            Access=DENY     Role=OTHERS
    ${rule10}=              Create Dictionary    Operation=PUT             Access=DENY     Role=OTHERS
    ${rule11}=              Create Dictionary    Operation=DELETE          Access=DENY     Role=OTHERS
    ${rule12}=              Create Dictionary    Operation=SEARCH          Access=DENY     Role=OTHERS
    ${rule13}=              Create Dictionary    Operation=GETRANGE        Access=DENY     Role=OTHERS
    ${rule14}=              Create Dictionary    Operation=GETRANGEHASH    Access=DENY     Role=OTHERS


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
        ${rule1}=               Create Dictionary    Operation=GET             Access=ALLOW    Role=${role}
        ${rule2}=               Create Dictionary    Operation=GETRANGE        Access=ALLOW    Role=${role}
        ${rule3}=               Create Dictionary    Operation=GETRANGEHASH    Access=ALLOW    Role=${role}
        ${rule4}=               Create Dictionary    Operation=HEAD            Access=DENY     Role=${role}
        ${eACL_gen}=            Create List    ${rule1}    ${rule2}    ${rule3}    ${rule4}
                                Form eACL json common file    gen_eacl_compound_get_${role}    ${eACL_gen}
                                Set Global Variable    ${EACL_COMPOUND_GET_${role}}    gen_eacl_compound_get_${role}
    END

    # eACL rules for Compound operations: DELETE
    @{Roles} =	        Create List    OTHERS    USER    SYSTEM
    FOR	${role}	IN	@{Roles}
        ${rule1}=               Create Dictionary    Operation=DELETE          Access=ALLOW    Role=${role}
        ${rule2}=               Create Dictionary    Operation=PUT             Access=DENY     Role=${role}
        ${rule3}=               Create Dictionary    Operation=HEAD            Access=DENY     Role=${role}
        ${eACL_gen}=            Create List    ${rule1}    ${rule2}    ${rule3}
                                Form eACL json common file    gen_eacl_compound_del_${role}    ${eACL_gen}
                                Set Global Variable    ${EACL_COMPOUND_DELETE_${role}}    gen_eacl_compound_del_${role}
    END

    # eACL rules for Compound operations: GETRANGEHASH
    @{Roles} =	        Create List    OTHERS    USER    SYSTEM
    FOR	${role}	IN	@{Roles}
        ${rule1}=               Create Dictionary    Operation=GETRANGEHASH    Access=ALLOW    Role=${role}
        ${rule2}=               Create Dictionary    Operation=GETRANGE        Access=DENY     Role=${role}
        ${rule3}=               Create Dictionary    Operation=GET             Access=DENY     Role=${role}
        ${eACL_gen}=            Create List    ${rule1}    ${rule2}    ${rule3}
                                Form eACL json common file    gen_eacl_compound_get_hash_${role}    ${eACL_gen}
                                Set Global Variable    ${EACL_COMPOUND_GET_HASH_${role}}    gen_eacl_compound_get_hash_${role}
    END



    # eACL for X-Header Other DENY and ALLOW for all
    ${filters}=             Create Dictionary    headerType=REQUEST    matchType=STRING_EQUAL    key=a    value=2

    ${rule1}=               Create Dictionary    Operation=GET             Access=DENY     Role=OTHERS    Filters=${filters}
    ${rule2}=               Create Dictionary    Operation=HEAD            Access=DENY     Role=OTHERS    Filters=${filters}
    ${rule3}=               Create Dictionary    Operation=PUT             Access=DENY     Role=OTHERS    Filters=${filters}
    ${rule4}=               Create Dictionary    Operation=DELETE          Access=DENY     Role=OTHERS    Filters=${filters}
    ${rule5}=               Create Dictionary    Operation=SEARCH          Access=DENY     Role=OTHERS    Filters=${filters}
    ${rule6}=               Create Dictionary    Operation=GETRANGE        Access=DENY     Role=OTHERS    Filters=${filters}
    ${rule7}=               Create Dictionary    Operation=GETRANGEHASH    Access=DENY     Role=OTHERS    Filters=${filters}
    ${eACL_gen}=            Create List    ${rule1}    ${rule2}    ${rule3}    ${rule4}    ${rule5}    ${rule6}    ${rule7}
                            Form eACL json common file    gen_eacl_xheader_deny_all    ${eACL_gen}
                            Set Global Variable           ${EACL_XHEADER_DENY_ALL}     gen_eacl_xheader_deny_all



    # eACL for X-Header Other ALLOW and DENY for all
    ${filters}=             Create Dictionary    headerType=REQUEST    matchType=STRING_EQUAL    key=a    value=2

    ${rule1}=               Create Dictionary    Operation=GET             Access=ALLOW     Role=OTHERS    Filters=${filters}
    ${rule2}=               Create Dictionary    Operation=HEAD            Access=ALLOW     Role=OTHERS    Filters=${filters}
    ${rule3}=               Create Dictionary    Operation=PUT             Access=ALLOW     Role=OTHERS    Filters=${filters}
    ${rule4}=               Create Dictionary    Operation=DELETE          Access=ALLOW     Role=OTHERS    Filters=${filters}
    ${rule5}=               Create Dictionary    Operation=SEARCH          Access=ALLOW     Role=OTHERS    Filters=${filters}
    ${rule6}=               Create Dictionary    Operation=GETRANGE        Access=ALLOW     Role=OTHERS    Filters=${filters}
    ${rule7}=               Create Dictionary    Operation=GETRANGEHASH    Access=ALLOW     Role=OTHERS    Filters=${filters}
    ${rule8}=               Create Dictionary    Operation=GET             Access=DENY     Role=OTHERS
    ${rule9}=               Create Dictionary    Operation=HEAD            Access=DENY     Role=OTHERS
    ${rule10}=              Create Dictionary    Operation=PUT             Access=DENY     Role=OTHERS
    ${rule11}=              Create Dictionary    Operation=DELETE          Access=DENY     Role=OTHERS
    ${rule12}=              Create Dictionary    Operation=SEARCH          Access=DENY     Role=OTHERS
    ${rule13}=              Create Dictionary    Operation=GETRANGE        Access=DENY     Role=OTHERS
    ${rule14}=              Create Dictionary    Operation=GETRANGEHASH    Access=DENY     Role=OTHERS
    ${eACL_gen}=            Create List    ${rule1}    ${rule2}    ${rule3}     ${rule4}     ${rule5}     ${rule6}     ${rule7}
                            ...            ${rule8}    ${rule9}    ${rule10}    ${rule11}    ${rule12}    ${rule13}    ${rule14}
                            Form eACL json common file    gen_eacl_xheader_allow_all    ${eACL_gen}
                            Set Global Variable           ${EACL_XHEADER_ALLOW_ALL}     gen_eacl_xheader_allow_all



Check eACL Deny and Allow All
    [Arguments]     ${KEY}       ${DENY_EACL}    ${ALLOW_EACL}

    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object                 ${USER_KEY}     ${FILE_S}            ${CID}            ${EMPTY}            ${FILE_USR_HEADER}
    ${D_OID_USER} =         Put object                 ${USER_KEY}     ${FILE_S}            ${CID}            ${EMPTY}            ${FILE_USR_HEADER_DEL}
    @{S_OBJ_H} =	        Create List	               ${S_OID_USER}

                            Put object                 ${KEY}    ${FILE_S}            ${CID}            ${EMPTY}            ${FILE_OTH_HEADER}

                            Get object                 ${KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl
                            Search object              ${KEY}    ${CID}        ${EMPTY}                 ${EMPTY}            ${FILE_USR_HEADER}    ${S_OBJ_H}
                            Head object                ${KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}

                            Get Range                  ${KEY}    ${CID}        ${S_OID_USER}            s_get_range       ${EMPTY}            0:256
                            Get Range Hash             ${KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}          0:256
                            Delete object              ${KEY}    ${CID}        ${D_OID_USER}            ${EMPTY}

                            Set eACL                   ${USER_KEY}     ${CID}        ${DENY_EACL}    --await

                            # The current ACL cache lifetime is 30 sec
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                            Run Keyword And Expect Error        *
                            ...  Put object                          ${KEY}    ${FILE_S}    ${CID}           ${EMPTY}            ${FILE_USR_HEADER}
                            Run Keyword And Expect Error        *
                            ...  Get object                          ${KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}            local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Search object                       ${KEY}    ${CID}       ${EMPTY}         ${EMPTY}            ${FILE_USR_HEADER}       ${S_OBJ_H}
                            Run Keyword And Expect Error        *
                            ...  Head object                         ${KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${KEY}    ${CID}       ${S_OID_USER}    s_get_range         ${EMPTY}            0:256
                            Run Keyword And Expect Error        *
                            ...  Get Range Hash                      ${KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}            0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}

                            Set eACL                            ${USER_KEY}    ${CID}       ${ALLOW_EACL}    --await

                            # The current ACL cache lifetime is 30 sec
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                            Put object                 ${KEY}    ${FILE_S}     ${CID}              ${EMPTY}            ${FILE_OTH_HEADER}
                            Get object               ${KEY}    ${CID}        ${S_OID_USER}       ${EMPTY}            local_file_eacl
                            Search object                       ${KEY}    ${CID}        ${EMPTY}            ${EMPTY}            ${FILE_USR_HEADER}     ${S_OBJ_H}
                            Head object                         ${KEY}    ${CID}        ${S_OID_USER}       ${EMPTY}
                            Get Range                           ${KEY}    ${CID}        ${S_OID_USER}       s_get_range          ${EMPTY}            0:256
                            Get Range Hash                      ${KEY}    ${CID}        ${S_OID_USER}       ${EMPTY}             0:256
                            Delete object                       ${KEY}    ${CID}        ${S_OID_USER}       ${EMPTY}

