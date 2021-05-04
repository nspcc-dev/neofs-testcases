*** Settings ***
Variables   ../../../variables/common.py

Library     ${KEYWORDS}/wallet.py

*** Variables ***
${RULE_FOR_ALL} =           REP 2 IN X CBF 1 SELECT 4 FROM * AS X


*** Keywords ***

Generate Keys
    ${WALLET}   ${ADDR}     ${USER_KEY_GEN} =   Init Wallet with Address    ${TEMP_DIR}
    ${WALLET_OTH}   ${ADDR_OTH}     ${OTHER_KEY_GEN} =   Init Wallet with Address    ${TEMP_DIR}

    ${SYSTEM_KEY_GEN} =     Set Variable            ${NEOFS_IR_WIF}
    ${SYSTEM_KEY_GEN_SN} =  Set Variable            ${NEOFS_SN_WIF}

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

    ${TX} =                 Transfer Mainnet Gas    wallets/wallet.json     ${DEF_WALLET_ADDR}    ${ADDR}     3
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
