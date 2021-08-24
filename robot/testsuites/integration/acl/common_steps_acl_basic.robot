*** Settings ***
Variables   ../../../variables/common.py

*** Keywords ***

Create Containers
                            Log	                   Create Private Container
    ${PRIV_CID_GEN} =       Create container       ${USER_KEY}        0x18888888              ${COMMON_PLACEMENT_RULE}
                            Container Existing     ${USER_KEY}        ${PRIV_CID_GEN}

                            Log	                   Create Public Container
    ${PUBLIC_CID_GEN} =     Create container       ${USER_KEY}        0x1FFFFFFF              ${COMMON_PLACEMENT_RULE}
                            Container Existing     ${USER_KEY}        ${PUBLIC_CID_GEN}

                            Log	                   Create Read-Only Container
    ${READONLY_CID_GEN} =   Create container       ${USER_KEY}        0x1FFF88FF              ${COMMON_PLACEMENT_RULE}
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
