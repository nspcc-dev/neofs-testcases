*** Settings ***
Variables   common.py
Variables   wellknown_acl.py

Library     container.py

*** Keywords ***

Create Private Container
    [Arguments]    ${USER_KEY}
    ${PRIV_CID_GEN} =       Create container       ${USER_KEY}        basic_acl=${PRIVATE_ACL_F}
    [Return]    ${PRIV_CID_GEN}

Create Public Container
    [Arguments]    ${USER_KEY}
    ${PUBLIC_CID_GEN} =     Create container       ${USER_KEY}        basic_acl=${PUBLIC_ACL_F}
    [Return]    ${PUBLIC_CID_GEN}

Create Read-Only Container
    [Arguments]    ${USER_KEY}
    ${READONLY_CID_GEN} =   Create container       ${USER_KEY}        basic_acl=${READONLY_ACL_F}
    [Return]    ${READONLY_CID_GEN}


Generate file
    [Arguments]             ${SIZE}
    ${FILE_S_GEN} =         Generate file of bytes    ${SIZE}
    ${FILE_S_HASH_GEN} =    Get file hash             ${FILE_S_GEN}
    [Return]     ${FILE_S_GEN}    ${FILE_S_HASH_GEN}
