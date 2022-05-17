*** Keywords ***

Generate file
    [Arguments]             ${SIZE}
    ${FILE_S_GEN} =         Generate file of bytes    ${SIZE}
    ${FILE_S_HASH_GEN} =    Get file hash             ${FILE_S_GEN}
    [Return]     ${FILE_S_GEN}    ${FILE_S_HASH_GEN}
