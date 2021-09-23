*** Settings ***
Variables   ../../../variables/common.py
Variables   ../../../variables/acl.py

*** Variables ***
${FILE_USR_HEADER} =        key1=1,key2=abc
${FILE_USR_HEADER_DEL} =    key1=del,key2=del
${FILE_OTH_HEADER} =        key1=oth,key2=oth
${CONTAINER_WAIT_INTERVAL} =    1 min

*** Keywords ***
Create Container Public
    [Arguments]    ${USER_KEY}
    ${PUBLIC_CID_GEN} =     Create container      ${USER_KEY}    0x0FFFFFFF     ${COMMON_PLACEMENT_RULE}
                            Wait Until Keyword Succeeds    ${MORPH_BLOCK_TIME}       ${CONTAINER_WAIT_INTERVAL}
                            ...     Container Existing     ${USER_KEY}        ${PUBLIC_CID_GEN}
    [Return]                ${PUBLIC_CID_GEN}


Create Container Inaccessible
    [Arguments]    ${USER_KEY}
    ${INACCESSIBLE_CID_GEN} =     Create container      ${USER_KEY}     ${INACCESSIBLE_ACL}     ${COMMON_PLACEMENT_RULE}
                            Wait Until Keyword Succeeds    ${MORPH_BLOCK_TIME}       ${CONTAINER_WAIT_INTERVAL}
                            ...     Container Existing     ${USER_KEY}        ${INACCESSIBLE_CID_GEN}
    [Return]                ${INACCESSIBLE_CID_GEN}


Generate file
    [Arguments]             ${SIZE}

    ${FILE_S_GEN} =         Generate file of bytes    ${SIZE}
    [Return]                ${FILE_S_GEN}


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
        ${EACL_FILE} =          Form eACL JSON Common File    ${eACL_gen}
                                Set Global Variable    ${EACL_DENY_ALL_${role}}    ${EACL_FILE}
    END
    [Return]    gen_eacl_deny_all_${role}
