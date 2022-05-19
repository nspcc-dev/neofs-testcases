*** Settings ***
Variables   common.py

*** Keywords ***

Prepare eACL Role rules
    [Arguments]    ${CID}

    # eACL rules for all operations and similar permissions
    @{Roles} =	        Create List    others    user    system
    FOR	${role}	IN	@{Roles}
        ${rule1} =    Set Variable    deny get ${role}
        ${rule2} =    Set Variable    deny head ${role}
        ${rule3} =    Set Variable    deny put ${role}
        ${rule4} =    Set Variable    deny delete ${role}
        ${rule5} =    Set Variable    deny search ${role}
        ${rule6} =    Set Variable    deny getrange ${role}
        ${rule7} =    Set Variable    deny getrangehash ${role}

        ${eACL_gen} =           Create List    ${rule1}    ${rule2}    ${rule3}    ${rule4}    ${rule5}    ${rule6}    ${rule7}
        ${EACL_FILE} =          Create eACL    ${CID}    ${eACL_gen}
                                Set Global Variable    ${EACL_DENY_ALL_${role}}    ${EACL_FILE}
    END
    [Return]    gen_eacl_deny_all_${role}
