*** Settings ***
Variables                   ../../../variables/common.py
  
Library                     ../${RESOURCES}/neofs.py
Library                     ../${RESOURCES}/payment_neogo.py

Resource                    common_steps_acl_basic.robot


*** Test cases ***
Basic ACL Operations for Public Container
    [Documentation]         Testcase to validate NeoFS operations with ACL for Public Container.
    [Tags]                  ACL  NeoFS  NeoCLI
    [Timeout]               20 min

                            Generate Keys
    
                            Create Containers
                            Generate file    1024
                            Check Public Container    Simple

                            Create Containers
                            Generate file    70e+6
                            Check Public Container    Complex

    [Teardown]              Cleanup  
    
 
*** Keywords ***

Check Public Container
    [Arguments]     ${RUN_TYPE}

    # Storage group Operations (Put, List, Get, Delete)
                            Log    Storage group Operations for each Role keys
    
    # Put target object to use in storage groups
    ${S_OID} =              Put object    ${USER_KEY}    ${FILE_S}    ${PUBLIC_CID}    ${EMPTY}    ${EMPTY} 
    
    @{Roles_keys} =	        Create List    ${USER_KEY}    ${OTHER_KEY}    ${SYSTEM_KEY_IR}    ${SYSTEM_KEY_SN}
    
    FOR	${role_key}	IN	@{Roles_keys}
        ${SG_OID_1} =       Put Storagegroup    ${USER_KEY}    ${PUBLIC_CID}   ${EMPTY}    ${S_OID}
                            List Storagegroup    ${USER_KEY}    ${PUBLIC_CID}   ${EMPTY}    ${SG_OID_1}  
        @{EXPECTED_OIDS} =  Run Keyword If    "${RUN_TYPE}" == "Complex"    Get Split objects    ${USER_KEY}    ${PUBLIC_CID}   ${S_OID}
                            ...    ELSE IF   "${RUN_TYPE}" == "Simple"    Create List   ${S_OID} 		
                            Get Storagegroup    ${USER_KEY}    ${PUBLIC_CID}    ${SG_OID_1}   ${EMPTY}    ${EMPTY}    @{EXPECTED_OIDS}
                            Delete Storagegroup    ${USER_KEY}    ${PUBLIC_CID}    ${SG_OID_1}    ${EMPTY}
    END


Cleanup
                            Cleanup Files      
                            Get Docker Logs    acl_basic