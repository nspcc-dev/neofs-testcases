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

    # Put
    ${S_OID_USER} =         Put object                 ${USER_KEY}         ${FILE_S}    ${PUBLIC_CID}    ${EMPTY}    ${EMPTY} 
    ${S_OID_OTHER} =        Put object                 ${OTHER_KEY}        ${FILE_S}    ${PUBLIC_CID}    ${EMPTY}    ${EMPTY} 
    ${S_OID_SYS_IR} =       Put object                 ${SYSTEM_KEY_IR}    ${FILE_S}    ${PUBLIC_CID}    ${EMPTY}    ${EMPTY} 
    ${S_OID_SYS_SN} =       Put object                 ${SYSTEM_KEY_SN}    ${FILE_S}    ${PUBLIC_CID}    ${EMPTY}    ${EMPTY} 

    # Get
                            Get object               ${USER_KEY}         ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read
                            Get object               ${OTHER_KEY}        ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read
                            Get object               ${SYSTEM_KEY_IR}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read
                            Get object               ${SYSTEM_KEY_SN}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read 

    # Get Range
                            Get Range                           ${USER_KEY}         ${PUBLIC_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Get Range                           ${OTHER_KEY}        ${PUBLIC_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Get Range                           ${SYSTEM_KEY_IR}    ${PUBLIC_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Get Range                           ${SYSTEM_KEY_SN}    ${PUBLIC_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256


    # Get Range Hash
                            Get Range Hash                      ${USER_KEY}         ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Get Range Hash                      ${OTHER_KEY}        ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Get Range Hash                      ${SYSTEM_KEY_IR}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Get Range Hash                      ${SYSTEM_KEY_SN}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    0:256

    # Search
    @{S_OBJ_PRIV} =	        Create List	                        ${S_OID_USER}       ${S_OID_OTHER}    ${S_OID_SYS_SN}    ${S_OID_SYS_IR}
                            Search object                       ${USER_KEY}         ${PUBLIC_CID}     --root    ${EMPTY}    ${EMPTY}    ${S_OBJ_PRIV}
                            Search object                       ${OTHER_KEY}        ${PUBLIC_CID}     --root    ${EMPTY}    ${EMPTY}    ${S_OBJ_PRIV}
                            Search object                       ${SYSTEM_KEY_IR}    ${PUBLIC_CID}     --root    ${EMPTY}    ${EMPTY}    ${S_OBJ_PRIV}
                            Search object                       ${SYSTEM_KEY_SN}    ${PUBLIC_CID}     --root    ${EMPTY}    ${EMPTY}    ${S_OBJ_PRIV}

    # Head
                            Head object                         ${USER_KEY}         ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                            Head object                         ${OTHER_KEY}        ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                            Head object                         ${SYSTEM_KEY_IR}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                            Head object                         ${SYSTEM_KEY_SN}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}

                            Head object                         ${USER_KEY}         ${PUBLIC_CID}    ${S_OID_OTHER}    ${EMPTY}    ${EMPTY}
                            Head object                         ${OTHER_KEY}        ${PUBLIC_CID}    ${S_OID_OTHER}    ${EMPTY}    ${EMPTY}
                            Head object                         ${SYSTEM_KEY_IR}    ${PUBLIC_CID}    ${S_OID_OTHER}    ${EMPTY}    ${EMPTY}
                            Head object                         ${SYSTEM_KEY_SN}    ${PUBLIC_CID}    ${S_OID_OTHER}    ${EMPTY}    ${EMPTY}

                            Head object                         ${USER_KEY}         ${PUBLIC_CID}    ${S_OID_SYS_SN}    ${EMPTY}    ${EMPTY}
                            Head object                         ${OTHER_KEY}        ${PUBLIC_CID}    ${S_OID_SYS_SN}    ${EMPTY}    ${EMPTY}
                            Head object                         ${SYSTEM_KEY_IR}    ${PUBLIC_CID}    ${S_OID_SYS_SN}    ${EMPTY}    ${EMPTY}
                            Head object                         ${SYSTEM_KEY_SN}    ${PUBLIC_CID}    ${S_OID_SYS_SN}    ${EMPTY}    ${EMPTY}


    # Delete
                            Delete object                       ${USER_KEY}         ${PUBLIC_CID}    ${S_OID_SYS_IR}    ${EMPTY}     
                            Delete object                       ${OTHER_KEY}        ${PUBLIC_CID}    ${S_OID_SYS_SN}    ${EMPTY}
                            Delete object                       ${SYSTEM_KEY_IR}    ${PUBLIC_CID}    ${S_OID_USER}      ${EMPTY}  
                            Delete object                       ${SYSTEM_KEY_SN}    ${PUBLIC_CID}    ${S_OID_OTHER}     ${EMPTY}


    # Storage group Operations (Put, List, Get, Delete)
                            Log    Storage group Operations for each Role keys
    ${S_OID} =              Put object                 ${USER_KEY}         ${FILE_S}    ${PUBLIC_CID}    ${EMPTY}    ${EMPTY} 
    @{Roles_keys} =	        Create List    ${USER_KEY}    ${OTHER_KEY}    ${SYSTEM_KEY_IR}    ${SYSTEM_KEY_SN}
    FOR	${role_key}	IN	@{Roles_keys}
        ${SG_OID_1} =       Put Storagegroup    ${USER_KEY}    ${PUBLIC_CID}   ${EMPTY}    ${S_OID}
                            List Storagegroup    ${USER_KEY}    ${PUBLIC_CID}    ${SG_OID_1}  
        @{EXPECTED_OIDS} =  Run Keyword If    "${RUN_TYPE}" == "Complex"    Get Split objects    ${USER_KEY}    ${PUBLIC_CID}   ${S_OID}
                            ...    ELSE IF   "${RUN_TYPE}" == "Simple"    Convert Str To List   ${S_OID} 		
                            Get Storagegroup    ${USER_KEY}    ${PUBLIC_CID}    ${SG_OID_1}    ${EMPTY}    @{EXPECTED_OIDS}
                            Delete Storagegroup    ${USER_KEY}    ${PUBLIC_CID}    ${SG_OID_1}

    END


Cleanup
    @{CLEANUP_FILES} =      Create List	       ${FILE_S}    s_file_read    s_get_range  
                            Cleanup Files      @{CLEANUP_FILES}
                            Get Docker Logs    acl_basic