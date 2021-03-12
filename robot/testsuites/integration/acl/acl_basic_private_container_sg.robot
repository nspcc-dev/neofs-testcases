***Settings***
Variables                   ../../../variables/common.py

Library                     ../${RESOURCES}/neofs.py
Library                     ../${RESOURCES}/payment_neogo.py

Resource                    common_steps_acl_basic.robot

*** Test cases ***
Storage group ACL Operations for Private Container
    [Documentation]         Testcase to validate NeoFS storage group operations.
    [Tags]                  ACL  NeoFS  NeoCLI
    [Timeout]               20 min

                            Generate Keys

                            Create Containers
                            Generate file    1024
                            Check Private Container    Simple

                            Create Containers
                            Generate file    70e+6
                            Check Private Container    Complex

    [Teardown]              Cleanup  

*** Keywords ***


Check storage group operations
    [Arguments]     ${RUN_TYPE}

# Storage group Operations (Put, List, Get, Delete)
    ${SG_OID_INV} =     Put Storagegroup    ${USER_KEY}    ${PRIV_CID}   ${EMPTY}    ${S_OID_USER}
    ${SG_OID_1} =       Put Storagegroup    ${USER_KEY}    ${PRIV_CID}   ${EMPTY}    ${S_OID_USER}
                        List Storagegroup    ${USER_KEY}    ${PRIV_CID}    ${SG_OID_1}  ${SG_OID_INV}
    @{EXPECTED_OIDS} =  Run Keyword If    "${RUN_TYPE}" == "Complex"    Get Split objects    ${USER_KEY}    ${PRIV_CID}   ${S_OID_USER}
                        ...    ELSE IF   "${RUN_TYPE}" == "Simple"    Create List   ${S_OID_USER} 		
                        Get Storagegroup    ${USER_KEY}    ${PRIV_CID}    ${SG_OID_1}    ${EMPTY}    @{EXPECTED_OIDS}
                        Delete Storagegroup    ${USER_KEY}    ${PRIV_CID}    ${SG_OID_1}


    ${SG_OID_1} =       Put Storagegroup    ${SYSTEM_KEY_SN}    ${PRIV_CID}   ${EMPTY}    ${S_OID_USER}
                        List Storagegroup    ${SYSTEM_KEY_SN}    ${PRIV_CID}    ${SG_OID_1}  ${SG_OID_INV}                        
    @{EXPECTED_OIDS} =  Run Keyword If    "${RUN_TYPE}" == "Complex"    Get Split objects    ${SYSTEM_KEY_SN}    ${PRIV_CID}   ${S_OID_USER}
                        ...    ELSE IF    "${RUN_TYPE}" == "Simple"    Create List    ${S_OID_USER}
                        Get Storagegroup    ${SYSTEM_KEY_SN}    ${PRIV_CID}    ${SG_OID_1}    ${EMPTY}    @{EXPECTED_OIDS}
                        Run Keyword And Expect Error        *
                        ...  Delete Storagegroup    ${SYSTEM_KEY_SN}    ${PRIV_CID}    ${SG_OID_1}

                        Run Keyword And Expect Error        *
                        ...  Put Storagegroup    ${OTHER_KEY}    ${PRIV_CID}   ${EMPTY}    ${S_OID_USER}
                        Run Keyword And Expect Error        *
                        ...  List Storagegroup    ${OTHER_KEY}    ${PRIV_CID}    ${SG_OID_INV}
                        Run Keyword And Expect Error        *
                        ...  Get Storagegroup    ${OTHER_KEY}    ${PRIV_CID}    ${SG_OID_INV}    ${EMPTY}    ${S_OID_USER}
                        Run Keyword And Expect Error        *
                        ...  Delete Storagegroup    ${OTHER_KEY}    ${PRIV_CID}    ${SG_OID_INV}

                        Run Keyword And Expect Error        *
                        ...  Put Storagegroup    ${SYSTEM_KEY_IR}    ${PRIV_CID}   ${EMPTY}    ${S_OID_USER}
                        Run Keyword And Expect Error        *
                        ...  List Storagegroup    ${SYSTEM_KEY_IR}    ${PRIV_CID}    ${SG_OID_INV}
                        
                        @{EXPECTED_OIDS} =  Run Keyword If    "${RUN_TYPE}" == "Complex"    Get Split objects    ${USER_KEY}    ${PRIV_CID}   ${S_OID_USER}
                        ...    ELSE IF   "${RUN_TYPE}" == "Simple"    Create List   ${S_OID_USER} 
                        Get Storagegroup    ${SYSTEM_KEY_IR}    ${PRIV_CID}    ${SG_OID_INV}    ${EMPTY}    @{EXPECTED_OIDS}

                        Run Keyword And Expect Error        *
                        ...  Delete Storagegroup    ${SYSTEM_KEY_IR}    ${PRIV_CID}    ${SG_OID_INV}


Cleanup
    @{CLEANUP_FILES} =      Create List	       ${FILE_S}    s_file_read    s_get_range  
                            Cleanup Files      @{CLEANUP_FILES}
                            Get Docker Logs    acl_basic