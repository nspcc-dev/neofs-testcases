*** Settings ***
Variables    ../../../variables/common.py

Library      ../${RESOURCES}/neofs.py
Library      ../${RESOURCES}/payment_neogo.py

Resource     common_steps_acl_basic.robot
Resource     ../${RESOURCES}/payment_operations.robot
Resource     ../${RESOURCES}/setup_teardown.robot


*** Test cases ***
Basic ACL Operations for Read-Only Container
    [Documentation]         Testcase to validate NeoFS operations with ACL for Read-Only Container.
    [Tags]                  ACL  NeoFS  NeoCLI
    [Timeout]               20 min

    [Setup]                 Setup

                            Generate Keys

                            Create Containers
                            Generate file    ${SIMPLE_OBJ_SIZE}
                            Check Read-Only Container    Simple

                            Create Containers
                            Generate file    ${COMPLEX_OBJ_SIZE}
                            Check Read-Only Container    Complex

    [Teardown]              Teardown    acl_basic_readonly_container_storagegroup


*** Keywords ***


Check Read-Only Container
    [Arguments]     ${RUN_TYPE}

    # Put target object to use in storage groups
    ${S_OID_USER} =         Put object    ${USER_KEY}    ${FILE_S}    ${READONLY_CID}    ${EMPTY}    ${EMPTY}

    # Storage group Operations (Put, List, Get, Delete) for Read-only container

    ${SG_OID_INV} =     Put Storagegroup    ${USER_KEY}    ${READONLY_CID}   ${EMPTY}    ${S_OID_USER}
    ${SG_OID_1} =       Put Storagegroup    ${USER_KEY}    ${READONLY_CID}   ${EMPTY}    ${S_OID_USER}
                        List Storagegroup    ${USER_KEY}    ${READONLY_CID}   ${EMPTY}    ${SG_OID_1}  ${SG_OID_INV}
    @{EXPECTED_OIDS} =  Run Keyword If    "${RUN_TYPE}" == "Complex"    Get Split objects    ${USER_KEY}    ${READONLY_CID}   ${S_OID_USER}
                        ...    ELSE IF   "${RUN_TYPE}" == "Simple"    Create List   ${S_OID_USER}
                        Get Storagegroup    ${USER_KEY}    ${READONLY_CID}    ${SG_OID_1}   ${EMPTY}    ${EMPTY}    @{EXPECTED_OIDS}
                        Delete Storagegroup    ${USER_KEY}    ${READONLY_CID}    ${SG_OID_1}    ${EMPTY}


                        Run Keyword And Expect Error        *
                        ...  Put Storagegroup    ${OTHER_KEY}    ${READONLY_CID}   ${EMPTY}    ${S_OID_USER}
                        List Storagegroup    ${OTHER_KEY}    ${READONLY_CID}   ${EMPTY}    ${SG_OID_INV}
    @{EXPECTED_OIDS} =  Run Keyword If    "${RUN_TYPE}" == "Complex"    Get Split objects    ${USER_KEY}    ${READONLY_CID}   ${S_OID_USER}
                        ...    ELSE IF   "${RUN_TYPE}" == "Simple"    Create List   ${S_OID_USER}
                        Get Storagegroup    ${OTHER_KEY}    ${READONLY_CID}    ${SG_OID_INV}   ${EMPTY}    ${EMPTY}    @{EXPECTED_OIDS}
                        Run Keyword And Expect Error        *
                        ...  Delete Storagegroup    ${OTHER_KEY}    ${READONLY_CID}    ${SG_OID_INV}    ${EMPTY}


                        Run Keyword And Expect Error        *
                        ...  Put Storagegroup    ${SYSTEM_KEY_IR}    ${READONLY_CID}   ${EMPTY}    ${S_OID_USER}
                        List Storagegroup    ${SYSTEM_KEY_IR}    ${READONLY_CID}   ${EMPTY}    ${SG_OID_INV}
    @{EXPECTED_OIDS} =  Run Keyword If    "${RUN_TYPE}" == "Complex"    Get Split objects    ${USER_KEY}    ${READONLY_CID}   ${S_OID_USER}
                        ...    ELSE IF   "${RUN_TYPE}" == "Simple"    Create List   ${S_OID_USER}
                        Get Storagegroup    ${SYSTEM_KEY_IR}    ${READONLY_CID}    ${SG_OID_INV}   ${EMPTY}    ${EMPTY}    @{EXPECTED_OIDS}
                        Run Keyword And Expect Error        *
                        ...  Delete Storagegroup    ${SYSTEM_KEY_IR}    ${READONLY_CID}    ${SG_OID_INV}    ${EMPTY}
