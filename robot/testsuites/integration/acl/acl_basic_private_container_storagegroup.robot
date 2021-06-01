*** Settings ***
Variables       ../../../variables/common.py
Library     Collections
Library     ../${RESOURCES}/neofs.py
Library     ../${RESOURCES}/payment_neogo.py
Library     ../${RESOURCES}/utility_keywords.py

Resource    common_steps_acl_basic.robot
Resource    ../${RESOURCES}/payment_operations.robot


*** Test cases ***
Basic ACL Operations for Private Container
    [Documentation]         Testcase to validate NeoFS operations with ACL for Private Container.
    [Tags]                  ACL  NeoFS  NeoCLI
    [Timeout]               20 min

    [Setup]                 Create Temporary Directory

                            Generate Keys

                            Create Containers
                            Generate file    ${SIMPLE_OBJ_SIZE}
                            Check Private Container    Simple

                            Create Containers
                            Generate file    ${COMPLEX_OBJ_SIZE}
                            Check Private Container    Complex

    [Teardown]              Cleanup


*** Keywords ***

Check Private Container
    [Arguments]     ${RUN_TYPE}

    # Put target object to use in storage groups
    ${S_OID_USER} =         Put object    ${USER_KEY}    ${FILE_S}    ${PRIV_CID}    ${EMPTY}    ${EMPTY}


    # Storage group Operations (Put, List, Get, Delete) with different Keys
    # User group key
    ${SG_OID_INV} =     Put Storagegroup    ${USER_KEY}    ${PRIV_CID}   ${EMPTY}    ${S_OID_USER}
    ${SG_OID_1} =       Put Storagegroup    ${USER_KEY}    ${PRIV_CID}   ${EMPTY}    ${S_OID_USER}
                        List Storagegroup    ${USER_KEY}    ${PRIV_CID}   ${EMPTY}    ${SG_OID_1}  ${SG_OID_INV}
    @{EXPECTED_OIDS} =  Run Keyword If    "${RUN_TYPE}" == "Complex"    Get Split objects    ${USER_KEY}    ${PRIV_CID}   ${S_OID_USER}
                        ...    ELSE IF   "${RUN_TYPE}" == "Simple"    Create List   ${S_OID_USER}
                        Get Storagegroup    ${USER_KEY}    ${PRIV_CID}    ${SG_OID_1}   ${EMPTY}    ${EMPTY}    @{EXPECTED_OIDS}
                        Delete Storagegroup    ${USER_KEY}    ${PRIV_CID}    ${SG_OID_1}    ${EMPTY}


    # "Others" group key
                        Run Keyword And Expect Error        *
                        ...  Put Storagegroup    ${OTHER_KEY}    ${PRIV_CID}   ${EMPTY}    ${S_OID_USER}
                        Run Keyword And Expect Error        *
                        ...  List Storagegroup    ${OTHER_KEY}    ${PRIV_CID}   ${EMPTY}    ${SG_OID_INV}
                        Run Keyword And Expect Error        *
                        ...  Get Storagegroup    ${OTHER_KEY}    ${PRIV_CID}    ${SG_OID_INV}   ${EMPTY}    ${EMPTY}    ${S_OID_USER}
                        Run Keyword And Expect Error        *
                        ...  Delete Storagegroup    ${OTHER_KEY}    ${PRIV_CID}    ${SG_OID_INV}    ${EMPTY}


    # System group key (storage node)
    ${SG_OID_1} =       Put Storagegroup    ${SYSTEM_KEY_SN}    ${PRIV_CID}   ${EMPTY}    ${S_OID_USER}
                        List Storagegroup    ${SYSTEM_KEY_SN}    ${PRIV_CID}   ${EMPTY}    ${SG_OID_1}  ${SG_OID_INV}
    @{EXPECTED_OIDS} =  Run Keyword If    "${RUN_TYPE}" == "Complex"    Get Split objects    ${SYSTEM_KEY_SN}    ${PRIV_CID}   ${S_OID_USER}
                        ...    ELSE IF    "${RUN_TYPE}" == "Simple"    Create List    ${S_OID_USER}
                        Get Storagegroup    ${SYSTEM_KEY_SN}    ${PRIV_CID}    ${SG_OID_1}   ${EMPTY}    ${EMPTY}    @{EXPECTED_OIDS}
                        Run Keyword And Expect Error        *
                        ...  Delete Storagegroup    ${SYSTEM_KEY_SN}    ${PRIV_CID}    ${SG_OID_1}    ${EMPTY}


    # System group key (Inner ring node)
                        Run Keyword And Expect Error        *
                        ...  Put Storagegroup    ${SYSTEM_KEY_IR}    ${PRIV_CID}   ${EMPTY}    ${S_OID_USER}
                        Run Keyword And Expect Error        *
                        ...  List Storagegroup    ${SYSTEM_KEY_IR}    ${PRIV_CID}   ${EMPTY}    ${SG_OID_INV}

                        @{EXPECTED_OIDS} =  Run Keyword If    "${RUN_TYPE}" == "Complex"    Get Split objects    ${USER_KEY}    ${PRIV_CID}   ${S_OID_USER}
                        ...    ELSE IF   "${RUN_TYPE}" == "Simple"    Create List   ${S_OID_USER}
                        Get Storagegroup    ${SYSTEM_KEY_IR}    ${PRIV_CID}    ${SG_OID_INV}   ${EMPTY}    ${EMPTY}    @{EXPECTED_OIDS}

                        Run Keyword And Expect Error        *
                        ...  Delete Storagegroup    ${SYSTEM_KEY_IR}    ${PRIV_CID}    ${SG_OID_INV}    ${EMPTY}



Cleanup
                            Cleanup Files
                            Get Docker Logs    acl_basic
