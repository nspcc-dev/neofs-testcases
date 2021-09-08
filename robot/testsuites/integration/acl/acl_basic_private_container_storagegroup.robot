*** Settings ***
Variables       ../../../variables/common.py
Library     ../${RESOURCES}/neofs.py
Library     ../${RESOURCES}/payment_neogo.py

Library     Collections

Resource    common_steps_acl_basic.robot
Resource    ../${RESOURCES}/payment_operations.robot
Resource    ../${RESOURCES}/setup_teardown.robot


*** Test cases ***
Basic ACL Operations for Private Container
    [Documentation]         Testcase to validate NeoFS operations with ACL for Private Container.
    [Tags]                  ACL  NeoFS  NeoCLI
    [Timeout]               20 min

    [Setup]                 Setup

    ${WALLET}   ${ADDR}     ${USER_KEY} =   Prepare Wallet And Deposit
    ${WALLET_OTH}   ${ADDR_OTH}     ${OTHER_KEY} =   Prepare Wallet And Deposit

    ${PRIV_CID} =           Create Private Container    ${USER_KEY}
    ${FILE_S}    ${FILE_S_HASH} =                        Generate file    ${SIMPLE_OBJ_SIZE}
                            Check Private Container    Simple    ${USER_KEY}    ${FILE_S}    ${PRIV_CID}    ${OTHER_KEY}

    ${PRIV_CID} =           Create Private Container    ${USER_KEY}
    ${FILE_S}    ${FILE_S_HASH} =             Generate file    ${COMPLEX_OBJ_SIZE}
                            Check Private Container    Complex    ${USER_KEY}    ${FILE_S}    ${PRIV_CID}    ${OTHER_KEY}

    [Teardown]              Teardown    acl_basic_private_container_storagegroup


*** Keywords ***

Check Private Container
    [Arguments]     ${RUN_TYPE}    ${USER_KEY}    ${FILE_S}    ${PRIV_CID}    ${OTHER_KEY}

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
    ${SG_OID_1} =       Put Storagegroup    ${NEOFS_SN_WIF}    ${PRIV_CID}   ${EMPTY}    ${S_OID_USER}
                        List Storagegroup    ${NEOFS_SN_WIF}    ${PRIV_CID}   ${EMPTY}    ${SG_OID_1}  ${SG_OID_INV}
    @{EXPECTED_OIDS} =  Run Keyword If    "${RUN_TYPE}" == "Complex"    Get Split objects    ${NEOFS_SN_WIF}    ${PRIV_CID}   ${S_OID_USER}
                        ...    ELSE IF    "${RUN_TYPE}" == "Simple"    Create List    ${S_OID_USER}
                        Get Storagegroup    ${NEOFS_SN_WIF}    ${PRIV_CID}    ${SG_OID_1}   ${EMPTY}    ${EMPTY}    @{EXPECTED_OIDS}
                        Run Keyword And Expect Error        *
                        ...  Delete Storagegroup    ${NEOFS_SN_WIF}    ${PRIV_CID}    ${SG_OID_1}    ${EMPTY}


    # System group key (Inner ring node)
                        Run Keyword And Expect Error        *
                        ...  Put Storagegroup    ${NEOFS_IR_WIF}    ${PRIV_CID}   ${EMPTY}    ${S_OID_USER}
                        Run Keyword And Expect Error        *
                        ...  List Storagegroup    ${NEOFS_IR_WIF}    ${PRIV_CID}   ${EMPTY}    ${SG_OID_INV}

                        @{EXPECTED_OIDS} =  Run Keyword If    "${RUN_TYPE}" == "Complex"    Get Split objects    ${USER_KEY}    ${PRIV_CID}   ${S_OID_USER}
                        ...    ELSE IF   "${RUN_TYPE}" == "Simple"    Create List   ${S_OID_USER}
                        Get Storagegroup    ${NEOFS_IR_WIF}    ${PRIV_CID}    ${SG_OID_INV}   ${EMPTY}    ${EMPTY}    @{EXPECTED_OIDS}

                        Run Keyword And Expect Error        *
                        ...  Delete Storagegroup    ${NEOFS_IR_WIF}    ${PRIV_CID}    ${SG_OID_INV}    ${EMPTY}
