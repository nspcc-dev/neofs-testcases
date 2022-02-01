*** Settings ***
Variables    common.py

Library     neofs.py
Library     neofs_verbs.py
Library     payment_neogo.py
Library     Collections

Resource    common_steps_acl_basic.robot
Resource    payment_operations.robot
Resource    setup_teardown.robot
Resource    complex_object_operations.robot


*** Test cases ***
Basic ACL Operations for Private Container
    [Documentation]         Testcase to validate NeoFS operations with ACL for Private Container.
    [Tags]                  ACL
    [Timeout]               20 min

    [Setup]                 Setup

    ${WALLET}   ${_}     ${_} =   Prepare Wallet And Deposit
    ${WALLET_OTH}   ${_}     ${_} =   Prepare Wallet And Deposit

    ${PRIV_CID} =           Create Private Container    ${WALLET}
    ${FILE_S}    ${_} =     Generate file    ${SIMPLE_OBJ_SIZE}
                            Check Private Container    Simple    ${WALLET}    ${FILE_S}    ${PRIV_CID}    ${WALLET_OTH}

    ${PRIV_CID} =           Create Private Container    ${WALLET}
    ${FILE_S}    ${_} =     Generate file    ${COMPLEX_OBJ_SIZE}
                            Check Private Container    Complex    ${WALLET}    ${FILE_S}    ${PRIV_CID}    ${WALLET_OTH}

    [Teardown]              Teardown    acl_basic_private_container_storagegroup


*** Keywords ***

Check Private Container
    [Arguments]     ${RUN_TYPE}    ${USER_WALLET}    ${FILE_S}    ${PRIV_CID}    ${OTHER_WALLET}

    ${WALLET_SN}    ${ADDR_SN} =     Prepare Wallet with WIF And Deposit    ${NEOFS_SN_WIF}
    ${WALLET_IR}    ${ADDR_IR} =     Prepare Wallet with WIF And Deposit    ${NEOFS_IR_WIF}

    # Put target object to use in storage groups
    ${S_OID_USER} =     Put object    ${USER_WALLET}    ${FILE_S}    ${PRIV_CID}


    # Storage group Operations (Put, List, Get, Delete) with different Keys
    # User group key
    ${SG_OID_INV} =     Put Storagegroup    ${USER_WALLET}    ${PRIV_CID}   ${EMPTY}    ${S_OID_USER}
    ${SG_OID} =         Put Storagegroup    ${USER_WALLET}    ${PRIV_CID}   ${EMPTY}    ${S_OID_USER}
                        List Storagegroup    ${USER_WALLET}    ${PRIV_CID}   ${EMPTY}    ${SG_OID}  ${SG_OID_INV}
    @{EXPECTED_OIDS} =  Run Keyword If    "${RUN_TYPE}" == "Complex"
                        ...     Get Object Parts By Link Object    ${USER_WALLET}    ${PRIV_CID}   ${S_OID_USER}
                        ...     ELSE IF   "${RUN_TYPE}" == "Simple"    Create List   ${S_OID_USER}
                        Get Storagegroup    ${USER_WALLET}    ${PRIV_CID}    ${SG_OID}   ${EMPTY}    ${EMPTY}    @{EXPECTED_OIDS}
                        Delete Storagegroup    ${USER_WALLET}    ${PRIV_CID}    ${SG_OID}    ${EMPTY}


    # "Others" group key
                        Run Keyword And Expect Error        *
                        ...  Put Storagegroup    ${OTHER_WALLET}    ${PRIV_CID}   ${EMPTY}    ${S_OID_USER}
                        Run Keyword And Expect Error        *
                        ...  List Storagegroup    ${OTHER_WALLET}    ${PRIV_CID}   ${EMPTY}    ${SG_OID_INV}
                        Run Keyword And Expect Error        *
                        ...  Get Storagegroup    ${OTHER_WALLET}    ${PRIV_CID}    ${SG_OID_INV}   ${EMPTY}    ${EMPTY}    ${S_OID_USER}
                        Run Keyword And Expect Error        *
                        ...  Delete Storagegroup    ${OTHER_WALLET}    ${PRIV_CID}    ${SG_OID_INV}    ${EMPTY}


    # System group key (Storage Node)
    ${SG_OID_SN} =      Put Storagegroup    ${WALLET_SN}    ${PRIV_CID}   ${EMPTY}    ${S_OID_USER}
                        List Storagegroup    ${WALLET_SN}    ${PRIV_CID}   ${EMPTY}    ${SG_OID_SN}  ${SG_OID_INV}
    @{EXPECTED_OIDS} =  Run Keyword If    "${RUN_TYPE}" == "Complex"
                        ...     Get Object Parts By Link Object    ${WALLET_SN}    ${PRIV_CID}   ${S_OID_USER}
                        ...     ELSE IF    "${RUN_TYPE}" == "Simple"    Create List    ${S_OID_USER}
                        Get Storagegroup    ${WALLET_SN}    ${PRIV_CID}    ${SG_OID_SN}   ${EMPTY}    ${EMPTY}    @{EXPECTED_OIDS}
                        Run Keyword And Expect Error        *
                        ...  Delete Storagegroup    ${WALLET_SN}    ${PRIV_CID}    ${SG_OID_SN}    ${EMPTY}


    # System group key (Inner Ring Node)
    ${SG_OID_IR} =      Put Storagegroup    ${WALLET_IR}    ${PRIV_CID}   ${EMPTY}    ${S_OID_USER}
                        List Storagegroup    ${WALLET_IR}    ${PRIV_CID}   ${EMPTY}    ${SG_OID_SN}    ${SG_OID_IR}    ${SG_OID_INV}
    @{EXPECTED_OIDS} =  Run Keyword If    "${RUN_TYPE}" == "Complex"
                        ...     Get Object Parts By Link Object    ${USER_WALLET}    ${PRIV_CID}   ${S_OID_USER}
                        ...     ELSE IF   "${RUN_TYPE}" == "Simple"    Create List   ${S_OID_USER}
                        Get Storagegroup    ${WALLET_IR}    ${PRIV_CID}    ${SG_OID_IR}   ${EMPTY}    ${EMPTY}    @{EXPECTED_OIDS}
                        Run Keyword And Expect Error        *
                        ...  Delete Storagegroup    ${WALLET_IR}    ${PRIV_CID}    ${SG_OID_IR}    ${EMPTY}
