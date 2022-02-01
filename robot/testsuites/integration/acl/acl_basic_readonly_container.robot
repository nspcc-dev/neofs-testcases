*** Settings ***
Variables    common.py

Library      neofs.py
Library      neofs_verbs.py
Library      payment_neogo.py

Resource     common_steps_acl_basic.robot
Resource     payment_operations.robot
Resource     setup_teardown.robot
Resource     complex_object_operations.robot


*** Test cases ***
Basic ACL Operations for Read-Only Container
    [Documentation]         Testcase to validate NeoFS operations with ACL for Read-Only Container.
    [Tags]                  ACL
    [Timeout]               20 min

    [Setup]                 Setup

    ${WALLET}   ${_}     ${_} =   Prepare Wallet And Deposit
    ${WALLET_OTH}   ${_}     ${_} =   Prepare Wallet And Deposit

    ${READONLY_CID} =       Create Read-Only Container    ${WALLET}
    ${FILE_S}    ${_} =     Generate file    ${SIMPLE_OBJ_SIZE}
                            Check Read-Only Container    Simple    ${WALLET}    ${FILE_S}    ${READONLY_CID}    ${WALLET_OTH}

    ${READONLY_CID} =       Create Read-Only Container    ${WALLET}
    ${FILE_S}    ${_} =     Generate file    ${COMPLEX_OBJ_SIZE}
                            Check Read-Only Container    Complex    ${WALLET}    ${FILE_S}    ${READONLY_CID}    ${WALLET_OTH}

    [Teardown]              Teardown    acl_basic_readonly_container


*** Keywords ***

Check Read-Only Container
    [Arguments]     ${RUN_TYPE}    ${USER_WALLET}    ${FILE_S}    ${READONLY_CID}    ${WALLET_OTH}

    ${WALLET_SN}    ${ADDR_SN} =     Prepare Wallet with WIF And Deposit    ${NEOFS_SN_WIF}
    ${WALLET_IR}    ${ADDR_IR} =     Prepare Wallet with WIF And Deposit    ${NEOFS_IR_WIF}

    # Put
    ${S_OID_USER} =         Put Object         ${USER_WALLET}    ${FILE_S}    ${READONLY_CID}
                            Run Keyword And Expect Error        *
                            ...  Put object    ${WALLET_OTH}    ${FILE_S}    ${READONLY_CID}
    ${S_OID_SYS_IR} =       Put Object         ${WALLET_IR}    ${FILE_S}    ${READONLY_CID}
    ${S_OID_SYS_SN} =       Put object         ${WALLET_SN}    ${FILE_S}    ${READONLY_CID}


    # Storage group Operations (Put, List, Get, Delete)
    ${SG_OID_INV} =     Put Storagegroup    ${USER_WALLET}    ${READONLY_CID}   ${EMPTY}    ${S_OID_USER}
    ${SG_OID_1} =       Put Storagegroup    ${USER_WALLET}    ${READONLY_CID}   ${EMPTY}    ${S_OID_USER}
                        List Storagegroup    ${USER_WALLET}    ${READONLY_CID}   ${EMPTY}    ${SG_OID_1}  ${SG_OID_INV}
    @{EXPECTED_OIDS} =  Run Keyword If    "${RUN_TYPE}" == "Complex"
                        ...     Get Object Parts By Link Object    ${USER_WALLET}    ${READONLY_CID}   ${S_OID_USER}
                        ...     ELSE IF   "${RUN_TYPE}" == "Simple"    Create List   ${S_OID_USER}
                        Get Storagegroup    ${USER_WALLET}    ${READONLY_CID}    ${SG_OID_1}   ${EMPTY}    ${EMPTY}    @{EXPECTED_OIDS}
                        Delete Storagegroup    ${USER_WALLET}    ${READONLY_CID}    ${SG_OID_1}    ${EMPTY}

                        Run Keyword And Expect Error        *
                        ...  Put Storagegroup    ${WALLET_OTH}    ${READONLY_CID}   ${EMPTY}    ${S_OID_USER}
                        List Storagegroup    ${WALLET_OTH}    ${READONLY_CID}   ${EMPTY}    ${SG_OID_INV}
    @{EXPECTED_OIDS} =  Run Keyword If    "${RUN_TYPE}" == "Complex"
                        ...     Get Object Parts By Link Object    ${USER_WALLET}    ${READONLY_CID}   ${S_OID_USER}
                        ...     ELSE IF   "${RUN_TYPE}" == "Simple"    Create List   ${S_OID_USER}
                        Get Storagegroup    ${WALLET_OTH}    ${READONLY_CID}    ${SG_OID_INV}   ${EMPTY}    ${EMPTY}    @{EXPECTED_OIDS}
                        Run Keyword And Expect Error        *
                        ...  Delete Storagegroup    ${WALLET_OTH}    ${READONLY_CID}    ${SG_OID_INV}    ${EMPTY}

    ${SG_OID_IR} =      Put Storagegroup    ${WALLET_IR}    ${READONLY_CID}   ${EMPTY}    ${S_OID_USER}
                        List Storagegroup    ${WALLET_IR}    ${READONLY_CID}   ${EMPTY}    ${SG_OID_INV}    ${SG_OID_IR}
    @{EXPECTED_OIDS} =  Run Keyword If    "${RUN_TYPE}" == "Complex"
                        ...     Get Object Parts By Link Object    ${USER_WALLET}    ${READONLY_CID}   ${S_OID_USER}
                        ...     ELSE IF   "${RUN_TYPE}" == "Simple"    Create List   ${S_OID_USER}
                        Get Storagegroup    ${WALLET_IR}    ${READONLY_CID}    ${SG_OID_IR}   ${EMPTY}    ${EMPTY}    @{EXPECTED_OIDS}
                        Run Keyword And Expect Error        *
                        ...  Delete Storagegroup    ${WALLET_IR}    ${READONLY_CID}    ${SG_OID_IR}    ${EMPTY}

    # Get
                        Get object    ${USER_WALLET}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read
                        Get Object    ${WALLET_OTH}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read
                        Get Object    ${WALLET_IR}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read
                        Get Object    ${WALLET_SN}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read

    # Get Range
                        Get Range           ${USER_WALLET}    ${READONLY_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                        Get Range           ${WALLET_OTH}    ${READONLY_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                        Run Keyword And Expect Error        *
                        ...    Get Range    ${WALLET_IR}    ${READONLY_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                        Run Keyword And Expect Error        *
                        ...    Get Range    ${WALLET_SN}    ${READONLY_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256


    # Get Range Hash
                        Get Range hash    ${USER_WALLET}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                        Get Range hash    ${WALLET_OTH}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                        Get Range hash    ${WALLET_IR}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                        Get Range hash    ${WALLET_SN}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    0:256

    # Search
    @{S_OBJ_RO} =       Create List       ${S_OID_USER}    ${S_OID_SYS_SN}    ${S_OID_SYS_IR}
                        Search Object     ${USER_WALLET}    ${READONLY_CID}    keys=--root    expected_objects_list=${S_OBJ_RO}
                        Search Object     ${WALLET_OTH}    ${READONLY_CID}    keys=--root    expected_objects_list=${S_OBJ_RO}
                        Search Object     ${WALLET_IR}    ${READONLY_CID}    keys=--root    expected_objects_list=${S_OBJ_RO}
                        Search Object     ${WALLET_SN}    ${READONLY_CID}    keys=--root    expected_objects_list=${S_OBJ_RO}


    # Head
                        Head Object    ${USER_WALLET}    ${READONLY_CID}    ${S_OID_USER}
                        Head Object    ${WALLET_OTH}    ${READONLY_CID}    ${S_OID_USER}
                        Head Object    ${WALLET_IR}    ${READONLY_CID}    ${S_OID_USER}
                        Head Object    ${WALLET_SN}    ${READONLY_CID}    ${S_OID_USER}

    # Delete
                        Run Keyword And Expect Error        *
                        ...  Delete object    ${WALLET_OTH}    ${READONLY_CID}    ${S_OID_USER}
                        Run Keyword And Expect Error        *
                        ...  Delete object    ${WALLET_IR}    ${READONLY_CID}    ${S_OID_USER}
                        Run Keyword And Expect Error        *
                        ...  Delete object    ${WALLET_SN}    ${READONLY_CID}    ${S_OID_USER}
                        Delete Object         ${USER_WALLET}    ${READONLY_CID}    ${S_OID_USER}
