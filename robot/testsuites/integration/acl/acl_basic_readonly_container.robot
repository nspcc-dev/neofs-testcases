*** Settings ***
Variables    common.py

Library      neofs.py
Library      payment_neogo.py

Resource     common_steps_acl_basic.robot
Resource     payment_operations.robot
Resource     setup_teardown.robot


*** Test cases ***
Basic ACL Operations for Read-Only Container
    [Documentation]         Testcase to validate NeoFS operations with ACL for Read-Only Container.
    [Tags]                  ACL  NeoFS  NeoCLI
    [Timeout]               20 min

    [Setup]                 Setup

    ${WALLET}   ${ADDR}     ${USER_KEY} =   Prepare Wallet And Deposit
    ${WALLET_OTH}   ${ADDR_OTH}     ${OTHER_KEY} =   Prepare Wallet And Deposit

    ${READONLY_CID} =       Create Read-Only Container    ${USER_KEY}
    ${FILE_S}    ${FILE_S_HASH} =                        Generate file    ${SIMPLE_OBJ_SIZE}
                            Check Read-Only Container    Simple    ${USER_KEY}    ${FILE_S}    ${READONLY_CID}    ${OTHER_KEY}

    ${READONLY_CID} =       Create Read-Only Container    ${USER_KEY}
    ${FILE_S}    ${FILE_S_HASH} =             Generate file    ${COMPLEX_OBJ_SIZE}
                            Check Read-Only Container    Complex    ${USER_KEY}    ${FILE_S}    ${READONLY_CID}    ${OTHER_KEY}

    [Teardown]              Teardown    acl_basic_readonly_container


*** Keywords ***


Check Read-Only Container
    [Arguments]     ${RUN_TYPE}    ${USER_KEY}    ${FILE_S}    ${READONLY_CID}    ${OTHER_KEY}

    # Put
    ${S_OID_USER} =         Put Object         ${USER_KEY}    ${FILE_S}    ${READONLY_CID}    ${EMPTY}    ${EMPTY}
                            Run Keyword And Expect Error        *
                            ...  Put object    ${OTHER_KEY}    ${FILE_S}    ${READONLY_CID}    ${EMPTY}    ${EMPTY}
    ${S_OID_SYS_IR} =       Put Object         ${NEOFS_IR_WIF}    ${FILE_S}    ${READONLY_CID}    ${EMPTY}    ${EMPTY}
    ${S_OID_SYS_SN} =       Put object         ${NEOFS_SN_WIF}    ${FILE_S}    ${READONLY_CID}    ${EMPTY}    ${EMPTY}


    # Storage group Operations (Put, List, Get, Delete)
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

    ${SG_OID_IR} =      Put Storagegroup    ${NEOFS_IR_WIF}    ${READONLY_CID}   ${EMPTY}    ${S_OID_USER}
                        List Storagegroup    ${NEOFS_IR_WIF}    ${READONLY_CID}   ${EMPTY}    ${SG_OID_INV}    ${SG_OID_IR}
    @{EXPECTED_OIDS} =  Run Keyword If    "${RUN_TYPE}" == "Complex"    Get Split objects    ${USER_KEY}    ${READONLY_CID}   ${S_OID_USER}
                        ...    ELSE IF   "${RUN_TYPE}" == "Simple"    Create List   ${S_OID_USER}
                        Get Storagegroup    ${NEOFS_IR_WIF}    ${READONLY_CID}    ${SG_OID_IR}   ${EMPTY}    ${EMPTY}    @{EXPECTED_OIDS}
                        Run Keyword And Expect Error        *
                        ...  Delete Storagegroup    ${NEOFS_IR_WIF}    ${READONLY_CID}    ${SG_OID_IR}    ${EMPTY}

    # Get
                        Get object    ${USER_KEY}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read
                        Get Object    ${OTHER_KEY}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read
                        Get Object    ${NEOFS_IR_WIF}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read
                        Get Object    ${NEOFS_SN_WIF}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read

    # Get Range
                        Get Range           ${USER_KEY}    ${READONLY_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                        Get Range           ${OTHER_KEY}    ${READONLY_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                        Run Keyword And Expect Error        *
                        ...    Get Range    ${NEOFS_IR_WIF}    ${READONLY_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                        Run Keyword And Expect Error        *
                        ...    Get Range    ${NEOFS_SN_WIF}    ${READONLY_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256


    # Get Range Hash
                        Get Range hash    ${USER_KEY}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                        Get Range hash    ${OTHER_KEY}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                        Get Range hash    ${NEOFS_IR_WIF}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                        Get Range hash    ${NEOFS_SN_WIF}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    0:256

    # Search
    @{S_OBJ_RO} =       Create List       ${S_OID_USER}    ${S_OID_SYS_SN}    ${S_OID_SYS_IR}
                        Search Object     ${USER_KEY}    ${READONLY_CID}    --root    ${EMPTY}    ${EMPTY}    ${S_OBJ_RO}
                        Search Object     ${OTHER_KEY}    ${READONLY_CID}    --root    ${EMPTY}    ${EMPTY}    ${S_OBJ_RO}
                        Search Object     ${NEOFS_IR_WIF}    ${READONLY_CID}    --root    ${EMPTY}    ${EMPTY}    ${S_OBJ_RO}
                        Search Object     ${NEOFS_SN_WIF}    ${READONLY_CID}    --root    ${EMPTY}    ${EMPTY}    ${S_OBJ_RO}


    # Head
                        Head Object    ${USER_KEY}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                        Head Object    ${OTHER_KEY}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                        Head Object    ${NEOFS_IR_WIF}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                        Head Object    ${NEOFS_SN_WIF}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}

    # Delete
                        Run Keyword And Expect Error        *
                        ...  Delete object    ${OTHER_KEY}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}
                        Run Keyword And Expect Error        *
                        ...  Delete object    ${NEOFS_IR_WIF}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}
                        Run Keyword And Expect Error        *
                        ...  Delete object    ${NEOFS_SN_WIF}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}
                        Delete Object         ${USER_KEY}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}
