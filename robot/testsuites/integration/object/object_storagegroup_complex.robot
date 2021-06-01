*** Settings ***
Variables   ../../../variables/common.py
Library     Collections
Library     ../${RESOURCES}/neofs.py
Library     ../${RESOURCES}/payment_neogo.py

Resource    common_steps_object.robot
Resource    ../${RESOURCES}/setup_teardown.robot
Resource    ../${RESOURCES}/payment_operations.robot

*** Test cases ***
NeoFS Complex Storagegroup
    [Documentation]     Testcase to validate NeoFS operations with Storagegroup.
    [Tags]              Object  NeoFS  NeoCLI
    [Timeout]           20 min

    [Setup]             Setup

    ${WALLET}   ${ADDR}     ${WIF} =   Init Wallet with Address    ${ASSETS_DIR}
    Payment Operations      ${ADDR}     ${WIF}
    Prepare container       ${WIF}

    ${FILE_S} =         Generate file of bytes            ${COMPLEX_OBJ_SIZE}
    ${FILE_HASH_S} =    Get file hash                     ${FILE_S}

    # Put two Simple Object
    ${S_OID_1} =        Put object    ${WIF}    ${FILE_S}    ${CID}    ${EMPTY}    ${EMPTY}
    ${S_OID_2} =        Put object    ${WIF}    ${FILE_S}    ${CID}    ${EMPTY}    ${FILE_USR_HEADER}

    @{S_OBJ_ALL} =	Create List    ${S_OID_1}    ${S_OID_2}

                        Log    Storage group with 1 object
    ${SG_OID_1} =       Put Storagegroup    ${WIF}    ${CID}   ${EMPTY}    ${S_OID_1}
                        List Storagegroup    ${WIF}    ${CID}   ${EMPTY}    ${SG_OID_1}
    @{SPLIT_OBJ_1} =    Get Split objects    ${WIF}    ${CID}   ${S_OID_1}
                        Get Storagegroup    ${WIF}    ${CID}    ${SG_OID_1}   ${EMPTY}    ${COMPLEX_OBJ_SIZE}    @{SPLIT_OBJ_1}
    ${Tombstone} =      Delete Storagegroup    ${WIF}    ${CID}    ${SG_OID_1}    ${EMPTY}
                        Verify Head tombstone    ${WIF}    ${CID}    ${Tombstone}    ${SG_OID_1}    ${ADDR}
                        Run Keyword And Expect Error    *
                        ...  Get Storagegroup    ${WIF}    ${CID}    ${SG_OID_1}   ${EMPTY}    ${COMPLEX_OBJ_SIZE}    @{SPLIT_OBJ_1}
                        List Storagegroup    ${WIF}    ${CID}   ${EMPTY}    @{EMPTY}

                        Log    Storage group with 2 objects
    ${SG_OID_2} =       Put Storagegroup    ${WIF}    ${CID}    ${EMPTY}    @{S_OBJ_ALL}
                        List Storagegroup    ${WIF}    ${CID}   ${EMPTY}    ${SG_OID_2}
    @{SPLIT_OBJ_2} =    Get Split objects    ${WIF}    ${CID}   ${S_OID_2}
    @{SPLIT_OBJ_ALL} =  Combine Lists    ${SPLIT_OBJ_1}    ${SPLIT_OBJ_2}
    ${EXPECTED_SIZE} =  Evaluate    2*${COMPLEX_OBJ_SIZE}
                        Get Storagegroup    ${WIF}    ${CID}    ${SG_OID_2}   ${EMPTY}    ${EXPECTED_SIZE}    @{SPLIT_OBJ_ALL}
    ${Tombstone} =      Delete Storagegroup    ${WIF}    ${CID}    ${SG_OID_2}    ${EMPTY}
                        Verify Head tombstone    ${WIF}    ${CID}    ${Tombstone}    ${SG_OID_2}    ${ADDR}
                        Run Keyword And Expect Error    *
                        ...  Get Storagegroup    ${WIF}    ${CID}    ${SG_OID_2}   ${EMPTY}    ${EXPECTED_SIZE}    @{SPLIT_OBJ_ALL}
                        List Storagegroup    ${WIF}    ${CID}   ${EMPTY}    @{EMPTY}

                        Log    Incorrect input

                        Run Keyword And Expect Error    *
                        ...  Put Storagegroup    ${WIF}    ${CID}    ${EMPTY}    ${UNEXIST_OID}
                        Run Keyword And Expect Error    *
                        ...  Delete Storagegroup    ${WIF}    ${CID}    ${UNEXIST_OID}    ${EMPTY}

    [Teardown]          Teardown    object_storage_group_complex
