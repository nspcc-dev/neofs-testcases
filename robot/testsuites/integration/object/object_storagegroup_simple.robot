*** Settings ***
Variables   common.py

Library     neofs.py
Library     neofs_verbs.py
Library     payment_neogo.py

Resource    common_steps_object.robot
Resource    setup_teardown.robot
Resource    payment_operations.robot

*** Variables ***
${UNEXIST_OID} =        B2DKvkHnLnPvapbDgfpU1oVUPuXQo5LTfKVxmNDZXQff
&{USER_HEADER} =        key1=1      key2=2

*** Test cases ***
NeoFS Simple Storagegroup
    [Documentation]     Testcase to validate NeoFS operations with Storagegroup.
    [Tags]              Object  NeoFS  NeoCLI
    [Timeout]           20 min

    [Setup]             Setup

    ${WALLET}   ${ADDR}     ${WIF} =   Prepare Wallet And Deposit
    ${CID} =    Prepare container      ${WIF}    ${WALLET}

    ${FILE_S} =         Generate file of bytes            ${SIMPLE_OBJ_SIZE}
    ${FILE_HASH_S} =    Get file hash                     ${FILE_S}


    # Put two Simple Object
    ${S_OID_1} =        Put object    ${WALLET}    ${FILE_S}    ${CID}
    ${S_OID_2} =        Put object    ${WALLET}    ${FILE_S}    ${CID}    user_headers=&{USER_HEADER}

    @{S_OBJ_ALL} =	    Create List    ${S_OID_1}    ${S_OID_2}

                        Log    Storage group with 1 object
    ${SG_OID_1} =       Put Storagegroup    ${WALLET}    ${CID}   ${EMPTY}    ${S_OID_1}
                        List Storagegroup    ${WALLET}    ${CID}   ${EMPTY}    ${SG_OID_1}
                        Get Storagegroup    ${WALLET}    ${CID}    ${SG_OID_1}   ${EMPTY}    ${SIMPLE_OBJ_SIZE}    ${S_OID_1}
    ${Tombstone} =      Delete Storagegroup    ${WALLET}    ${CID}    ${SG_OID_1}    ${EMPTY}
                        Verify Head tombstone    ${WALLET}    ${CID}    ${Tombstone}    ${SG_OID_1}    ${ADDR}
                        Run Keyword And Expect Error    *
                        ...  Get Storagegroup    ${WALLET}    ${CID}    ${SG_OID_1}   ${EMPTY}    ${SIMPLE_OBJ_SIZE}    ${S_OID_1}
                        List Storagegroup    ${WALLET}    ${CID}   ${EMPTY}    @{EMPTY}


                        Log    Storage group with 2 objects
    ${SG_OID_2} =       Put Storagegroup    ${WALLET}    ${CID}    ${EMPTY}    @{S_OBJ_ALL}
                        List Storagegroup    ${WALLET}    ${CID}   ${EMPTY}    ${SG_OID_2}
    ${EXPECTED_SIZE} =  Evaluate    2*${SIMPLE_OBJ_SIZE}
                        Get Storagegroup    ${WALLET}    ${CID}    ${SG_OID_2}   ${EMPTY}    ${EXPECTED_SIZE}    @{S_OBJ_ALL}
    ${Tombstone} =      Delete Storagegroup    ${WALLET}    ${CID}    ${SG_OID_2}    ${EMPTY}
                        Verify Head tombstone    ${WALLET}    ${CID}    ${Tombstone}    ${SG_OID_2}    ${ADDR}
                        Run Keyword And Expect Error    *
                        ...  Get Storagegroup    ${WALLET}    ${CID}    ${SG_OID_2}   ${EMPTY}    ${EXPECTED_SIZE}    @{S_OBJ_ALL}
                        List Storagegroup    ${WALLET}    ${CID}   ${EMPTY}    @{EMPTY}

                        Log    Incorrect input

                        Run Keyword And Expect Error    *
                        ...  Put Storagegroup    ${WALLET}    ${CID}    ${EMPTY}    ${UNEXIST_OID}
                        Run Keyword And Expect Error    *
                        ...  Delete Storagegroup    ${WALLET}    ${CID}    ${UNEXIST_OID}    ${EMPTY}

    [Teardown]          Teardown    object_storage_group_simple
