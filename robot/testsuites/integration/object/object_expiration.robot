*** Settings ***
Variables   ../../../variables/common.py

Library     ../${RESOURCES}/neofs.py
Library     ../${RESOURCES}/payment_neogo.py

Resource    common_steps_object.robot
Resource    ../${RESOURCES}/setup_teardown.robot
Resource    ../${RESOURCES}/payment_operations.robot

*** Variables ***
${CLEANUP_TIMEOUT} =    10s

*** Test cases ***
NeoFS Simple Object Operations
    [Documentation]     Testcase to validate NeoFS object expiration option.
    [Tags]              Object  NeoFS  NeoCLI
    [Timeout]           20 min

    [Setup]             Setup

    ${WALLET}   ${ADDR}     ${WIF} =   Init Wallet with Address    ${ASSETS_DIR}
    Payment Operations      ${ADDR}     ${WIF}
    Prepare container       ${WIF}

    ${FILE} =           Generate file of bytes    ${SIMPLE_OBJ_SIZE}
    ${FILE_HASH} =      Get file hash    ${FILE}

    ${EPOCH} =          Get Epoch

    ${EPOCH_PRE} =      Evaluate    ${EPOCH}-1
    ${EPOCH_NEXT} =     Evaluate    ${EPOCH}+1
    ${EPOCH_POST} =     Evaluate    ${EPOCH}+1000

                        # Failed on attempt to create epoch from the past
                        Run Keyword And Expect Error        *
                        ...  Put object    ${WIF}    ${FILE}    ${CID}    ${EMPTY}    __NEOFS__EXPIRATION_EPOCH=${EPOCH_PRE}

                        # Put object with different expiration epoch numbers (current, next, and from the distant future)
    ${OID_CUR} =        Put object    ${WIF}    ${FILE}    ${CID}    ${EMPTY}    __NEOFS__EXPIRATION_EPOCH=${EPOCH}
    ${OID_NXT} =        Put object    ${WIF}    ${FILE}    ${CID}    ${EMPTY}    __NEOFS__EXPIRATION_EPOCH=${EPOCH_NEXT}
    ${OID_PST} =        Put object    ${WIF}    ${FILE}    ${CID}    ${EMPTY}    __NEOFS__EXPIRATION_EPOCH=${EPOCH_POST}

                        # Check objects for existence
                        Get object    ${WIF}    ${CID}    ${OID_CUR}    ${EMPTY}    file_read_cur
                        Get object    ${WIF}    ${CID}    ${OID_NXT}    ${EMPTY}    file_read_nxt
                        Get object    ${WIF}    ${CID}    ${OID_PST}    ${EMPTY}    file_read_pst

                        # Increment epoch to check that expired objects (OID_CUR) will be removed
                        Tick Epoch
                        # we assume that during this time objects must be deleted
                        Sleep   ${CLEANUP_TIMEOUT}
                        Run Keyword And Expect Error        *
                        ...  Get object    ${WIF}    ${CID}    ${OID_CUR}    ${EMPTY}    file_read

                        # Check that correct object with expiration in the future is existed
                        Get object    ${WIF}    ${CID}    ${OID_NXT}    ${EMPTY}    file_read
                        Get object    ${WIF}    ${CID}    ${OID_PST}    ${EMPTY}    file_read_pst

                        # Increment one more epoch to check that expired object (OID_NXT) will be removed
                        Tick Epoch
                        # we assume that during this time objects must be deleted
                        Sleep   ${CLEANUP_TIMEOUT}
                        Run Keyword And Expect Error        *
                        ...  Get object    ${WIF}    ${CID}    ${OID_NXT}    ${EMPTY}    file_read

                        # Check that correct object with expiration in the distant future is existed
                        Get object    ${WIF}    ${CID}    ${OID_PST}    ${EMPTY}    file_read_pst

    [Teardown]          Teardown    object_expiration
