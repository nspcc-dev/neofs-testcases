*** Settings ***
Variables   ../../../variables/common.py

Library     ../${RESOURCES}/neofs.py
Library     ../${RESOURCES}/payment_neogo.py
Library     ../${RESOURCES}/utility_keywords.py
Resource    common_steps_object.robot


*** Test cases ***
NeoFS Simple Object Operations
    [Documentation]     Testcase to validate NeoFS object expiration option.
    [Tags]              Object  NeoFS  NeoCLI
    [Timeout]           20 min

    [Setup]             Create Temporary Directory

                        Payment operations
                        Prepare container

    ${FILE} =           Generate file of bytes    ${SIMPLE_OBJ_SIZE}
    ${FILE_HASH} =      Get file hash    ${FILE}

    ${EPOCH} =          Get Epoch    ${PRIV_KEY}

    ${EPOCH_PRE} =      Evaluate    ${EPOCH}-1
    ${EPOCH_NEXT} =     Evaluate    ${EPOCH}+1
    ${EPOCH_POST} =     Evaluate    ${EPOCH}+1000

                        # Failed on attempt to create epoch from the past
                        Run Keyword And Expect Error        *
                        ...  Put object    ${PRIV_KEY}    ${FILE}    ${CID}    ${EMPTY}    __NEOFS__EXPIRATION_EPOCH=${EPOCH_PRE}

                        # Put object with different expiration epoch numbers (current, next, and from the distant future)
    ${OID_CUR} =        Put object    ${PRIV_KEY}    ${FILE}    ${CID}    ${EMPTY}    __NEOFS__EXPIRATION_EPOCH=${EPOCH}
    ${OID_NXT} =        Put object    ${PRIV_KEY}    ${FILE}    ${CID}    ${EMPTY}    __NEOFS__EXPIRATION_EPOCH=${EPOCH_NEXT}
    ${OID_PST} =        Put object    ${PRIV_KEY}    ${FILE}    ${CID}    ${EMPTY}    __NEOFS__EXPIRATION_EPOCH=${EPOCH_POST}

                        # Check objects for existence
                        Get object    ${PRIV_KEY}    ${CID}    ${OID_CUR}    ${EMPTY}    file_read_cur
                        Get object    ${PRIV_KEY}    ${CID}    ${OID_NXT}    ${EMPTY}    file_read_nxt
                        Get object    ${PRIV_KEY}    ${CID}    ${OID_PST}    ${EMPTY}    file_read_pst

                        # Wait one epoch to check that expired objects (OID_CUR) will be removed
                        Sleep   ${NEOFS_EPOCH_TIMEOUT}

                        Run Keyword And Expect Error        *
                        ...  Get object    ${PRIV_KEY}    ${CID}    ${OID_CUR}    ${EMPTY}    file_read

                        # Check that correct object with expiration in the future is existed
                        Get object    ${PRIV_KEY}    ${CID}    ${OID_NXT}    ${EMPTY}    file_read
                        Get object    ${PRIV_KEY}    ${CID}    ${OID_PST}    ${EMPTY}    file_read_pst

                        # Wait one more epoch to check that expired object (OID_NXT) will be removed
                        Sleep   ${NEOFS_EPOCH_TIMEOUT}

                        Run Keyword And Expect Error        *
                        ...  Get object    ${PRIV_KEY}    ${CID}    ${OID_NXT}    ${EMPTY}    file_read

                        # Check that correct object with expiration in the distant future is existed
                        Get object    ${PRIV_KEY}    ${CID}    ${OID_PST}    ${EMPTY}    file_read_pst

    [Teardown]          Cleanup



*** Keywords ***

Cleanup
                        Cleanup Files
                        Get Docker Logs    object_expiration





