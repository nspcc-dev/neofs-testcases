*** Settings ***
Variables   common.py

Library     neofs_verbs.py
Library     container.py
Library     contract_keywords.py
Library     utility_keywords.py

Resource    setup_teardown.robot
Resource    payment_operations.robot

*** Variables ***
${CLEANUP_TIMEOUT} =    10s

*** Test cases ***
NeoFS Simple Object Operations
    [Documentation]     Testcase to validate NeoFS object expiration option.
    [Tags]              Object  NeoFS  NeoCLI
    [Timeout]           20 min

    [Setup]             Setup

    ${WALLET}    ${_}    ${_} =   Prepare Wallet And Deposit
    ${CID} =            Create container    ${WALLET}

    ${FILE}    ${FILE_HASH} =    Generate File    ${SIMPLE_OBJ_SIZE}
    ${EPOCH} =          Get Epoch

    ${EPOCH_PRE} =      Evaluate    ${EPOCH}-1
    ${EPOCH_NEXT} =     Evaluate    ${EPOCH}+1
    ${EPOCH_POST} =     Evaluate    ${EPOCH}+1000

                        # Failed on attempt to create epoch from the past
                        Run Keyword And Expect Error        *
                        ...  Put object    ${WALLET}    ${FILE}    ${CID}    options= --attributes __NEOFS__EXPIRATION_EPOCH=${EPOCH_PRE}

                        # Put object with different expiration epoch numbers (current, next, and from the distant future)
    ${OID_CUR} =        Put object    ${WALLET}    ${FILE}    ${CID}    options= --attributes __NEOFS__EXPIRATION_EPOCH=${EPOCH}
    ${OID_NXT} =        Put object    ${WALLET}    ${FILE}    ${CID}    options= --attributes __NEOFS__EXPIRATION_EPOCH=${EPOCH_NEXT}
    ${OID_PST} =        Put object    ${WALLET}    ${FILE}    ${CID}    options= --attributes __NEOFS__EXPIRATION_EPOCH=${EPOCH_POST}

                        # Check objects for existence
                        Get object    ${WALLET}    ${CID}    ${OID_CUR}    ${EMPTY}    file_read_cur
                        Get object    ${WALLET}    ${CID}    ${OID_NXT}    ${EMPTY}    file_read_nxt
                        Get object    ${WALLET}    ${CID}    ${OID_PST}    ${EMPTY}    file_read_pst

                        # Increment epoch to check that expired objects (OID_CUR) will be removed
                        Tick Epoch
                        # we assume that during this time objects must be deleted
                        Sleep   ${CLEANUP_TIMEOUT}
                        Run Keyword And Expect Error        *
                        ...  Get object    ${WALLET}    ${CID}    ${OID_CUR}    ${EMPTY}    file_read

                        # Check that correct object with expiration in the future is existed
                        Get object    ${WALLET}    ${CID}    ${OID_NXT}    ${EMPTY}    file_read
                        Get object    ${WALLET}    ${CID}    ${OID_PST}    ${EMPTY}    file_read_pst

                        # Increment one more epoch to check that expired object (OID_NXT) will be removed
                        Tick Epoch
                        # we assume that during this time objects must be deleted
                        Sleep   ${CLEANUP_TIMEOUT}
                        Run Keyword And Expect Error        *
                        ...  Get object    ${WALLET}    ${CID}    ${OID_NXT}    ${EMPTY}    file_read

                        # Check that correct object with expiration in the distant future is existed
                        Get object    ${WALLET}    ${CID}    ${OID_PST}    ${EMPTY}    file_read_pst

    [Teardown]          Teardown    object_expiration
