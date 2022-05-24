*** Settings ***
Variables   common.py

Library     neofs.py
Library     neofs_verbs.py
Library     container.py
Library     contract_keywords.py
Library     Collections
Library     utility_keywords.py

Resource    payment_operations.robot
Resource    setup_teardown.robot

*** Variables ***
${CLEANUP_TIMEOUT} =    10s
&{FILE_USR_HEADER} =       key1=1     key2=abc
&{FILE_USR_HEADER_OTH} =   key1=2


*** Test cases ***
NeoFS Simple Object Operations
    [Documentation]     Testcase to validate NeoFS operations with simple object.
    [Tags]              Object
    [Timeout]           10 min

    [Setup]             Setup

    ${WALLET}    ${ADDR}    ${_} =   Prepare Wallet And Deposit
    ${CID} =            Create container    ${WALLET}

    ${FILE}    ${FILE_HASH} =    Generate file    ${SIMPLE_OBJ_SIZE}

    ${S_OID} =          Put object          ${WALLET}    ${FILE}       ${CID}
    ${H_OID} =          Put object          ${WALLET}    ${FILE}       ${CID}      user_headers=${FILE_USR_HEADER}
    ${H_OID_OTH} =      Put object          ${WALLET}    ${FILE}       ${CID}      user_headers=${FILE_USR_HEADER_OTH}

                        Validate storage policy for object  ${WALLET}    2         ${CID}            ${S_OID}
                        Validate storage policy for object  ${WALLET}    2         ${CID}            ${H_OID}
                        Validate storage policy for object  ${WALLET}    2         ${CID}            ${H_OID_OTH}

    @{S_OBJ_ALL} =	Create List         ${S_OID}       ${H_OID}      ${H_OID_OTH}
    @{S_OBJ_H} =	Create List         ${H_OID}
    @{S_OBJ_H_OTH} =    Create List         ${H_OID_OTH}

    ${GET_OBJ_S} =      Get object          ${WALLET}    ${CID}        ${S_OID}
    ${GET_OBJ_H} =      Get object          ${WALLET}    ${CID}        ${H_OID}

    ${FILE_HASH_S} =    Get file hash            ${GET_OBJ_S}
    ${FILE_HASH_H} =    Get file hash            ${GET_OBJ_H}

                        Should Be Equal        ${FILE_HASH_S}   ${FILE_HASH}
                        Should Be Equal        ${FILE_HASH_H}   ${FILE_HASH}

                        Get Range Hash          ${WALLET}    ${CID}        ${S_OID}            ${EMPTY}       0:10
                        Get Range Hash          ${WALLET}    ${CID}        ${H_OID}            ${EMPTY}       0:10

                        Get Range               ${WALLET}    ${CID}        ${S_OID}            s_get_range    ${EMPTY}       0:10
                        Get Range               ${WALLET}    ${CID}        ${H_OID}            h_get_range    ${EMPTY}       0:10

                        Search object           ${WALLET}    ${CID}        expected_objects_list=${S_OBJ_ALL}
                        Search object           ${WALLET}    ${CID}        filters=${FILE_USR_HEADER}        expected_objects_list=${S_OBJ_H}
                        Search object           ${WALLET}    ${CID}        filters=${FILE_USR_HEADER_OTH}    expected_objects_list=${S_OBJ_H_OTH}

                        Head object             ${WALLET}    ${CID}        ${S_OID}
    &{RESPONSE} =       Head object             ${WALLET}    ${CID}        ${H_OID}
                        Dictionary Should Contain Sub Dictionary
                            ...     ${RESPONSE}[header][attributes]
                            ...     ${FILE_USR_HEADER}
                            ...     msg="There are no User Headers in HEAD response"

    ${TOMBSTONE_S} =    Delete object                       ${WALLET}    ${CID}        ${S_OID}
    ${TOMBSTONE_H} =    Delete object                       ${WALLET}    ${CID}        ${H_OID}

                        Verify Head tombstone               ${WALLET}    ${CID}        ${TOMBSTONE_S}     ${S_OID}    ${ADDR}
                        Verify Head tombstone               ${WALLET}    ${CID}        ${TOMBSTONE_H}     ${H_OID}    ${ADDR}

                        Tick Epoch
                        # we assume that during this time objects must be deleted
                        Sleep   ${CLEANUP_TIMEOUT}

                        Run Keyword And Expect Error        *
                        ...  Get object          ${WALLET}    ${CID}        ${S_OID}           ${EMPTY}       ${GET_OBJ_S}

                        Run Keyword And Expect Error        *
                        ...  Get object          ${WALLET}    ${CID}        ${H_OID}           ${EMPTY}       ${GET_OBJ_H}

    [Teardown]          Teardown    object_simple
