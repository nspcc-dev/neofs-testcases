*** Settings ***
Variables   common.py

Library     neofs_verbs.py
Library     complex_object_actions.py
Library     neofs.py
Library     payment_neogo.py
Library     contract_keywords.py
Library     Collections

Resource    common_steps_object.robot
Resource    setup_teardown.robot
Resource    payment_operations.robot

*** Variables ***
${CLEANUP_TIMEOUT} =    10s
&{FILE_USR_HEADER} =        key1=1      key2=abc
&{FILE_USR_HEADER_OTH} =    key1=2
${ALREADY_REMOVED_ERROR} =    code = 1024 message = object already removed


*** Test cases ***
NeoFS Complex Object Operations
    [Documentation]     Testcase to validate NeoFS operations with complex object.
    [Tags]              Object
    [Timeout]           20 min

    [Setup]             Setup

    ${WALLET}   ${ADDR}     ${WIF} =   Prepare Wallet And Deposit
    ${CID} =            Prepare container       ${WIF}

    ${FILE} =           Generate file of bytes              ${COMPLEX_OBJ_SIZE}
    ${FILE_HASH} =      Get file hash                       ${FILE}

    ${S_OID} =          Put object                 ${WIF}    ${FILE}       ${CID}
    ${H_OID} =          Put object                 ${WIF}    ${FILE}       ${CID}        user_headers=${FILE_USR_HEADER}
    ${H_OID_OTH} =      Put object                 ${WIF}    ${FILE}       ${CID}        user_headers=${FILE_USR_HEADER_OTH}

    Should Be True     '${S_OID}'!='${H_OID}' and '${H_OID}'!='${H_OID_OTH}'

                        Validate storage policy for object  ${WIF}    2             ${CID}         ${S_OID}
                        Validate storage policy for object  ${WIF}    2             ${CID}         ${H_OID}
                        Validate storage policy for object  ${WIF}    2             ${CID}         ${H_OID_OTH}

    @{S_OBJ_ALL} =      Create List    ${S_OID}       ${H_OID}     ${H_OID_OTH}
    @{S_OBJ_H} =        Create List    ${H_OID}
    @{S_OBJ_H_OTH} =    Create List    ${H_OID_OTH}

                        Search Object    ${WIF}    ${CID}        --root       expected_objects_list=${S_OBJ_ALL}

    ${GET_OBJ_S} =      Get object               ${WIF}    ${CID}        ${S_OID}
    ${GET_OBJ_H} =      Get object               ${WIF}    ${CID}        ${H_OID}

    ${FILE_HASH_S} =    Get file hash            ${GET_OBJ_S}
    ${FILE_HASH_H} =    Get file hash            ${GET_OBJ_H}

                        Should Be Equal          ${FILE_HASH_S}   ${FILE_HASH}
                        Should Be Equal          ${FILE_HASH_H}   ${FILE_HASH}

                        Get Range Hash           ${WIF}    ${CID}        ${S_OID}          ${EMPTY}       0:10
                        Get Range Hash           ${WIF}    ${CID}        ${H_OID}          ${EMPTY}       0:10

                        Get Range                ${WIF}    ${CID}        ${S_OID}          s_get_range    ${EMPTY}       0:10
                        Get Range                ${WIF}    ${CID}        ${H_OID}          h_get_range    ${EMPTY}       0:10

                        Search object            ${WIF}    ${CID}        --root        expected_objects_list=${S_OBJ_ALL}
                        Search object            ${WIF}    ${CID}        --root        filters=${FILE_USR_HEADER}      expected_objects_list=${S_OBJ_H}
                        Search object            ${WIF}    ${CID}        --root        filters=${FILE_USR_HEADER_OTH}  expected_objects_list=${S_OBJ_H_OTH}

    &{S_RESPONSE} =     Head object              ${WIF}    ${CID}        ${S_OID}
    &{H_RESPONSE} =     Head object              ${WIF}    ${CID}        ${H_OID}
                        Dictionary Should Contain Sub Dictionary
                            ...     ${H_RESPONSE}[header][attributes]
                            ...     ${FILE_USR_HEADER}
                            ...     msg="There are no User Headers in HEAD response"

    ${PAYLOAD_LENGTH}    ${SPLIT_ID}     ${SPLIT_OBJECTS} =      Restore Large Object By Last
                                                ...     ${WIF}    ${CID}        ${S_OID}
    ${H_PAYLOAD_LENGTH}    ${H_SPLIT_ID}       ${H_SPLIT_OBJECTS} =  Restore Large Object By Last
                                                ...     ${WIF}    ${CID}        ${H_OID}

                        Compare With Link Object    ${WIF}  ${CID}  ${S_OID}    ${SPLIT_ID}     ${SPLIT_OBJECTS}
                        Compare With Link Object    ${WIF}  ${CID}  ${H_OID}    ${H_SPLIT_ID}     ${H_SPLIT_OBJECTS}

                        Should Be Equal As Numbers     ${S_RESPONSE.header.payloadLength}  ${PAYLOAD_LENGTH}
                        Should Be Equal As Numbers     ${H_RESPONSE.header.payloadLength}  ${H_PAYLOAD_LENGTH}

    ${TOMBSTONE_S} =    Delete object            ${WIF}    ${CID}        ${S_OID}
    ${TOMBSTONE_H} =    Delete object            ${WIF}    ${CID}        ${H_OID}

                        Verify Head tombstone    ${WIF}    ${CID}        ${TOMBSTONE_S}     ${S_OID}    ${ADDR}
                        Verify Head tombstone    ${WIF}    ${CID}        ${TOMBSTONE_H}     ${H_OID}    ${ADDR}

                        Tick Epoch
                        # we assume that during this time objects must be deleted
                        Sleep   ${CLEANUP_TIMEOUT}

     ${ERR_MSG} =       Run Keyword And Expect Error        *
                        ...  Get object          ${WIF}    ${CID}        ${S_OID}
                        Should Contain      ${ERR_MSG}      ${ALREADY_REMOVED_ERROR}
     ${ERR_MSG} =       Run Keyword And Expect Error        *
                        ...  Get object          ${WIF}    ${CID}        ${H_OID}
                        Should Contain      ${ERR_MSG}      ${ALREADY_REMOVED_ERROR}

    [Teardown]          Teardown    object_complex


*** Keywords ***

Restore Large Object By Last
    [Documentation]     In this keyword we assemble Large Object from its parts. First, we search for the
             ...        Last Object; then, we try to restore the Large Object using Split Chain. We check
             ...        that all Object Parts have identical SplitID, accumulate total payload length and
             ...        compile a list of Object Parts. For the first part of split we also check if is
             ...        has the only `splitID` field in the split header.
             ...        The keyword returns total payload length, SplitID and list of Part Objects for
             ...        these data might be verified by other keywords.

    [Arguments]     ${WIF}  ${CID}  ${LARGE_OID}

    ${LAST_OID} =           Get Last Object     ${WIF}  ${CID}  ${LARGE_OID}
    &{LAST_OBJ_HEADER} =    Head Object         ${WIF}  ${CID}  ${LAST_OID}   is_raw=True
                            Should Be Equal     ${LARGE_OID}    ${LAST_OBJ_HEADER.header.split.parent}

    ${SPLIT_ID} =           Set Variable    ${LAST_OBJ_HEADER.header.split.splitID}
    ${PART_OID} =           Set Variable    ${LAST_OBJ_HEADER.objectID}
    ${PAYLOAD_LENGTH} =     Set Variable    0
    @{PART_OBJECTS} =       Create List

    FOR     ${i}    IN RANGE    1000
        &{SPLIT_HEADER} =       Head object     ${WIF}  ${CID}  ${PART_OID}  is_raw=True

        ${PAYLOAD_LENGTH} =     Evaluate    ${PAYLOAD_LENGTH} + ${SPLIT_HEADER.header.payloadLength}

        # Every Object of the given split contains the same SplitID
        Should Be Equal         ${SPLIT_HEADER.header.split.splitID}     ${SPLIT_ID}
        Should Be Equal         ${SPLIT_HEADER.header.objectType}        REGULAR

                                Append To List   ${PART_OBJECTS}     ${PART_OID}

        # If we have reached the First Object, it has no `previous` field.
        # Asserting this condition and exiting the loop.
        ${PASSED} =     Run Keyword And Return Status
                ...     Should Be Equal
                ...     ${SPLIT_HEADER.header.split.previous}   ${None}

        Exit For Loop If    ${PASSED}
        ${PART_OID} =       Set Variable    ${SPLIT_HEADER.header.split.previous}
    END

    [Return]    ${PAYLOAD_LENGTH}   ${SPLIT_ID}     ${PART_OBJECTS}


Compare With Link Object
    [Documentation]     The keyword accepts Large Object SplitID and its Part Objects as
                ...     a parameters. Then it requests the Link Object and verifies that
                ...     a Split Chain which it stores is equal to the Part Objects list.
                ...     In this way we check that Part Objects list restored from Last
                ...     Object and the Split Chain from Link Object are equal and the
                ...     system is able to restore the Large Object using any of these ways.

    [Arguments]         ${WIF}  ${CID}  ${LARGE_OID}    ${SPLIT_ID}      ${SPLIT_OBJECTS}

    ${LINK_OID} =       Get Link Object     ${WIF}  ${CID}  ${LARGE_OID}
    &{LINK_HEADER} =    Head Object         ${WIF}  ${CID}  ${LINK_OID}    is_raw=True

                        Reverse List    ${SPLIT_OBJECTS}
                        Lists Should Be Equal
                        ...     ${LINK_HEADER.header.split.children}
                        ...     ${SPLIT_OBJECTS}

                        Should Be Equal As Numbers
                        ...     ${LINK_HEADER.header.payloadLength}     0

                        Should Be Equal
                        ...     ${LINK_HEADER.header.objectType}    REGULAR

                        Should Be Equal
                        ...     ${LINK_HEADER.header.split.splitID}    ${SPLIT_ID}
