*** Settings ***
Variables   common.py


Library     container.py
Library     complex_object_actions.py
Library     contract_keywords.py
Library     neofs_verbs.py
Library     neofs.py
Library     storage_policy.py
Library     utility_keywords.py

Library     Collections

Resource    setup_teardown.robot
Resource    verbs.robot
Resource    payment_operations.robot


*** Test cases ***
NeoFS Complex Object Operations
    [Documentation]     Testcase to validate NeoFS operations with complex object.
    [Tags]              Object
    [Timeout]           20 min

    [Setup]             Setup

    ${WALLET}   ${_}     ${_} =   Prepare Wallet And Deposit
    ${CID} =            Create container    ${WALLET}

    ${OID} =
    ...                 Run All Verbs Except Delete And Expect Success
    ...                 ${WALLET}   ${CID}      Complex

     ${COPIES} =        Get Complex Object Copies       ${WALLET}   ${CID}  ${OID}
                        Should Be Equal As Numbers  2   ${COPIES}

    ${PAYLOAD_LENGTH}
    ...     ${SPLIT_ID}
    ...     ${SPLIT_OBJECTS} =
    ...                 Restore Large Object By Last    ${WALLET}   ${CID}  ${OID}
                        Compare With Link Object        ${WALLET}   ${CID}  ${OID}    ${SPLIT_ID}     ${SPLIT_OBJECTS}
    &{RESPONSE} =       Head object                     ${WALLET}   ${CID}  ${OID}
                        Should Be Equal As Numbers      ${RESPONSE.header.payloadLength}  ${PAYLOAD_LENGTH}

                        Delete Object And Validate Tombstone
                        ...     ${WALLET}   ${CID}  ${OID}

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

    [Arguments]     ${WALLET}  ${CID}  ${LARGE_OID}

    ${LAST_OID} =           Get Last Object     ${WALLET}  ${CID}  ${LARGE_OID}
    &{LAST_OBJ_HEADER} =    Head Object         ${WALLET}  ${CID}  ${LAST_OID}   is_raw=True
                            Should Be Equal     ${LARGE_OID}    ${LAST_OBJ_HEADER.header.split.parent}

    ${SPLIT_ID} =           Set Variable    ${LAST_OBJ_HEADER.header.split.splitID}
    ${PART_OID} =           Set Variable    ${LAST_OBJ_HEADER.objectID}
    ${PAYLOAD_LENGTH} =     Set Variable    0
    @{PART_OBJECTS} =       Create List

    FOR     ${i}    IN RANGE    1000
        &{SPLIT_HEADER} =       Head object     ${WALLET}  ${CID}  ${PART_OID}  is_raw=True

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

    [Arguments]         ${WALLET}  ${CID}  ${LARGE_OID}    ${SPLIT_ID}      ${SPLIT_OBJECTS}

    ${LINK_OID} =       Get Link Object     ${WALLET}  ${CID}  ${LARGE_OID}
    &{LINK_HEADER} =    Head Object         ${WALLET}  ${CID}  ${LINK_OID}    is_raw=True

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
