*** Settings ***
Variables   common.py

Library     epoch.py
Library     tombstone.py
Library     neofs_verbs.py
Library     utility_keywords.py
Library     Collections
Library     OperatingSystem

*** Variables ***
${CLEANUP_TIMEOUT} =    10s

*** Keywords ***

Run All Verbs Except Delete And Expect Success
    [Arguments]   ${WALLET}    ${CID}   ${COMPLEXITY}

    ${OBJ_SIZE} =       Run Keyword If  """${COMPLEXITY}""" == """Simple"""
                        ...     Set Variable    ${SIMPLE_OBJ_SIZE}
                        ...     ELSE
                        ...     Set Variable    ${COMPLEX_OBJ_SIZE}

    ${FILE}    ${FILE_HASH} =    Generate file    ${OBJ_SIZE}

    ${OID} =            Put object          ${WALLET}    ${FILE}      ${CID}
    ${OBJ_PATH} =       Get object          ${WALLET}    ${CID}       ${OID}
    ${DOWNLOADED_FILE_HASH} =
        ...             Get file hash       ${OBJ_PATH}
                        Should Be Equal     ${DOWNLOADED_FILE_HASH}   ${FILE_HASH}

    # TODO: get rid of ${EMPTY}
    ${RANGE_FILE}
    ...     ${DATA_RANGE} =
    ...                 Get Range           ${WALLET}    ${CID}    ${OID}   ${EMPTY}    ${EMPTY}   0:10
    ${FILE_CONTENT} =   Get Binary File     ${FILE}
                        Should Contain      ${FILE_CONTENT}     ${DATA_RANGE}

    # TODO: get rid of ${EMPTY}
    ${RANGE_HASH} =     Get Range Hash      ${WALLET}    ${CID}    ${OID}   ${EMPTY}    0:10
    ${GR_HASH} =        Get File Hash       ${RANGE_FILE}
                        Should Be Equal     ${GR_HASH}      ${RANGE_HASH}

    ${FOUND_OBJECTS} =  Search object       ${WALLET}    ${CID}    keys=${OID}
                        List Should Contain Value
                        ...     ${FOUND_OBJECTS}
                        ...     ${OID}

    &{RESPONSE} =       Head object         ${WALLET}    ${CID}    ${OID}

    [Return]    ${OID}

Delete Object And Validate Tombstone
    [Arguments]   ${WALLET}    ${CID}   ${OID}

    ${TOMBSTONE_ID} =   Delete object               ${WALLET}    ${CID}    ${OID}
                        Verify Head tombstone       ${WALLET}    ${CID}    ${TOMBSTONE_ID}     ${OID}

                        Tick Epoch
                        # we assume that during this time objects must be deleted
                        Sleep   ${CLEANUP_TIMEOUT}

    ${ERR} =            Run Keyword And Expect Error        *
                        ...  Get object     ${WALLET}    ${CID}    ${OID}
                        Should Contain      ${ERR}      code = 2052 message = object already removed
