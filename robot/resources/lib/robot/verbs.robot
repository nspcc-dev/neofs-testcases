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
    [Arguments]   ${WALLET}    ${CID}   ${COMPLEXITY}   ${BEARER_TOKEN}=${EMPTY}    ${REQUEST_HEADERS}=${EMPTY}

    ${OBJ_SIZE} =       Run Keyword If  """${COMPLEXITY}""" == """Simple"""
                        ...     Set Variable    ${SIMPLE_OBJ_SIZE}
                        ...     ELSE
                        ...     Set Variable    ${COMPLEX_OBJ_SIZE}

    ${FILE}    ${FILE_HASH} =    Generate file    ${OBJ_SIZE}

    ${OID} =            Put object          ${WALLET}    ${FILE}      ${CID}
                                            ...     bearer=${BEARER_TOKEN}
                                            ...     options=${REQUEST_HEADERS}
    ${OBJ_PATH} =       Get object          ${WALLET}    ${CID}       ${OID}
                                            ...     bearer_token=${BEARER_TOKEN}
                                            ...     options=${REQUEST_HEADERS}
    ${DOWNLOADED_FILE_HASH} =
        ...             Get file hash       ${OBJ_PATH}
                        Should Be Equal     ${DOWNLOADED_FILE_HASH}   ${FILE_HASH}

    ${RANGE_FILE}
    ...     ${DATA_RANGE} =
    ...                 Get Range           ${WALLET}    ${CID}    ${OID}
                                            ...     bearer=${BEARER_TOKEN}
                                            ...     range_cut=0:10
                                            ...     options=${REQUEST_HEADERS}
    ${FILE_CONTENT} =   Get Binary File     ${FILE}
                        Should Contain      ${FILE_CONTENT}     ${DATA_RANGE}

    ${RANGE_HASH} =     Get Range Hash      ${WALLET}    ${CID}    ${OID}
                                            ...     bearer_token=${BEARER_TOKEN}
                                            ...     range_cut=0:10
                                            ...     options=${REQUEST_HEADERS}
    ${GR_HASH} =        Get File Hash       ${RANGE_FILE}
                        Should Be Equal     ${GR_HASH}      ${RANGE_HASH}

    ${FOUND_OBJECTS} =  Search object       ${WALLET}    ${CID}    keys=${OID}
                                            ...     bearer=${BEARER_TOKEN}
                                            ...     options=${REQUEST_HEADERS}
                        List Should Contain Value
                        ...     ${FOUND_OBJECTS}
                        ...     ${OID}

    &{RESPONSE} =       Head object         ${WALLET}    ${CID}    ${OID}
                                            ...     bearer_token=${BEARER_TOKEN}
                                            ...     options=${REQUEST_HEADERS}

    [Return]    ${OID}

Delete Object And Validate Tombstone
    [Arguments]   ${WALLET}    ${CID}   ${OID}   ${BEARER_TOKEN}=${EMPTY}    ${REQUEST_HEADERS}=${EMPTY}

    ${TOMBSTONE_ID} =   Delete object   ${WALLET}    ${CID}    ${OID}
                                        ...     bearer=${BEARER_TOKEN}
                                        ...     options=${REQUEST_HEADERS}
                        Verify Head tombstone       ${WALLET}    ${CID}    ${TOMBSTONE_ID}     ${OID}
                                        ...     bearer=${BEARER_TOKEN}
                                        ...     options=${REQUEST_HEADERS}

                        Tick Epoch
                        # we assume that during this time objects must be deleted
                        Sleep   ${CLEANUP_TIMEOUT}

    ${ERR} =            Run Keyword And Expect Error        *
                        ...  Get object     ${WALLET}    ${CID}    ${OID}
                                        ...     bearer_token=${BEARER_TOKEN}
                                        ...     options=${REQUEST_HEADERS}
                        Should Contain      ${ERR}      code = 2052 message = object already removed

Run All Verbs And Expect Failure
    [Arguments]   ${ERROR}   ${WALLET}    ${CID}   ${OID}

    ${FILE}    ${_} =    Generate file    ${SIMPLE_OBJ_SIZE}

    ${ERR} =    Run Keyword And Expect Error       *
                ...     Put object      ${WALLET}   ${FILE}      ${CID}
                Should Contain          ${ERR}    ${ERROR}

    ${ERR} =    Run Keyword And Expect Error       *
                ...     Get object      ${WALLET}   ${CID}       ${OID}
                Should Contain          ${ERR}    ${ERROR}

    ${ERR} =    Run Keyword And Expect Error       *
                ...     Get Range       ${WALLET}   ${CID}    ${OID}   0:10
                Should Contain          ${ERR}    ${ERROR}

    # TODO: get rid of ${EMPTY}
    ${ERR} =    Run Keyword And Expect Error       *
                ...     Get Range Hash  ${WALLET}   ${CID}    ${OID}   ${EMPTY}    0:10
                Should Contain          ${ERR}    ${ERROR}

    ${ERR} =    Run Keyword And Expect Error       *
                ...     Search object   ${WALLET}   ${CID}    keys=${OID}
                Should Contain          ${ERR}    ${ERROR}

    ${ERR} =    Run Keyword And Expect Error       *
                ...     Head object     ${WALLET}   ${CID}    ${OID}
                Should Contain          ${ERR}    ${ERROR}

    ${ERR} =    Run Keyword And Expect Error       *
                ...     Delete object   ${WALLET}   ${CID}    ${OID}
                Should Contain          ${ERR}    ${ERROR}
