*** Settings ***
Variables   ../../../variables/common.py

Library     ../${RESOURCES}/neofs.py
Library     ../${RESOURCES}/payment_neogo.py

Resource    common_steps_object.robot
Resource    ../${RESOURCES}/setup_teardown.robot
Resource    ../${RESOURCES}/payment_operations.robot


*** Test cases ***
NeoFS Complex Object Operations
    [Documentation]     Testcase to validate NeoFS operations with simple object.
    [Tags]              Object  NeoFS  NeoCLI
    [Timeout]           20 min

    [Setup]             Setup

    ${WALLET}   ${ADDR}     ${WIF} =   Init Wallet with Address    ${ASSETS_DIR}
    Payment Operations      ${ADDR}     ${WIF}
    Prepare container       ${WIF}

    ${FILE} =           Generate file of bytes              ${COMPLEX_OBJ_SIZE}
    ${FILE_HASH} =      Get file hash                       ${FILE}

    ${S_OID} =          Put object                 ${WIF}    ${FILE}       ${CID}            ${EMPTY}         ${EMPTY}
    ${H_OID} =          Put object                 ${WIF}    ${FILE}       ${CID}            ${EMPTY}         ${FILE_USR_HEADER}
    ${H_OID_OTH} =      Put object                 ${WIF}    ${FILE}       ${CID}            ${EMPTY}         ${FILE_USR_HEADER_OTH}

    Should Be True     '${S_OID}'!='${H_OID}' and '${H_OID}'!='${H_OID_OTH}'

                        Validate storage policy for object  ${WIF}    2             ${CID}         ${S_OID}
                        Validate storage policy for object  ${WIF}    2             ${CID}         ${H_OID}
                        Validate storage policy for object  ${WIF}    2             ${CID}         ${H_OID_OTH}

    @{S_OBJ_ALL} =	Create List	                        ${S_OID}       ${H_OID}     ${H_OID_OTH}
    @{S_OBJ_H} =	Create List	                        ${H_OID}
    @{S_OBJ_H_OTH} =    Create List	                        ${H_OID_OTH}

                        Search object                       ${WIF}    ${CID}        --root           ${EMPTY}       ${EMPTY}    ${S_OBJ_ALL}

    ${GET_OBJ_S} =      Get object               ${WIF}    ${CID}        ${S_OID}           ${EMPTY}       s_file_read
    ${GET_OBJ_H} =      Get object               ${WIF}    ${CID}        ${H_OID}           ${EMPTY}       h_file_read

                        Verify file hash                    ${GET_OBJ_S}   ${FILE_HASH}
                        Verify file hash                    ${GET_OBJ_H}   ${FILE_HASH}

                        Get Range Hash                      ${WIF}    ${CID}        ${S_OID}          ${EMPTY}       0:10
                        Get Range Hash                      ${WIF}    ${CID}        ${H_OID}          ${EMPTY}       0:10

                        Get Range                           ${WIF}    ${CID}        ${S_OID}          s_get_range    ${EMPTY}       0:10
                        Get Range                           ${WIF}    ${CID}        ${H_OID}          h_get_range    ${EMPTY}       0:10

                        Search object                       ${WIF}    ${CID}        --root            ${EMPTY}       ${EMPTY}                ${S_OBJ_ALL}
                        Search object                       ${WIF}    ${CID}        --root            ${EMPTY}       ${FILE_USR_HEADER}      ${S_OBJ_H}
                        Search object                       ${WIF}    ${CID}        --root            ${EMPTY}       ${FILE_USR_HEADER_OTH}  ${S_OBJ_H_OTH}

                        Head object                         ${WIF}    ${CID}        ${S_OID}          ${EMPTY}
                        Head object                         ${WIF}    ${CID}        ${H_OID}          ${EMPTY}       ${FILE_USR_HEADER}

                        Verify Split Chain                  ${WIF}    ${CID}        ${S_OID}
                        Verify Split Chain                  ${WIF}    ${CID}        ${H_OID}

    ${TOMBSTONE_S} =    Delete object                       ${WIF}    ${CID}        ${S_OID}          ${EMPTY}
    ${TOMBSTONE_H} =    Delete object                       ${WIF}    ${CID}        ${H_OID}          ${EMPTY}

                        Verify Head tombstone               ${WIF}    ${CID}        ${TOMBSTONE_S}     ${S_OID}    ${ADDR}
                        Verify Head tombstone               ${WIF}    ${CID}        ${TOMBSTONE_H}     ${H_OID}    ${ADDR}

                        Sleep                               2min

                        Run Keyword And Expect Error        *
                        ...  Get object          ${WIF}    ${CID}        ${S_OID}           ${EMPTY}       ${GET_OBJ_S}
                        Run Keyword And Expect Error        *
                        ...  Get object          ${WIF}    ${CID}        ${H_OID}           ${EMPTY}       ${GET_OBJ_H}

    [Teardown]          Teardown    object_complex
