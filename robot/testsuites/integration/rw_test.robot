*** Settings ***
Variables   ../../variables/common.py

Library     ${RESOURCES}/neofs.py
Library     ${RESOURCES}/assertions.py

*** Variables ***
${OBJECT}       ${ABSOLUTE_FILE_PATH}/test_file
${READ_OBJECT}  ${ABSOLUTE_FILE_PATH}/read_file

*** Test cases ***
Read and Write to NeoFS
    ${CID} =   Create container
    ${OID} =   Write object to NeoFS   ${OBJECT}   ${CID}
    Read object from NeoFS  ${CID}  ${OID}  ${READ_OBJECT}
    Should Be Equal as Binaries    ${OBJECT}   ${READ_OBJECT}
