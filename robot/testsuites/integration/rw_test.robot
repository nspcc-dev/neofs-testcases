*** Settings ***
Variables   ../../variables/common.py

Library     ${RESOURCES}/kws_module.py
Library     ${RESOURCES}/assertions.py

*** Variables ***
${OBJECT}       ${TESTSUITES}/test_file
${READ_OBJECT}  ${TESTSUITES}/read_file

*** Test cases ***
Read and Write to NeoFS
    ${CID} =   Create container
    ${OID} =   Write object to NeoFS   ${OBJECT}   ${CID}
    Read object from NeoFS  ${CID}  ${OID}  ${READ_OBJECT}
    Should Be Equal as Binaries    ${OBJECT}   ${READ_OBJECT}
