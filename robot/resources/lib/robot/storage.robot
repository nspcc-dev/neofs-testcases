
*** Settings ***
Variables   common.py

Library     Process

*** Keywords ***

Drop object
    [Arguments]   ${NODE}    ${WIF_STORAGE}    ${CID}    ${OID}

    ${DROP_SIMPLE} =    Run Process    ${NEOFS_CLI_EXEC} control drop-objects --endpoint ${NODE} --wif ${WIF_STORAGE} -o ${CID}/${OID}
                                        ...     shell=True
                        Log Many    stdout: ${DROP_SIMPLE.stdout}    stderr: ${DROP_SIMPLE.stderr}
                        Should Be Equal As Integers    ${DROP_SIMPLE.rc}    0   Got non-zero return code from CLI
