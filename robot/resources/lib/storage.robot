
*** Settings ***
Variables   ../../variables/common.py

Library     Process

*** Keywords ***

Drop object
    [Arguments]   ${NODE}    ${WIF_STORAGE}    ${CID}    ${OID}

    ${DROP_SIMPLE} =        Run Process    neofs-cli control drop-objects -r ${NODE} --wif ${WIF_STORAGE} -o ${CID}/${OID}    shell=True
                            Log Many    stdout: ${DROP_SIMPLE.stdout}    stderr: ${DROP_SIMPLE.stderr}
                            Should Be Equal As Integers    ${DROP_SIMPLE.rc}    0
                            

