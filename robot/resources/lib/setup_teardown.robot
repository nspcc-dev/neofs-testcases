*** Settings ***
Variables   ../../variables/common.py

Library     OperatingSystem

Library     utility_keywords.py

*** Keywords ***

Setup
    Create Directory    ${ASSETS_DIR}

Teardown
    [Arguments]     ${LOGFILE}
    Remove Directory    ${ASSETS_DIR}   True
    Get Docker Logs     ${LOGFILE}
