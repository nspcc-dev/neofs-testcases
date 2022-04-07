*** Settings ***
Variables   common.py

Library     neofs_verbs.py
Library     complex_object_operations.py


*** Keywords ***

Get Object Parts By Link Object
    [Documentation]     The keyword accepts the ID of a Large Object, retrieves its split
            ...         header and returns all Part Object IDs from Link Object.

    [Arguments]         ${WIF}  ${CID}  ${LARGE_OID}


    &{RESPONSE} =       Get Link Object     ${WIF}  ${CID}  ${LARGE_OID}
    &{LINK_HEADER} =    Head Object         ${WIF}  ${CID}  ${RESPONSE.link}    is_raw=True

    [Return]    ${LINK_HEADER.header.split.children}
