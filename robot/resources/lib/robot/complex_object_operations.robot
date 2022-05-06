*** Settings ***
Variables   common.py

Library     neofs_verbs.py
Library     complex_object_actions.py


*** Keywords ***

Get Object Parts By Link Object
    [Documentation]     The keyword accepts the ID of a Large Object, retrieves its split
            ...         header and returns all Part Object IDs from Link Object.

    [Arguments]         ${WALLET}  ${CID}  ${LARGE_OID}     ${BEARER}=${EMPTY}


    ${LINK_OID} =       Get Link Object     ${WALLET}  ${CID}  ${LARGE_OID}
                        ...                 bearer_token=${BEARER}
    &{LINK_HEADER} =    Head Object         ${WALLET}  ${CID}  ${LINK_OID}
                        ...                 is_raw=True   bearer_token=${BEARER}

    [Return]    ${LINK_HEADER.header.split.children}
