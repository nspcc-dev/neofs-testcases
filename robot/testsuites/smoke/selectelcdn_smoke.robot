# -*- coding: robot -*-

*** Settings ***
Variables   ../../variables/common.py
Variables   ../../variables/selectelcdn_smoke.py


Library     ${RESOURCES}/neofs.py
Library     ${RESOURCES}/payment_neogo.py
Library     ${RESOURCES}/gates.py


*** Test cases ***

NeoFS Storage Smoke
    [Documentation]     Creates container and does PUT, GET and LIST on it via CLI and via HTTP Gate
    [Timeout]   5 min


    ${TX_DEPOSIT} =     NeoFS Deposit                       ${WALLET}               ${ADDR}     ${SCRIPT_HASH}      50      one
                        Wait Until Keyword Succeeds         1 min          15 sec
                        ...  Transaction accepted in block  ${TX_DEPOSIT}
                        Get Transaction                     ${TX_DEPOSIT}

    ${CID} =            Create container                    ${PRIV_KEY}     public
                        Wait Until Keyword Succeeds         2 min          30 sec
                        ...  Container Existing             ${PRIV_KEY}    ${CID}

    ${FILE} =           Generate file of bytes              1024
    ${S_OID} =          Put object to NeoFS                 ${PRIV_KEY}    ${FILE}       ${CID}            ${EMPTY}         ${EMPTY}
                        Get object from NeoFS               ${PRIV_KEY}    ${CID}        ${S_OID}           ${EMPTY}       s_file_read

    ${FILEPATH} =       Get via HTTP Gate                   ${CID}      ${S_OID}
