*** Settings ***
Library     container.py
Library     neofs_verbs.py
Library     storage_policy.py
Library     utility_keywords.py

Library     Collections

Resource    payment_operations.robot

*** Test cases ***
NeoFS Simple Netmap
    [Documentation]     Testcase to validate NeoFS Netmap.
    [Tags]              Netmap
    [Timeout]           20 min


    ${WALLET}   ${_}    ${_} =    Prepare Wallet And Deposit

    Validate Object Copies    ${WALLET}    REP 2 IN X CBF 2 SELECT 2 FROM * AS X    2

    Validate Object Copies    ${WALLET}    REP 2 IN X CBF 1 SELECT 2 FROM * AS X    2

    Validate Object Copies    ${WALLET}    REP 3 IN X CBF 1 SELECT 3 FROM * AS X    3

    Validate Object Copies    ${WALLET}    REP 1 IN X CBF 1 SELECT 1 FROM * AS X    1

    Validate Object Copies    ${WALLET}    REP 1 IN X CBF 2 SELECT 1 FROM * AS X    1

    Validate Object Copies    ${WALLET}    REP 4 IN X CBF 1 SELECT 4 FROM * AS X    4

    Validate Object Copies    ${WALLET}    REP 2 IN X CBF 1 SELECT 4 FROM * AS X    2

    @{EXPECTED} =       Create List    s01.neofs.devenv:8080    s02.neofs.devenv:8080    s03.neofs.devenv:8080    s04.neofs.devenv:8080
    Validate Selected Nodes     ${WALLET}    REP 4 IN X CBF 1 SELECT 4 FROM * AS X    4    @{EXPECTED}

    @{EXPECTED} =       Create List    s03.neofs.devenv:8080
    Validate Selected Nodes     ${WALLET}    REP 1 IN LOC_PLACE CBF 1 SELECT 1 FROM LOC_SW AS LOC_PLACE FILTER Country EQ Sweden AS LOC_SW
    ...    1    @{EXPECTED}

    @{EXPECTED} =       Create List    s02.neofs.devenv:8080
    Validate Selected Nodes     ${WALLET}    REP 1 CBF 1 SELECT 1 FROM LOC_SPB FILTER 'UN-LOCODE' EQ 'RU LED' AS LOC_SPB    1    @{EXPECTED}

    @{EXPECTED} =       Create List    s01.neofs.devenv:8080    s02.neofs.devenv:8080
    Validate Selected Nodes     ${WALLET}
    ...     REP 1 IN LOC_SPB_PLACE REP 1 IN LOC_MSK_PLACE CBF 1 SELECT 1 FROM LOC_SPB AS LOC_SPB_PLACE SELECT 1 FROM LOC_MSK AS LOC_MSK_PLACE FILTER 'UN-LOCODE' EQ 'RU LED' AS LOC_SPB FILTER 'UN-LOCODE' EQ 'RU MOW' AS LOC_MSK
    ...     2           @{EXPECTED}

    @{EXPECTED} =       Create List    s01.neofs.devenv:8080    s02.neofs.devenv:8080    s03.neofs.devenv:8080    s04.neofs.devenv:8080
    Validate Selected Nodes     ${WALLET}    REP 4 CBF 1 SELECT 4 FROM LOC_EU FILTER Continent EQ Europe AS LOC_EU    4    @{EXPECTED}

    @{EXPECTED} =       Create List    s02.neofs.devenv:8080
    Validate Selected Nodes     ${WALLET}
    ...     REP 1 CBF 1 SELECT 1 FROM LOC_SPB FILTER 'UN-LOCODE' NE 'RU MOW' AND 'UN-LOCODE' NE 'SE STO' AND 'UN-LOCODE' NE 'FI HEL' AS LOC_SPB
    ...     1           @{EXPECTED}

    @{EXPECTED} =       Create List    s01.neofs.devenv:8080    s02.neofs.devenv:8080
    Validate Selected Nodes     ${WALLET}    REP 2 CBF 1 SELECT 2 FROM LOC_RU FILTER SubDivCode NE 'AB' AND SubDivCode NE '18' AS LOC_RU    2    @{EXPECTED}

    @{EXPECTED} =       Create List    s01.neofs.devenv:8080    s02.neofs.devenv:8080
    Validate Selected Nodes     ${WALLET}    REP 2 CBF 1 SELECT 2 FROM LOC_RU FILTER Country EQ 'Russia' AS LOC_RU    2    @{EXPECTED}

    @{EXPECTED} =       Create List    s03.neofs.devenv:8080    s04.neofs.devenv:8080
    Validate Selected Nodes     ${WALLET}    REP 2 CBF 1 SELECT 2 FROM LOC_EU FILTER Country NE 'Russia' AS LOC_EU    2    @{EXPECTED}

    ${ERR} =           Run Keyword And Expect Error    *
                       ...  Validate Selected Nodes    ${WALLET}    REP 2 IN X CBF 2 SELECT 6 FROM * AS X    2
                       Should Contain  ${ERR}      code = 1024 message = netmap: not enough nodes to SELECT from


*** Keywords ***

Validate Object Copies
    [Arguments]    ${WALLET}    ${POLICY}    ${EXPECTED_COPIES}

    ${FILE}
    ...     ${_} =      Generate file           ${SIMPLE_OBJ_SIZE}
    ${CID} =            Create container        ${WALLET}    rule=${POLICY}
    ${OID} =            Put object              ${WALLET}    ${FILE}       ${CID}
    ${COPIES} =         Get Simple Object Copies    ${WALLET}   ${CID}  ${OID}
                        Should Be Equal As Numbers  ${EXPECTED_COPIES}  ${COPIES}
    [Return]            ${CID}  ${OID}


Validate Selected Nodes
    [Arguments]    ${WALLET}    ${POLICY}    ${EXPECTED_COPIES}     @{EXPECTED_NODES}

    ${CID}
    ...     ${OID} =    Validate Object Copies      ${WALLET}   ${POLICY}    ${EXPECTED_COPIES}
    ${NODES} =          Get Nodes With Object       ${WALLET}   ${CID}  ${OID}
                        Lists Should Be Equal       ${EXPECTED_NODES}   ${NODES}
