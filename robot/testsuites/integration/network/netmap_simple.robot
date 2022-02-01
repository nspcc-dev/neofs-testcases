*** Settings ***
Variables   common.py

Library     neofs.py
Library     neofs_verbs.py
Library     payment_neogo.py
Library     wallet_keywords.py
Library     rpc_call_keywords.py

Resource    payment_operations.robot
Resource    setup_teardown.robot

*** Variables ***
${CONTAINER_WAIT_INTERVAL} =    1 min

*** Test cases ***
NeoFS Simple Netmap
    [Documentation]     Testcase to validate NeoFS Netmap.
    [Tags]              Netmap  NeoFS  NeoCLI
    [Timeout]           20 min

    [Setup]             Setup

    ${WALLET}   ${_}    ${_} =    Prepare Wallet And Deposit
    ${FILE} =          Generate file of bytes    ${SIMPLE_OBJ_SIZE}

    Validate Policy    ${USER_WALLET}    ${FILE_S}    REP 2 IN X CBF 2 SELECT 2 FROM * AS X    2    @{EMPTY}

    Validate Policy    ${USER_WALLET}    ${FILE_S}    REP 2 IN X CBF 1 SELECT 2 FROM * AS X    2    @{EMPTY}

    Validate Policy    ${USER_WALLET}    ${FILE_S}    REP 3 IN X CBF 1 SELECT 3 FROM * AS X    3    @{EMPTY}

    Validate Policy    ${USER_WALLET}    ${FILE_S}    REP 1 IN X CBF 1 SELECT 1 FROM * AS X    1    @{EMPTY}

    Validate Policy    ${USER_WALLET}    ${FILE_S}    REP 1 IN X CBF 2 SELECT 1 FROM * AS X    1    @{EMPTY}

    Validate Policy    ${USER_WALLET}    ${FILE_S}    REP 4 IN X CBF 1 SELECT 4 FROM * AS X    4    @{EMPTY}

    Validate Policy    ${USER_WALLET}    ${FILE_S}    REP 2 IN X CBF 1 SELECT 4 FROM * AS X    2    @{EMPTY}

    @{EXPECTED} =	   Create List    s01.neofs.devenv:8080    s02.neofs.devenv:8080    s03.neofs.devenv:8080    s04.neofs.devenv:8080
    Validate Policy    ${USER_WALLET}    ${FILE_S}    REP 4 IN X CBF 1 SELECT 4 FROM * AS X    4    @{EXPECTED}

    @{EXPECTED} =	   Create List    s03.neofs.devenv:8080
    Validate Policy    ${USER_WALLET}    ${FILE_S}    REP 1 IN LOC_PLACE CBF 1 SELECT 1 FROM LOC_SW AS LOC_PLACE FILTER Country EQ Sweden AS LOC_SW
    ...    1    @{EXPECTED}

    @{EXPECTED} =	   Create List    s02.neofs.devenv:8080
    Validate Policy    ${USER_WALLET}    ${FILE_S}    REP 1 CBF 1 SELECT 1 FROM LOC_SPB FILTER 'UN-LOCODE' EQ 'RU LED' AS LOC_SPB    1    @{EXPECTED}

    @{EXPECTED} =	   Create List    s01.neofs.devenv:8080    s02.neofs.devenv:8080
    Validate Policy    ${USER_WALLET}    ${FILE_S}    REP 1 IN LOC_SPB_PLACE REP 1 IN LOC_MSK_PLACE CBF 1 SELECT 1 FROM LOC_SPB AS LOC_SPB_PLACE SELECT 1 FROM LOC_MSK AS LOC_MSK_PLACE FILTER 'UN-LOCODE' EQ 'RU LED' AS LOC_SPB FILTER 'UN-LOCODE' EQ 'RU MOW' AS LOC_MSK
    ...          2       @{EXPECTED}

    @{EXPECTED} =	   Create List    s01.neofs.devenv:8080    s02.neofs.devenv:8080    s03.neofs.devenv:8080    s04.neofs.devenv:8080
    Validate Policy    ${USER_WALLET}    ${FILE_S}    REP 4 CBF 1 SELECT 4 FROM LOC_EU FILTER Continent EQ Europe AS LOC_EU    4    @{EXPECTED}

    @{EXPECTED} =	   Create List    s02.neofs.devenv:8080
    Validate Policy    ${USER_WALLET}    ${FILE_S}    REP 1 CBF 1 SELECT 1 FROM LOC_SPB FILTER 'UN-LOCODE' NE 'RU MOW' AND 'UN-LOCODE' NE 'SE STO' AND 'UN-LOCODE' NE 'FI HEL' AS LOC_SPB
    ...           1       @{EXPECTED}

    @{EXPECTED} =	   Create List    s01.neofs.devenv:8080    s02.neofs.devenv:8080
    Validate Policy    ${USER_WALLET}    ${FILE_S}    REP 2 CBF 1 SELECT 2 FROM LOC_RU FILTER SubDivCode NE 'AB' AND SubDivCode NE '18' AS LOC_RU    2    @{EXPECTED}

    @{EXPECTED} =	   Create List    s01.neofs.devenv:8080    s02.neofs.devenv:8080
    Validate Policy    ${USER_WALLET}    ${FILE_S}    REP 2 CBF 1 SELECT 2 FROM LOC_RU FILTER Country EQ 'Russia' AS LOC_RU    2    @{EXPECTED}

    @{EXPECTED} =      Create List    s03.neofs.devenv:8080    s04.neofs.devenv:8080
    Validate Policy    ${USER_WALLET}    ${FILE_S}    REP 2 CBF 1 SELECT 2 FROM LOC_EU FILTER Country NE 'Russia' AS LOC_EU    2    @{EXPECTED}

    Log	               Put operation should be failed with error "not enough nodes to SELECT from: 'X'"
                       Run Keyword And Expect Error    *
                       ...  Validate Policy    ${USER_WALLET}    ${FILE_S}    REP 2 IN X CBF 2 SELECT 6 FROM * AS X    2    @{EMPTY}

    [Teardown]         Teardown     netmap_simple

*** Keywords ***

Validate Policy
    [Arguments]    ${WALLET}    ${FILE}    ${POLICY}    ${EXPECTED_VAL}     @{EXPECTED_LIST}

                        Log	                   Container with rule ${POLICY}

    ${CID} =            Create container               ${WALLET}    ${EMPTY}      ${POLICY}
                        Wait Until Keyword Succeeds    ${MORPH_BLOCK_TIME}      ${CONTAINER_WAIT_INTERVAL}
                        ...     Container Existing         ${WALLET}    ${CID}
    ${S_OID} =          Put object               ${WALLET}    ${FILE}       ${CID}
                        Validate storage policy for object      ${WALLET}    ${EXPECTED_VAL}           ${CID}       ${S_OID}   ${EXPECTED_LIST}
                        Get object               ${WALLET}    ${CID}    ${S_OID}    ${EMPTY}    s_file_read
