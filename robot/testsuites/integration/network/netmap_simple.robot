*** Settings ***
Variables   ../../../variables/common.py

Library     ../${RESOURCES}/neofs.py
Library     ../${RESOURCES}/payment_neogo.py
Library     ${KEYWORDS}/wallet_keywords.py
Library     ${KEYWORDS}/rpc_call_keywords.py

Resource    ../${RESOURCES}/payment_operations.robot
Resource    ../${RESOURCES}/setup_teardown.robot


*** Test cases ***
NeoFS Simple Netmap
    [Documentation]     Testcase to validate NeoFS Netmap.
    [Tags]              Netmap  NeoFS  NeoCLI
    [Timeout]           20 min

    [Setup]             Setup

    Generate Key and Pre-payment

    Generate file

    Validate Policy    REP 2 IN X CBF 2 SELECT 2 FROM * AS X    2    @{EMPTY}

    Validate Policy    REP 2 IN X CBF 1 SELECT 2 FROM * AS X    2    @{EMPTY}

    Validate Policy    REP 3 IN X CBF 1 SELECT 3 FROM * AS X    3    @{EMPTY}

    Validate Policy    REP 1 IN X CBF 1 SELECT 1 FROM * AS X    1    @{EMPTY}

    Validate Policy    REP 1 IN X CBF 2 SELECT 1 FROM * AS X    1    @{EMPTY}

    Validate Policy    REP 4 IN X CBF 1 SELECT 4 FROM * AS X    4    @{EMPTY}

    Validate Policy    REP 2 IN X CBF 1 SELECT 4 FROM * AS X    2    @{EMPTY}

    @{EXPECTED} =	   Create List    s01.neofs.devenv:8080    s02.neofs.devenv:8080    s03.neofs.devenv:8080    s04.neofs.devenv:8080
    Validate Policy    REP 4 IN X CBF 1 SELECT 4 FROM * AS X    4    @{EXPECTED}

    @{EXPECTED} =	   Create List    s03.neofs.devenv:8080
    Validate Policy    REP 1 IN LOC_PLACE CBF 1 SELECT 1 FROM LOC_SW AS LOC_PLACE FILTER Country EQ Sweden AS LOC_SW    1    @{EXPECTED}

    @{EXPECTED} =	   Create List    s02.neofs.devenv:8080
    Validate Policy    REP 1 CBF 1 SELECT 1 FROM LOC_SPB FILTER 'UN-LOCODE' EQ 'RU LED' AS LOC_SPB    1    @{EXPECTED}

    @{EXPECTED} =	   Create List    s01.neofs.devenv:8080    s02.neofs.devenv:8080
    Validate Policy    REP 1 IN LOC_SPB_PLACE REP 1 IN LOC_MSK_PLACE CBF 1 SELECT 1 FROM LOC_SPB AS LOC_SPB_PLACE SELECT 1 FROM LOC_MSK AS LOC_MSK_PLACE FILTER 'UN-LOCODE' EQ 'RU LED' AS LOC_SPB FILTER 'UN-LOCODE' EQ 'RU MOW' AS LOC_MSK          2       @{EXPECTED}

    @{EXPECTED} =	   Create List    s01.neofs.devenv:8080    s02.neofs.devenv:8080    s03.neofs.devenv:8080    s04.neofs.devenv:8080
    Validate Policy    REP 4 CBF 1 SELECT 4 FROM LOC_EU FILTER Continent EQ Europe AS LOC_EU    4    @{EXPECTED}

    @{EXPECTED} =	   Create List    s02.neofs.devenv:8080
    Validate Policy    REP 1 CBF 1 SELECT 1 FROM LOC_SPB FILTER 'UN-LOCODE' NE 'RU MOW' AND 'UN-LOCODE' NE 'SE STO' AND 'UN-LOCODE' NE 'FI HEL' AS LOC_SPB           1       @{EXPECTED}

    @{EXPECTED} =	   Create List    s01.neofs.devenv:8080    s02.neofs.devenv:8080
    Validate Policy    REP 2 CBF 1 SELECT 2 FROM LOC_RU FILTER SubDivCode NE 'AB' AND SubDivCode NE '18' AS LOC_RU    2    @{EXPECTED}

    @{EXPECTED} =	   Create List    s01.neofs.devenv:8080    s02.neofs.devenv:8080
    Validate Policy    REP 2 CBF 1 SELECT 2 FROM LOC_RU FILTER Country EQ 'Russia' AS LOC_RU    2    @{EXPECTED}

    @{EXPECTED} =      Create List    s03.neofs.devenv:8080    s04.neofs.devenv:8080
    Validate Policy    REP 2 CBF 1 SELECT 2 FROM LOC_EU FILTER Country NE 'Russia' AS LOC_EU    2    @{EXPECTED}

    Log	               Put operation should be failed with error "not enough nodes to SELECT from: 'X'"
                       Run Keyword And Expect Error    *
                       ...  Validate Policy    REP 2 IN X CBF 2 SELECT 6 FROM * AS X    2    @{EMPTY}

    [Teardown]         Teardown     netmap_simple

*** Keywords ***


Generate file
    ${FILE} =           Generate file of bytes    ${SIMPLE_OBJ_SIZE}
                        Set Global Variable       ${FILE}    ${FILE}

Generate Key and Pre-payment
    ${WALLET}   ${ADDR}     ${USER_KEY_GEN} =   Init Wallet with Address    ${ASSETS_DIR}
                        Set Global Variable     ${PRIV_KEY}     ${USER_KEY_GEN}
                        Payment Operations      ${ADDR}      ${PRIV_KEY}


Validate Policy
    [Arguments]    ${POLICY}    ${EXPECTED_VAL}     @{EXPECTED_LIST}

                        Log	                                Container with rule ${POLICY}

    ${CID} =            Create container                    ${PRIV_KEY}    ${EMPTY}      ${POLICY}
                        Container Existing                  ${PRIV_KEY}    ${CID}
    ${S_OID} =          Put object                 ${PRIV_KEY}    ${FILE}       ${CID}        ${EMPTY}     ${EMPTY}
                        Validate storage policy for object  ${PRIV_KEY}    ${EXPECTED_VAL}             ${CID}       ${S_OID}   @{EXPECTED_LIST}
                        Get object               ${PRIV_KEY}    ${CID}    ${S_OID}    ${EMPTY}    s_file_read
