*** Settings ***
Variables    common.py
Variables    wellknown_acl.py

Library     contract_keywords.py
Library     neofs.py
Library     neofs_verbs.py

Library     payment_neogo.py
Library     String
Library     Process

Resource    setup_teardown.robot
Resource    payment_operations.robot
Resource    storage.robot
Resource    complex_object_operations.robot

*** Variables ***
${CONTAINER_WAIT_INTERVAL} =    1 min

*** Test Cases ***
Drop command in control group
    [Documentation]         Testcase to check drop-objects command from control group.
    [Tags]                  NeoFSCLI
    [Timeout]               10 min

    [Setup]                 Setup

    ${_}    ${NODE}    ${WIF} =     Get control endpoint with wif
    ${LOCODE} =         Get Locode

    ${FILE_SIMPLE} =    Generate file of bytes    ${SIMPLE_OBJ_SIZE}
    ${FILE_COMPLEX} =   Generate file of bytes    ${COMPLEX_OBJ_SIZE}

    ${_}    ${_}    ${USER_KEY} =    Prepare Wallet And Deposit

    ${PRIV_CID} =       Create container             ${USER_KEY}    ${PRIVATE_ACL_F}   REP 1 CBF 1 SELECT 1 FROM * FILTER 'UN-LOCODE' EQ '${LOCODE}' AS LOC
                        Wait Until Keyword Succeeds      ${MORPH_BLOCK_TIME}    ${CONTAINER_WAIT_INTERVAL}
                        ...  Container Existing       ${USER_KEY}    ${PRIV_CID}

    #########################
    # Dropping simple object
    #########################

    ${S_OID} =          Put object    ${USER_KEY}    ${FILE_SIMPLE}    ${PRIV_CID}
                        Get object    ${USER_KEY}    ${PRIV_CID}    ${S_OID}    ${EMPTY}    s_file_read
                        Head object    ${USER_KEY}    ${PRIV_CID}    ${S_OID}

                        Drop object    ${NODE}    ${WIF}    ${PRIV_CID}    ${S_OID}

                        Wait Until Keyword Succeeds    3x    ${SHARD_0_GC_SLEEP}
                        ...  Run Keyword And Expect Error    Error:*
                        ...  Get object    ${USER_KEY}    ${PRIV_CID}    ${S_OID}    ${EMPTY}    s_file_read    options=--ttl 1
                        Wait Until Keyword Succeeds    3x    ${SHARD_0_GC_SLEEP}
                        ...  Run Keyword And Expect Error    Error:*
                        ...  Head object    ${USER_KEY}    ${PRIV_CID}    ${S_OID}    options=--ttl 1

                        Drop object    ${NODE}    ${WIF}    ${PRIV_CID}    ${S_OID}

    ##########################
    # Dropping complex object
    ##########################

    ${C_OID} =          Put object    ${USER_KEY}    ${FILE_COMPLEX}    ${PRIV_CID}
                        Get object    ${USER_KEY}    ${PRIV_CID}    ${C_OID}    ${EMPTY}    s_file_read
                        Head object    ${USER_KEY}    ${PRIV_CID}    ${C_OID}

                        Drop object    ${NODE}    ${WIF}    ${PRIV_CID}    ${C_OID}

                        Get object    ${USER_KEY}    ${PRIV_CID}    ${C_OID}    ${EMPTY}    s_file_read
                        Head object    ${USER_KEY}    ${PRIV_CID}    ${C_OID}

    @{SPLIT_OIDS} =     Get Object Parts By Link Object    ${USER_KEY}    ${PRIV_CID}   ${C_OID}
    FOR    ${CHILD_OID}    IN    @{SPLIT_OIDS}
        Drop object    ${NODE}    ${WIF}    ${PRIV_CID}    ${CHILD_OID}

    END

                        Wait Until Keyword Succeeds    3x    ${SHARD_0_GC_SLEEP}
                        ...  Run Keyword And Expect Error    Error:*
                        ...  Get object    ${USER_KEY}    ${PRIV_CID}    ${C_OID}    ${EMPTY}    s_file_read    options=--ttl 1
                        Wait Until Keyword Succeeds    3x    ${SHARD_0_GC_SLEEP}
                        ...  Run Keyword And Expect Error    Error:*
                        ...  Head object    ${USER_KEY}    ${PRIV_CID}    ${C_OID}    options=--ttl 1


    [Teardown]    Teardown    netmap_control_drop
