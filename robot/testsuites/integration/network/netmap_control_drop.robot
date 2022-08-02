*** Settings ***
Variables    common.py
Variables    wellknown_acl.py

Library     container.py
Library     node_management.py
Library     neofs_verbs.py
Library     utility_keywords.py

Resource    setup_teardown.robot
Resource    payment_operations.robot
Resource    storage.robot
Resource    complex_object_operations.robot

*** Test Cases ***
Drop command in control group
    [Documentation]         Testcase to check drop-objects command from control group.
    [Timeout]               10 min


    ${_}    ${NODE}    ${STORAGE_WALLET}=     Get control endpoint and wallet
    ${LOCODE} =         Get Locode

    ${FILE_SIMPLE}    ${_} =    Generate file    ${SIMPLE_OBJ_SIZE}
    ${FILE_COMPLEX}    ${_} =   Generate file    ${COMPLEX_OBJ_SIZE}

    ${WALLET}    ${_}    ${_} =    Prepare Wallet And Deposit

    ${PRIV_CID} =       Create Container    ${WALLET}
                        ...     rule=REP 1 CBF 1 SELECT 1 FROM * FILTER 'UN-LOCODE' EQ '${LOCODE}' AS LOC

    #########################
    # Dropping simple object
    #########################

    ${S_OID} =          Put object    ${WALLET}    ${FILE_SIMPLE}    ${PRIV_CID}
                        Get object    ${WALLET}    ${PRIV_CID}    ${S_OID}    ${EMPTY}    s_file_read
                        Head object    ${WALLET}    ${PRIV_CID}    ${S_OID}

                        Drop object    ${NODE}    ${STORAGE_WALLET}    ${PRIV_CID}    ${S_OID}

                        Wait Until Keyword Succeeds    3x    ${SHARD_0_GC_SLEEP}
                        ...  Run Keyword And Expect Error    Error:*
                        ...  Get object    ${WALLET}    ${PRIV_CID}    ${S_OID}    ${EMPTY}    s_file_read    options=--ttl 1
                        Wait Until Keyword Succeeds    3x    ${SHARD_0_GC_SLEEP}
                        ...  Run Keyword And Expect Error    Error:*
                        ...  Head object    ${WALLET}    ${PRIV_CID}    ${S_OID}    options=--ttl 1

                        Drop object    ${NODE}    ${STORAGE_WALLET}    ${PRIV_CID}    ${S_OID}

    ##########################
    # Dropping complex object
    ##########################

    ${C_OID} =          Put object    ${WALLET}    ${FILE_COMPLEX}    ${PRIV_CID}
                        Get object    ${WALLET}    ${PRIV_CID}    ${C_OID}    ${EMPTY}    s_file_read
                        Head object    ${WALLET}    ${PRIV_CID}    ${C_OID}

                        Drop object    ${NODE}    ${STORAGE_WALLET}    ${PRIV_CID}    ${C_OID}

                        Get object    ${WALLET}    ${PRIV_CID}    ${C_OID}    ${EMPTY}    s_file_read
                        Head object    ${WALLET}    ${PRIV_CID}    ${C_OID}

    @{SPLIT_OIDS} =     Get Object Parts By Link Object    ${WALLET}    ${PRIV_CID}   ${C_OID}
    FOR    ${CHILD_OID}    IN    @{SPLIT_OIDS}
        Drop object    ${NODE}    ${STORAGE_WALLET}    ${PRIV_CID}    ${CHILD_OID}

    END

                        Wait Until Keyword Succeeds    3x    ${SHARD_0_GC_SLEEP}
                        ...  Run Keyword And Expect Error    Error:*
                        ...  Get object    ${WALLET}    ${PRIV_CID}    ${C_OID}    ${EMPTY}    s_file_read    options=--ttl 1
                        Wait Until Keyword Succeeds    3x    ${SHARD_0_GC_SLEEP}
                        ...  Run Keyword And Expect Error    Error:*
                        ...  Head object    ${WALLET}    ${PRIV_CID}    ${C_OID}    options=--ttl 1


