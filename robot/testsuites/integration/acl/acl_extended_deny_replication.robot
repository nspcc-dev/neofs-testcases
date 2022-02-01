*** Settings ***
Variables   common.py

Library     Collections
Library     neofs.py
Library     neofs_verbs.py
Library     acl.py
Library     payment_neogo.py

Library     contract_keywords.py
Library     wallet_keywords.py

Resource    eacl_tables.robot
Resource    common_steps_acl_bearer.robot
Resource    payment_operations.robot
Resource    setup_teardown.robot
Resource    storage.robot

*** Variables ***
${FULL_PLACEMENT_RULE} =    REP 4 IN X CBF 1 SELECT 4 FROM * AS X
${EXPECTED_COPIES} =        ${4}
${DEPOSIT} =                ${30}


*** Test cases ***
eACL Deny Replication Operations
    [Documentation]         Testcase to validate NeoFS replication with eACL deny rules.
    [Tags]                  ACL  NeoFS_CLI  Replication
    [Timeout]               20 min

    [Setup]                 Setup

    ${_}     ${NODE}    ${WIF_STORAGE} =     Get control endpoint with wif
    ${WALLET_STORAGE}    ${_} =    Prepare Wallet with WIF And Deposit    ${WIF_STORAGE}

    ${WALLET}    ${_}    ${_} =    Prepare Wallet And Deposit

                        Log    Check Replication with eACL deny - object should be replicated
                        # https://github.com/nspcc-dev/neofs-node/issues/881

    ${FILE} =           Generate file of bytes    ${SIMPLE_OBJ_SIZE}

    ${CID} =            Create container    ${WALLET}    ${PUBLIC_ACL}   ${FULL_PLACEMENT_RULE}
                        Wait Until Keyword Succeeds    ${MORPH_BLOCK_TIME}    ${CONTAINER_WAIT_INTERVAL}
                        ...     Container Existing    ${WALLET}    ${CID}

                        Prepare eACL Role rules    ${CID}

    ${OID} =            Put object    ${WALLET}    ${FILE}    ${CID}

                        Validate storage policy for object    ${WALLET}    ${EXPECTED_COPIES}    ${CID}    ${OID}

                        Set eACL    ${WALLET}    ${CID}    ${EACL_DENY_ALL_USER}

                        Run Keyword And Expect Error    *
                        ...  Put object    ${WALLET}    ${FILE}    ${CID}

                        # Drop object to check replication
                        Drop object    ${NODE}    ${WALLET_STORAGE}    ${CID}    ${OID}

                        Tick Epoch

                        # We assume that during one epoch object should be replicated
                        Wait Until Keyword Succeeds    ${NEOFS_EPOCH_TIMEOUT}    1m
                        ...     Validate storage policy for object    ${WALLET_STORAGE}    ${EXPECTED_COPIES}    ${CID}    ${OID}

    [Teardown]          Teardown    acl_deny_replication
