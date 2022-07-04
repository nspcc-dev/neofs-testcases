*** Settings ***
Variables   common.py

Library     acl.py
Library     container.py
Library     epoch.py
Library     neofs.py
Library     neofs_verbs.py
Library     storage_policy.py
Library     utility_keywords.py

Resource    eacl_tables.robot
Resource    common_steps_acl_bearer.robot
Resource    payment_operations.robot
Resource    setup_teardown.robot
Resource    storage.robot

*** Variables ***
${FULL_PLACEMENT_RULE} =    REP 4 IN X CBF 1 SELECT 4 FROM * AS X
${EXPECTED_COPIES} =        ${4}

*** Test cases ***
eACL Deny Replication Operations
    [Documentation]         Testcase to validate NeoFS replication with eACL deny rules.
    [Tags]                  ACL   Replication
    [Timeout]               20 min

    [Setup]                 Setup

    ${_}    ${NODE}    ${STORAGE_WALLET} =    Get control endpoint and wallet

    ${WALLET}    ${_}    ${_} =    Prepare Wallet And Deposit

                        # https://github.com/nspcc-dev/neofs-node/issues/881

    ${FILE}    ${_} =    Generate file      ${SIMPLE_OBJ_SIZE}
    ${CID} =            Create container            ${WALLET}    basic_acl=eacl-public-read-write   rule=${FULL_PLACEMENT_RULE}
                        Prepare eACL Role rules     ${CID}

    ${OID} =            Put object    ${WALLET}    ${FILE}    ${CID}

    ${COPIES} =         Get Object Copies   Simple  ${WALLET}   ${CID}  ${OID}
                        Should Be Equal As Numbers     ${EXPECTED_COPIES}  ${COPIES}

                        Set eACL    ${WALLET}    ${CID}    ${EACL_DENY_ALL_USER}

                        Run Keyword And Expect Error    *
                        ...  Put object    ${WALLET}    ${FILE}    ${CID}

                        # Drop object to check replication
                        Drop object    ${NODE}    ${STORAGE_WALLET}    ${CID}    ${OID}

                        Tick Epoch
                        Sleep   ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                        # We assume that during one epoch object should be replicated
    ${COPIES} =         Get Object Copies   Simple      ${STORAGE_WALLET}   ${CID}  ${OID}
                        Should Be Equal As Numbers      ${EXPECTED_COPIES}  ${COPIES}
                        ...     msg="Dropped object should be replicated in one epoch"

    [Teardown]          Teardown    acl_deny_replication
