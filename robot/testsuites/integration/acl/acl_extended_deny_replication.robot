*** Settings ***
Variables   ../../../variables/common.py

Library     Collections
Library     Process
Library     neofs.py
Library     acl.py
Library     payment_neogo.py

Library     rpc_call_keywords.py
Library     contract_keywords.py

Resource    ../../../variables/eacl_tables.robot
Resource    common_steps_acl_bearer.robot
Resource    ../${RESOURCES}/payment_operations.robot
Resource    ../${RESOURCES}/setup_teardown.robot


*** Variables ***
${FULL_PLACEMENT_RULE} =    REP 4 IN X CBF 1 SELECT 4 FROM * AS X
${EXPECTED_COPIES} =        ${4}


*** Test cases ***
eACL Deny Replication Operations
    [Documentation]         Testcase to validate NeoFS replication with eACL deny rules.
    [Tags]                  ACL  NeoFS_CLI  Replication
    [Timeout]               20 min

    [Setup]                 Setup

    ${NODE_NUM}    ${NODE}    ${WIF_STORAGE} =    Get control endpoint with wif
    ${WALLET}   ${ADDR}    ${WIF_USER} =    Prepare Wallet And Deposit

                            Prepare eACL Role rules

                            Log    Check Replication with eACL deny - object should be replicated
                            # https://github.com/nspcc-dev/neofs-node/issues/881
                            
    ${FILE} =               Generate file of bytes    ${SIMPLE_OBJ_SIZE}
    
    ${CID} =                Create container    ${WIF_USER}    0x0FFFFFFF    ${FULL_PLACEMENT_RULE}
                            Wait Until Keyword Succeeds    ${MORPH_BLOCK_TIME}    ${CONTAINER_WAIT_INTERVAL}
                            ...     Container Existing    ${WIF_USER}    ${CID}

    ${OID} =                Put object    ${WIF_USER}    ${FILE}    ${CID}    ${EMPTY}    ${FILE_USR_HEADER}

                            Validate storage policy for object    ${WIF_USER}    ${EXPECTED_COPIES}    ${CID}    ${OID}

                            Set eACL    ${WIF_USER}    ${CID}    ${EACL_DENY_ALL_USER}

                            Run Keyword And Expect Error    *
                            ...  Put object    ${WIF_USER}    ${FILE}    ${CID}    ${EMPTY}    ${FILE_USR_HEADER}
                            
                            # Drop object to check replication
                            Drop object    ${NODE}    ${WIF_STORAGE}    ${CID}    ${OID}

                            Tick Epoch

                            # We assume that during one epoch object should be replicated
                            Wait Until Keyword Succeeds    ${NEOFS_EPOCH_TIMEOUT}    1m
                            ...     Validate storage policy for object    ${WIF_STORAGE}    ${EXPECTED_COPIES}    ${CID}    ${OID}

    [Teardown]              Teardown    acl_deny_replication


*** Keywords ***

Drop object
    [Arguments]   ${NODE}    ${WIF_STORAGE}    ${CID}    ${OID}

    ${DROP_SIMPLE} =        Run Process    neofs-cli control drop-objects -r ${NODE} --wif ${WIF_STORAGE} -o ${CID}/${OID}    shell=True
                            Log Many    stdout: ${DROP_SIMPLE.stdout}    stderr: ${DROP_SIMPLE.stderr}
                            Should Be Equal As Integers    ${DROP_SIMPLE.rc}    0
                            

