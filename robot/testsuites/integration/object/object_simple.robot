*** Settings ***
Variables   common.py

Library     container.py
Library     storage_policy.py
Library     utility_keywords.py

Resource    payment_operations.robot
Resource    setup_teardown.robot
Resource    verbs.robot

*** Test cases ***
NeoFS Simple Object Operations
    [Documentation]     Testcase to validate NeoFS operations with simple object.
    [Tags]              Object
    [Timeout]           10 min


    ${WALLET}    ${_}    ${_} =   Prepare Wallet And Deposit
    ${CID} =            Create container    ${WALLET}

    ${OID} =
    ...     Run All Verbs Except Delete And Expect Success
    ...     ${WALLET}   ${CID}  Simple

    ${COPIES} =         Get Simple Object Copies    ${WALLET}   ${CID}  ${OID}
                        Should Be Equal As Numbers      2       ${COPIES}

                        Delete Object And Validate Tombstone
                        ...     ${WALLET}   ${CID}  ${OID}


