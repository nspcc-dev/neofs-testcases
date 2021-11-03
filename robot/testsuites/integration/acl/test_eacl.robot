*** Settings ***
Variables       ../../../variables/common.py
Library         ../${RESOURCES}/neofs.py
Library         ../${RESOURCES}/payment_neogo.py

Resource        common_steps_acl_basic.robot
Resource        ../${RESOURCES}/payment_operations.robot
Resource        ../${RESOURCES}/setup_teardown.robot
Resource    robot/testsuites/integration/acl/common_steps_acl_extended.robot


*** Test cases ***
Basic ACL Operations for Private Container
    [Documentation]         Testcase to validate NeoFS operations with ACL for Private Container.
    [Tags]                  ACL  NeoFS  NeoCLI
    [Timeout]               20 min

    [Setup]                 Setup

    ${WALLET}   ${ADDR}     ${USER_KEY} =   Prepare Wallet And Deposit
    ${WALLET_OTH}   ${ADDR_OTH}     ${OTHER_KEY} =   Prepare Wallet And Deposit

    ${CID} =    Create Container Public    ${USER_KEY}
    ${S_OID_USER} =         Put object    ${USER_KEY}    ./test.txt    ${CID}    ${EMPTY}    ${EMPTY}
    