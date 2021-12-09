*** Settings ***
Variables    common.py

Library     Collections
Library     neofs.py
Library     payment_neogo.py
Library     acl.py

Resource     common_steps_acl_extended.robot
Resource     payment_operations.robot
Resource     setup_teardown.robot
Resource     eacl_tables.robot

*** Test cases ***
Extended ACL Operations
    [Documentation]         Testcase to validate NeoFS operations with extended ACL.
    [Tags]                  ACL  eACL  NeoFS  NeoCLI
    [Timeout]               20 min

    [Setup]                 Setup

    ${WALLET}   ${ADDR}     ${USER_KEY} =   Prepare Wallet And Deposit

                            Log    Check extended ACL with simple object
                            Generate files    ${SIMPLE_OBJ_SIZE}
                            Check eACL Deny and Allow All User    ${USER_KEY}

                            Log    Check extended ACL with complex object
                            Generate files    ${COMPLEX_OBJ_SIZE}
                            Check eACL Deny and Allow All User    ${USER_KEY}

    [Teardown]              Teardown    acl_extended_action_user


*** Keywords ***

Check eACL Deny and Allow All User
    [Arguments]             ${USER_KEY}
                            Check eACL Deny and Allow All    ${USER_KEY}    ${EACL_DENY_ALL_USER}    ${EACL_ALLOW_ALL_USER}    ${USER_KEY}
