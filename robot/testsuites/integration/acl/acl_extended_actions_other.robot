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
    [Documentation]         Testcase to validate NeoFS operations with extended ACL with Other group key.
    [Tags]                  ACL  eACL  NeoFS  NeoCLI
    [Timeout]               20 min

    [Setup]                 Setup

    ${WALLET}   ${ADDR}     ${USER_KEY} =   Prepare Wallet And Deposit  
    ${WALLET_OTH}   ${ADDR_OTH}     ${OTHER_KEY} =   Prepare Wallet And Deposit

                            Log    Check extended ACL with simple object
                            Generate files    ${SIMPLE_OBJ_SIZE}
                            Check eACL Deny and Allow All Other    ${USER_KEY}    ${OTHER_KEY}

                            Log    Check extended ACL with complex object
                            Generate files    ${COMPLEX_OBJ_SIZE}
                            Check eACL Deny and Allow All Other    ${USER_KEY}    ${OTHER_KEY}

    [Teardown]              Teardown    acl_extended_actions_other


*** Keywords ***

Check eACL Deny and Allow All Other
    [Arguments]    ${USER_KEY}    ${OTHER_KEY}
                            Check eACL Deny and Allow All    ${OTHER_KEY}    ${EACL_DENY_ALL_OTHERS}    ${EACL_ALLOW_ALL_OTHERS}    ${USER_KEY} 
