*** Settings ***
Variables                   ../../../variables/common.py
Library                     Collections
Library                     ../${RESOURCES}/neofs.py
Library                     ../${RESOURCES}/payment_neogo.py

Resource                    common_steps_acl_extended.robot
     
*** Test cases ***
Extended ACL Operations
    [Documentation]         Testcase to validate NeoFS operations with extended ACL.
    [Tags]                  ACL  eACL  NeoFS  NeoCLI
    [Timeout]               20 min

                            Generate Keys
                            Prepare eACL Role rules

                            Log    Check extended ACL with simple object
                            Generate files    1024
                            Check eACL Deny and Allow All User
                            
                            Log    Check extended ACL with complex object
                            Generate files    70e+6
                            Check eACL Deny and Allow All User
                             
    [Teardown]              Cleanup  

    
*** Keywords ***

Check eACL Deny and Allow All User
                            Check eACL Deny and Allow All    ${USER_KEY}    ${EACL_DENY_ALL_USER}    ${EACL_ALLOW_ALL_USER}                  


Cleanup
                            Cleanup Files       
                            Get Docker Logs    acl_extended
