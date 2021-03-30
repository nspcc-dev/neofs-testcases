*** Settings ***
Variables                   ../../../variables/common.py
Library                     Collections
Library                     ../${RESOURCES}/neofs.py
Library                     ../${RESOURCES}/payment_neogo.py

Resource                    common_steps_acl_extended.robot
     
*** Test cases ***
Extended ACL Operations
    [Documentation]         Testcase to validate NeoFS operations with extended ACL with Other group key.
    [Tags]                  ACL  eACL  NeoFS  NeoCLI
    [Timeout]               20 min

                            Generate Keys
                            Prepare eACL Role rules

                            Log    Check extended ACL with simple object
                            Generate files    1024
                            Check eACL Deny and Allow All Other

                            Log    Check extended ACL with complex object
                            Generate files    70e+6
                            Check eACL Deny and Allow All Other
                                     
    [Teardown]              Cleanup  

    
*** Keywords ***

Check eACL Deny and Allow All Other
                            Check eACL Deny and Allow All    ${OTHER_KEY}    ${EACL_DENY_ALL_OTHER}    ${EACL_ALLOW_ALL_OTHER} 


Cleanup
                            Cleanup Files      
                            Get Docker Logs    acl_extended
