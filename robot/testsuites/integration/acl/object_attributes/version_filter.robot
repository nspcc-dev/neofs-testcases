*** Settings ***
Resource    common_steps_acl_extended.robot
Resource    setup_teardown.robot

*** Test cases ***
Version Object Filter for Extended ACL
    [Documentation]    Testcase to validate if $Object:version eACL filter is correctly handled.
    [Tags]             ACL  eACL  NeoFS  NeoCLI
    [Timeout]          20 min


    Log    Check eACL version Filter with MatchType String Equal 
    Check eACL Filters with MatchType String Equal    $Object:version  

