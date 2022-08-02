*** Settings ***
Resource        common_steps_acl_extended.robot
Resource        setup_teardown.robot

*** Test cases ***
Container ID Object Filter for Extended ACL
    [Documentation]    Testcase to validate if $Object:containerID eACL filter is correctly handled.
    [Tags]             ACL  eACL
    [Timeout]          20 min


    Log    Check eACL containerID Filter with MatchType String Equal
    Check eACL Filters with MatchType String Equal    $Object:containerID

