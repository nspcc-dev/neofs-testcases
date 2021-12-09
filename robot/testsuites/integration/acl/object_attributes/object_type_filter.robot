*** Settings ***
Resource        common_steps_acl_extended.robot
Resource        setup_teardown.robot

*** Test cases ***
Object Type Object Filter for Extended ACL
    [Documentation]    Testcase to validate if $Object:objectType eACL filter is correctly handled.
    [Tags]             ACL  eACL  NeoFS  NeoCLI
    [Timeout]          20 min

    [Setup]            Setup

    Log    Check eACL objectType Filter with MatchType String Equal
    Check eACL Filters with MatchType String Equal    $Object:objectType

    [Teardown]         Teardown    object_type_filter
