*** Settings ***

Resource        ../common_steps_acl_extended.robot
Resource        ../../${RESOURCES}/setup_teardown.robot

*** Test cases ***
Homomorphic Hash Object Filter for Extended ACL
    [Documentation]         Testcase to validate if $Object:homomorphicHash eACL filter is correctly handled.
    [Tags]                  ACL  eACL  NeoFS  NeoCLI
    [Timeout]               20 min

    [Setup]                 Setup

    Log    Check eACL homomorphicHash Filter with MatchType String Equal
    Check eACL Filters with MatchType String Equal    $Object:homomorphicHash
    
    [Teardown]          Teardown    homomorphic_hash_filter
