*** Settings ***

Resource        ../common_steps_acl_extended.robot
Resource        ../../${RESOURCES}/setup_teardown.robot

*** Test cases ***
Owner ID Object Filter for Extended ACL
    [Documentation]         Testcase to validate if $Object:ownerID eACL filter is correctly handled.
    [Tags]                  ACL  eACL  NeoFS  NeoCLI
    [Timeout]               20 min

    [Setup]                 Setup
    
    Log    Check eACL ownerID Filter with MatchType String Equal
    Check eACL Filters with MatchType String Equal    $Object:ownerID
    Log    Check eACL ownerID Filter with MatchType String Not Equal    
    Check eACL Filters with MatchType String Not Equal    $Object:ownerID

    [Teardown]          Teardown    owner_id_filter
