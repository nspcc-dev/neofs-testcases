*** Settings ***

Resource        ../common_steps_acl_extended.robot
Resource        ../../${RESOURCES}/setup_teardown.robot

*** Test cases ***
Container ID Object Filter for Extended ACL
    [Documentation]         Testcase to validate if $Object:containerID eACL filter is correctly handled.
    [Tags]                  ACL  eACL  NeoFS  NeoCLI
    [Timeout]               20 min

    [Setup]                 Setup
    
    Log    Check eACL containerID Filter with MatchType String Equal
    Check eACL Filters with MatchType String Equal    $Object:containerID

    [Teardown]          Teardown    container_id_filter
